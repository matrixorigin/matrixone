// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskservice

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

var (
	fetchInterval = time.Second * 3
)

type crons struct {
	state cronServiceState

	stopper *stopper.Stopper
	cron    *cron.Cron
	jobs    map[uint64]*cronJob
	entries map[uint64]cron.EntryID
}

func (s *taskService) StartScheduleCronTask() {
	if !s.crons.state.canStart() {
		s.rt.Logger().Info("cron task scheduler started or is stopping")
		return
	}

	s.crons.stopper = stopper.NewStopper("cronTasks")
	s.crons.jobs = make(map[uint64]*cronJob)
	s.crons.entries = make(map[uint64]cron.EntryID)
	s.crons.cron = cron.New(cron.WithParser(s.cronParser), cron.WithLogger(logutil.GetCronLogger(false)))
	s.crons.cron.Start()
	if err := s.crons.stopper.RunTask(s.fetchCronTasks); err != nil {
		panic(err)
	}
}

func (s *taskService) StopScheduleCronTask() {
	if !s.crons.state.canStop() {
		s.rt.Logger().Info("cron task scheduler stopped or is stopping")
		return
	}

	s.crons.stopper.Stop()
	<-s.crons.cron.Stop().Done()
	s.crons.cron = nil
	s.crons.jobs = nil
	s.crons.entries = nil
	s.crons.stopper = nil

	s.crons.state.endStop()
}

func (s *taskService) fetchCronTasks(ctx context.Context) {
	s.rt.Logger().Info("start to fetch cron tasks")
	defer func() {
		s.rt.Logger().Info("stop to fetch cron tasks")
	}()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c, cancel := context.WithTimeout(ctx, time.Second*10)
		tasks, err := s.QueryCronTask(c)
		cancel()
		if err != nil {
			s.rt.Logger().Error("query cron tasks failed",
				zap.Error(err))
			continue
		}

		s.rt.Logger().Debug("new cron tasks fetched",
			zap.Int("current-count", len(s.crons.entries)),
			zap.Int("fetch-count", len(tasks)))

		currentTasks := make(map[uint64]struct{}, len(tasks))
		// add new cron tasks to cron scheduler
		for _, v := range tasks {
			currentTasks[v.ID] = struct{}{}
			if job, ok := s.crons.jobs[v.ID]; !ok {
				s.addCronTask(v)
			} else {
				if job.state.canUpdate() {
					if job.task.TriggerTimes != v.TriggerTimes {
						s.rt.Logger().Info("cron task updated",
							zap.String("cause", "trigger-times changed"),
							zap.Uint64("old-trigger-times", job.task.TriggerTimes),
							zap.Uint64("new-trigger-times", v.TriggerTimes),
							zap.String("task", v.DebugString()))
						s.replaceCronTask(v)
					}
					job.state.endUpdate()
				}
			}
		}

		// remove deleted cron tasks
		for id := range s.crons.entries {
			if _, ok := currentTasks[id]; !ok {
				s.removeCronTask(id)
			}
		}
	}
}

func (s *taskService) addCronTask(task task.CronTask) {
	job := newCronJob(task, s)
	if time.Now().After(time.UnixMilli(task.NextTime)) {
		s.rt.Logger().Info("cron task triggered",
			zap.String("cause", "now > next"),
			zap.String("task", task.DebugString()))
		if err := s.crons.stopper.RunTask(func(ctx context.Context) { job.Run() }); err != nil {
			panic(err)
		}
	}

	entryId, err := s.crons.cron.AddJob(task.CronExpr, job)
	if err != nil {
		panic(err)
	}
	s.crons.jobs[task.ID] = job
	s.crons.entries[task.ID] = entryId
}

func (s *taskService) removeCronTask(id uint64) {
	s.crons.cron.Remove(s.crons.entries[id])
	delete(s.crons.jobs, id)
	delete(s.crons.entries, id)
}

func (s *taskService) replaceCronTask(v task.CronTask) {
	s.crons.cron.Remove(s.crons.entries[v.ID])

	job := newCronJob(v, s)
	entryId, err := s.crons.cron.AddJob(v.CronExpr, job)
	if err != nil {
		panic(err)
	}
	s.crons.jobs[v.ID] = job
	s.crons.entries[v.ID] = entryId
}

func newTaskFromMetadata(metadata task.TaskMetadata) task.AsyncTask {
	return task.AsyncTask{
		Metadata: metadata,
		Status:   task.TaskStatus_Created,
		CreateAt: time.Now().UnixMilli(),
	}
}

type cronJob struct {
	state    cronJobState
	s        *taskService
	schedule cron.Schedule
	task     task.CronTask
}

func newCronJob(task task.CronTask, s *taskService) *cronJob {
	schedule, err := s.cronParser.Parse(task.CronExpr)
	if err != nil {
		panic(err)
	}
	return &cronJob{
		schedule: schedule,
		task:     task,
		s:        s,
	}
}

func (j *cronJob) Run() {
	if !j.state.canRun() {
		return
	}
	defer func() {
		j.state.endRun()
	}()

	if j.task.Metadata.Options.Concurrency != 0 && !j.checkConcurrency() {
		return
	}

	j.s.rt.Logger().Info("cron task triggered",
		zap.String("cause", "normal"),
		zap.String("task", j.task.DebugString()))
	j.doRun()
}

func (j *cronJob) doRun() {
	now := time.Now()
	cronTasks, err := j.s.QueryCronTask(context.Background(), WithCronTaskId(EQ, j.task.ID))
	if err != nil {
		j.s.rt.Logger().Error("failed to query cron task", zap.Error(err))
		return
	}
	if len(cronTasks) != 1 {
		j.s.rt.Logger().Panic(fmt.Sprintf("query cron_task_id = %d, return %d records", j.task.ID, len(cronTasks)))
		return
	}
	cronTask := cronTasks[0]
	cronTask.UpdateAt = now.UnixMilli()
	cronTask.NextTime = j.schedule.Next(now).UnixMilli()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	cronTask.TriggerTimes++
	asyncTask := newTaskFromMetadata(cronTask.Metadata)
	asyncTask.ParentTaskID = asyncTask.Metadata.ID
	asyncTask.Metadata.ID = fmt.Sprintf("%s:%d", asyncTask.ParentTaskID, cronTask.TriggerTimes)

	_, err = j.s.store.UpdateCronTask(ctx, cronTask, asyncTask)
	if err != nil {
		j.s.rt.Logger().Error("trigger cron task failed",
			zap.String("cron-task", j.task.DebugString()),
			zap.Error(err))
		return
	}
	j.task = cronTask
}

func (j *cronJob) checkConcurrency() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	queryTask, err := j.s.QueryAsyncTask(ctx,
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskExecutorCond(EQ, j.task.Metadata.Executor))
	if err != nil ||
		uint32(len(queryTask)) >= j.task.Metadata.Options.Concurrency {
		j.s.rt.Logger().Debug("cron task not triggered",
			zap.String("cause", "reach max concurrency"),
			zap.String("task", j.task.DebugString()),
			zap.Error(err))
		return false
	}
	return true
}
