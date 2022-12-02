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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

var (
	fetchInterval = time.Second * 3
	retryInterval = time.Second * 10
)

func (s *taskService) StartScheduleCronTask() {
	s.crons.Lock()
	defer s.crons.Unlock()

	if s.crons.started || s.crons.stopping {
		return
	}

	s.crons.started = true
	s.crons.stopper = stopper.NewStopper("crontasks")
	s.crons.jobs = make(map[uint64]*cronJob)
	s.crons.entryIDs = make(map[uint64]cron.EntryID)
	s.crons.retryC = make(chan task.CronTask, 256)
	s.crons.cron = cron.New(cron.WithParser(s.cronParser), cron.WithLogger(logutil.GetCronLogger(false)))
	s.crons.cron.Start()
	if err := s.crons.stopper.RunTask(s.fetchCronTasks); err != nil {
		panic(err)
	}
	if err := s.crons.stopper.RunTask(s.retryTriggerCronTask); err != nil {
		panic(err)
	}
}

func (s *taskService) StopScheduleCronTask() {
	s.crons.Lock()
	if !s.crons.started {
		s.crons.Unlock()
		return
	}
	stopper := s.crons.stopper
	s.crons.started = false
	s.crons.stopping = true
	s.crons.stopper = nil
	s.crons.Unlock()

	stopper.Stop()

	s.crons.Lock()
	defer s.crons.Unlock()
	if s.crons.started {
		panic("StartScheduleCronTask and StopScheduleCronTask can not concurrently invoked")
	}
	<-s.crons.cron.Stop().Done()
	close(s.crons.retryC)
	s.crons.jobs = nil
	s.crons.entryIDs = nil
	s.crons.stopping = false
}

func (s *taskService) fetchCronTasks(ctx context.Context) {
	timer := time.NewTimer(fetchInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			c, cancel := context.WithTimeout(ctx, time.Second*10)
			tasks, err := s.QueryCronTask(c)
			cancel()
			if err != nil {
				s.rt.Logger().Error("query cron tasks failed",
					zap.Error(err))
				break
			}

			currentTasks := make(map[uint64]task.CronTask)
			for _, task := range tasks {
				currentTasks[task.ID] = task
			}

			s.crons.Lock()
			s.rt.Logger().Debug("new cron tasks fetched",
				zap.Int("current-count", len(s.crons.jobs)),
				zap.Int("fetch-count", len(tasks)))

			// add new cron tasks to cron schduler
			for id, v := range currentTasks {
				if _, ok := s.crons.jobs[id]; !ok {
					s.scheduleCronTaskLocked(v)
				}
			}

			// remove deleted cron tasks
			var removedTasks []uint64
			for id, v := range s.crons.jobs {
				if _, ok := currentTasks[id]; !ok {
					removedTasks = append(removedTasks, id)
					v.close()
				}
			}
			for _, id := range removedTasks {
				s.removeCronTaskLocked(id)
			}
			s.crons.Unlock()
		}
		timer.Reset(fetchInterval)
	}
}

// retryTriggerCronTask when a cron reaches its trigger time, the CronTask creates a task and hands
// it to the scheduler for execution. When the creation of a task fails, it goes to the retry queue.
func (s *taskService) retryTriggerCronTask(ctx context.Context) {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()

	handle := func() {
		for {
			select {
			case task := <-s.crons.retryC:
				s.retryScheduleCronTask(task)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			handle()
		}
		timer.Reset(retryInterval)
	}
}

func (s *taskService) scheduleCronTaskLocked(task task.CronTask) {
	job := newCronJob(task, s)
	if time.Now().After(time.UnixMilli(task.NextTime)) {
		s.rt.Logger().Debug("cron task triggered",
			zap.String("cause", "now > next"),
			zap.String("task", task.DebugString()))
		if err := s.crons.stopper.RunTask(func(ctx context.Context) { job.run() }); err != nil {
			panic(err)
		}
	}

	id, err := s.crons.cron.AddFunc(task.CronExpr, job.run)
	if err != nil {
		panic(err)
	}
	s.crons.entryIDs[task.ID] = id
	s.crons.jobs[task.ID] = job
}

func (s *taskService) retryScheduleCronTask(task task.CronTask) {
	s.crons.Lock()
	defer s.crons.Unlock()

	if !s.crons.started {
		return
	}

	if job, ok := s.crons.jobs[task.ID]; ok {
		if err := s.crons.stopper.RunTask(func(ctx context.Context) { job.retryRun(task) }); err != nil {
			panic(err)
		}
	}
}

func (s *taskService) removeCronTaskLocked(id uint64) {
	s.crons.cron.Remove(s.crons.entryIDs[id])
	delete(s.crons.entryIDs, id)
	delete(s.crons.jobs, id)
}

func (s *taskService) addToRetrySchedule(task task.CronTask) {
	s.crons.Lock()
	defer s.crons.Unlock()

	if !s.crons.started {
		return
	}

	s.crons.retryC <- task
}

func newTaskFromMetadata(metadata task.TaskMetadata) task.Task {
	return task.Task{
		Metadata: metadata,
		Status:   task.TaskStatus_Created,
		CreateAt: time.Now().UnixMilli(),
	}
}

type cronJob struct {
	sync.Mutex
	closed   bool
	s        *taskService
	schedule cron.Schedule
	task     task.CronTask
	version  uint64
}

func newCronJob(task task.CronTask, s *taskService) *cronJob {
	schedule, err := s.cronParser.Parse(task.CronExpr)
	if err != nil {
		panic(err)
	}
	return &cronJob{
		schedule: schedule,
		task:     task,
		version:  0,
		s:        s,
	}
}

func (j *cronJob) close() {
	j.Lock()
	defer j.Unlock()

	j.closed = true
}

func (j *cronJob) retryRun(task task.CronTask) {
	j.Lock()
	defer j.Unlock()

	if j.task.NextTime != task.NextTime {
		return
	}

	j.doRun()
}

func (j *cronJob) run() {
	j.Lock()
	defer j.Unlock()

	j.s.rt.Logger().Debug("cron task triggered",
		zap.String("cause", "normal"),
		zap.String("task", j.task.DebugString()))
	j.doRun()
}

func (j *cronJob) doRun() {
	if j.closed {
		return
	}

	now := time.Now()
	next := j.schedule.Next(now)
	new := j.task
	new.NextTime = next.UnixMilli()
	new.UpdateAt = now.UnixMilli()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	new.TriggerTimes++
	value := newTaskFromMetadata(new.Metadata)
	value.ParentTaskID = value.Metadata.ID
	value.Metadata.ID = fmt.Sprintf("%s:%d", value.ParentTaskID, new.TriggerTimes)

	_, err := j.s.store.UpdateCronTask(ctx, new, value)
	if err != nil {
		j.s.rt.Logger().Error("trigger cron task failed",
			zap.String("cron-task", j.task.Metadata.ID),
			zap.Error(err))
		j.s.addToRetrySchedule(j.task)
		return
	}
	j.task = new
}
