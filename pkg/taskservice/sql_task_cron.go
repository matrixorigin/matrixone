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

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

type sqlTaskCrons struct {
	state   cronServiceState
	stopper *stopper.Stopper
	jobs    map[uint64]*sqlTaskCronJob
}

type sqlTaskCronJob struct {
	state cronJobState
	s     *taskService
	task  SQLTask
	cron  *cron.Cron
	entry cron.EntryID
}

func (s *taskService) StartScheduleSQLTask() {
	if !s.sqlCrons.state.canStart() {
		s.rt.Logger().Debug("sql task scheduler started or is stopping")
		return
	}

	s.sqlCrons.stopper = stopper.NewStopper("sql-task-crons")
	s.sqlCrons.jobs = make(map[uint64]*sqlTaskCronJob)
	if err := s.sqlCrons.stopper.RunTask(s.refreshSQLTasks); err != nil {
		panic(err)
	}
}

func (s *taskService) StopScheduleSQLTask() {
	if !s.sqlCrons.state.canStop() {
		s.rt.Logger().Debug("sql task scheduler stopped or is stopping")
		return
	}

	if s.sqlCrons.stopper != nil {
		s.sqlCrons.stopper.Stop()
	}
	for id := range s.sqlCrons.jobs {
		s.removeSQLTask(id)
	}
	s.sqlCrons.jobs = nil
	s.sqlCrons.stopper = nil
	s.sqlCrons.state.endStop()
}

func (s *taskService) refreshSQLTasks(ctx context.Context) {
	s.rt.Logger().Info("start to fetch sql tasks")
	defer s.rt.Logger().Info("stop to fetch sql tasks")

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.loadSQLTasks(ctx)
	}
}

func (s *taskService) loadSQLTasks(ctx context.Context) {
	queryCtx, cancel := context.WithTimeoutCause(ctx, 10*time.Second, moerr.CauseFetchCronTasks)
	defer cancel()

	tasks, err := s.store.QuerySQLTask(queryCtx, WithSQLTaskEnabled(true))
	if err != nil {
		s.rt.Logger().Error("query sql tasks failed", zap.Error(moerr.AttachCause(queryCtx, err)))
		return
	}

	current := make(map[uint64]SQLTask, len(tasks))
	for _, t := range tasks {
		if t.CronExpr == "" {
			continue
		}
		current[t.TaskID] = t
		if job, ok := s.sqlCrons.jobs[t.TaskID]; !ok {
			s.addSQLTask(t)
		} else if sqlTaskNeedsRefresh(job.task, t) {
			s.replaceSQLTask(t)
		}
	}

	for id := range s.sqlCrons.jobs {
		if _, ok := current[id]; !ok {
			s.removeSQLTask(id)
		}
	}
}

func (s *taskService) addSQLTask(sqlTask SQLTask) {
	job, err := newSQLTaskCronJob(sqlTask, s)
	if err != nil {
		s.rt.Logger().Error("failed to add sql task cron", zap.Uint64("task-id", sqlTask.TaskID), zap.Error(err))
		return
	}
	if sqlTask.NextFireTime > 0 && time.Now().After(time.UnixMilli(sqlTask.NextFireTime)) {
		if err := s.sqlCrons.stopper.RunTask(func(context.Context) { job.Run() }); err != nil {
			panic(err)
		}
	}
	job.cron.Start()
	s.sqlCrons.jobs[sqlTask.TaskID] = job
}

func (s *taskService) replaceSQLTask(sqlTask SQLTask) {
	s.removeSQLTask(sqlTask.TaskID)
	s.addSQLTask(sqlTask)
}

func (s *taskService) removeSQLTask(taskID uint64) {
	job, ok := s.sqlCrons.jobs[taskID]
	if !ok {
		return
	}
	job.cron.Remove(job.entry)
	<-job.cron.Stop().Done()
	delete(s.sqlCrons.jobs, taskID)
}

func newSQLTaskCronJob(sqlTask SQLTask, s *taskService) (*sqlTaskCronJob, error) {
	spec := BuildSQLTaskCronSpec(sqlTask.CronExpr, sqlTask.Timezone)
	cronRunner := cron.New(cron.WithParser(s.cronParser), cron.WithLogger(logutil.GetCronLogger(false)))
	job := &sqlTaskCronJob{
		s:    s,
		task: sqlTask,
		cron: cronRunner,
	}
	entry, err := cronRunner.AddFunc(spec, job.Run)
	if err != nil {
		return nil, err
	}
	job.entry = entry
	return job, nil
}

func (j *sqlTaskCronJob) Run() {
	if !j.state.canRun() {
		return
	}
	defer j.state.endRun()

	now := time.Now()
	queryCtx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseDoRun)
	defer cancel()

	tasks, err := j.s.store.QuerySQLTask(queryCtx, WithTaskIDCond(EQ, j.task.TaskID))
	if err != nil || len(tasks) != 1 {
		if err != nil {
			j.s.rt.Logger().Error("query sql task failed", zap.Uint64("task-id", j.task.TaskID), zap.Error(moerr.AttachCause(queryCtx, err)))
		}
		return
	}
	current := tasks[0]
	if !current.Enabled || current.CronExpr == "" {
		return
	}

	nextFireTime, err := NextSQLTaskFireTime(j.s.cronParser, current.CronExpr, current.Timezone, now)
	if err != nil {
		j.s.rt.Logger().Error("parse sql task cron failed", zap.Uint64("task-id", current.TaskID), zap.Error(err))
		return
	}

	current.TriggerCount++
	current.NextFireTime = nextFireTime
	current.UpdatedAt = now

	spec := &task.SQLTaskContext{
		TaskId:         current.TaskID,
		TaskName:       current.TaskName,
		AccountId:      current.AccountID,
		DatabaseName:   current.DatabaseName,
		SQLBody:        current.SQLBody,
		GateCondition:  current.GateCondition,
		RetryLimit:     uint32(current.RetryLimit),
		TimeoutSeconds: uint32(current.TimeoutSeconds),
		Creator:        current.Creator,
		CreatorUserId:  current.CreatorUserID,
		CreatorRoleId:  current.CreatorRoleID,
		TriggerType:    SQLTaskTriggerScheduled,
		ScheduledAt:    now.UnixMilli(),
		TriggerCount:   current.TriggerCount,
	}
	asyncTask := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
	asyncTask.ParentTaskID = fmt.Sprintf("sql-task:%d", current.TaskID)
	asyncTask.CreateAt = now.UnixMilli()

	if _, err := j.s.store.TriggerSQLTask(queryCtx, current, asyncTask); err != nil {
		j.s.rt.Logger().Error("trigger sql task failed", zap.Uint64("task-id", current.TaskID), zap.Error(moerr.AttachCause(queryCtx, err)))
		return
	}
	j.task = current
}

func sqlTaskNeedsRefresh(oldTask, newTask SQLTask) bool {
	return oldTask.CronExpr != newTask.CronExpr ||
		oldTask.Timezone != newTask.Timezone ||
		oldTask.Enabled != newTask.Enabled ||
		oldTask.TriggerCount != newTask.TriggerCount ||
		oldTask.NextFireTime != newTask.NextFireTime
}
