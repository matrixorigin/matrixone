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

type taskService struct {
	store      TaskStorage
	cronParser cron.Parser
	logger     *zap.Logger

	crons struct {
		sync.Mutex

		started  bool
		stopper  *stopper.Stopper
		tasks    map[uint64]task.CronTask
		entryIDs map[uint64]cron.EntryID
		cron     *cron.Cron
		retryC   chan uint64
	}
}

// NewTaskService create a task service based on a task storage.
func NewTaskService(store TaskStorage, logger *zap.Logger) TaskService {
	return &taskService{
		store:  store,
		logger: logutil.Adjust(logger),
		cronParser: cron.NewParser(
			cron.Second |
				cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor),
	}
}

func (s *taskService) Create(ctx context.Context, value task.TaskMetadata) error {
	_, err := s.store.Add(ctx, newTaskFromMetadata(value))
	return err
}

func (s *taskService) CreateBatch(ctx context.Context, tasks []task.TaskMetadata) error {
	now := time.Now().UnixMilli()
	values := make([]task.Task, 0, len(tasks))
	for _, v := range tasks {
		values = append(values, task.Task{
			Metadata: v,
			Status:   task.TaskStatus_Created,
			CreateAt: now,
		})
	}
	_, err := s.store.Add(ctx, values...)
	return err
}

func (s *taskService) CreateCronTask(ctx context.Context, value task.TaskMetadata, cronExpr string) error {
	sche, err := s.cronParser.Parse(cronExpr)
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	next := sche.Next(time.UnixMilli(now))

	_, err = s.store.AddCronTask(ctx, task.CronTask{
		Metadata:     value,
		CronExpr:     cronExpr,
		NextTime:     next.UnixMilli(),
		TriggerTimes: 0,
		CreateAt:     now,
		UpdateAt:     now,
	})
	return err
}

func (s *taskService) Allocate(ctx context.Context, value task.Task, taskRunner string) error {
	exists, err := s.store.Query(ctx, WithTaskIDCond(EQ, value.ID))
	if err != nil {
		return nil
	}
	if len(exists) != 1 {
		return ErrInvalidTask
	}

	old := exists[0]
	switch old.Status {
	case task.TaskStatus_Running:
		old.Epoch++
		old.TaskRunner = taskRunner
		old.LastHeartbeat = time.Now().UnixMilli()
	case task.TaskStatus_Created:
		old.Status = task.TaskStatus_Running
		old.Epoch = 1
		old.TaskRunner = taskRunner
		old.LastHeartbeat = time.Now().UnixMilli()
	default:
		return ErrInvalidTask
	}

	n, err := s.store.Update(ctx,
		[]task.Task{old},
		WithTaskEpochCond(EQ, old.Epoch-1))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrInvalidTask
	}
	return nil
}

func (s *taskService) Complete(
	ctx context.Context,
	taskRunner string,
	value task.Task,
	result task.ExecuteResult) error {
	value.CompletedAt = time.Now().UnixMilli()
	value.Status = task.TaskStatus_Completed
	value.ExecuteResult = &result
	n, err := s.store.Update(ctx, []task.Task{value},
		WithTaskStatusCond(EQ, task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, taskRunner),
		WithTaskEpochCond(EQ, value.Epoch))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrInvalidTask
	}
	return nil
}

func (s *taskService) Heartbeat(ctx context.Context, value task.Task) error {
	value.LastHeartbeat = time.Now().UnixMilli()
	n, err := s.store.Update(ctx, []task.Task{value},
		WithTaskStatusCond(EQ, task.TaskStatus_Running),
		WithTaskEpochCond(LE, value.Epoch),
		WithTaskRunnerCond(EQ, value.TaskRunner))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrInvalidTask
	}
	return nil
}

func (s *taskService) QueryTask(ctx context.Context, conds ...Condition) ([]task.Task, error) {
	return s.store.Query(ctx, conds...)
}

func (s *taskService) QueryCronTask(ctx context.Context) ([]task.CronTask, error) {
	return s.store.QueryCronTask(ctx)
}

func (s *taskService) Close() error {
	return s.store.Close()
}

func (s *taskService) StartTriggerCronTask() {
	s.crons.Lock()
	defer s.crons.Unlock()

	if s.crons.started {
		return
	}

	s.crons.started = true
	s.crons.stopper = stopper.NewStopper("crontasks")
	s.crons.tasks = make(map[uint64]task.CronTask)
	s.crons.entryIDs = make(map[uint64]cron.EntryID)
	s.crons.retryC = make(chan uint64, 256)
	if err := s.crons.stopper.RunTask(s.runTriggerCron); err != nil {
		panic(err)
	}
}

func (s *taskService) runTriggerCron(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// c, cancel := context.WithTimeout(ctx, time.Second*10)
			// tasks, err := s.QueryCronTask(c)
			// cancel()
			// if err != nil {
			// 	s.logger.Error("query cron tasks failed",
			// 		zap.Error(err))
			// 	break
			// }

			// now := time.Now()
			// newTasks := make(map[uint64]task.CronTask)
			// for _, task := range tasks {
			// 	newTasks[task.ID] = task
			// }

			// s.crons.Lock()
			// // add new and update crons
			// for id, v := range newTasks {
			// 	old, ok := s.crons.tasks[id]
			// 	if !ok {

			// 	}
			// }

			// // remove deleted crons

			// // update crons
		}
		timer.Reset(time.Second)
	}
}

func (s *taskService) triggerNewCronTaskLocked(t task.CronTask) {
	s.crons.cron.AddFunc(t.CronExpr, func() {})
}

func (s *taskService) getCronJob(t task.CronTask) func() {
	return func() {
		if !s.hasCronTask(t.ID) {
			return
		}

		sche, err := s.cronParser.Parse(t.CronExpr)
		if err != nil {
			panic(err)
		}
		now := time.Now()
		next := sche.Next(now)
		t.NextTime = next.UnixMilli()
		t.UpdateAt = now.UnixMilli()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		t.TriggerTimes++
		value := newTaskFromMetadata(t.Metadata)
		value.ParentTaskID = value.Metadata.ID
		value.Metadata.ID = fmt.Sprintf("%s:%d", value.ParentTaskID, t.TriggerTimes)

		_, err = s.store.UpdateCronTask(ctx, t, value)
		if err == nil {
			s.updateCronTask(t)
			return
		}

		s.logger.Error("trigger cron task failed",
			zap.String("cron-task", t.Metadata.ID),
			zap.Error(err))
		s.crons.retryC <- t.ID
	}
}

func (s *taskService) hasCronTask(id uint64) bool {
	s.crons.Lock()
	defer s.crons.Unlock()
	if !s.crons.started {
		return false
	}

	_, ok := s.crons.entryIDs[id]
	return ok
}

func (s *taskService) updateCronTask(t task.CronTask) {
	s.crons.Lock()
	defer s.crons.Unlock()

	if !s.crons.started {
		return
	}

	if _, ok := s.crons.tasks[t.ID]; ok {
		s.crons.tasks[t.ID] = t
	}
}

func newTaskFromMetadata(metadata task.TaskMetadata) task.Task {
	return task.Task{
		Metadata: metadata,
		Status:   task.TaskStatus_Created,
		CreateAt: time.Now().UnixMilli(),
	}
}
