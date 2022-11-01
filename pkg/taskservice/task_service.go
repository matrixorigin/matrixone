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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		stopping bool
		stopper  *stopper.Stopper
		cron     *cron.Cron
		retryC   chan task.CronTask
		jobs     map[uint64]*cronJob
		entryIDs map[uint64]cron.EntryID
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
	for {
		select {
		case <-ctx.Done():
			s.logger.Error("create task timeout")
			return errNotReady
		default:
			if _, err := s.store.Add(ctx, newTaskFromMetadata(value)); err != nil {
				if err == errNotReady {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				return err
			}
			return nil
		}
	}
}

func (s *taskService) CreateBatch(ctx context.Context, tasks []task.TaskMetadata) error {
	values := make([]task.Task, 0, len(tasks))
	for _, v := range tasks {
		values = append(values, newTaskFromMetadata(v))
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Error("create task timeout")
			return errNotReady
		default:
			if _, err := s.store.Add(ctx, values...); err != nil {
				if err == errNotReady {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				return err
			}
			return nil
		}
	}
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
		panic(fmt.Sprintf("query task by primary key, return %d records", len(exists)))
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
		return moerr.NewInvalidTask(taskRunner, value.ID)
	}

	n, err := s.store.Update(ctx,
		[]task.Task{old},
		WithTaskIDCond(EQ, old.ID),
		WithTaskEpochCond(EQ, old.Epoch-1))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(taskRunner, value.ID)
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
		return moerr.NewInvalidTask(value.TaskRunner, value.ID)
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
		return moerr.NewInvalidTask(value.TaskRunner, value.ID)
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
	s.StopScheduleCronTask()
	return s.store.Close()
}

func (s *taskService) GetStorage() TaskStorage {
	return s.store
}
