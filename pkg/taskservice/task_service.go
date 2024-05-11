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
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/robfig/cron/v3"
)

type taskService struct {
	store      TaskStorage
	cronParser cron.Parser
	rt         runtime.Runtime

	crons crons
}

// NewTaskService create a task service based on a task storage.
func NewTaskService(
	rt runtime.Runtime,
	store TaskStorage) TaskService {
	return &taskService{
		rt:    rt,
		store: store,
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

func (s *taskService) CreateAsyncTask(ctx context.Context, value task.TaskMetadata) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), ErrNotReady)
		default:
			if _, err := s.store.AddAsyncTask(ctx, newTaskFromMetadata(value)); err != nil {
				if errors.Is(err, ErrNotReady) {
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
	values := make([]task.AsyncTask, 0, len(tasks))
	for _, v := range tasks {
		values = append(values, newTaskFromMetadata(v))
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), ErrNotReady)
		default:
			if _, err := s.store.AddAsyncTask(ctx, values...); err != nil {
				if errors.Is(err, ErrNotReady) {
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

func (s *taskService) CreateDaemonTask(ctx context.Context, metadata task.TaskMetadata, details *task.Details) error {
	now := time.Now()

	dt := task.DaemonTask{
		Metadata:   metadata,
		TaskType:   details.Type(),
		TaskStatus: task.TaskStatus_Created,
		Details:    details,
		CreateAt:   now,
		UpdateAt:   now,
	}
	_, err := s.store.AddDaemonTask(ctx, dt)
	return err
}

func (s *taskService) Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error {
	exists, err := s.store.QueryAsyncTask(ctx, WithTaskIDCond(EQ, value.ID))
	if err != nil {
		return err
	}
	if len(exists) != 1 {
		s.rt.Logger().Debug(fmt.Sprintf("queried tasks: %v", exists))
		s.rt.Logger().Fatal(fmt.Sprintf("query task by primary key, return %d records", len(exists)))
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
		return moerr.NewInvalidTask(ctx, taskRunner, value.ID)
	}

	n, err := s.store.UpdateAsyncTask(ctx,
		[]task.AsyncTask{old},
		WithTaskIDCond(EQ, old.ID),
		WithTaskEpochCond(EQ, old.Epoch-1))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, taskRunner, value.ID)
	}
	return nil
}

func (s *taskService) Complete(
	ctx context.Context,
	taskRunner string,
	value task.AsyncTask,
	result task.ExecuteResult) error {
	value.CompletedAt = time.Now().UnixMilli()
	value.Status = task.TaskStatus_Completed
	value.ExecuteResult = &result
	n, err := s.store.UpdateAsyncTask(ctx, []task.AsyncTask{value},
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, taskRunner),
		WithTaskEpochCond(EQ, value.Epoch))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, value.TaskRunner, value.ID)
	}
	return nil
}

func (s *taskService) Heartbeat(ctx context.Context, value task.AsyncTask) error {
	value.LastHeartbeat = time.Now().UnixMilli()
	n, err := s.store.UpdateAsyncTask(ctx, []task.AsyncTask{value},
		WithTaskIDCond(EQ, value.ID),
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskEpochCond(LE, value.Epoch),
		WithTaskRunnerCond(EQ, value.TaskRunner))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, value.TaskRunner, value.ID)
	}
	return nil
}

func (s *taskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	return s.store.QueryAsyncTask(ctx, conds...)
}

func (s *taskService) QueryCronTask(ctx context.Context, c ...Condition) ([]task.CronTask, error) {
	return s.store.QueryCronTask(ctx, c...)
}

func (s *taskService) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
	return s.store.UpdateDaemonTask(ctx, tasks, conds...)
}

func (s *taskService) QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error) {
	return s.store.QueryDaemonTask(ctx, conds...)
}

func (s *taskService) Close() error {
	s.StopScheduleCronTask()
	return s.store.Close()
}

func (s *taskService) GetStorage() TaskStorage {
	return s.store
}

func (s *taskService) HeartbeatDaemonTask(ctx context.Context, t task.DaemonTask) error {
	t.LastHeartbeat = time.Now().UTC()
	n, err := s.store.HeartbeatDaemonTask(ctx, []task.DaemonTask{t})
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, t.TaskRunner, t.ID)
	}
	return nil
}
