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
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/robfig/cron/v3"
)

type taskService struct {
	store      TaskStorage
	cronParser cron.Parser
}

// NewTaskService create a task service based on a task storage.
func NewTaskService(store TaskStorage) TaskService {
	return &taskService{
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

func (s *taskService) Create(ctx context.Context, value task.TaskMetadata) error {
	_, err := s.store.Add(ctx, task.Task{
		Metadata: value,
		Status:   task.TaskStatus_Created,
		CreateAt: time.Now().UnixMilli(),
	})
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
		Metadata: value,
		CronExpr: cronExpr,
		NextTime: next.UnixMilli(),
		CreateAt: now,
		UpdateAt: now,
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
