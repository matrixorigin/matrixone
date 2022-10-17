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

package task

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

type Scheduler struct {
	ctx               context.Context
	logger            *zap.Logger
	cfg               hakeeper.Config
	taskServiceGetter func() taskservice.TaskService

	mu struct {
		sync.RWMutex
		started bool
		ctx     context.Context
		cancel  context.CancelFunc
		wg      *sync.WaitGroup
	}
}

func NewTaskScheduler(taskServiceGetter func() taskservice.TaskService, cfg hakeeper.Config) *Scheduler {
	cfg.Fill()
	s := &Scheduler{
		ctx:               context.Background(),
		logger:            logutil.GetGlobalLogger().Named("hakeeper"),
		taskServiceGetter: taskServiceGetter,
		cfg:               cfg,
	}
	return s
}

func (s *Scheduler) Schedule(cnState logservice.CNState, currentTick uint64) {
	workingCN, expiredCN := parseCNStores(s.cfg, cnState, currentTick)

	runningTasks := s.queryRunningTasks()
	createdTasks := s.queryCreatedTasks()
	if runningTasks == nil && createdTasks == nil {
		return
	}
	orderedCN := getCNOrdered(runningTasks, workingCN)

	s.allocateTasks(createdTasks, orderedCN)

	expiredTasks := getExpiredTasks(runningTasks, expiredCN)
	s.allocateTasks(expiredTasks, orderedCN)
}

func (s *Scheduler) StartScheduleCronTask() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.started {
		return
	}
	s.mu.started = true
	s.mu.ctx, s.mu.cancel = context.WithCancel(context.Background())
	s.mu.wg = &sync.WaitGroup{}
	s.mu.wg.Add(1)
	go func(ctx context.Context) {
		defer s.mu.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ts := s.getTaskService()
				if ts != nil {
					ts.StartScheduleCronTask()
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
	}(s.mu.ctx)
}

func (s *Scheduler) StopScheduleCronTask() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.started {
		return
	}
	s.mu.started = false
	s.mu.cancel()
	s.mu.wg.Wait()
}

func (s *Scheduler) queryRunningTasks() []task.Task {
	ts := s.getTaskService()
	if ts == nil {
		return nil
	}

	tasks, err := ts.QueryTask(s.ctx, taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
	if err != nil {
		s.logger.Error("query running tasks error")
		return nil
	}
	return tasks
}

func (s *Scheduler) queryCreatedTasks() []task.Task {
	ts := s.getTaskService()
	if ts == nil {
		return nil
	}

	tasks, err := ts.QueryTask(s.ctx, taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	if err != nil {
		s.logger.Error("query created tasks error")
		return nil
	}
	return tasks
}

func (s *Scheduler) allocateTasks(tasks []task.Task, orderedCN *cnMap) {
	ts := s.getTaskService()
	if ts == nil {
		return
	}

	for _, t := range tasks {
		runner := orderedCN.min()
		if runner == "" {
			s.logger.Info("no CN available")
			return
		}
		if err := ts.Allocate(s.ctx, t, runner); err != nil {
			s.logger.Error("allocating task error",
				zap.Uint64("task-id", t.ID), zap.String("task-runner", runner))
			return
		}
		s.logger.Info("task allocated",
			zap.Uint64("task-id", t.ID),
			zap.String("task-runner", runner))
		orderedCN.inc(t.TaskRunner)
	}
}

func getExpiredTasks(tasks []task.Task, expiredCN []string) (expired []task.Task) {
	for _, t := range tasks {
		if contains(expiredCN, t.TaskRunner) {
			expired = append(expired, t)
		}
	}
	return
}

func getCNOrdered(tasks []task.Task, workingCN []string) *cnMap {
	orderedMap := newOrderedMap(workingCN)
	for _, t := range tasks {
		if contains(workingCN, t.TaskRunner) {
			orderedMap.inc(t.TaskRunner)
		}
	}

	return orderedMap
}

func (s *Scheduler) getTaskService() taskservice.TaskService {
	return s.taskServiceGetter()
}
