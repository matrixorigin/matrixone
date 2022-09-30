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

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

type Scheduler struct {
	ctx    context.Context
	logger *zap.Logger

	taskservice.TaskService
	cfg hakeeper.Config
}

func NewTaskScheduler(taskService taskservice.TaskService, cfg hakeeper.Config) *Scheduler {
	cfg.Fill()
	return &Scheduler{
		ctx:    context.Background(),
		logger: logutil.GetGlobalLogger().Named("hakeeper"),

		TaskService: taskService,
		cfg:         cfg,
	}
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

func (s *Scheduler) queryRunningTasks() []task.Task {
	tasks, err := s.QueryTask(s.ctx, taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
	if err != nil {
		s.logger.Error("query running tasks error")
		return nil
	}
	return tasks
}

func (s *Scheduler) queryCreatedTasks() []task.Task {
	tasks, err := s.QueryTask(s.ctx, taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	if err != nil {
		s.logger.Error("query created tasks error")
		return nil
	}
	return tasks
}

func (s *Scheduler) allocateTasks(tasks []task.Task, orderedCN *cnMap) {
	for _, t := range tasks {
		runner := orderedCN.min()
		if runner == "" {
			s.logger.Info("no CN available")
			return
		}
		err := s.Allocate(s.ctx, t, runner)
		if err != nil {
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
