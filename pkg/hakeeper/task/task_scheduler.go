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

type scheduler struct {
	ctx    context.Context
	logger *zap.Logger

	taskservice.TaskService
	cfg hakeeper.Config
}

var _ hakeeper.TaskScheduler = (*scheduler)(nil)

func NewScheduler(taskService taskservice.TaskService, cfg hakeeper.Config) hakeeper.TaskScheduler {
	cfg.Fill()
	return &scheduler{
		ctx:    context.Background(),
		logger: logutil.GetGlobalLogger().Named("hakeeper"),

		TaskService: taskService,
		cfg:         cfg,
	}
}

func (s *scheduler) Schedule(cnState logservice.CNState, currentTick uint64) {
	workingCN, expiredCN := parseCNStores(s.cfg, cnState, currentTick)

	runningTasks := s.queryTasks(task.TaskStatus_Running)
	createdTasks := s.queryTasks(task.TaskStatus_Created)
	if runningTasks == nil && createdTasks == nil {
		return
	}
	orderedCN := getCNOrdered(runningTasks, workingCN)

	s.allocateTasks(createdTasks, orderedCN)

	expiredTasks := getExpiredTasks(runningTasks, expiredCN)
	s.allocateTasks(expiredTasks, orderedCN)
}

func (s *scheduler) queryTasks(status task.TaskStatus) []task.Task {
	tasks, err := s.QueryTask(s.ctx, taskservice.WithTaskStatusCond(taskservice.EQ, status))
	if err != nil {
		s.logger.Error("failed to query tasks",
			zap.String("status", status.String()),
			zap.Error(err))
		return nil
	}
	return tasks
}

func (s *scheduler) allocateTasks(tasks []task.Task, orderedCN *cnMap) {
	for _, t := range tasks {
		runner := orderedCN.min()
		if runner == "" {
			s.logger.Warn("no CN available")
			return
		}
		err := s.Allocate(s.ctx, t, runner)
		if err != nil {
			s.logger.Error("allocating task error",
				zap.Uint64("task-id", t.ID),
				zap.String("task-runner", runner),
				zap.Error(err))
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
