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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

const (
	taskSchedulerDefaultTimeout = 2 * time.Second
)

type scheduler struct {
	logger            *zap.Logger
	cfg               hakeeper.Config
	taskServiceGetter func() taskservice.TaskService
}

var _ hakeeper.TaskScheduler = (*scheduler)(nil)

func NewScheduler(taskServiceGetter func() taskservice.TaskService, cfg hakeeper.Config, logger *zap.Logger) hakeeper.TaskScheduler {
	cfg.Fill()
	s := &scheduler{
		logger:            logutil.Adjust(logger),
		taskServiceGetter: taskServiceGetter,
		cfg:               cfg,
	}
	return s
}

func (s *scheduler) Schedule(cnState logservice.CNState, currentTick uint64) {
	workingCN, expiredCN := parseCNStores(s.cfg, cnState, currentTick)

	runningTasks := s.queryTasks(task.TaskStatus_Running)
	createdTasks := s.queryTasks(task.TaskStatus_Created)
	s.logger.Debug("task schedule query tasks", zap.Int("created", len(createdTasks)),
		zap.Int("running", len(runningTasks)))
	if len(runningTasks) == 0 && len(createdTasks) == 0 {
		return
	}
	orderedCN := getCNOrdered(runningTasks, workingCN)

	s.allocateTasks(createdTasks, orderedCN)

	expiredTasks := getExpiredTasks(runningTasks, expiredCN)
	s.allocateTasks(expiredTasks, orderedCN)
}

func (s *scheduler) Create(ctx context.Context, tasks []task.TaskMetadata) error {
	ts := s.taskServiceGetter()
	if ts == nil {
		return moerr.NewInternalError("failed to get task service")
	}
	if err := ts.CreateBatch(ctx, tasks); err != nil {
		s.logger.Error("failed to create new tasks", zap.Error(err))
		return err
	}
	s.logger.Debug("new tasks created", zap.Int("created", len(tasks)))
	v, err := ts.QueryTask(ctx)
	if len(v) == 0 && err == nil {
		panic("cannot read created tasks")
	}
	s.logger.Debug("new tasks created, query", zap.Int("created", len(v)), zap.Error(err))
	return nil
}

func (s *scheduler) StartScheduleCronTask() {
	if ts := s.taskServiceGetter(); ts != nil {
		ts.StartScheduleCronTask()
	}
}

func (s *scheduler) StopScheduleCronTask() {
	if ts := s.taskServiceGetter(); ts != nil {
		ts.StopScheduleCronTask()
	}
}

func (s *scheduler) queryTasks(status task.TaskStatus) []task.Task {
	ts := s.taskServiceGetter()
	if ts == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskSchedulerDefaultTimeout)
	defer cancel()

	tasks, err := ts.QueryTask(ctx, taskservice.WithTaskStatusCond(taskservice.EQ, status))
	if err != nil {
		s.logger.Error("failed to query tasks",
			zap.String("status", status.String()),
			zap.Error(err))
		return nil
	}
	return tasks
}

func (s *scheduler) allocateTasks(tasks []task.Task, orderedCN *cnMap) {
	ts := s.taskServiceGetter()
	if ts == nil {
		return
	}

	for _, t := range tasks {
		s.allocateTask(ts, t, orderedCN)
	}
}

func (s *scheduler) allocateTask(ts taskservice.TaskService, t task.Task, orderedCN *cnMap) {
	runner := orderedCN.min()
	if runner == "" {
		s.logger.Warn("no CN available")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskSchedulerDefaultTimeout)
	defer cancel()

	if err := ts.Allocate(ctx, t, runner); err != nil {
		s.logger.Error("failed to allocate task",
			zap.Uint64("task-id", t.ID),
			zap.String("task-metadata-id", t.Metadata.ID),
			zap.String("task-runner", runner),
			zap.Error(err))
		return
	}
	orderedCN.inc(t.TaskRunner)
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
