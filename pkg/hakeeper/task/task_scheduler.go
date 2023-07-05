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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

const (
	taskSchedulerDefaultTimeout = 10 * time.Second
)

type scheduler struct {
	cfg               hakeeper.Config
	taskServiceGetter func() taskservice.TaskService
}

var _ hakeeper.TaskScheduler = (*scheduler)(nil)

func NewScheduler(taskServiceGetter func() taskservice.TaskService, cfg hakeeper.Config) hakeeper.TaskScheduler {
	cfg.Fill()
	s := &scheduler{
		taskServiceGetter: taskServiceGetter,
		cfg:               cfg,
	}
	return s
}

func (s *scheduler) Schedule(cnState logservice.CNState, currentTick uint64) {
	workingCN := getWorkingCNs(s.cfg, cnState, currentTick)

	runningTasks := s.queryTasks(task.TaskStatus_Running)
	createdTasks := s.queryTasks(task.TaskStatus_Created)
	tasks := append(runningTasks, createdTasks...)
	for _, task := range tasks {
		if task.IsInitTask() {
			runtime.ProcessLevelRuntime().
				SubLogger(runtime.SystemInit).Debug(
				"task schedule query init task",
				zap.String("task", task.Metadata.String()))
		}
	}

	runtime.ProcessLevelRuntime().Logger().Debug("task schedule query tasks", zap.Int("created", len(createdTasks)),
		zap.Int("running", len(runningTasks)))
	if len(tasks) == 0 {
		return
	}
	orderedCN, expiredTasks := getCNOrderedAndExpiredTasks(runningTasks, workingCN)
	s.allocateTasks(createdTasks, orderedCN)
	s.allocateTasks(expiredTasks, orderedCN)
}

func (s *scheduler) Create(ctx context.Context, tasks []task.TaskMetadata) error {
	ts := s.taskServiceGetter()
	if ts == nil {
		return moerr.NewInternalError(ctx, "failed to get task service")
	}
	if err := ts.CreateBatch(ctx, tasks); err != nil {
		runtime.ProcessLevelRuntime().Logger().Error("failed to create new tasks")
		return err
	}
	runtime.ProcessLevelRuntime().Logger().Debug("new tasks created", zap.Int("created", len(tasks)))
	v, err := ts.QueryTask(ctx)
	if len(v) == 0 && err == nil {
		panic("cannot read created tasks")
	}
	runtime.ProcessLevelRuntime().Logger().Debug("new tasks created, query", zap.Int("created", len(v)), zap.Error(err))
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
		runtime.ProcessLevelRuntime().Logger().Error("failed to query tasks",
			zap.String("status", status.String()))
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
		runtime.ProcessLevelRuntime().Logger().Warn("no CN available")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskSchedulerDefaultTimeout)
	defer cancel()

	if err := ts.Allocate(ctx, t, runner); err != nil {
		runtime.ProcessLevelRuntime().Logger().Error("failed to allocate task",
			zap.Uint64("task-id", t.ID),
			zap.String("task-metadata-id", t.Metadata.ID),
			zap.String("task-runner", runner))
		return
	}
	orderedCN.inc(t.TaskRunner)
}

func getCNOrderedAndExpiredTasks(tasks []task.Task, workingCN []string) (orderedMap *cnMap, expired []task.Task) {
	orderedMap = newOrderedMap(workingCN)
	for _, t := range tasks {
		if contains(workingCN, t.TaskRunner) {
			orderedMap.inc(t.TaskRunner)
		} else {
			expired = append(expired, t)
		}
	}
	for _, t := range tasks {
		if time.Since(time.UnixMilli(t.LastHeartbeat)) > taskSchedulerDefaultTimeout {
			for _, e := range expired {
				if t.ID == e.ID {
					break
				}
			}
			expired = append(expired, t)
		}
	}
	return orderedMap, expired
}
