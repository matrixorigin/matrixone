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
	runningTasks := s.queryTasks(task.TaskStatus_Running)
	createdTasks := s.queryTasks(task.TaskStatus_Created)
	tasks := append(runningTasks, createdTasks...)
	//for _, t := range tasks {
	//	if t.IsInitTask() {
	//		runtime.ProcessLevelRuntime().
	//			SubLogger(runtime.SystemInit).Debug(
	//			"task schedule query init task",
	//			zap.String("task", t.Metadata.String()))
	//	}
	//}

	runtime.ProcessLevelRuntime().Logger().Debug("task schedule query tasks",
		zap.Int("created", len(createdTasks)),
		zap.Int("running", len(runningTasks)))
	if len(tasks) == 0 {
		return
	}
	workingCNs := selectCNs(cnState, notExpired(s.cfg, currentTick))
	orderedCN, expiredTasks := getCNOrderedAndExpiredTasks(runningTasks, getUUIDs(workingCNs))
	runtime.ProcessLevelRuntime().Logger().Info("task schedule query tasks",
		zap.Int("created", len(createdTasks)),
		zap.Int("expired", len(expiredTasks)))
	s.allocateTasks(createdTasks, orderedCN)
	s.allocateTasks(expiredTasks, orderedCN)
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

func (s *scheduler) queryTasks(status task.TaskStatus) []task.AsyncTask {
	ts := s.taskServiceGetter()
	if ts == nil {
		runtime.ProcessLevelRuntime().Logger().Error("task service is nil",
			zap.String("status", status.String()))
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskSchedulerDefaultTimeout)
	defer cancel()

	tasks, err := ts.QueryAsyncTask(ctx, taskservice.WithTaskStatusCond(status))
	if err != nil {
		runtime.ProcessLevelRuntime().Logger().Error("failed to query tasks",
			zap.String("status", status.String()),
			zap.Error(err))
		return nil
	}
	return tasks
}

func (s *scheduler) allocateTasks(tasks []task.AsyncTask, orderedCN *cnMap) {
	ts := s.taskServiceGetter()
	if ts == nil {
		return
	}

	for _, t := range tasks {
		allocateTask(ts, t, orderedCN)
	}
}

func allocateTask(ts taskservice.TaskService, t task.AsyncTask, orderedCN *cnMap) {
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
			zap.String("task-runner", runner),
			zap.Error(err))
		return
	}
	runtime.ProcessLevelRuntime().Logger().Info("task allocated",
		zap.Uint64("task-id", t.ID),
		zap.String("task-metadata-id", t.Metadata.ID),
		zap.String("task-runner", runner))
	orderedCN.inc(t.TaskRunner)
}

func getCNOrderedAndExpiredTasks(tasks []task.AsyncTask, workingCN map[string]struct{}) (*cnMap, []task.AsyncTask) {
	orderedMap := newOrderedMap(workingCN)
	n := 0
	for _, t := range tasks {
		if _, ok := workingCN[t.TaskRunner]; ok {
			orderedMap.inc(t.TaskRunner)
		} else {
			n++
		}
	}
	if n == 0 {
		return orderedMap, nil
	}
	expired := make([]task.AsyncTask, 0, n)
	for _, t := range tasks {
		if _, ok := workingCN[t.TaskRunner]; !ok {
			expired = append(expired, t)
		}
	}
	return orderedMap, expired
}
