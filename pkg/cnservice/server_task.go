// Copyright 2021 - 2022 Matrix Origin
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

package cnservice

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/file"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"go.uber.org/zap"
)

const (
	defaultSystemInitTimeout = time.Minute * 5
)

func (s *service) adjustSQLAddress() {
	if s.cfg.SQLAddress == "" {
		ip := "127.0.0.1"
		if s.cfg.Frontend.Host != "" &&
			s.cfg.Frontend.Host != "0.0.0.0" {
			ip = s.cfg.Frontend.Host
		}

		s.cfg.SQLAddress = fmt.Sprintf("%s:%d",
			ip,
			s.cfg.Frontend.Port)
	}
}

func (s *service) initTaskServiceHolder() {
	s.adjustSQLAddress()

	s.task.Lock()
	defer s.task.Unlock()
	if s.task.storageFactory == nil {
		s.task.holder = taskservice.NewTaskServiceHolder(
			runtime.ProcessLevelRuntime(),
			func(context.Context) (string, error) { return s.cfg.SQLAddress, nil })
	} else {
		s.task.holder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
			runtime.ProcessLevelRuntime(),
			func(context.Context) (string, error) { return s.cfg.SQLAddress, nil },
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
	}

	if err := s.stopper.RunTask(s.waitSystemInitCompleted); err != nil {
		panic(err)
	}
}

func (s *service) createTaskService(command *logservicepb.CreateTaskService) {
	// Notify frontend to setup the special account used to task framework create and query async tasks.
	// The account is always in the memory.
	frontend.SetSpecialUser(command.User.Username, []byte(command.User.Password))

	if err := s.task.holder.Create(*command); err != nil {
		s.logger.Error("create task service failed", zap.Error(err))
		return
	}
	s.startTaskRunner()
}

func (s *service) startTaskRunner() {
	s.task.Lock()
	defer s.task.Unlock()

	if s.task.runner != nil {
		return
	}

	ts, ok := s.task.holder.Get()
	if !ok {
		panic("task service must created")
	}

	s.task.runner = taskservice.NewTaskRunner(s.cfg.UUID,
		ts,
		taskservice.WithRunnerLogger(s.logger),
		taskservice.WithOptions(
			s.cfg.TaskRunner.QueryLimit,
			s.cfg.TaskRunner.Parallelism,
			s.cfg.TaskRunner.MaxWaitTasks,
			s.cfg.TaskRunner.FetchInterval.Duration,
			s.cfg.TaskRunner.FetchTimeout.Duration,
			s.cfg.TaskRunner.RetryInterval.Duration,
			s.cfg.TaskRunner.HeartbeatInterval.Duration,
		),
	)

	s.registerExecutorsLocked()
	if err := s.task.runner.Start(); err != nil {
		s.logger.Error("start task runner failed",
			zap.Error(err))
	}
}

func (s *service) GetTaskRunner() taskservice.TaskRunner {
	s.task.RLock()
	defer s.task.RUnlock()
	return s.task.runner
}

func (s *service) GetTaskService() (taskservice.TaskService, bool) {
	s.task.RLock()
	defer s.task.RUnlock()
	return s.task.holder.Get()
}

func (s *service) WaitSystemInitCompleted(ctx context.Context) error {
	s.waitSystemInitCompleted(ctx)
	return ctx.Err()
}

func (s *service) waitSystemInitCompleted(ctx context.Context) {
	defer logutil.LogAsyncTask(s.logger, "cnservice/wait-system-init-task")()

	startAt := time.Now()
	s.logger.Debug("wait all init task completed task started")
	wait := func() {
		time.Sleep(time.Second)
	}
	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("wait all init task completed task stopped")
			return
		default:
			ts, ok := s.GetTaskService()
			if ok {
				tasks, err := ts.QueryTask(ctx,
					taskservice.WithTaskExecutorCond(taskservice.EQ, task.TaskCode_SystemInit),
					taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Completed))
				if err != nil {
					s.logger.Error("wait all init task completed failed", zap.Error(err))
					break
				}
				s.logger.Debug("waiting all init task completed",
					zap.Int("completed", len(tasks)))
				if len(tasks) > 0 {
					if err := file.WriteFile(s.metadataFS,
						"./system_init_completed",
						[]byte("OK")); err != nil {
						panic(err)
					}
					return
				}
			}
		}
		wait()
		if time.Since(startAt) > defaultSystemInitTimeout {
			panic("wait system init timeout")
		}
	}
}

func (s *service) stopTask() error {
	defer logutil.LogClose(s.logger, "cnservice/task")()

	s.task.Lock()
	defer s.task.Unlock()
	if err := s.task.holder.Close(); err != nil {
		return err
	}
	if s.task.runner != nil {
		return s.task.runner.Stop()
	}
	return nil
}

func (s *service) registerExecutorsLocked() {
	if s.task.runner == nil {
		return
	}

	pu := config.NewParameterUnit(
		&s.cfg.Frontend,
		nil,
		nil,
		nil)
	pu.StorageEngine = s.storeEngine
	pu.TxnClient = s._txnClient
	s.cfg.Frontend.SetDefaultValues()
	pu.FileService = s.fileService
	moServerCtx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(pu, s.mo.GetRoutineManager().GetAutoIncrCache())
	}

	ts, ok := s.task.holder.Get()
	if !ok {
		panic(moerr.NewInternalErrorNoCtx("task Service not ok"))
	}
	s.task.runner.RegisterExecutor(task.TaskCode_SystemInit,
		func(ctx context.Context, t task.Task) error {
			if err := frontend.InitSysTenant(moServerCtx, s.mo.GetRoutineManager().GetAutoIncrCache()); err != nil {
				return err
			}
			if err := sysview.InitSchema(moServerCtx, ieFactory); err != nil {
				return err
			}
			if err := metric.InitSchema(moServerCtx, ieFactory); err != nil {
				return err
			}
			if err := motrace.InitSchema(moServerCtx, ieFactory); err != nil {
				return err
			}

			// init metric/log merge task cron rule
			if err := export.CreateCronTask(moServerCtx, task.TaskCode_MetricLogMerge, ts); err != nil {
				return err
			}

			// init metric task
			if err := metric.CreateCronTask(moServerCtx, task.TaskCode_MetricStorageUsage, ts); err != nil {
				return err
			}

			return nil
		})

	// init metric/log merge task executor
	s.task.runner.RegisterExecutor(task.TaskCode_MetricLogMerge,
		export.MergeTaskExecutorFactory(export.WithFileService(s.fileService)))
	// init metric task
	s.task.runner.RegisterExecutor(task.TaskCode_MetricStorageUsage,
		metric.GetMetricStorageUsageExecutor(ieFactory))
}
