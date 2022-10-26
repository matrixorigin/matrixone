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

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/file"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	initTasks = []task.TaskCode{task.TaskCode_TraceInit,
		task.TaskCode_SysViewInit,
		task.TaskCode_FrontendInit,
		task.TaskCode_MetricInit}
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
		s.task.holder = taskservice.NewTaskServiceHolder(s.logger,
			func() (string, error) { return s.cfg.SQLAddress, nil })
	} else {
		s.task.holder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(s.logger,
			func() (string, error) { return s.cfg.SQLAddress, nil },
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
	}

	if err := s.stopper.RunTask(s.waitAllInitTaskCompleted); err != nil {
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

	s.registerExecutors()
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

func (s *service) waitAllInitTaskCompleted(ctx context.Context) {
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
					taskservice.WithTaskExecutorCond(taskservice.LE, uint32(task.TaskCode_FrontendInit)),
					taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Completed))
				if err != nil {
					s.logger.Error("wait all init task completed failed", zap.Error(err))
					break
				}
				s.logger.Debug("waiting all init task completed",
					zap.Int("all", len(initTasks)),
					zap.Int("completed", len(tasks)))
				if len(tasks) == len(initTasks) {
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
	}
}

func (s *service) stopTask() error {
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

func (s *service) registerExecutors() {
	if s.task.runner == nil {
		return
	}

	pu := config.NewParameterUnit(&s.cfg.Frontend, nil, nil, nil)
	pu.StorageEngine = s.storeEngine
	pu.TxnClient = s._txnClient
	s.cfg.Frontend.SetDefaultValues()
	pu.FileService = s.fileService
	moServerCtx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(pu)
	}

	executors := map[task.TaskCode]func(context.Context, func() ie.InternalExecutor) error{
		task.TaskCode_TraceInit:   trace.InitSchema,
		task.TaskCode_MetricInit:  metric.InitSchema,
		task.TaskCode_SysViewInit: sysview.InitSchema,
		task.TaskCode_FrontendInit: func(moServerCtx context.Context, _ func() ie.InternalExecutor) error {
			return frontend.InitSysTenant(moServerCtx)
		},
	}

	for code, exec := range executors {
		fn := func(handler func(context.Context, func() ie.InternalExecutor) error) taskservice.TaskExecutor {
			return func(ctx context.Context, task task.Task) error {
				if err := handler(moServerCtx, ieFactory); err != nil {
					panic(err)
				}
				return nil
			}
		}
		s.task.runner.RegisterExecutor(uint32(code), fn(exec))
	}
}
