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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	moconnector "github.com/matrixorigin/matrixone/pkg/stream/connector"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"go.uber.org/zap"
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

	getClient := func() util.HAKeeperClient {
		client, _ := s.getHAKeeperClient()
		return client
	}
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.storageFactory == nil {
		s.task.holder = taskservice.NewTaskServiceHolder(
			runtime.ProcessLevelRuntime(),
			util.AddressFunc(getClient))
	} else {
		s.task.holder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
			runtime.ProcessLevelRuntime(),
			util.AddressFunc(getClient),
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
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

	ts, ok := s.task.holder.Get()
	if !ok {
		panic("no task service is initialized")
	}
	s.pu.TaskService = ts
}

func (s *service) initSqlWriterFactory() {
	getClient := func() util.HAKeeperClient {
		client, _ := s.getHAKeeperClient()
		return client
	}
	db_holder.SetSQLWriterDBAddressFunc(util.AddressFunc(getClient))
}

func (s *service) createSQLLogger(command *logservicepb.CreateTaskService) {
	frontend.SetSpecialUser(db_holder.MOLoggerUser, []byte(command.User.Password))
	db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, command.User.Password)
}

func (s *service) canClaimDaemonTask(taskAccount string) bool {
	const accountKey = "account"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	state, err := s._hakeeperClient.GetClusterState(ctx)
	if err != nil {
		return false
	}
	stores := state.CNState.Stores

	info, ok := stores[s.cfg.UUID]
	// 1. Cannot find current CN service UUID in cluster state.
	if !ok {
		return false
	}

	// We assume that the runner is a shard runner.
	localShared := true

	// 2. If the current runner has the same account info, return true.
	for _, account := range info.Labels[accountKey].Labels {
		if account != "" {
			// This CN node has account, so it is not a shared one.
			localShared = false
		}
		if strings.EqualFold(account, taskAccount) {
			return true
		}
	}

	isSysTask := strings.EqualFold(taskAccount, frontend.GetDefaultTenant())

	var taskHasRunner bool
	for _, store := range stores {
		for key, labelInfo := range store.Labels {
			if strings.EqualFold(accountKey, key) {
				for _, label := range labelInfo.Labels {
					if strings.EqualFold(label, taskAccount) {
						taskHasRunner = true
					}
				}
			}
		}
	}

	// 3. If there are no other runners for this task, and local runner is a shared one or the
	// task is belongs to sys account, we could run it.
	if !taskHasRunner && (localShared || isSysTask) {
		return true
	}

	// 4. Otherwise, we could not run this task.
	return false
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
		s.canClaimDaemonTask,
		taskservice.WithRunnerLogger(s.logger),
		taskservice.WithOptions(
			s.cfg.TaskRunner.QueryLimit,
			s.cfg.TaskRunner.Parallelism,
			s.cfg.TaskRunner.MaxWaitTasks,
			s.cfg.TaskRunner.FetchInterval.Duration,
			s.cfg.TaskRunner.FetchTimeout.Duration,
			s.cfg.TaskRunner.RetryInterval.Duration,
			s.cfg.TaskRunner.HeartbeatInterval.Duration,
			s.cfg.TaskRunner.HeartbeatTimeout.Duration,
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
	pu.LockService = s.lockService
	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor()
	}

	ts, ok := s.task.holder.Get()
	if !ok {
		panic(moerr.NewInternalErrorNoCtx("task Service not ok"))
	}

	// init metric/log merge task executor
	s.task.runner.RegisterExecutor(task.TaskCode_MetricLogMerge,
		export.MergeTaskExecutorFactory(export.WithFileService(s.etlFS)))
	// init metric task
	s.task.runner.RegisterExecutor(task.TaskCode_MetricStorageUsage,
		mometric.GetMetricStorageUsageExecutor(ieFactory))
	// streaming connector task
	s.task.runner.RegisterExecutor(task.TaskCode_ConnectorKafkaSink,
		moconnector.KafkaSinkConnectorExecutor(s.logger, ts, ieFactory, s.task.runner.Attach))
	s.task.runner.RegisterExecutor(task.TaskCode_MergeObject,
		func(ctx context.Context, task task.Task) error {
			metadata := task.GetMetadata()
			var mergeTask api.MergeTaskEntry
			err := mergeTask.Unmarshal(metadata.Context)
			if err != nil {
				return err
			}

			objs := make([]string, len(mergeTask.ToMergeObjs))
			for i, b := range mergeTask.ToMergeObjs {
				stats := objectio.ObjectStats(b)
				objs[i] = stats.ObjectName().String()
			}
			sql := fmt.Sprintf("select mo_ctl('DN', 'MERGEOBJECTS', '%s.%s:%s')",
				mergeTask.DbName, mergeTask.TableName, strings.Join(objs, ","))
			ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
			opts := executor.Options{}.WithAccountID(mergeTask.AccountId).WithWaitCommittedLogApplied()
			_, err = s.sqlExecutor.Exec(ctx, sql, opts)
			return err
		},
	)
}
