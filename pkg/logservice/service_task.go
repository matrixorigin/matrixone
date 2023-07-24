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

package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"go.uber.org/zap"
)

func (s *Service) initSqlWriterFactory() {
	getClient := func() util.HAKeeperClient {
		return s.haClient
	}
	db_holder.SetSQLWriterDBAddressFunc(util.AddressFunc(getClient))
}

func (s *Service) createSQLLogger(command *logservicepb.CreateTaskService) {
	db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, command.User.Password)
}

func (s *Service) initTaskHolder() {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.holder != nil {
		return
	}

	getClient := func() util.HAKeeperClient {
		return s.haClient
	}

	if s.task.storageFactory != nil {
		s.task.holder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
			runtime.ProcessLevelRuntime(),
			util.AddressFunc(getClient),
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
		return
	}
	s.task.holder = taskservice.NewTaskServiceHolder(runtime.ProcessLevelRuntime(), util.AddressFunc(getClient))
}

func (s *Service) createTaskService(command *logservicepb.CreateTaskService) {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.created {
		return
	}
	if err := s.task.holder.Create(*command); err != nil {
		s.runtime.Logger().Error("create task service failed",
			zap.Error(err))
		return
	}
	s.task.created = true
}

func (s *Service) taskServiceCreated() bool {
	s.task.RLock()
	defer s.task.RUnlock()
	return s.task.created
}

func (s *Service) getTaskService() taskservice.TaskService {
	s.task.RLock()
	defer s.task.RUnlock()
	if s.task.holder == nil {
		return nil
	}

	ts, ok := s.task.holder.Get()
	if !ok {
		return nil
	}
	return ts
}
