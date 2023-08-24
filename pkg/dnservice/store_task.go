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

package dnservice

import (
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"go.uber.org/zap"
)

func (s *store) initSqlWriterFactory() {
	getClient := func() util.HAKeeperClient {
		return s.hakeeperClient
	}
	db_holder.SetSQLWriterDBAddressFunc(util.AddressFunc(getClient))
}

func (s *store) createSQLLogger(command *logservicepb.CreateTaskService) {
	// convert username to "mo_logger"
	frontend.SetSpecialUser(db_holder.MOLoggerUser, []byte(command.User.Password))
	db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, command.User.Password)
}

func (s *store) initTaskHolder() {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.serviceHolder != nil {
		return
	}

	getClient := func() util.HAKeeperClient {
		return s.hakeeperClient
	}
	if s.task.storageFactory != nil {
		s.task.serviceHolder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
			s.rt,
			util.AddressFunc(getClient),
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
		return
	}

	s.task.serviceHolder = taskservice.NewTaskServiceHolder(s.rt, util.AddressFunc(getClient))
}

func (s *store) createTaskService(command *logservicepb.CreateTaskService) {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.serviceCreated {
		return
	}

	// Notify frontend to set up the special account used to task framework create and query async tasks.
	// The account is always in the memory.
	frontend.SetSpecialUser(command.User.Username, []byte(command.User.Password))
	if err := s.task.serviceHolder.Create(*command); err != nil {
		s.rt.Logger().Error("create task service failed",
			zap.Error(err))
		return
	}
	s.task.serviceCreated = true
}

func (s *store) taskServiceCreated() bool {
	s.task.RLock()
	defer s.task.RUnlock()
	return s.task.serviceCreated
}

func (s *store) GetTaskService() (taskservice.TaskService, bool) {
	s.task.RLock()
	defer s.task.RUnlock()
	if s.task.serviceHolder == nil {
		return nil, false
	}
	return s.task.serviceHolder.Get()
}
