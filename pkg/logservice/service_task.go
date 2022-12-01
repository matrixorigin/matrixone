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
	"context"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

func (s *Service) initTaskHolder() {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.holder != nil {
		return
	}

	addressFunc := func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx,
			time.Second*5)
		defer cancel()
		if s.haClient == nil {
			return "", nil
		}
		details, err := s.haClient.GetClusterDetails(ctx)
		if err != nil {
			return "", err
		}
		if len(details.CNStores) == 0 {
			return "", moerr.NewInvalidState(ctx, "no cn in the cluster")
		}

		n := rand.Intn(len(details.CNStores))
		return details.CNStores[n].SQLAddress, nil
	}

	if s.task.storageFactory != nil {
		s.task.holder = taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
			runtime.ProcessLevelRuntime(),
			addressFunc,
			func(_, _, _ string) taskservice.TaskStorageFactory {
				return s.task.storageFactory
			})
		return
	}
	s.task.holder = taskservice.NewTaskServiceHolder(runtime.ProcessLevelRuntime(), addressFunc)
}

func (s *Service) createTaskService(command *logservicepb.CreateTaskService) {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.created {
		return
	}
	if err := s.task.holder.Create(*command); err != nil {
		s.logger.Error("create task service failed",
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
