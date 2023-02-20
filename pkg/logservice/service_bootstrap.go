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
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
)

func (s *Service) BootstrapHAKeeper(ctx context.Context, cfg Config) error {
	replicaID, bootstrapping := cfg.Bootstrapping()
	if !bootstrapping {
		return nil
	}
	members, err := cfg.GetInitHAKeeperMembers()
	if err != nil {
		return err
	}
	if err := s.store.startHAKeeperReplica(replicaID, members, false); err != nil {
		// let's be a little less strict, when HAKeeper replica is already
		// running as a result of store.startReplicas(), we just ignore the
		// dragonboat.ErrShardAlreadyExist error below.
		if err != dragonboat.ErrShardAlreadyExist {
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to start hakeeper replica", zap.Error(err))
			return err
		}
	}
	numOfLogShards := cfg.BootstrapConfig.NumOfLogShards
	numOfDNShards := cfg.BootstrapConfig.NumOfDNShards
	numOfLogReplicas := cfg.BootstrapConfig.NumOfLogShardReplicas
	for i := 0; i < checkBootstrapCycles; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := s.store.setInitialClusterInfo(numOfLogShards,
			numOfDNShards, numOfLogReplicas); err != nil {
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to set initial cluster info", zap.Error(err))
			if err == dragonboat.ErrShardNotFound {
				return nil
			}
			time.Sleep(time.Second)
			continue
		}
		s.runtime.SubLogger(runtime.SystemInit).Info("initial cluster info set")
		break
	}
	for i := 0; i < checkBootstrapCycles; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := s.createInitTasks(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (s *Service) createInitTasks(ctx context.Context) error {
	if err := s.store.taskScheduler.Create(ctx, []task.TaskMetadata{{
		ID:       task.TaskCode_SystemInit.String(),
		Executor: task.TaskCode_SystemInit,
	}}); err != nil {
		s.runtime.SubLogger(runtime.SystemInit).Error("failed to create init tasks", zap.Error(err))
		return err
	}
	s.runtime.SubLogger(runtime.SystemInit).Info("init tasks created")
	return nil
}
