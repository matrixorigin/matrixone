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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func (s *Service) handleCommands(cmds []pb.ScheduleCommand) {
	for _, cmd := range cmds {
		if cmd.GetConfigChange() != nil {
			switch cmd.ConfigChange.ChangeType {
			case pb.AddReplica:
				s.handleAddReplica(cmd)
			case pb.RemoveReplica:
				s.handleRemoveReplica(cmd)
			case pb.StartReplica:
				s.handleStartReplica(cmd)
			case pb.StopReplica:
				s.handleStopReplica(cmd)
			default:
				panic("unknown type")
			}
			return
		}

		if cmd.GetShutdownStore() != nil {
			// FIXME: add logic to shutdown store
		}
	}
}

func (s *Service) handleAddReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	target := cmd.ConfigChange.Replica.UUID
	if err := s.store.addReplica(shardID, replicaID, target, epoch); err != nil {
		plog.Errorf("failed to add replica %v", err)
	}
}

func (s *Service) handleRemoveReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	if err := s.store.removeReplica(shardID, replicaID, epoch); err != nil {
		plog.Errorf("failed to remove replica %v", err)
	}
}

func (s *Service) handleStartReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.StartHAKeeperReplica(replicaID, nil, false); err != nil {
			plog.Errorf("failed to start HAKeeper replica %v", err)
		}
	} else {
		if err := s.store.StartReplica(shardID, replicaID, nil, false); err != nil {
			plog.Errorf("failed to start log replica %v", err)
		}
	}
}

func (s *Service) handleStopReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	if err := s.store.stopReplica(shardID, replicaID); err != nil {
		plog.Errorf("failed to stop replica %v", err)
	}
}
