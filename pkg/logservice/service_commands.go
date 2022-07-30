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
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func (s *Service) handleCommands(cmds []pb.ScheduleCommand) {
	for _, cmd := range cmds {
		plog.Infof("%s applying cmd: %s", s.ID(), cmd.LogString())
		if cmd.GetConfigChange() != nil {
			plog.Infof("applying schedule command: %s", cmd.LogString())
			switch cmd.ConfigChange.ChangeType {
			case pb.AddReplica:
				s.handleAddReplica(cmd)
			case pb.RemoveReplica:
				// FIXME: when remove replica cmd is received, we need to stop the zombie
				// replica running on the local store.
				s.handleRemoveReplica(cmd)
			case pb.StartReplica:
				s.handleStartReplica(cmd)
			case pb.StopReplica:
				s.handleStopReplica(cmd)
			default:
				panic("unknown config change cmd type")
			}
		} else if cmd.GetShutdownStore() != nil {
			s.handleShutdownStore(cmd)
		} else {
			panic("unknown schedule command type")
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
	join := len(cmd.ConfigChange.InitialMembers) == 0
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.startHAKeeperReplica(replicaID,
			cmd.ConfigChange.InitialMembers, join); err != nil {
			plog.Errorf("failed to start HAKeeper replica %v", err)
		}
	} else {
		if err := s.store.startReplica(shardID,
			replicaID, cmd.ConfigChange.InitialMembers, join); err != nil {
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

func (s *Service) handleShutdownStore(cmd pb.ScheduleCommand) {
	panic("not implemented")
}

func (s *Service) heartbeatWorker(ctx context.Context) {
	// TODO: check tick interval
	if s.cfg.HeartbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	ticker := time.NewTicker(s.cfg.HeartbeatInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			plog.Infof("heartbeat worker stopped")
			return
		case <-ticker.C:
			s.heartbeat(ctx)
			// I'd call this an ugly hack to just workaround select's
			// policy of randomly picking a ready channel from the case list.
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (s *Service) heartbeat(ctx context.Context) {
	ctx2, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if s.haClient == nil {
		if reflect.DeepEqual(s.cfg.HAKeeperClientConfig, HAKeeperClientConfig{}) {
			panic("empty HAKeeper client config")
		}
		cc, err := NewLogHAKeeperClient(ctx2, s.cfg.GetHAKeeperClientConfig())
		if err != nil {
			plog.Errorf("failed to create HAKeeper client, %v", err)
			return
		}
		s.haClient = cc
	}

	hb := s.store.getHeartbeatMessage()
	cb, err := s.haClient.SendLogHeartbeat(ctx2, hb)
	if err != nil {
		plog.Errorf("failed to send log service heartbeat, %v", err)
		return
	}
	s.handleCommands(cb.Commands)
}
