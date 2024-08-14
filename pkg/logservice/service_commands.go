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
	"fmt"
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

func (s *Service) handleCommands(cmds []pb.ScheduleCommand) {
	for _, cmd := range cmds {
		s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
		if cmd.GetConfigChange() != nil {
			s.runtime.Logger().Debug("applying schedule command:", zap.String("command", cmd.LogString()))
			switch cmd.ConfigChange.ChangeType {
			case pb.AddReplica:
				s.handleAddReplica(cmd)
			case pb.AddNonVotingReplica:
				s.handleAddNonVotingReplica(cmd)
			case pb.RemoveReplica:
				s.handleRemoveReplica(cmd)
			case pb.RemoveNonVotingReplica:
				s.handleRemoveReplica(cmd)
			case pb.StartReplica:
				s.handleStartReplica(cmd)
			case pb.StartNonVotingReplica:
				s.handleStartNonVotingReplica(cmd)
			case pb.StopReplica:
				s.handleStopReplica(cmd)
			case pb.StopNonVotingReplica:
				s.handleStopReplica(cmd)
			case pb.KillZombie:
				s.handleKillZombie(cmd)
			default:
				panic(fmt.Sprintf("unknown config change cmd type %d", cmd.ConfigChange.ChangeType))
			}
		} else if cmd.GetShutdownStore() != nil {
			s.handleShutdownStore(cmd)
		} else if cmd.GetCreateTaskService() != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
		} else if cmd.GetAddLogShard() != nil {
			s.handleAddLogShard(cmd)
		} else if cmd.GetBootstrapShard() != nil {
			s.handleBootstrapShard(cmd)
		} else {
			panic(fmt.Sprintf("unknown schedule command type: %+v", cmd))
		}
	}
}

func (s *Service) handleAddReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	target := cmd.ConfigChange.Replica.UUID
	if err := s.store.addReplica(shardID, replicaID, target, epoch); err != nil {
		s.runtime.Logger().Error("failed to add replica", zap.Error(err))
	}
}

func (s *Service) handleAddNonVotingReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	target := cmd.ConfigChange.Replica.UUID
	if err := s.store.addNonVotingReplica(shardID, replicaID, target, epoch); err != nil {
		s.runtime.Logger().Error("failed to add non-voting replica", zap.Error(err))
	}
}

func (s *Service) handleRemoveReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	if err := s.store.removeReplica(shardID, replicaID, epoch); err != nil {
		s.runtime.Logger().Error("failed to remove replica", zap.Error(err))
	}
}

func (s *Service) handleStartReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	join := len(cmd.ConfigChange.InitialMembers) == 0
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.startHAKeeperReplica(replicaID,
			cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start HAKeeper replica", zap.Error(err))
		}
	} else {
		if err := s.store.startReplica(shardID,
			replicaID, cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start log replica", zap.Error(err))
		}
	}
}

func (s *Service) handleStartNonVotingReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	join := len(cmd.ConfigChange.InitialMembers) == 0
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.startHAKeeperNonVotingReplica(replicaID,
			cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start HAKeeper replica",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Error(err),
			)
		}
	} else {
		if err := s.store.startNonVotingReplica(shardID,
			replicaID, cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start log replica",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Error(err),
			)
		}
	}
}

func (s *Service) handleStopReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	if err := s.store.stopReplica(shardID, replicaID); err != nil {
		s.runtime.Logger().Error("failed to stop replica", zap.Error(err))
	}
}

func (s *Service) handleKillZombie(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	s.handleStopReplica(cmd)
	s.store.removeMetadata(shardID, replicaID)
}

func (s *Service) handleShutdownStore(_ pb.ScheduleCommand) {
	// notify main routine that have received shutdown cmd
	select {
	case s.shutdownC <- struct{}{}:
	default:
	}
}

func (s *Service) heartbeatWorker(ctx context.Context) {
	// TODO: check tick interval
	if s.cfg.HeartbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	defer func() {
		s.runtime.Logger().Info("heartbeat worker stopped")
	}()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval.Duration)
	defer ticker.Stop()
	ctx, span := trace.Start(ctx, "heartbeatWorker")
	defer span.End()

	for {
		select {
		case <-ctx.Done():
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
	start := time.Now()
	defer func() {
		v2.LogHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()
	ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if s.haClient == nil {
		if reflect.DeepEqual(s.cfg.HAKeeperClientConfig, HAKeeperClientConfig{}) {
			panic("empty HAKeeper client config")
		}
		cc, err := NewLogHAKeeperClient(ctx2, s.cfg.UUID, s.cfg.GetHAKeeperClientConfig())
		if err != nil {
			s.runtime.Logger().Error("failed to create HAKeeper client", zap.Error(err))
			return
		}
		s.haClient = cc
	}

	hb := s.store.getHeartbeatMessage()
	hb.TaskServiceCreated = s.taskServiceCreated()
	hb.ConfigData = s.config.GetData()

	cb, err := s.haClient.SendLogHeartbeat(ctx2, hb)
	if err != nil {
		v2.LogHeartbeatFailureCounter.Inc()
		s.runtime.Logger().Error("failed to send log service heartbeat", zap.Error(err))
		return
	}

	s.config.DecrCount()

	s.handleCommands(cb.Commands)
}
