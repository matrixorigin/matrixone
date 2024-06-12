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

package tnservice

import (
	"context"
	"time"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

func (s *store) heartbeatTask(ctx context.Context) {
	if s.cfg.HAKeeper.HeatbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	defer func() {
		s.rt.Logger().Info("dn heartbeat task stopped")
	}()

	ticker := time.NewTicker(s.cfg.HAKeeper.HeatbeatInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.heartbeat(ctx)
			// see pkg/logservice/service_commands.go#130
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (s *store) heartbeat(ctx context.Context) {
	start := time.Now()
	defer func() {
		v2.TNHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()
	ctx2, cancel := context.WithTimeout(ctx, s.cfg.HAKeeper.HeatbeatTimeout.Duration)
	defer cancel()

	hb := logservicepb.TNStoreHeartbeat{
		UUID:                 s.cfg.UUID,
		ServiceAddress:       s.txnServiceServiceAddr(),
		Shards:               s.getTNShardInfo(),
		TaskServiceCreated:   s.taskServiceCreated(),
		LogtailServerAddress: s.logtailServiceServiceAddr(),
		LockServiceAddress:   s.lockServiceServiceAddr(),
		ShardServiceAddress:  s.shardServiceServiceAddr(),
		ConfigData:           s.config.GetData(),
	}

	if s.queryService != nil {
		hb.QueryAddress = s.queryServiceServiceAddr()
	}

	cb, err := s.hakeeperClient.SendTNHeartbeat(ctx2, hb)
	if err != nil {
		v2.TNHeartbeatFailureCounter.Inc()
		s.rt.Logger().Error("failed to send tn heartbeat", zap.Error(err))
		return
	}

	s.config.DecrCount()
	s.handleCommands(cb.Commands)
}

func (s *store) handleCommands(cmds []logservicepb.ScheduleCommand) {
	for _, cmd := range cmds {
		if cmd.ServiceType != logservicepb.TNService {
			s.rt.Logger().Fatal("received invalid command", zap.String("command", cmd.LogString()))
		}
		s.rt.Logger().Debug("applying schedule command:", zap.String("command", cmd.LogString()))
		if cmd.ConfigChange != nil {
			switch cmd.ConfigChange.ChangeType {
			case logservicepb.AddReplica, logservicepb.StartReplica:
				s.handleAddReplica(cmd)
			case logservicepb.RemoveReplica, logservicepb.StopReplica:
				s.handleRemoveReplica(cmd)
			}
		} else if cmd.GetShutdownStore() != nil {
			s.handleShutdownStore(cmd)
		} else if cmd.CreateTaskService != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
		}
	}
}

func (s *store) handleAddReplica(cmd logservicepb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	logShardID := cmd.ConfigChange.Replica.LogShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	address := s.cfg.ServiceAddress
	if err := s.createReplica(metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    shardID,
			LogShardID: logShardID,
		},
		ReplicaID: replicaID,
		Address:   address,
	}); err != nil {
		s.rt.Logger().Error("failed to add replica", zap.Error(err))
	}
}

func (s *store) handleRemoveReplica(cmd logservicepb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	if err := s.removeReplica(shardID); err != nil {
		s.rt.Logger().Error("failed to remove replica", zap.Error(err))
	}
}

func (s *store) handleShutdownStore(_ logservicepb.ScheduleCommand) {
	// notify main routine that have received shutdown cmd
	select {
	case s.shutdownC <- struct{}{}:
	default:
	}
}
