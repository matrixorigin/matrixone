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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

func (s *service) startCNStoreHeartbeat() error {
	if s._hakeeperClient == nil {
		if _, err := s.getHAKeeperClient(); err != nil {
			return err
		}
	}
	return s.stopper.RunNamedTask("cnservice-heartbeat", s.heartbeatTask)
}

func (s *service) heartbeatTask(ctx context.Context) {
	if s.cfg.HAKeeper.HeatbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	defer logutil.LogAsyncTask(s.logger, "cnservice/heartbeat-task")()
	defer func() {
		s.logger.Info("cn heartbeat task stopped")
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

func (s *service) heartbeat(ctx context.Context) {
	start := time.Now()
	defer func() {
		v2.CNHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()

	ctx2, cancel := context.WithTimeout(ctx, s.cfg.HAKeeper.HeatbeatTimeout.Duration)
	defer cancel()

	hb := logservicepb.CNStoreHeartbeat{
		UUID:               s.cfg.UUID,
		ServiceAddress:     s.pipelineServiceServiceAddr(),
		SQLAddress:         s.cfg.SQLAddress,
		LockServiceAddress: s.lockServiceServiceAddr(),
		Role:               s.metadata.Role,
		TaskServiceCreated: s.GetTaskRunner() != nil,
		QueryAddress:       s.queryServiceServiceAddr(),
		InitWorkState:      s.cfg.InitWorkState,
		ConfigData:         s.config.GetData(),
		Resource: logservicepb.Resource{
			CPUTotal:     uint64(system.NumCPU()),
			CPUAvailable: system.CPUAvailable(),
			MemTotal:     system.MemoryTotal(),
			MemAvailable: system.MemoryAvailable(),
		},
	}
	if s.gossipNode != nil {
		hb.GossipAddress = s.gossipServiceAddr()
		hb.GossipJoined = s.gossipNode.Joined()
	}

	cb, err := s._hakeeperClient.SendCNHeartbeat(ctx2, hb)
	if err != nil {
		v2.CNHeartbeatFailureCounter.Inc()
		s.logger.Error("failed to send cn heartbeat", zap.Error(err))
		return
	}

	select {
	case <-s.hakeeperConnected:
	default:
		s.initTaskServiceHolder()
		close(s.hakeeperConnected)
	}
	s.config.DecrCount()
	s.handleCommands(cb.Commands)
}

func (s *service) handleCommands(cmds []logservicepb.ScheduleCommand) {
	for _, cmd := range cmds {
		if cmd.ServiceType != logservicepb.CNService {
			s.logger.Fatal("received invalid command", zap.String("command", cmd.LogString()))
		}
		s.logger.Info("applying schedule command", zap.String("command", cmd.LogString()))
		if cmd.CreateTaskService != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
		} else if s.gossipNode.Created() && cmd.JoinGossipCluster != nil {
			s.gossipNode.SetJoined()

			// Start an async task to join the gossip cluster to avoid the long time joining, and if
			// it fails to join cluster, unset the joined state to give it another try.
			if err := s.stopper.RunNamedTask("join gossip cluster", func(ctx context.Context) {
				// The local state may be large, so do not set a timeout context.
				if err := s.gossipNode.Join(cmd.JoinGossipCluster.Existing); err != nil {
					s.logger.Error("failed to join gossip cluster", zap.Error(err))
					s.gossipNode.UnsetJoined()
				}
			}); err != nil {
				s.logger.Error("failed to start task to join gossip cluster", zap.Error(err))
				s.gossipNode.UnsetJoined()
			}
		}
	}
}
