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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var tnCommandPollFailed = logutil.Event{
	Name:    "tn.schedule-command.poll.failed",
	Message: "failed to poll tn schedule commands",
}

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

func (s *store) controlTask(ctx context.Context) {
	commandDone := make(chan struct{})
	go func() {
		defer close(commandDone)
		s.commandTask(ctx)
	}()
	s.heartbeatTask(ctx)
	<-commandDone
}

func (s *store) commandTask(ctx context.Context) {
	client, ok := s.hakeeperClient.(logservice.ScheduleCommandHAKeeperClient)
	if !ok {
		return
	}
	poll := func() {
		// Keep the normal heartbeat as the primary delivery path. This read is
		// issued while that RPC is in flight, or after a failed/deadline heartbeat
		// until one succeeds; healthy clusters do not receive duplicate proposals.
		if !s.heartbeatInFlight.Load() && !s.commandPollNeeded.Load() {
			return
		}
		start := time.Now()
		defer func() {
			v2.TNCommandPollHistogram.Observe(time.Since(start).Seconds())
		}()
		ctx2, cancel := context.WithTimeout(ctx, logservice.ScheduleCommandPollTimeout)
		defer cancel()
		batch, err := client.GetScheduleCommands(ctx2, logservicepb.TNService)
		if err != nil {
			if ctx.Err() == nil {
				v2.TNCommandPollFailureCounter.Inc()
				tnCommandPollFailed.Error(
					zap.String("uuid", s.cfg.UUID),
					zap.Error(err))
			}
			return
		}
		if ctx2.Err() != nil {
			return
		}
		s.handleCommandBatch(batch)
	}

	poll()
	ticker := time.NewTicker(logservice.ScheduleCommandPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

func (s *store) heartbeat(ctx context.Context) {
	start := time.Now()
	defer func() {
		v2.TNHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()
	ctx2, cancel := context.WithTimeoutCause(ctx, s.cfg.HAKeeper.HeatbeatTimeout.Duration, moerr.CauseTnServiceHeartbeat)
	defer cancel()

	hb := logservicepb.TNStoreHeartbeat{
		UUID:                        s.cfg.UUID,
		ServiceAddress:              s.txnServiceServiceAddr(),
		Shards:                      s.getTNShardInfo(),
		TaskServiceCreated:          s.taskServiceCreated(),
		LogtailServerAddress:        s.logtailServiceServiceAddr(),
		LockServiceAddress:          s.lockServiceServiceAddr(),
		ShardServiceAddress:         s.shardServiceServiceAddr(),
		ConfigData:                  s.config.GetData(),
		AutoIncrEpochFenceSupported: true,
		AckedCommandBatchID:         s.ackedCommandBatchID.Load(),
		CommandDeliveryAckSupported: true,
		// if the replayed LSN is 0, then it is the master TN.
		ReplayedLsn: 0,
	}

	if s.queryService != nil {
		hb.QueryAddress = s.queryServiceServiceAddr()
	}

	s.heartbeatInFlight.Store(true)
	cb, err := func() (logservicepb.CommandBatch, error) {
		defer s.heartbeatInFlight.Store(false)
		return s.hakeeperClient.SendTNHeartbeat(ctx2, hb)
	}()
	if err != nil {
		s.commandPollNeeded.Store(true)
		err = moerr.AttachCause(ctx2, err)
		v2.TNHeartbeatFailureCounter.Inc()
		s.rt.Logger().Error("failed to send tn heartbeat", zap.Error(err))
		return
	}
	if ctx2.Err() != nil {
		s.commandPollNeeded.Store(true)
		return
	}
	s.commandPollNeeded.Store(false)

	s.config.DecrCount()
	s.handleCommandBatch(cb)
}

func (s *store) handleCommands(cmds []logservicepb.ScheduleCommand) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	s.handleCommandsLocked(cmds)
}

func (s *store) handleCommandBatch(batch logservicepb.CommandBatch) {
	if len(batch.Commands) == 0 {
		return
	}
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	if batch.BatchID == 0 {
		fingerprint := logservice.ScheduleCommandBatchFingerprint(batch)
		if s.legacyDedupeArmed && fingerprint == s.lastCommandHash {
			s.legacyDedupeArmed = false
			s.handleRetryableCommandsLocked(batch.Commands)
			return
		}
		s.legacyDedupeArmed = false
	} else {
		if batch.BatchID < s.lastCommandBatchID {
			return
		}
		if batch.BatchID == s.lastCommandBatchID {
			s.handleRetryableCommandsLocked(batch.Commands)
			return
		}
	}
	s.handleCommandsLocked(batch.Commands)
	if batch.BatchID != 0 {
		s.lastCommandBatchID = batch.BatchID
		s.ackedCommandBatchID.Store(batch.BatchID)
		s.lastCommandHash = logservice.ScheduleCommandBatchFingerprint(batch)
		s.legacyDedupeArmed = true
	}
}

func (s *store) handleRetryableCommandsLocked(cmds []logservicepb.ScheduleCommand) {
	for i := range cmds {
		if logservice.IsRetryableScheduleCommand(cmds[i]) {
			s.handleCommandsLocked(cmds[i : i+1])
		}
	}
}

func (s *store) handleCommandsLocked(cmds []logservicepb.ScheduleCommand) {
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
	if err := s.StartTNReplica(metadata.TNShard{
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.removeReplicaLocked(shardID); err != nil {
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
