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
		s.handlePolledCommandBatch(ctx, batch)
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

	hb := s.getHeartbeatMessage()

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
	s.handleHeartbeatResponse(hb.AckedCommandBatchID, cb)
	if s.shutdownBatchID.Load() != 0 {
		s.acknowledgeShutdown(ctx)
	}
}

func (s *store) getHeartbeatMessage() logservicepb.TNStoreHeartbeat {
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
	return hb
}

func (s *store) handleCommands(cmds []logservicepb.ScheduleCommand) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	s.handleCommandsLocked(cmds)
}

func (s *store) handleCommandBatch(batch logservicepb.CommandBatch) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	s.handleCommandBatchLocked(batch)
}

func (s *store) handleCommandBatchLocked(batch logservicepb.CommandBatch) {
	if len(batch.Commands) == 0 {
		return
	}
	if batch.BatchID == 0 {
		s.appliedCommands = nil
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
	commands, applied := logservice.FilterUnappliedScheduleCommands(
		batch.Commands,
		s.appliedCommands,
	)
	shutdown := batch.BatchID != 0 && hasShutdownCommand(batch.Commands)
	if shutdown {
		// Shutdown is the one command whose side effect prevents a later
		// heartbeat from carrying its acknowledgement. Apply the rest of the
		// batch now, but defer process termination until HAKeeper has committed
		// the exact batch acknowledgement.
		s.handleNonShutdownCommandsLocked(commands)
	} else {
		s.handleCommandsLocked(commands)
	}
	s.appliedCommands = applied
	if batch.BatchID != 0 {
		s.lastCommandBatchID = batch.BatchID
		s.ackedCommandBatchID.Store(batch.BatchID)
		s.lastCommandHash = logservice.ScheduleCommandBatchFingerprint(batch)
		s.legacyDedupeArmed = true
	}
	if shutdown {
		s.shutdownBatchID.Store(batch.BatchID)
	}
}

func hasShutdownCommand(cmds []logservicepb.ScheduleCommand) bool {
	for i := range cmds {
		if cmds[i].GetShutdownStore() != nil {
			return true
		}
	}
	return false
}

func (s *store) handleNonShutdownCommandsLocked(cmds []logservicepb.ScheduleCommand) {
	for i := range cmds {
		if cmds[i].GetShutdownStore() != nil {
			if cmds[i].ServiceType != logservicepb.TNService {
				s.rt.Logger().Fatal("received invalid command", zap.String("command", cmds[i].LogString()))
			}
			s.rt.Logger().Debug(
				"deferring shutdown schedule command until acknowledgement",
				zap.String("command", cmds[i].LogString()),
			)
			continue
		}
		s.handleCommandsLocked(cmds[i : i+1])
	}
}

func (s *store) handlePolledCommandBatch(ctx context.Context, batch logservicepb.CommandBatch) {
	s.handleCommandBatch(batch)
	if s.shutdownBatchID.Load() != 0 {
		s.acknowledgeShutdown(ctx)
	}
}

// handleHeartbeatResponse records commands before using the response as proof
// that an acknowledgement was committed. A stale acknowledgement cannot
// complete a newer shutdown generation, and a response that still contains a
// shutdown command explicitly proves that HAKeeper retained it.
func (s *store) handleHeartbeatResponse(
	sentAck uint64,
	batch logservicepb.CommandBatch,
) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	if sentAck != 0 && sentAck == s.lastCommandBatchID &&
		(batch.BatchID == 0 || batch.BatchID == sentAck) {
		s.appliedCommands = nil
	}
	s.handleCommandBatchLocked(batch)
	for {
		pending := s.shutdownBatchID.Load()
		if pending == 0 || sentAck < pending || hasShutdownCommand(batch.Commands) {
			return
		}
		if s.shutdownBatchID.CompareAndSwap(pending, 0) {
			s.handleShutdownStore(logservicepb.ScheduleCommand{})
			return
		}
	}
}

// acknowledgeShutdown uses the existing heartbeat protocol to close the
// terminal command's delivery loop before terminating the process. It is only
// called for ShutdownStore, so ordinary heartbeat and command paths pay no
// extra RPC cost. Poll and heartbeat delivery can race; serialize this rare
// path so only one acknowledgement attempt is active.
func (s *store) acknowledgeShutdown(ctx context.Context) {
	s.shutdownAckMu.Lock()
	defer s.shutdownAckMu.Unlock()

	ctx2, cancel := context.WithTimeoutCause(
		ctx,
		s.cfg.HAKeeper.HeatbeatTimeout.Duration,
		moerr.CauseTnServiceHeartbeat,
	)
	defer cancel()
	for s.shutdownBatchID.Load() != 0 {
		hb := s.getHeartbeatMessage()
		batch, err := s.hakeeperClient.SendTNHeartbeat(ctx2, hb)
		if err != nil {
			s.commandPollNeeded.Store(true)
			if ctx.Err() == nil {
				err = moerr.AttachCause(ctx2, err)
				v2.TNHeartbeatFailureCounter.Inc()
				s.rt.Logger().Error(
					"failed to acknowledge tn shutdown command",
					zap.Uint64("batch-id", hb.AckedCommandBatchID),
					zap.Error(err),
				)
			}
			return
		}
		if ctx2.Err() != nil {
			s.commandPollNeeded.Store(true)
			return
		}
		s.handleHeartbeatResponse(hb.AckedCommandBatchID, batch)
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
