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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/version"
)

var cnCommandPollFailed = logutil.Event{
	Name:    "cn.schedule-command.poll.failed",
	Message: "failed to poll cn schedule commands",
}

func (s *service) startCNStoreHeartbeat() error {
	if s._hakeeperClient == nil {
		if _, err := s.getHAKeeperClient(); err != nil {
			return err
		}
	}
	return s.stopper.RunNamedTask("cnservice-control-plane", s.controlTask)
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

func (s *service) controlTask(ctx context.Context) {
	s.commandPollWakeup = make(chan struct{}, 1)
	commandDone := make(chan struct{})
	go func() {
		defer close(commandDone)
		s.commandTask(ctx)
	}()
	s.heartbeatTask(ctx)
	<-commandDone
}

func (s *service) commandTask(ctx context.Context) {
	client, ok := s._hakeeperClient.(logservice.ScheduleCommandHAKeeperClient)
	if !ok {
		return
	}
	poll := func() {
		// A healthy heartbeat remains the primary command-delivery RPC. Polling
		// is a bounded hedge while it is in flight, and remains enabled after a
		// failed/deadline heartbeat until one succeeds; the healthy steady state
		// therefore adds no network or Raft traffic.
		start := time.Now()
		defer func() {
			v2.CNCommandPollHistogram.Observe(time.Since(start).Seconds())
		}()
		ctx2, cancel := context.WithTimeout(ctx, logservice.ScheduleCommandPollTimeout)
		defer cancel()
		batch, err := client.GetScheduleCommands(ctx2, logservicepb.CNService)
		if err != nil {
			if ctx.Err() == nil {
				v2.CNCommandPollFailureCounter.Inc()
				cnCommandPollFailed.Error(
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

	active := func() bool {
		return s.heartbeatInFlight.Load() || s.commandPollNeeded.Load()
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time
	arm := func(delay time.Duration) {
		timer.Reset(delay)
		timerC = timer.C
	}
	disarm := func() {
		if timerC != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.commandPollWakeup:
			if !active() {
				disarm()
			} else if timerC == nil {
				arm(logservice.ScheduleCommandInitialPollDelay(s.cfg.UUID))
			}
		case <-timerC:
			timerC = nil
			if !active() {
				continue
			}
			pollStarted := time.Now()
			poll()
			if active() {
				// Pull delivery has no server-side wakeup. Keep a start-to-start
				// cadence so RPC latency cannot silently extend the discovery bound.
				delay := logservice.ScheduleCommandPollInterval - time.Since(pollStarted)
				if delay < 0 {
					delay = 0
				}
				arm(delay)
			}
		}
	}
}

func (s *service) notifyCommandPoll() {
	select {
	case s.commandPollWakeup <- struct{}{}:
	default:
	}
}

func (s *service) heartbeat(ctx context.Context) {
	start := time.Now()
	defer func() {
		v2.CNHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()

	ctx2, cancel := context.WithTimeoutCause(ctx, s.cfg.HAKeeper.HeatbeatTimeout.Duration, moerr.CauseHeartbeat)
	defer cancel()

	hb := logservicepb.CNStoreHeartbeat{
		UUID:                s.cfg.UUID,
		ServiceAddress:      s.pipelineServiceServiceAddr(),
		SQLAddress:          s.cfg.SQLAddress,
		LockServiceAddress:  s.lockServiceServiceAddr(),
		ShardServiceAddress: s.shardServiceServiceAddr(),
		Role:                s.metadata.Role,
		TaskServiceCreated:  s.GetTaskRunner() != nil,
		QueryAddress:        s.queryServiceServiceAddr(),
		InitWorkState:       s.cfg.InitWorkState,
		ConfigData:          s.config.GetData(),
		Resource: logservicepb.Resource{
			CPUTotal:     uint64(system.NumCPU()),
			CPUAvailable: system.CPUAvailable(),
			MemTotal:     system.MemoryTotal(),
			MemAvailable: system.MemoryAvailable(),
		},
		CommitID:                    version.CommitID,
		AckedCommandBatchID:         s.ackedCommandBatchID.Load(),
		CommandDeliveryAckSupported: true,
	}
	if s.gossipNode != nil {
		hb.GossipAddress = s.gossipServiceAddr()
		hb.GossipJoined = s.gossipNode.Joined()
	}

	s.heartbeatInFlight.Store(true)
	s.notifyCommandPoll()
	cb, err := s._hakeeperClient.SendCNHeartbeat(ctx2, hb)
	s.heartbeatInFlight.Store(false)
	if err != nil {
		s.commandPollNeeded.Store(true)
		s.notifyCommandPoll()
		err = moerr.AttachCause(ctx2, err)
		v2.CNHeartbeatFailureCounter.Inc()
		s.logger.Error("failed to send cn heartbeat", zap.Error(err))
		return
	}
	if ctx2.Err() != nil {
		s.commandPollNeeded.Store(true)
		s.notifyCommandPoll()
		return
	}
	s.commandPollNeeded.Store(false)
	s.notifyCommandPoll()

	select {
	case <-s.hakeeperConnected:
	default:
		s.initTaskServiceHolder()
		close(s.hakeeperConnected)
	}
	s.config.DecrCount()
	s.handleHeartbeatResponse(hb.AckedCommandBatchID, cb)
}

func (s *service) handleCommandBatch(batch logservicepb.CommandBatch) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	s.handleCommandBatchLocked(batch)
}

func (s *service) handleCommandBatchLocked(batch logservicepb.CommandBatch) {
	if len(batch.Commands) == 0 {
		return
	}
	if batch.BatchID == 0 {
		s.appliedCommandIDs = nil
		fingerprint := logservice.ScheduleCommandBatchFingerprint(batch)
		if s.legacyDedupeArmed && fingerprint == s.lastCommandHash {
			s.legacyDedupeArmed = false
			return
		}
		s.legacyDedupeArmed = false
		s.handleCommandsLocked(batch.Commands)
		return
	} else {
		if batch.BatchID <= s.lastCommandBatchID {
			return
		}
	}
	commands, applied, ok := logservice.FilterUnappliedScheduleCommands(
		batch,
		s.appliedCommandIDs,
	)
	if !ok {
		s.logger.Error("received acknowledged schedule-command batch without stable command IDs",
			zap.Uint64("batch-id", batch.BatchID))
		return
	}
	s.handleCommandsLocked(commands)
	s.appliedCommandIDs = applied
	if batch.BatchID != 0 {
		s.lastCommandBatchID = batch.BatchID
		s.ackedCommandBatchID.Store(batch.BatchID)
		s.lastCommandHash = logservice.ScheduleCommandBatchFingerprint(batch)
		s.legacyDedupeArmed = true
	}
}

func (s *service) handleHeartbeatResponse(
	sentAck uint64,
	batch logservicepb.CommandBatch,
) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	if sentAck != 0 && sentAck == s.lastCommandBatchID &&
		(batch.BatchID == 0 || batch.BatchID == sentAck) {
		s.appliedCommandIDs = nil
	}
	s.handleCommandBatchLocked(batch)
}

func (s *service) handleCommandsLocked(cmds []logservicepb.ScheduleCommand) {
	for _, cmd := range cmds {
		if cmd.ServiceType != logservicepb.CNService {
			s.logger.Fatal("received invalid command", zap.String("command", cmd.LogString()))
		}
		s.logger.Info("applying schedule command", zap.String("command", cmd.LogString()))
		if cmd.CreateTaskService != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
			s.createProxyUser(cmd.CreateTaskService)
		} else if s.gossipNode.Created() && !s.gossipNode.Joined() && cmd.JoinGossipCluster != nil {
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
