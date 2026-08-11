// Copyright 2026 Matrix Origin
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
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

const (
	walRecoveryStatusConfigKey          = "logservice.internal.wal-recovery-status"
	walRecoveryStatusPending            = "pending"
	walRecoveryStatusCoordinatorPending = "coordinator-pending"
	walRecoveryStatusComplete           = "complete"
	walRecoveryCoordinatorWait          = 5 * time.Minute
)

func (s *Service) setWALRecoveryInProgress(pending bool) {
	if !s.walRecovery.configured {
		return
	}
	s.walRecovery.pending.Store(pending)
}

func (s *Service) addWALRecoveryStatus(hb *pb.LogStoreHeartbeat) {
	if !s.walRecovery.configured || hb == nil {
		return
	}
	status := walRecoveryStatusComplete
	if s.walRecovery.pending.Load() {
		status = walRecoveryStatusPending
		if s.walRecovery.coordinator {
			status = walRecoveryStatusCoordinatorPending
		}
	}
	content := make(map[string]*pb.ConfigItem)
	if hb.ConfigData != nil {
		for key, item := range hb.ConfigData.Content {
			content[key] = item
		}
	}
	content[walRecoveryStatusConfigKey] = &pb.ConfigItem{
		Name:         walRecoveryStatusConfigKey,
		CurrentValue: status,
		DefaultValue: walRecoveryStatusComplete,
		Internal:     "internal",
	}
	hb.ConfigData = &pb.ConfigData{Content: content}
}

func walRecoveryPending(state *pb.CheckerState) bool {
	if state == nil {
		return false
	}
	// Replicated completion is authoritative. A coordinator can crash after
	// committing it but before its next heartbeat clears the local status.
	if state.LogServiceRecoveryCompleted {
		return false
	}
	if state.LogServiceRecoveryPending {
		return true
	}
	for _, store := range state.LogState.Stores {
		if store.ConfigData == nil {
			continue
		}
		item := store.ConfigData.Content[walRecoveryStatusConfigKey]
		if item != nil && (item.CurrentValue == walRecoveryStatusPending ||
			item.CurrentValue == walRecoveryStatusCoordinatorPending) {
			return true
		}
	}
	return false
}

// walRecoveryCoordinator returns the lexicographically first recovering store
// after every voting replica of the new Log shard has reported to HAKeeper.
// Waiting for the complete membership prevents two recovering processes from
// observing different partial store sets and both starting WAL replay.
func walRecoveryCoordinator(state *pb.CheckerState, expectedReplicas uint64) (string, bool) {
	if state == nil || expectedReplicas == 0 {
		return "", false
	}
	shard, ok := state.LogState.Shards[firstLogShardID]
	if !ok || uint64(len(shard.Replicas)) < expectedReplicas {
		return "", false
	}

	pending := make([]string, 0, len(shard.Replicas))
	for _, storeID := range shard.Replicas {
		store, ok := state.LogState.Stores[storeID]
		if !ok {
			return "", false
		}
		if store.ConfigData == nil {
			return "", false
		}
		item := store.ConfigData.Content[walRecoveryStatusConfigKey]
		if item == nil {
			return "", false
		}
		switch item.CurrentValue {
		case walRecoveryStatusPending, walRecoveryStatusComplete:
		case walRecoveryStatusCoordinatorPending:
			pending = append(pending, storeID)
		default:
			return "", false
		}
	}
	if len(pending) == 0 {
		return "", false
	}
	sort.Strings(pending)
	return pending[0], true
}

func (s *Service) waitWALRecoveryCoordinator(ctx context.Context, expectedReplicas uint64) (bool, error) {
	logger := s.runtime.SubLogger(runtime.SystemInit)
	timer := time.NewTimer(walRecoveryCoordinatorWait)
	defer timer.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		state, err := s.store.getCheckerState()
		if err != nil {
			lastErr = err
		} else if coordinator, ready := walRecoveryCoordinator(state, expectedReplicas); ready {
			if coordinator == s.cfg.UUID {
				logger.Info("WAL recovery: selected as recovery coordinator",
					zap.String("coordinator", coordinator))
				return true, nil
			}
			logger.Info("WAL recovery: another LogService is the recovery coordinator",
				zap.String("coordinator", coordinator))
			return false, nil
		}

		select {
		case <-ctx.Done():
			return false, moerr.AttachCause(ctx, ctx.Err())
		case <-timer.C:
			if lastErr != nil {
				return false, moerr.NewInternalErrorf(ctx,
					"timeout waiting for WAL recovery coordinator: last HAKeeper error: %v", lastErr)
			}
			return false, moerr.NewInternalError(ctx,
				"timeout waiting for all LogService replicas to elect a WAL recovery coordinator")
		case <-ticker.C:
		}
	}
}
