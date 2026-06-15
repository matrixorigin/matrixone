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
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/lni/dragonboat/v4"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const logShardRepairReasonPrefix = "__mo_log_shard_repair__:"

type logShardRepairReason struct {
	Reason                 string              `json:"reason,omitempty"`
	CleanupReplicasByStore map[string][]uint64 `json:"cleanupReplicasByStore,omitempty"`
}

func decodeLogShardRepairReason(reason string) logShardRepairReason {
	if !strings.HasPrefix(reason, logShardRepairReasonPrefix) {
		return logShardRepairReason{Reason: reason}
	}
	var ret logShardRepairReason
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reason, logShardRepairReasonPrefix)), &ret); err != nil {
		return logShardRepairReason{Reason: reason}
	}
	return ret
}

func (l *store) cleanRequestedReplicasFromRepairState(ctx context.Context, shards []metadata.LogShard) error {
	if !hasLocalLogShard(shards) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	client, err := NewLogHAKeeperClient(ctx, l.cfg.UUID, l.cfg.GetHAKeeperClientConfig())
	if err != nil {
		l.runtime.Logger().Warn("skip repair cleanup: failed to create HAKeeper client",
			zap.Error(err))
		return nil
	}
	defer func() {
		if err := client.Close(); err != nil {
			l.runtime.Logger().Warn("failed to close HAKeeper client for repair cleanup",
				zap.Error(err))
		}
	}()

	state, err := client.GetClusterState(ctx)
	if err != nil {
		l.runtime.Logger().Warn("skip repair cleanup: failed to read HAKeeper state",
			zap.Error(err))
		return nil
	}
	for shardID, repair := range state.LogShardRepairs {
		reason := decodeLogShardRepairReason(repair.Reason)
		replicas := reason.CleanupReplicasByStore[l.cfg.UUID]
		if len(replicas) == 0 {
			continue
		}
		for _, replicaID := range replicas {
			if shardID == hakeeper.DefaultHAKeeperShardID {
				return moerr.NewInvalidInputNoCtxf(
					"repair cleanup must not clean HAKeeper shard %d replica %d",
					shardID,
					replicaID,
				)
			}
			l.runtime.Logger().Warn("clean local log replica requested by HAKeeper repair state",
				zap.Uint64("shardID", shardID),
				zap.Uint64("replicaID", replicaID),
				zap.String("uuid", l.cfg.UUID))
			if err := l.snapshotMgr.RemoveReplica(shardID, replicaID); err != nil {
				return err
			}
			if err := l.nh.RemoveData(shardID, replicaID); err != nil &&
				!errors.Is(err, dragonboat.ErrShardNotFound) {
				return err
			}
			l.removeMetadata(shardID, replicaID)
			if l.onReplicaChanged != nil {
				l.onReplicaChanged(shardID, replicaID, DelReplica)
			}
		}
	}
	return nil
}

func hasLocalLogShard(shards []metadata.LogShard) bool {
	for _, shard := range shards {
		if shard.ShardID != hakeeper.DefaultHAKeeperShardID {
			return true
		}
	}
	return false
}
