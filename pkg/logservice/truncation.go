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
	"errors"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"go.uber.org/zap"
)

type snapshotInfo struct {
	// truncatedLsn is LSN that has been take in snapshot manager.
	// Log entries before it have been removed.
	truncatedLsn uint64
	// snapshotIndex is the latest snapshot index.
	snapshotIndex uint64
}

type shardSnapshotInfo map[uint64]*snapshotInfo // shardID => snapshotInfo

func newShardSnapshotInfo() shardSnapshotInfo {
	return make(shardSnapshotInfo)
}

func (s shardSnapshotInfo) init(shardID uint64) {
	s[shardID] = &snapshotInfo{
		truncatedLsn:  0,
		snapshotIndex: 0,
	}
}

func (s shardSnapshotInfo) forwardTruncate(shardID uint64, lsn uint64) {
	if _, ok := s[shardID]; !ok {
		s.init(shardID)
	}
	if lsn > s[shardID].truncatedLsn {
		s[shardID].truncatedLsn = lsn
	}
}

func (s shardSnapshotInfo) forwardSnapshot(shardID uint64, index uint64) {
	if _, ok := s[shardID]; !ok {
		s.init(shardID)
	}
	if index > s[shardID].snapshotIndex {
		s[shardID].snapshotIndex = index
	}
}

func (s shardSnapshotInfo) getTruncatedLsn(shardID uint64) uint64 {
	if _, ok := s[shardID]; !ok {
		s.init(shardID)
		return 0
	}
	return s[shardID].truncatedLsn
}

func (s shardSnapshotInfo) getSnapshotIndex(shardID uint64) uint64 {
	if _, ok := s[shardID]; !ok {
		s.init(shardID)
		return 0
	}
	return s[shardID].snapshotIndex
}

func (l *store) truncationWorker(ctx context.Context) {
	defer func() {
		l.runtime.Logger().Info("truncation worker stopped")
	}()

	if l.cfg.TruncateInterval.Duration == 0 || l.cfg.HAKeeperTruncateInterval.Duration == 0 {
		panic("TruncateInterval is 0")
	}
	ticker := time.NewTicker(l.cfg.TruncateInterval.Duration)
	defer ticker.Stop()

	haTicker := time.NewTicker(l.cfg.HAKeeperTruncateInterval.Duration)
	defer haTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.processTruncateLog(ctx); err != nil {
				l.runtime.Logger().Error("truncate failed", zap.Error(err))
			}
		case <-haTicker.C:
			if err := l.processHAKeeperTruncation(ctx); err != nil {
				l.runtime.Logger().Error("HAKeeper truncate failed", zap.Error(err))
			}
		}
	}
}

// processTruncateLog process log truncation for all shards excpet
// hakeeper shard.
func (l *store) processTruncateLog(ctx context.Context) error {
	for _, shard := range l.getShards() {
		if shard.ShardID == hakeeper.DefaultHAKeeperShardID {
			continue
		}
		if err := l.processShardTruncateLog(ctx, shard.ShardID); err != nil {
			l.runtime.Logger().Error("process truncate log failed", zap.Error(err))
		}
	}
	return nil
}

// exportSnapshot just export the snapshot but do NOT truncate logs.
func (l *store) exportSnapshot(ctx context.Context, shardID uint64, replicaID uint64) error {
	opts := dragonboat.SnapshotOption{
		// set to true means just export snapshot, do NOT truncate logs.
		Exported:   true,
		ExportPath: l.snapshotMgr.exportPath(shardID, replicaID),
	}
	// Just take a snapshot and export it. This snapshot is invisible to system.
	idx, err := l.nh.SyncRequestSnapshot(ctx, shardID, opts)
	if err != nil {
		l.runtime.Logger().Error("request export snapshot failed", zap.Error(err))
		return err
	}
	// Add the exported snapshot to snapshot manager.
	if err := l.snapshotMgr.Add(shardID, replicaID, idx); err != nil {
		l.runtime.Logger().Error("add exported snapshot failed", zap.Error(err))
		return err
	}
	// forward the snapshot index.
	l.shardSnapshotInfo.forwardSnapshot(shardID, idx)
	return nil
}

func (l *store) importSnapshot(
	ctx context.Context, shardID uint64, replicaID uint64, lsn uint64, dir string,
) error {
	// Import a snapshot to override the snapshot in system.
	if err := l.nh.SyncRequestImportSnapshot(ctx, shardID, replicaID, dir); err != nil {
		l.runtime.Logger().Error("import snapshot failed", zap.Error(err))
		return err
	}
	// Then remove the exported snapshot in manager.
	if err := l.snapshotMgr.Remove(shardID, replicaID, lsn); err != nil {
		l.runtime.Logger().Error("remove exported snapshots failed")
		return err
	}
	// forward the truncate lsn.
	l.shardSnapshotInfo.forwardTruncate(shardID, lsn)
	return nil
}

// shouldProcess checks whether we should do the truncation for the shard.
// The param lsn comes from statemachine, that client sets.
// TODO(liubo): add more policies for truncation.
func (l *store) shouldProcess(shardID uint64, lsn uint64) bool {
	// the first 4 entries for a 3-replica raft group are tiny anyway
	return lsn > 1
}

// shouldDoImport checks whether compact logs and import snapshot.
// The param lsn is the biggest one from exported snapshots that are
// managed by snapshotManager.
// This check avoid doing the truncation at the same lsn.
func (l *store) shouldDoImport(shardID uint64, lsn uint64, dir string) bool {
	if len(dir) == 0 {
		return false
	}
	return lsn > l.shardSnapshotInfo.getTruncatedLsn(shardID)
}

// shouldDoExport checks whether current applied index is greater than
// snapshot index, which is last applied index. Means that, if applied
// index does not advance, do not do export.
func (l *store) shouldDoExport(ctx context.Context, shardID uint64, replicaID uint64) bool {
	if l.snapshotMgr.Count(shardID, replicaID) >= l.cfg.MaxExportedSnapshot {
		return false
	}
	v, err := l.read(ctx, shardID, indexQuery{})
	if err != nil {
		return false
	}
	return v.(uint64) > l.shardSnapshotInfo.getSnapshotIndex(shardID)
}

func (l *store) processShardTruncateLog(ctx context.Context, shardID uint64) error {
	// Do NOT process before leader is OK.
	leaderID, err := l.leaderID(shardID)
	if err != nil {
		l.runtime.Logger().Debug("cannot get leader ID, skip truncate",
			zap.Uint64("shard ID", shardID))
		return nil
	}
	if leaderID == 0 {
		l.runtime.Logger().Debug("no leader yet, skip truncate",
			zap.Uint64("shard ID", shardID))
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	lsnInSM, err := l.getTruncatedLsn(ctx, shardID)
	if err != nil {
		if !errors.Is(err, dragonboat.ErrTimeout) && !errors.Is(err, dragonboat.ErrInvalidDeadline) {
			l.runtime.Logger().Error("get truncated lsn in state machine failed",
				zap.Uint64("shard ID", shardID), zap.Error(err))
			return err
		}
		return nil
	}

	if !l.shouldProcess(shardID, lsnInSM) {
		return nil
	}

	replicaID := uint64(l.getReplicaID(shardID))
	dir, lsn := l.snapshotMgr.EvalImportSnapshot(shardID, replicaID, lsnInSM)
	if l.shouldDoImport(shardID, lsn, dir) {
		if err := l.importSnapshot(ctx, shardID, replicaID, lsn, dir); err != nil {
			l.runtime.Logger().Error("do truncate log failed",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Uint64("lsn", lsn),
				zap.String("dir", dir),
				zap.Error(err))
			return err
		}
		return nil
	}

	if l.shouldDoExport(ctx, shardID, replicaID) {
		if err := l.exportSnapshot(ctx, shardID, replicaID); err != nil {
			l.runtime.Logger().Error("export snapshot failed",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Uint64("lsn", lsn),
				zap.Error(err))
			return err
		}
	}
	return nil
}

// processHAKeeperTruncation processes the truncation for HAKeeper shard.
// It is different from the other shards. For HAKeeper shard, we just send
// snapshot request to dragonboat directly and nothing else needs to do.
func (l *store) processHAKeeperTruncation(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	v, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.IndexQuery{})
	if err != nil {
		return err
	}
	lsn := v.(uint64)
	if lsn > 1 {
		opts := dragonboat.SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionIndex:            lsn - 1,
		}
		if _, err := l.nh.SyncRequestSnapshot(ctx, hakeeper.DefaultHAKeeperShardID, opts); err != nil {
			l.runtime.Logger().Error("SyncRequestSnapshot failed", zap.Error(err))
			return err
		}
		l.runtime.Logger().Info("HAKeeper shard truncated", zap.Uint64("LSN", lsn))
	}
	return nil
}
