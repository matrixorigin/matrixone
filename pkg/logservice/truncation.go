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
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
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

// setSnapshotIndex overwrites the recorded snapshotIndex unconditionally.
// Unlike forwardSnapshot, this accessor can move the value *down*, which
// is needed after the truncation loop's escape valves delete exported
// snapshots from snapshotMgr. Leaving snapshotIndex pointing at a deleted
// item's index would cause shouldDoExport to suppress a re-export on a
// quiescent shard and wedge the loop. See issue #24315.
func (s shardSnapshotInfo) setSnapshotIndex(shardID uint64, index uint64) {
	if _, ok := s[shardID]; !ok {
		s.init(shardID)
	}
	s[shardID].snapshotIndex = index
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
	activeReplicas := l.startedReplicaIDs()
	processed := make(map[uint64]struct{})
	for _, shard := range l.getShards() {
		if shard.ShardID == hakeeper.DefaultHAKeeperShardID {
			continue
		}
		replicas := activeReplicas[shard.ShardID]
		if _, ok := replicas[shard.ReplicaID]; !ok {
			if len(replicas) > 0 {
				l.cleanupStaleReplica(shard.ShardID, shard.ReplicaID,
					l.newestActiveSnapshotIndex(shard.ShardID, replicas))
			}
			continue
		}
		if _, ok := processed[shard.ShardID]; ok {
			continue
		}
		processed[shard.ShardID] = struct{}{}
		if err := l.processShardTruncateLogWithReplica(ctx, shard.ShardID, shard.ReplicaID); err != nil {
			l.runtime.Logger().Error("process truncate log failed", zap.Error(err))
		}
	}
	return nil
}

func (l *store) startedReplicaIDs() map[uint64]map[uint64]struct{} {
	opts := dragonboat.NodeHostInfoOption{SkipLogInfo: true}
	nhi := l.nh.GetNodeHostInfo(opts)
	replicas := make(map[uint64]map[uint64]struct{})
	for _, ci := range nhi.ShardInfoList {
		if ci.Pending {
			continue
		}
		if _, ok := replicas[ci.ShardID]; !ok {
			replicas[ci.ShardID] = make(map[uint64]struct{})
		}
		replicas[ci.ShardID][ci.ReplicaID] = struct{}{}
	}
	return replicas
}

func (l *store) newestActiveSnapshotIndex(
	shardID uint64, replicas map[uint64]struct{},
) uint64 {
	var index uint64
	for replicaID := range replicas {
		if newest := l.snapshotMgr.NewestIndex(shardID, replicaID); newest > index {
			index = newest
		}
	}
	return index
}

func (l *store) cleanupStaleReplica(
	shardID uint64, replicaID uint64, activeSnapshotIndex uint64,
) {
	if err := l.snapshotMgr.RemoveReplica(shardID, replicaID); err != nil {
		l.runtime.Logger().Error("remove stale exported snapshots failed",
			zap.Uint64("shardID", shardID),
			zap.Uint64("replicaID", replicaID),
			zap.Error(err),
		)
		return
	}
	l.shardSnapshotInfo.setSnapshotIndex(shardID, activeSnapshotIndex)
	l.runtime.Logger().Info("remove stale log replica metadata",
		zap.Uint64("shardID", shardID),
		zap.Uint64("replicaID", replicaID),
		zap.Uint64("activeSnapshotIndex", activeSnapshotIndex),
	)
	l.removeMetadata(shardID, replicaID)
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
	l.runtime.Logger().Info("export snapshot success",
		zap.Uint64("shard", shardID),
		zap.Uint64("replica", replicaID),
		zap.Uint64("index", idx),
	)
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
	l.runtime.Logger().Info("import snapshot success",
		zap.Uint64("shard", shardID),
		zap.Uint64("replica", replicaID),
		zap.Uint64("index", lsn),
		zap.String("dir", dir),
	)
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
func (l *store) shouldDoImport(
	ctx context.Context, shardID uint64, lsnInSM uint64, lsn uint64, dir string,
) bool {
	if len(dir) == 0 {
		// Normal path: no exported snapshot with index <= lsnInSM yet.
		// Happens every tick until TN advances TruncatedLsn past the
		// oldest exported snapshot. DEBUG to avoid log spam.
		l.runtime.Logger().Debug("skip import snapshot: no eligible exported snapshot",
			zap.Uint64("shardID", shardID),
			zap.Uint64("lsnInSM", lsnInSM),
		)
		return false
	}
	ctx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseProcessTruncationReadState)
	defer cancel()
	v, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.StateQuery{})
	if err != nil {
		l.runtime.Logger().Error("read state from HAKeeper failed",
			zap.Uint64("shardID", shardID),
			zap.Uint64("lsnInSM", lsnInSM),
			zap.Uint64("evalImportLsn", lsn),
			zap.Error(err),
		)
		return false
	}
	tnStore := v.(*pb.CheckerState).TNState
	// the replayed LSN of all TN instances should greater than the request LSN.
	// otherwise, return false.
	// if the replayed LSN is 0, do not check it, because it is the master TN.
	for id, ss := range tnStore.Stores {
		if ss.ReplayedLsn > 0 && ss.ReplayedLsn < lsn {
			l.runtime.Logger().Warn("cannot import snapshot, as the replayed "+
				"LSN is lower than the requested LSN",
				zap.Uint64("shardID", shardID),
				zap.Uint64("lsnInSM", lsnInSM),
				zap.String("tn uuid", id),
				zap.Uint64("replayed LSN", ss.ReplayedLsn),
				zap.Uint64("requested LSN", lsn),
			)
			return false
		}
	}
	if lsn <= l.shardSnapshotInfo.getTruncatedLsn(shardID) {
		// Normal path: EvalImportSnapshot picked the same snapshot that
		// was already imported on a previous tick. DEBUG to avoid log
		// spam while waiting for TN to advance.
		l.runtime.Logger().Debug("skip import snapshot: lsn not greater than last truncated",
			zap.Uint64("shardID", shardID),
			zap.Uint64("lsnInSM", lsnInSM),
			zap.Uint64("evalImportLsn", lsn),
			zap.Uint64("lastTruncatedLsn", l.shardSnapshotInfo.getTruncatedLsn(shardID)),
		)
		return false
	}
	return true
}

// shouldDoExport checks whether current applied index is greater than
// snapshot index, which is last applied index. Means that, if applied
// index does not advance, do not do export.
func (l *store) shouldDoExport(ctx context.Context, shardID uint64, replicaID uint64) bool {
	if l.snapshotMgr.Count(shardID, replicaID) >= l.cfg.MaxExportedSnapshot {
		// Normal path: the gap between two successful imports naturally
		// fills the quota. DEBUG to avoid log spam; processShardTruncateLog
		// already emits an INFO when the escape valve actually drains the
		// quota, which is the interesting event.
		l.runtime.Logger().Debug("skip export snapshot: exported snapshot quota is full",
			zap.Uint64("shardID", shardID),
			zap.Uint64("replicaID", replicaID),
			zap.Int("count", l.snapshotMgr.Count(shardID, replicaID)),
			zap.Int("max", l.cfg.MaxExportedSnapshot),
		)
		return false
	}
	v, err := l.read(ctx, shardID, indexQuery{})
	if err != nil {
		l.runtime.Logger().Warn("skip export snapshot: failed to read applied index",
			zap.Uint64("shardID", shardID),
			zap.Uint64("replicaID", replicaID),
			zap.Error(err),
		)
		return false
	}
	if v.(uint64) <= l.shardSnapshotInfo.getSnapshotIndex(shardID) {
		// applied index has not advanced since last export; no new content to export.
		return false
	}
	return true
}

// dropNewestOnTimeout is the escape valve used when getTruncatedLsn keeps
// timing out and the exported-snapshot quota is already full. Without a
// valid lsnInSM a regular purge cannot run, which would leave the loop
// wedged indefinitely (no quota -> no export -> no import -> no WAL
// compaction, while the WAL grows at shard write rate).
//
// We drop the *newest* tracked item rather than the oldest. Dropping
// the oldest would preferentially remove exactly the item a future
// import would consume (once SyncRead recovers, EvalImportSnapshot
// returns the largest item.index <= lsnInSM, which tends to be among
// the older items). Dropping the newest preserves older, more likely
// importable items at the cost of wasting the most recently exported
// (and therefore most speculative) one.
//
// When the manager holds only a single item we refuse to drop: that
// item is the sole importable candidate, and losing it would trade a
// recoverable stall for a state where no import is possible until TN
// / TruncatedLsn catches up to whatever the next fresh export lands at.
// Under MaxExportedSnapshot=1 the escape valve is therefore a no-op
// and the WAL will grow until SyncRead recovers. See issue #24315.
func (l *store) dropNewestOnTimeout(shardID uint64, replicaID uint64) {
	before := l.snapshotMgr.Count(shardID, replicaID)
	if before <= 1 {
		// Preserve the sole importable candidate. See function comment.
		return
	}
	droppedIdx, err := l.snapshotMgr.DropNewest(shardID, replicaID)
	if err != nil {
		l.runtime.Logger().Error("drop newest exported snapshot failed",
			zap.Uint64("shardID", shardID),
			zap.Uint64("replicaID", replicaID),
			zap.Error(err))
		return
	}
	if droppedIdx == 0 {
		return
	}
	// Realign shardSnapshotInfo.snapshotIndex with whatever item is now
	// newest. Without this, shouldDoExport may suppress the next export
	// on a quiescent shard because applied index has not advanced past
	// the stale high-water mark pointing at the snapshot we just
	// deleted. See issue #24315.
	l.shardSnapshotInfo.setSnapshotIndex(shardID,
		l.snapshotMgr.NewestIndex(shardID, replicaID))
	l.runtime.Logger().Info("dropped newest exported snapshot to unblock truncation",
		zap.Uint64("shardID", shardID),
		zap.Uint64("replicaID", replicaID),
		zap.Uint64("droppedIndex", droppedIdx),
		zap.Int("before", before),
		zap.Int("after", l.snapshotMgr.Count(shardID, replicaID)),
	)
}

func (l *store) processShardTruncateLog(ctx context.Context, shardID uint64) error {
	replicaID := l.getReplicaID(shardID)
	if replicaID <= 0 {
		return nil
	}
	return l.processShardTruncateLogWithReplica(ctx, shardID, uint64(replicaID))
}

func (l *store) processShardTruncateLogWithReplica(ctx context.Context, shardID uint64, replicaID uint64) error {
	// Do NOT process before leader is OK.
	leaderID, err := l.leaderID(shardID)
	if err != nil {
		l.runtime.Logger().Warn("cannot get leader ID, skip truncate",
			zap.Uint64("shard ID", shardID))
		return nil
	}
	if leaderID == 0 {
		l.runtime.Logger().Warn("no leader yet, skip truncate",
			zap.Uint64("shard ID", shardID))
		return nil
	}

	ctx, cancel := context.WithTimeoutCause(ctx, 3*time.Second, moerr.CauseProcessShardTruncateLog)
	defer cancel()

	lsnInSM, err := l.getTruncatedLsn(ctx, shardID)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		if !errors.Is(err, dragonboat.ErrTimeout) && !errors.Is(err, dragonboat.ErrInvalidDeadline) {
			l.runtime.Logger().Error("get truncated lsn in state machine failed",
				zap.Uint64("shard ID", shardID), zap.Error(err))
			return err
		}
		// Timeout on SyncRead is the single largest silent failure mode of
		// this loop. Make it observable, and if the exported-snapshot quota
		// is already full, free one slot by dropping the oldest item so
		// the loop can make forward progress on subsequent ticks instead
		// of wedging permanently. See issue #24315.
		l.runtime.Logger().Warn("get truncated lsn timed out, tick skipped",
			zap.Uint64("shardID", shardID), zap.Error(err))
		if l.snapshotMgr.Count(shardID, replicaID) >= l.cfg.MaxExportedSnapshot {
			l.dropNewestOnTimeout(shardID, replicaID)
		}
		return nil
	}

	if !l.shouldProcess(shardID, lsnInSM) {
		return nil
	}

	dir, lsn := l.snapshotMgr.EvalImportSnapshot(shardID, replicaID, lsnInSM)
	if l.shouldDoImport(ctx, shardID, lsnInSM, lsn, dir) {
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

	// Note on non-timeout-path escape valves: an earlier iteration tried
	// to free quota slots here by calling snapshotMgr.Remove(..., lsnInSM)
	// whenever shouldDoImport declined. That was never safe: the <= lsnInSM
	// items are exactly the importable candidates, and deleting them
	// would leave a quiescent shard with no import possible until fresh
	// traffic arrived. A truncatedLsn-based variant is theoretically
	// safe but unreachable in practice -- importSnapshot itself already
	// removes every item <= the imported lsn as its last step, so there
	// is no obsolete residue for a post-hoc purge to clean up.
	//
	// The quota-full / no-import stall is therefore handled exclusively
	// by the timeout-path DropNewest escape valve above. See #24315.

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
	ctx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseProcessHAKeeperTruncation)
	defer cancel()
	v, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.IndexQuery{})
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}
	lsn := v.(uint64)
	if lsn > 1 {
		opts := dragonboat.SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionIndex:            lsn - 1,
		}
		if _, err := l.nh.SyncRequestSnapshot(ctx, hakeeper.DefaultHAKeeperShardID, opts); err != nil {
			err = moerr.AttachCause(ctx, err)
			l.runtime.Logger().Error("SyncRequestSnapshot failed", zap.Error(err))
			return err
		}
		l.runtime.Logger().Info("HAKeeper shard truncated", zap.Uint64("LSN", lsn))
	}
	return nil
}
