// Copyright 2021 Matrix Origin
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

package metadata

import (
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
	"matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"sync"
)

type Store = logstore.Store

func defaultHandler(r io.Reader, entry LogEntry) (LogEntry, int64, error) {
	n, err := entry.ReadFrom(r)
	if err != nil {
		return nil, int64(n), err
	}
	return entry, int64(n), nil
}

type replayEntry struct {
	typ          LogEntryType
	blkEntry     *blockLogEntry
	tbl          *Table
	tblEntry     *tableLogEntry
	segEntry     *segmentLogEntry
	catalogEntry *catalogLogEntry
}

type replayCache struct {
	replayer   *catalogReplayer
	entries    []*replayEntry
	checkpoint *catalogLogEntry
	safeIds    map[uint64]uint64
}

func newReplayCache(replayer *catalogReplayer) *replayCache {
	return &replayCache{
		replayer: replayer,
		entries:  make([]*replayEntry, 0),
		safeIds:  make(map[uint64]uint64),
	}
}

func (cache *replayCache) OnShardSafeId(id shard.SafeId) {
	cache.safeIds[id.ShardId] = id.Id
}

func (cache *replayCache) Append(entry *replayEntry) {
	cache.entries = append(cache.entries, entry)
	if entry.typ == logstore.ETCheckpoint {
		cache.checkpoint = entry.catalogEntry
	}
}

func (cache *replayCache) onApply(entry *replayEntry, catalog *Catalog, r *common.Range) error {
	switch entry.typ {
	case ETCreateBlock:
		if r != nil {
			if !r.LT(entry.blkEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.blkEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.blkEntry.CommitInfo.CommitId)
		catalog.Sequence.TryUpdateBlockId(entry.blkEntry.Id)
		catalog.onReplayCreateBlock(entry.blkEntry)
	case ETUpgradeBlock:
		if r != nil {
			if !r.LT(entry.blkEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.blkEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.blkEntry.CommitInfo.CommitId)
		catalog.onReplayUpgradeBlock(entry.blkEntry)
	case ETCreateTable:
		if r != nil {
			if !r.LT(entry.tbl.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.tbl.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.tbl.CommitInfo.CommitId)
		catalog.Sequence.TryUpdateTableId(entry.tbl.Id)
		catalog.onReplayCreateTable(entry.tbl)
	case ETSoftDeleteTable:
		if r != nil {
			if !r.LT(entry.tblEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.tblEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.tblEntry.CommitInfo.CommitId)
		catalog.onReplaySoftDeleteTable(entry.tblEntry)
	case ETHardDeleteTable:
		if r != nil {
			if !r.LT(entry.tblEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.tblEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.tblEntry.CommitInfo.CommitId)
		catalog.onReplayHardDeleteTable(entry.tblEntry)
	case ETCreateSegment:
		if r != nil {
			if !r.LT(entry.segEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.segEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.segEntry.CommitInfo.CommitId)
		catalog.Sequence.TryUpdateSegmentId(entry.segEntry.Id)
		catalog.onReplayCreateSegment(entry.segEntry)
	case ETUpgradeSegment:
		if r != nil {
			if !r.LT(entry.segEntry.CommitInfo.CommitId) {
				// logutil.Infof("%s - %d Skipped", r.String(), entry.segEntry.CommitInfo.CommitId)
				return nil
			}
		}
		catalog.Sequence.TryUpdateCommitId(entry.segEntry.CommitInfo.CommitId)
		catalog.onReplayUpgradeSegment(entry.segEntry)
	case logstore.ETCheckpoint:
	default:
		panic(fmt.Sprintf("unkown entry type: %d", entry.typ))
	}
	return nil
}

func (cache *replayCache) applyNoCheckpoint() error {
	for _, entry := range cache.entries {
		if err := cache.onApply(entry, cache.replayer.catalog, nil); err != nil {
			return err
		}
	}
	return nil
}

func (cache *replayCache) Apply() error {
	if cache.checkpoint == nil {
		if err := cache.applyNoCheckpoint(); err != nil {
			return err
		}
	} else {
		if err := cache.replayer.catalog.rebuild(cache.checkpoint.Catalog.TableSet, &cache.checkpoint.Range); err != nil {
			return err
		}
		for _, entry := range cache.entries {
			if err := cache.onApply(entry, cache.replayer.catalog, &cache.checkpoint.Range); err != nil {
				return err
			}
		}
		cache.replayer.catalog.Store.SetCheckpointId(cache.checkpoint.Range.Right)
	}
	cache.replayer.catalog.Store.SetSyncedId(cache.replayer.catalog.Sequence.nextCommitId)
	return nil
}

type catalogReplayer struct {
	catalog  *Catalog
	replayed int
	offset   int64
	cache    *replayCache
}

func newCatalogReplayer() *catalogReplayer {
	replayer := &catalogReplayer{}
	replayer.cache = newReplayCache(replayer)
	return replayer
}

func (replayer *catalogReplayer) RebuildCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg,
	store logstore.AwareStore, indexWal wal.ShardWal) (*Catalog, error) {
	replayer.catalog = NewCatalogWithDriver(mu, cfg, store, indexWal)
	if err := replayer.Replay(replayer.catalog.Store); err != nil {
		return nil, err
	}
	replayer.catalog.Store.TryCompact()
	replayer.cache = nil
	return replayer.catalog, nil
}

func (replayer *catalogReplayer) RebuildCatalog(mu *sync.RWMutex, cfg *CatalogCfg) (*Catalog, error) {
	replayer.catalog = NewCatalog(mu, cfg)
	if err := replayer.Replay(replayer.catalog.Store); err != nil {
		return nil, err
	}
	replayer.catalog.Store.TryCompact()
	replayer.cache = nil
	return replayer.catalog, nil
}

func (replayer *catalogReplayer) doReplay(r *logstore.VersionFile, observer logstore.ReplayObserver) error {
	entry := logstore.NewAsyncBaseEntry()
	defer entry.Free()
	meta := entry.GetMeta()
	_, err := meta.ReadFrom(r)
	if err != nil {
		return err
	}
	if entry, n, err := defaultHandler(r, entry); err != nil {
		return err
	} else {
		if n != int64(meta.PayloadSize()) {
			return errors.New(fmt.Sprintf("payload mismatch: %d != %d", n, meta.PayloadSize()))
		}
		if err = replayer.onReplayEntry(entry, observer); err != nil {
			return err
		}
	}
	replayer.offset += int64(meta.Size())
	replayer.offset += int64(meta.PayloadSize())
	replayer.replayed++
	return nil
}

func (replayer *catalogReplayer) Replay(s Store) error {
	err := s.ReplayVersions(replayer.doReplay)
	logutil.Infof("Total %d entries replayed", replayer.replayed)
	replayer.cache.Apply()
	return err
}

func (replayer *catalogReplayer) GetOffset() int64 {
	return replayer.offset
}

func (replayer *catalogReplayer) Truncate(s Store) error {
	return s.Truncate(replayer.offset)
}

func (replayer *catalogReplayer) TotalEntries() int {
	return replayer.replayed
}

func (replayer *catalogReplayer) String() string {
	s := fmt.Sprintf("<CatalogReplayer>(Entries:%d,Offset:%d)", replayer.replayed, replayer.offset)
	return s
}

func (replayer *catalogReplayer) RegisterEntryHandler(_ LogEntryType, _ logstore.EntryHandler) error {
	return nil
}

func (replayer *catalogReplayer) onReplayEntry(entry LogEntry, observer logstore.ReplayObserver) error {
	switch entry.GetMeta().GetType() {
	case shard.ETShardWalSafeId:
		safeId, _ := shard.EntryToSafeId(entry)
		logutil.Infof("Replay SafeId: %v", safeId)
		replayer.cache.OnShardSafeId(safeId)
	case ETCreateBlock:
		blk := &blockLogEntry{}
		blk.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		blk.CommitLocked(commitId)
		observer.OnReplayCommit(blk.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateBlock,
			blkEntry: blk,
		})
	case ETUpgradeBlock:
		blk := &blockLogEntry{}
		blk.Unmarshal(entry.GetPayload())
		observer.OnReplayCommit(blk.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETUpgradeBlock,
			blkEntry: blk,
		})
	case ETCreateTable:
		tbl := &Table{}
		tbl.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		tbl.CommitLocked(commitId)
		observer.OnReplayCommit(tbl.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ: ETCreateTable,
			tbl: tbl,
		})
	case ETSoftDeleteTable:
		tbl := &tableLogEntry{}
		tbl.Unmarshal(entry.GetPayload())
		observer.OnReplayCommit(tbl.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETSoftDeleteTable,
			tblEntry: tbl,
		})
	case ETHardDeleteTable:
		tbl := &tableLogEntry{}
		tbl.Unmarshal(entry.GetPayload())
		observer.OnReplayCommit(tbl.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETHardDeleteTable,
			tblEntry: tbl,
		})
	case ETCreateSegment:
		seg := &segmentLogEntry{}
		seg.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		seg.CommitLocked(commitId)
		observer.OnReplayCommit(seg.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateSegment,
			segEntry: seg,
		})
	case ETUpgradeSegment:
		seg := &segmentLogEntry{}
		seg.Unmarshal(entry.GetPayload())
		observer.OnReplayCommit(seg.CommitInfo.CommitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETUpgradeSegment,
			segEntry: seg,
		})
	case logstore.ETCheckpoint:
		c := &catalogLogEntry{}
		c.Unmarshal(entry.GetPayload())
		observer.OnReplayCheckpoint(c.Range)
		replayer.cache.Append(&replayEntry{
			typ:          logstore.ETCheckpoint,
			catalogEntry: c,
		})
	case logstore.ETFlush:
	default:
		panic(fmt.Sprintf("unkown entry type: %d", entry.GetMeta().GetType()))
	}
	return nil
}
