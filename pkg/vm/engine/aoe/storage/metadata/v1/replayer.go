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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
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
	commitId     uint64
	typ          LogEntryType
	db           *Database
	dbEntry      *databaseLogEntry
	tblEntry     *tableLogEntry
	segEntry     *segmentLogEntry
	catalogEntry *catalogLogEntry
	blkEntry     *blockLogEntry
	replaceEntry *dbReplaceLogEntry
	txnStore     *TxnStore
}

type replayCache struct {
	replayer          *catalogReplayer
	checkpointEntries []*replayEntry
	entries           []*replayEntry
	checkpoint        *catalogLogEntry
	safeIds           map[uint64]uint64
}

func newReplayCache(replayer *catalogReplayer) *replayCache {
	return &replayCache{
		replayer: replayer,
		entries:  make([]*replayEntry, 0),
		safeIds:  make(map[uint64]uint64),
	}
}

func (cache *replayCache) OnShardSafeId(id shard.SafeId) {
	old, ok := cache.safeIds[id.ShardId]
	if !ok || old < id.Id {
		cache.safeIds[id.ShardId] = id.Id
	}
}

func (cache *replayCache) Append(entry *replayEntry) {
	if entry.typ == logstore.ETCheckpoint {
		cache.checkpoint = entry.catalogEntry
		cache.checkpointEntries = append(cache.checkpointEntries, entry)
		return
	}
	cache.entries = append(cache.entries, entry)
}

func (cache *replayCache) onReplayTxnEntry(entry LogEntry) error {
	logType := entry.GetMeta().GetType()
	switch logType {
	case ETCreateDatabase:
		db := new(Database)
		db.Unmarshal(entry.GetPayload())
		cache.replayer.catalog.Sequence.TryUpdateTableId(db.Id)
		cache.replayer.catalog.onReplayCreateDatabase(db)
	case ETSoftDeleteDatabase:
		dbEntry := &databaseLogEntry{}
		dbEntry.Unmarshal(entry.GetPayload())
		cache.replayer.catalog.onReplaySoftDeleteDatabase(dbEntry)
	case ETSoftDeleteTable:
		tblEntry := &tableLogEntry{}
		tblEntry.Unmarshal(entry.GetPayload())
		cache.replayer.catalog.onReplayTableOperation(tblEntry)
	case ETCreateTable:
		tblEntry := &tableLogEntry{}
		tblEntry.Unmarshal(entry.GetPayload())
		cache.replayer.catalog.Sequence.TryUpdateTableId(tblEntry.Table.Id)
		cache.replayer.catalog.onReplayCreateTable(tblEntry)
	default:
		panic("not supported")
	}
	return nil
}

func (cache *replayCache) onReplayTxn(store *TxnStore) error {
	for _, buf := range store.Logs {
		entry := logstore.NewAsyncBaseEntry()
		defer entry.Free()
		r := bytes.NewReader(buf)
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
			if err = cache.onReplayTxnEntry(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cache *replayCache) applyReplayEntry(entry *replayEntry, catalog *Catalog, r *common.Range) error {
	switch entry.typ {
	case logstore.ETCheckpoint:
		catalog.Sequence.TryUpdateCommitId(entry.catalogEntry.Range.Right)
		err := catalog.onReplayCheckpoint(entry.catalogEntry)
		return err
	}
	if r != nil {
		if !r.LT(entry.commitId) {
			return nil
		}
	}
	var err error
	catalog.Sequence.TryUpdateCommitId(entry.commitId)
	switch entry.typ {
	case ETCreateDatabase:
		catalog.Sequence.TryUpdateTableId(entry.db.Id)
		err = catalog.onReplayCreateDatabase(entry.db)
	case ETSoftDeleteDatabase:
		err = catalog.onReplaySoftDeleteDatabase(entry.dbEntry)
	case ETHardDeleteDatabase:
		err = catalog.onReplayHardDeleteDatabase(entry.dbEntry)
	case ETSplitDatabase:
		err = catalog.onReplayReplaceDatabase(entry.replaceEntry, true)
	case ETReplaceDatabase:
		err = catalog.onReplayReplaceDatabase(entry.replaceEntry, false)
	case ETCreateBlock:
		catalog.Sequence.TryUpdateBlockId(entry.blkEntry.Id)
		err = catalog.onReplayCreateBlock(entry.blkEntry)
	case ETUpgradeBlock:
		err = catalog.onReplayUpgradeBlock(entry.blkEntry)
	case ETCreateTable:
		catalog.Sequence.TryUpdateTableId(entry.tblEntry.Table.Id)
		err = catalog.onReplayCreateTable(entry.tblEntry)
	case ETAddIndice, ETDropIndice, ETSoftDeleteTable, ETHardDeleteTable:
		err = catalog.onReplayTableOperation(entry.tblEntry)
	case ETCreateSegment:
		catalog.Sequence.TryUpdateSegmentId(entry.segEntry.Id)
		err = catalog.onReplayCreateSegment(entry.segEntry)
	case ETUpgradeSegment:
		err = catalog.onReplayUpgradeSegment(entry.segEntry)
	case ETTransaction:
		err = cache.onReplayTxn(entry.txnStore)
	default:
		panic(fmt.Sprintf("unkown entry type: %d", entry.typ))
	}
	return err
}

func (cache *replayCache) applyNoCheckpoint() error {
	for _, entry := range cache.entries {
		if err := cache.applyReplayEntry(entry, cache.replayer.catalog, nil); err != nil {
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
		// TODO
		// if err := cache.replayer.catalog.rebuild(cache.checkpoint.Catalog.TableSet, cache.checkpoint.Range); err != nil {
		// 	return err
		// }
		for _, entry := range cache.checkpointEntries{
			if err := cache.applyReplayEntry(entry, cache.replayer.catalog, cache.checkpoint.Range); err != nil {
				return err
			}
		}
		for _, entry := range cache.entries {
			if err := cache.applyReplayEntry(entry, cache.replayer.catalog, cache.checkpoint.Range); err != nil {
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
	version  uint64
}

func newCatalogReplayer() *catalogReplayer {
	replayer := &catalogReplayer{}
	replayer.cache = newReplayCache(replayer)
	return replayer
}

func (replayer *catalogReplayer) rebuildStats() {
	for _, db := range replayer.catalog.Databases {
		db.rebuildStats()
	}
}

func (replayer *catalogReplayer) restoreWal() {
	if replayer.catalog.IndexWal == nil {
		return
	}
	for _, database := range replayer.catalog.Databases {
		if database.IsHardDeletedLocked() {
			continue
		}
		if database.IsDeleted() {
			database.InitWal(database.CommitInfo.GetIndex())
		}
		safeId, ok := replayer.cache.safeIds[database.GetShardId()]
		if !ok {
			logutil.Warnf("Cannot get safeid of shardId %d", database.GetShardId())
			safeId = 0
		}
		database.InitWal(safeId)
	}
	logutil.Info(replayer.catalog.IndexWal.String())
}

func (replayer *catalogReplayer) RebuildCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg,
	store logstore.AwareStore, indexWal wal.ShardAwareWal) (*Catalog, error) {
	replayer.catalog = NewCatalogWithDriver(mu, cfg, store, indexWal)
	if err := replayer.Replay(replayer.catalog.Store); err != nil {
		return nil, err
	}
	replayer.restoreWal()
	replayer.rebuildStats()
	replayer.catalog.DebugCheckReplayedState()
	replayer.catalog.Store.TryCompact()
	replayer.cache = nil
	logutil.Infof(replayer.catalog.PString(PPL0, 0))
	return replayer.catalog, nil
}

func (replayer *catalogReplayer) RebuildCatalog(mu *sync.RWMutex, cfg *CatalogCfg) (*Catalog, error) {
	replayer.catalog = NewCatalog(mu, cfg)
	if err := replayer.Replay(replayer.catalog.Store); err != nil {
		return nil, err
	}
	replayer.restoreWal()
	replayer.rebuildStats()
	replayer.catalog.DebugCheckReplayedState()
	replayer.catalog.Store.TryCompact()
	replayer.cache = nil
	logutil.Infof(replayer.catalog.PString(PPL0, 0))
	return replayer.catalog, nil
}

func (replayer *catalogReplayer) doReplay(r *logstore.VersionFile, observer logstore.ReplayObserver) error {
	if replayer.version != r.Version {
		replayer.offset = 0
		replayer.version = r.Version
	}
	if r.Size == replayer.offset {
		// Have read to the end of the file.
		// No longer need additional overhead
		return io.EOF
	}
	entry := logstore.NewAsyncBaseEntry()
	defer entry.Free()
	meta := entry.GetMeta()
	metaSize, err := meta.ReadFrom(r)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		replayer.tryTruncate()
		return err
	}
	if entry, n, err := defaultHandler(r, entry); err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		// Only metadata is written
		replayer.tryTruncate()
		return err
	} else {
		if n != int64(meta.PayloadSize()) {
			if r.Size == replayer.offset+int64(metaSize)+n {
				// Have read to the end of the file
				replayer.tryTruncate()
				return io.EOF
			} else {
				return errors.New(fmt.Sprintf("payload mismatch: %d != %d", n, meta.PayloadSize()))
			}
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
	if err != nil {
		return err
	}
	logutil.Infof("Total %d entries replayed", replayer.replayed)
	return replayer.cache.Apply()
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
	logType := entry.GetMeta().GetType()
	if observer != nil {
		switch logType {
		case shard.ETShardWalSafeId:
		case logstore.ETCheckpoint:
		case logstore.ETFlush:
			break
		case logstore.ETInvalid:
			logutil.Infof("get invalid entry")
			return nil
		default:
			observer.OnReplayCommit(GetCommitIdFromLogEntry(entry))
		}
	}
	switch logType {
	case shard.ETShardWalSafeId:
		safeId, _ := shard.EntryToSafeId(entry)
		replayer.cache.OnShardSafeId(safeId)
	case ETCreateBlock:
		blk := &blockLogEntry{}
		blk.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		blk.CommitLocked(commitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateBlock,
			blkEntry: blk,
			commitId: commitId,
		})
	case ETUpgradeBlock:
		blk := &blockLogEntry{}
		blk.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      ETUpgradeBlock,
			blkEntry: blk,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETCreateDatabase:
		db := &Database{}
		db.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		db.CommitLocked(commitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateDatabase,
			db:       db,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETSoftDeleteDatabase:
		db := &databaseLogEntry{}
		db.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      ETSoftDeleteDatabase,
			dbEntry:  db,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETHardDeleteDatabase:
		db := &databaseLogEntry{}
		db.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      ETHardDeleteDatabase,
			dbEntry:  db,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETSplitDatabase:
		replace := newDbReplaceLogEntry()
		replace.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		replace.commitId = commitId
		replayer.cache.Append(&replayEntry{
			typ:          ETReplaceDatabase,
			replaceEntry: replace,
			commitId:     GetCommitIdFromLogEntry(entry),
		})
	case ETReplaceDatabase:
		replace := newDbReplaceLogEntry()
		replace.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		replace.commitId = commitId
		replayer.cache.Append(&replayEntry{
			typ:          ETReplaceDatabase,
			replaceEntry: replace,
			commitId:     GetCommitIdFromLogEntry(entry),
		})
	case ETCreateTable:
		tbl := &tableLogEntry{}
		tbl.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		tbl.Table.CommitLocked(commitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateTable,
			tblEntry: tbl,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETAddIndice, ETDropIndice, ETSoftDeleteTable, ETHardDeleteTable:
		tbl := &tableLogEntry{}
		tbl.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      logType,
			tblEntry: tbl,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETCreateSegment:
		seg := &segmentLogEntry{}
		seg.Unmarshal(entry.GetPayload())
		commitId := GetCommitIdFromLogEntry(entry)
		seg.CommitLocked(commitId)
		replayer.cache.Append(&replayEntry{
			typ:      ETCreateSegment,
			segEntry: seg,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETUpgradeSegment:
		seg := &segmentLogEntry{}
		seg.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      ETUpgradeSegment,
			segEntry: seg,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case ETTransaction:
		txnStore := new(TxnStore)
		txnStore.Unmarshal(entry.GetPayload())
		replayer.cache.Append(&replayEntry{
			typ:      ETTransaction,
			txnStore: txnStore,
			commitId: GetCommitIdFromLogEntry(entry),
		})
	case logstore.ETCheckpoint:
		c := &catalogLogEntry{}
		c.Unmarshal(entry.GetPayload())
		observer.OnReplayCheckpoint(*c.Range)
		for shardid, id := range c.SafeIds {
			safeId := shard.SafeId{ShardId: shardid, Id: id}
			replayer.cache.OnShardSafeId(safeId)
		}
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

func (replayer *catalogReplayer) tryTruncate() {
	err := replayer.Truncate(replayer.catalog.Store)
	if err != nil {
		panic(fmt.Sprintf("doReplay Truncate: %v", err.Error()))
	}
}
