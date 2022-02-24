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
	"fmt"
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/jiangxinmeng1/logstore/pkg/store"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type Store = store.Store

type replayCache struct {
	replayer *catalogReplayer
	safeIds  map[uint64]uint64
}

func newReplayCache(replayer *catalogReplayer) *replayCache {
	return &replayCache{
		replayer: replayer,
		safeIds:  make(map[uint64]uint64),
	}
}

func (cache *replayCache) OnShardSafeId(id shard.SafeId) {
	old, ok := cache.safeIds[id.ShardId]
	if !ok || old < id.Id {
		cache.safeIds[id.ShardId] = id.Id
	}
}

func (cache *replayCache) onReplayTxnEntry(entry LogEntry) error {
	logType := entry.GetType()
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

type catalogReplayer struct {
	catalog *Catalog
	cache   *replayCache
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
	commitId := replayer.catalog.Store.GetSynced(shard.WalGroupName)
	replayer.catalog.IndexWal.SetLogstoreCommitId(commitId)
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
	store store.Store, indexWal wal.ShardAwareWal) (*Catalog, error) {
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

func (replayer *catalogReplayer) Replay(s store.Store) error {
	err := s.Replay(replayer.ApplyEntry)
	return err
}

func (replayer *catalogReplayer) ApplyEntry(group string, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
	if typ == entry.ETCheckpoint {
		catalog := replayer.catalog
		ckps := info.(*entry.CheckpointInfo)
		catalogCkp := ckps.CheckpointRanges[CatalogEntryGroupName]
		if catalogCkp != nil {
			catalog.Sequence.TryUpdateCommitId(catalogCkp.End)
		}
		c := &catalogLogEntry{}
		c.Unmarshal(payload)
		err = catalog.onReplayCheckpoint(c)
		for shardid, id := range c.SafeIds {
			safeId := shard.SafeId{ShardId: shardid, Id: id}
			replayer.cache.OnShardSafeId(safeId)
		}
		return err
	}
	switch group {
	case shard.WalGroupName:
		switch typ {
		case shard.ETShardWalSafeId:
			safeId := &shard.SafeId{}
			safeId.Unmarshal(payload)
			replayer.cache.OnShardSafeId(*safeId)
		}
	case CatalogEntryGroupName:
		catalog := replayer.catalog
		catalog.Sequence.TryUpdateCommitId(commitId)
		switch typ {
		case ETCreateDatabase:
			db := &Database{}
			db.Unmarshal(payload)
			db.CommitLocked(commitId)
			catalog.Sequence.TryUpdateTableId(db.Id)
			err = catalog.onReplayCreateDatabase(db)
		case ETSoftDeleteDatabase:
			db := &databaseLogEntry{}
			db.Unmarshal(payload)
			err = catalog.onReplaySoftDeleteDatabase(db)
		case ETHardDeleteDatabase:
			db := &databaseLogEntry{}
			db.Unmarshal(payload)
			err = catalog.onReplayHardDeleteDatabase(db)
		case ETSplitDatabase:
			replace := newDbReplaceLogEntry()
			replace.Unmarshal(payload)
			replace.commitId = commitId
			err = catalog.onReplayReplaceDatabase(replace, true)
		case ETReplaceDatabase:
			replace := newDbReplaceLogEntry()
			replace.Unmarshal(payload)
			replace.commitId = commitId
			err = catalog.onReplayReplaceDatabase(replace, false)
		case ETCreateBlock:
			blk := &blockLogEntry{}
			blk.Unmarshal(payload)
			blk.CommitLocked(commitId)
			catalog.Sequence.TryUpdateBlockId(blk.Id)
			err = catalog.onReplayCreateBlock(blk)
		case ETUpgradeBlock:
			blk := &blockLogEntry{}
			blk.Unmarshal(payload)
			err = catalog.onReplayUpgradeBlock(blk)
		case ETCreateTable:
			tbl := &tableLogEntry{}
			tbl.Unmarshal(payload)
			tbl.Table.CommitLocked(commitId)
			catalog.Sequence.TryUpdateTableId(tbl.Table.Id)
			err = catalog.onReplayCreateTable(tbl)
		case ETAddIndice, ETDropIndice, ETSoftDeleteTable, ETHardDeleteTable:
			tbl := &tableLogEntry{}
			tbl.Unmarshal(payload)
			err = catalog.onReplayTableOperation(tbl)
		case ETCreateSegment:
			seg := &segmentLogEntry{}
			seg.Unmarshal(payload)
			seg.CommitLocked(commitId)
			catalog.Sequence.TryUpdateSegmentId(seg.Id)
			err = catalog.onReplayCreateSegment(seg)
		case ETUpgradeSegment:
			seg := &segmentLogEntry{}
			seg.Unmarshal(payload)
			err = catalog.onReplayUpgradeSegment(seg)
		case ETTransaction:
			// txnStore := new(TxnStore)
			// txnStore.Unmarshal(ent.GetPayload())
			// for _, buf := range txnStore.Logs {
			// 	entry := logstore.NewAsyncBaseEntry()
			// 	defer entry.Free()
			// 	r := bytes.NewReader(buf)
			// 	meta := entry.GetMeta()
			// 	_, err := meta.ReadFrom(r)
			// 	if err != nil {
			// 		return err
			// 	}
			// 	if entry, n, err := defaultHandler(r, entry); err != nil {
			// 		return err
			// 	} else {
			// 		if n != int64(meta.PayloadSize()) {
			// 			return errors.New(fmt.Sprintf("payload mismatch: %d != %d", n, meta.PayloadSize()))
			// 		}
			// 		if err = cache.onReplayTxnEntry(entry); err != nil {
			// 			return err
			// 		}
			// 	}
			// }
		default:
			panic(fmt.Sprintf("unkown entry type: %d", typ))
		}
	}
	return
}
