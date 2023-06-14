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
// limitations under the License.

package txnimpl

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type txnStore struct {
	ctx context.Context
	txnbase.NoopTxnStore
	mu            sync.RWMutex
	transferTable *model.HashPageTable
	dbs           map[uint64]*txnDB
	driver        wal.Driver
	indexCache    model.LRUCache
	txn           txnif.AsyncTxn
	catalog       *catalog.Catalog
	cmdMgr        *commandManager
	logs          []entry.Entry
	warChecker    *warChecker
	dataFactory   *tables.DataFactory
	writeOps      atomic.Uint32

	wg sync.WaitGroup
}

var TxnStoreFactory = func(
	ctx context.Context,
	catalog *catalog.Catalog,
	driver wal.Driver,
	transferTable *model.HashPageTable,
	indexCache model.LRUCache,
	dataFactory *tables.DataFactory,
	maxMessageSize uint64) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		return newStore(ctx, catalog, driver, transferTable, indexCache, dataFactory, maxMessageSize)
	}
}

func newStore(
	ctx context.Context,
	catalog *catalog.Catalog,
	driver wal.Driver,
	transferTable *model.HashPageTable,
	indexCache model.LRUCache,
	dataFactory *tables.DataFactory,
	maxMessageSize uint64) *txnStore {
	return &txnStore{
		ctx:           ctx,
		transferTable: transferTable,
		dbs:           make(map[uint64]*txnDB),
		catalog:       catalog,
		cmdMgr:        newCommandManager(driver, maxMessageSize),
		driver:        driver,
		logs:          make([]entry.Entry, 0),
		dataFactory:   dataFactory,
		indexCache:    indexCache,
		wg:            sync.WaitGroup{},
	}
}

func (store *txnStore) IsReadonly() bool {
	return store.writeOps.Load() == 0
}

func (store *txnStore) IncreateWriteCnt() int {
	return int(store.writeOps.Add(1))
}

func (store *txnStore) LogTxnEntry(dbId uint64, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.LogTxnEntry(tableId, entry, readed)
}

func (store *txnStore) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	cmd := txnbase.NewTxnStateCmd(
		store.txn.GetID(),
		store.txn.GetTxnState(false),
		store.txn.GetCommitTS(),
	)
	var buf []byte
	if buf, err = cmd.MarshalBinary(); err != nil {
		return
	}
	logEntry = entry.GetBase()
	logEntry.SetType(IOET_WALEntry_TxnRecord)
	if err = logEntry.SetPayload(buf); err != nil {
		return
	}
	info := &entry.Info{
		Group: wal.GroupC,
	}
	logEntry.SetInfo(info)
	var lsn uint64
	lsn, err = store.driver.AppendEntry(wal.GroupC, logEntry)
	if err != nil {
		return
	}
	if sync {
		err = logEntry.WaitDone()
	}
	logutil.Debugf("LogTxnState LSN=%d, Size=%d", lsn, len(buf))
	return
}

func (store *txnStore) Close() error {
	var err error
	for _, db := range store.dbs {
		if err = db.Close(); err != nil {
			break
		}
	}
	store.dbs = nil
	store.cmdMgr = nil
	store.logs = nil
	store.warChecker = nil
	return err
}

func (store *txnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *txnStore) BatchDedup(dbId, id uint64, pk containers.Vector) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }

	return db.BatchDedup(id, pk)
}

func (store *txnStore) Append(ctx context.Context, dbId, id uint64, data *containers.Batch) error {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if db.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.Append(ctx, id, data)
}

func (store *txnStore) AddBlksWithMetaLoc(
	dbId, tid uint64,
	metaLoc []objectio.Location,
) error {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	return db.AddBlksWithMetaLoc(tid, metaLoc)
}

func (store *txnStore) RangeDelete(
	id *common.ID, start, end uint32, dt handle.DeleteType, checkTs types.TS,
) (err error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	return db.RangeDelete(id, start, end, dt, checkTs)
}

func (store *txnStore) UpdateMetaLoc(id *common.ID, metaLoc objectio.Location) (err error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.UpdateMetaLoc(id, metaLoc)
}

func (store *txnStore) UpdateDeltaLoc(id *common.ID, deltaLoc objectio.Location) (err error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.UpdateDeltaLoc(id, deltaLoc)
}

func (store *txnStore) GetByFilter(dbId, tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	// if table.IsDeleted() {
	// 	err = txnbase.ErrNotFound
	// 	return
	// }
	return db.GetByFilter(tid, filter)
}

func (store *txnStore) GetValue(id *common.ID, row uint32, colIdx uint16) (v any, isNull bool, err error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return
	}
	// if table.IsDeleted() {
	// 	err = txnbase.ErrNotFound
	// 	return
	// }
	return db.GetValue(id, row, colIdx)
}

func (store *txnStore) DatabaseNames() (names []string) {
	it := newDBIt(store.txn, store.catalog)
	for it.Valid() {
		names = append(names, it.GetCurr().GetName())
		it.Next()
	}
	return
}

func (store *txnStore) UseDatabase(name string) (err error) {
	return err
}

func (store *txnStore) UnsafeGetDatabase(id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.GetDatabaseByID(id)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) GetDatabase(name string) (h handle.Database, err error) {
	defer func() {
		if err == moerr.GetOkExpectedEOB() {
			err = moerr.NewBadDBNoCtx(name)
		}
	}()
	meta, err := store.catalog.TxnGetDBEntryByName(name, store.txn)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) GetDatabaseByID(id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.TxnGetDBEntryByID(id, store.txn)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) CreateDatabase(name, createSql, datTyp string) (h handle.Database, err error) {
	meta, err := store.catalog.CreateDBEntry(name, createSql, datTyp, store.txn)
	if err != nil {
		return nil, err
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	if err = db.SetCreateEntry(meta); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) CreateDatabaseWithID(name, createSql, datTyp string, id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.CreateDBEntryWithID(name, createSql, datTyp, id, store.txn)
	if err != nil {
		return nil, err
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	if err = db.SetCreateEntry(meta); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) DropDatabase(name string) (h handle.Database, err error) {
	hasNewEntry, meta, err := store.catalog.DropDBEntry(name, store.txn)
	if err != nil {
		return
	}
	db, err := store.getOrSetDB(meta.GetID())
	if err != nil {
		return
	}
	if hasNewEntry {
		if err = db.SetDropEntry(meta); err != nil {
			return
		}
	}
	h = buildDB(db)
	return
}

func (store *txnStore) ObserveTxn(
	visitDatabase func(db any),
	visitTable func(tbl any),
	rotateTable func(dbName, tblName string, dbid, tid uint64),
	visitMetadata func(block any),
	visitSegment func(seg any),
	visitAppend func(bat any),
	visitDelete func(vnode txnif.DeleteNode)) {
	for _, db := range store.dbs {
		if db.createEntry != nil || db.dropEntry != nil {
			visitDatabase(db.entry)
		}
		dbName := db.entry.GetName()
		dbid := db.entry.ID
		for _, tbl := range db.tables {
			tblName := tbl.GetLocalSchema().Name
			tid := tbl.entry.ID
			rotateTable(dbName, tblName, dbid, tid)
			if tbl.createEntry != nil || tbl.dropEntry != nil {
				visitTable(tbl.entry)
			}
			for _, iTxnEntry := range tbl.txnEntries.entries {
				switch txnEntry := iTxnEntry.(type) {
				case *catalog.SegmentEntry:
					visitSegment(txnEntry)
				case *catalog.BlockEntry:
					visitMetadata(txnEntry)
				case *updates.DeleteNode:
					visitDelete(txnEntry)
				case *catalog.TableEntry:
					if tbl.createEntry != nil || tbl.dropEntry != nil {
						continue
					}
					visitTable(txnEntry)
				}
			}
			if tbl.localSegment != nil {
				for _, node := range tbl.localSegment.nodes {
					anode, ok := node.(*anode)
					if ok {
						schema := anode.table.GetLocalSchema()
						bat := &containers.BatchWithVersion{
							Version:    schema.Version,
							NextSeqnum: uint16(schema.Extra.NextColSeqnum),
							Seqnums:    schema.AllSeqnums(),
							Batch:      anode.data,
						}
						visitAppend(bat)
					}
				}
			}
		}
	}
}
func (store *txnStore) AddWaitEvent(cnt int) {
	store.wg.Add(cnt)
}
func (store *txnStore) DoneWaitEvent(cnt int) {
	store.wg.Add(-cnt)
}
func (store *txnStore) DropDatabaseByID(id uint64) (h handle.Database, err error) {
	hasNewEntry, meta, err := store.catalog.DropDBEntryByID(id, store.txn)
	if err != nil {
		return
	}
	db, err := store.getOrSetDB(meta.GetID())
	if err != nil {
		return
	}
	if hasNewEntry {
		if err = db.SetDropEntry(meta); err != nil {
			return
		}
	}
	h = buildDB(db)
	return
}

func (store *txnStore) CreateRelation(dbId uint64, def any) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.CreateRelation(def)
}

func (store *txnStore) CreateRelationWithTableId(dbId uint64, tableId uint64, def any) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.CreateRelationWithTableId(tableId, def)
}

func (store *txnStore) DropRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.DropRelationByName(name)
}

func (store *txnStore) DropRelationByID(dbId uint64, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.DropRelationByID(id)
}

func (store *txnStore) UnsafeGetRelation(dbId, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.UnsafeGetRelation(id)
}

func (store *txnStore) GetRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.GetRelationByName(name)
}

func (store *txnStore) GetRelationByID(dbId uint64, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.GetRelationByID(id)
}

func (store *txnStore) GetSegment(id *common.ID) (seg handle.Segment, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	return db.GetSegment(id)
}

func (store *txnStore) CreateSegment(dbId, tid uint64, is1PC bool) (seg handle.Segment, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateSegment(tid, is1PC)
}

func (store *txnStore) CreateNonAppendableSegment(dbId, tid uint64, is1PC bool) (seg handle.Segment, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateNonAppendableSegment(tid, is1PC)
}

func (store *txnStore) getOrSetDB(id uint64) (db *txnDB, err error) {
	store.mu.RLock()
	db = store.dbs[id]
	store.mu.RUnlock()
	if db != nil {
		return
	}
	var entry *catalog.DBEntry
	if entry, err = store.catalog.GetDatabaseByID(id); err != nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	db = store.dbs[id]
	if db != nil {
		return
	}
	db = newTxnDB(store, entry)
	db.idx = len(store.dbs)
	store.dbs[id] = db
	return
}

func (store *txnStore) CreateNonAppendableBlock(id *common.ID, opts *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	perfcounter.Update(store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.CreateNonAppendable.Add(1)
	})
	return db.CreateNonAppendableBlock(id, opts)
}

func (store *txnStore) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	return db.GetBlock(id)
}

func (store *txnStore) CreateBlock(id *common.ID, is1PC bool) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	perfcounter.Update(store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.Create.Add(1)
	})
	return db.CreateBlock(id, is1PC)
}

func (store *txnStore) SoftDeleteBlock(id *common.ID) (err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	perfcounter.Update(store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.SoftDelete.Add(1)
	})
	return db.SoftDeleteBlock(id)
}

func (store *txnStore) SoftDeleteSegment(id *common.ID) (err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	perfcounter.Update(store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Segment.SoftDelete.Add(1)
	})
	return db.SoftDeleteSegment(id)
}

func (store *txnStore) ApplyRollback() (err error) {
	if store.cmdMgr.GetCSN() != 0 {
		for _, db := range store.dbs {
			if err = db.ApplyRollback(); err != nil {
				break
			}
		}
	}
	store.CleanUp()
	return
}

func (store *txnStore) WaitPrepared() (err error) {
	for _, db := range store.dbs {
		if err = db.WaitPrepared(); err != nil {
			return
		}
	}
	trace.WithRegion(context.Background(), "Wait for WAL to be flushed", func() {
		for _, e := range store.logs {
			if err = e.WaitDone(); err != nil {
				break
			}
			e.Free()
		}
	})
	store.wg.Wait()
	return
}

func (store *txnStore) ApplyCommit() (err error) {
	for _, db := range store.dbs {
		if err = db.ApplyCommit(); err != nil {
			break
		}
	}
	store.CleanUp()
	return
}

func (store *txnStore) Freeze() (err error) {
	for _, db := range store.dbs {
		if db.NeedRollback() {
			if err = db.PrepareRollback(); err != nil {
				return
			}
			delete(store.dbs, db.entry.GetID())
		}
		if err = db.Freeze(); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) PrePrepare() (err error) {
	for _, db := range store.dbs {
		if err = db.PrePrepare(); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) PrepareCommit() (err error) {
	if store.warChecker != nil {
		if err = store.warChecker.checkAll(
			store.txn.GetPrepareTS()); err != nil {
			return err
		}
	}
	for _, db := range store.dbs {
		if err = db.PrepareCommit(); err != nil {
			break
		}
	}

	return
}

func (store *txnStore) PreApplyCommit() (err error) {
	// now := time.Now()
	for _, db := range store.dbs {
		if err = db.PreApplyCommit(); err != nil {
			return
		}
	}
	// logutil.Debugf("Txn-%X PrepareCommit Takes %s", store.txn.GetID(), time.Since(now))
	return
}

func (store *txnStore) PrepareWAL() (err error) {
	// now := time.Now()
	if err = store.CollectCmd(); err != nil {
		return
	}

	if store.cmdMgr.GetCSN() == 0 {
		return
	}

	// Apply the record from the command list.
	// Split the commands by max message size.
	for store.cmdMgr.cmd.MoreCmds() {
		logEntry, err := store.cmdMgr.ApplyTxnRecord(store.txn.GetID(), store.txn)
		if err != nil {
			return err
		}
		if logEntry != nil {
			store.logs = append(store.logs, logEntry)
		}
	}

	for _, db := range store.dbs {
		if err = db.Apply1PCCommit(); err != nil {
			return
		}
	}
	// logutil.Debugf("Txn-%X PrepareWAL Takes %s", store.txn.GetID(), time.Since(now))
	return
}

func (store *txnStore) CollectCmd() (err error) {
	dbs := make([]*txnDB, len(store.dbs))
	for _, db := range store.dbs {
		dbs[db.idx] = db
	}
	for _, db := range dbs {
		if err = db.CollectCmd(store.cmdMgr); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// TODO
}

func (store *txnStore) PrepareRollback() error {
	var err error
	for _, db := range store.dbs {
		if err = db.PrepareRollback(); err != nil {
			break
		}
	}

	return err
}

func (store *txnStore) GetLSN() uint64 { return store.cmdMgr.lsn }

func (store *txnStore) CleanUp() {
	for _, db := range store.dbs {
		db.CleanUp()
	}
}
