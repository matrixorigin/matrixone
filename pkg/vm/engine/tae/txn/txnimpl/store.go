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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type dirtyPoint = txnif.DirtyPoint
type dirtySet = txnif.DirtySet

// dirtyMemo intercepts txn to record changed segments and blocks, or catalog
//
// no locks to protect dirtyMemo because it is expected to be quried by other goroutines
// until the txn enqueued, and after that, no one will change it
type dirtyMemo struct {
	// tableChanges records modified segments and blocks in current txn
	tableChanges map[uint64]dirtySet
	// catalogChanged indicates whether create/drop db/table
	catalogChanged bool
}

func newDirtyMemo() *dirtyMemo {
	return &dirtyMemo{
		tableChanges: make(map[uint64]dirtySet, 0),
	}
}

func (m *dirtyMemo) recordBlk(id common.ID) {
	point := dirtyPoint{
		SegID: id.SegmentID,
		BlkID: id.BlockID,
	}
	m.recordDirty(id.TableID, point)
}

func (m *dirtyMemo) recordSeg(tid, sid uint64) {
	point := dirtyPoint{
		SegID: sid,
	}
	m.recordDirty(tid, point)
}

func (m *dirtyMemo) recordDirty(tid uint64, point dirtyPoint) {
	dirties, exist := m.tableChanges[tid]
	if !exist {
		dirties = make(map[dirtyPoint]struct{}, 0)
		m.tableChanges[tid] = dirties
	}
	dirties[point] = struct{}{}
}

func (m *dirtyMemo) recordCatalogChange() {
	m.catalogChanged = true
}

type txnStore struct {
	txnbase.NoopTxnStore
	dbs         map[uint64]*txnDB
	mu          sync.RWMutex
	driver      wal.Driver
	nodesMgr    base.INodeManager
	txn         txnif.AsyncTxn
	catalog     *catalog.Catalog
	cmdMgr      *commandManager
	logs        []entry.Entry
	warChecker  *warChecker
	dataFactory *tables.DataFactory
	writeOps    uint32

	dirtyMemo *dirtyMemo
}

var TxnStoreFactory = func(catalog *catalog.Catalog, driver wal.Driver, txnBufMgr base.INodeManager, dataFactory *tables.DataFactory) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		return newStore(catalog, driver, txnBufMgr, dataFactory)
	}
}

func newStore(catalog *catalog.Catalog, driver wal.Driver, txnBufMgr base.INodeManager, dataFactory *tables.DataFactory) *txnStore {
	return &txnStore{
		dbs:         make(map[uint64]*txnDB),
		catalog:     catalog,
		cmdMgr:      newCommandManager(driver),
		driver:      driver,
		logs:        make([]entry.Entry, 0),
		dataFactory: dataFactory,
		nodesMgr:    txnBufMgr,
		dirtyMemo:   newDirtyMemo(),
	}
}

func (store *txnStore) IsReadonly() bool {
	return atomic.LoadUint32(&store.writeOps) == 0
}

func (store *txnStore) IncreateWriteCnt() int {
	return int(atomic.AddUint32(&store.writeOps, uint32(1)))
}

func (store *txnStore) LogTxnEntry(dbId uint64, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.LogTxnEntry(tableId, entry, readed)
}

func (store *txnStore) LogSegmentID(dbId, tid, sid uint64) {
	db, _ := store.getOrSetDB(dbId)
	db.LogSegmentID(tid, sid)
}

func (store *txnStore) LogBlockID(dbId, tid, bid uint64) {
	db, _ := store.getOrSetDB(dbId)
	db.LogBlockID(tid, bid)
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

func (store *txnStore) Append(dbId, id uint64, data *containers.Batch) error {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if db.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.Append(id, data)
}

func (store *txnStore) RangeDelete(dbId uint64, id *common.ID, start, end uint32, dt handle.DeleteType) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.RangeDelete(id, start, end, dt)
}

func (store *txnStore) UpdateMetaLoc(dbId uint64, id *common.ID, metaLoc string) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.UpdateMetaLoc(id, metaLoc)
}

func (store *txnStore) UpdateDeltaLoc(dbId uint64, id *common.ID, deltaLoc string) (err error) {
	db, err := store.getOrSetDB(dbId)
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

func (store *txnStore) GetValue(dbId uint64, id *common.ID, row uint32, colIdx uint16) (v any, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	// if table.IsDeleted() {
	// 	err = txnbase.ErrNotFound
	// 	return
	// }
	return db.GetValue(id, row, colIdx)
}

func (store *txnStore) Update(dbId uint64, id *common.ID, row uint32, colIdx uint16, v any) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.Update(id, row, colIdx, v)
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

func (store *txnStore) GetDatabase(name string) (h handle.Database, err error) {
	meta, err := store.catalog.GetDBEntry(name, store.txn)
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

func (store *txnStore) CreateDatabase(name string) (h handle.Database, err error) {
	meta, err := store.catalog.CreateDBEntry(name, store.txn)
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

func (store *txnStore) CreateRelation(dbId uint64, def any) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.CreateRelation(def)
}

func (store *txnStore) DropRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.DropRelationByName(name)
}

func (store *txnStore) GetRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.GetRelationByName(name)
}

func (store *txnStore) GetSegment(dbId uint64, id *common.ID) (seg handle.Segment, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
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

func (store *txnStore) CreateNonAppendableSegment(dbId, tid uint64) (seg handle.Segment, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateNonAppendableSegment(tid)
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

func (store *txnStore) CreateNonAppendableBlock(dbId uint64, id *common.ID) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateNonAppendableBlock(id)
}

func (store *txnStore) GetBlock(dbId uint64, id *common.ID) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.GetBlock(id)
}

func (store *txnStore) CreateBlock(dbId, tid, sid uint64, is1PC bool) (blk handle.Block, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateBlock(tid, sid, is1PC)
}

func (store *txnStore) SoftDeleteBlock(dbId uint64, id *common.ID) (err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.SoftDeleteBlock(id)
}

func (store *txnStore) SoftDeleteSegment(dbId uint64, id *common.ID) (err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.SoftDeleteSegment(id)
}

func (store *txnStore) ApplyRollback() (err error) {
	if store.cmdMgr.GetCSN() == 0 {
		return
	}
	for _, db := range store.dbs {
		if err = db.ApplyRollback(); err != nil {
			break
		}
	}
	return
}

func (store *txnStore) WaitPrepared() (err error) {
	for _, db := range store.dbs {
		if err = db.WaitPrepared(); err != nil {
			return
		}
	}
	for _, e := range store.logs {
		if err = e.WaitDone(); err != nil {
			break
		}
		e.Free()
	}
	return
}

func (store *txnStore) ApplyCommit() (err error) {
	for _, db := range store.dbs {
		if err = db.ApplyCommit(); err != nil {
			break
		}
	}
	return
}

func (store *txnStore) PrePrepare() (err error) {
	for _, db := range store.dbs {
		if db.NeedRollback() {
			if err = db.PrepareRollback(); err != nil {
				return
			}
			delete(store.dbs, db.entry.GetID())
		}
		if err = db.PrePrepare(); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) PrepareCommit() (err error) {
	if store.warChecker != nil {
		if err = store.warChecker.check(); err != nil {
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
	now := time.Now()
	for _, db := range store.dbs {
		if err = db.PreApplyCommit(); err != nil {
			return
		}
	}
	if err = store.CollectCmd(); err != nil {
		return
	}

	if store.cmdMgr.GetCSN() == 0 {
		return
	}

	//TODO:How to distinguish prepare log of 2PC entry from commit log entry of 1PC?
	logEntry, err := store.cmdMgr.ApplyTxnRecord(store.txn.GetID(), store.txn)
	if err != nil {
		return
	}
	if logEntry != nil {
		store.logs = append(store.logs, logEntry)
	}
	for _, db := range store.dbs {
		if err = db.Apply1PCCommit(); err != nil {
			return
		}
	}
	logutil.Debugf("Txn-%s PrepareCommit Takes %s", store.txn.GetID(), time.Since(now))
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

func (store *txnStore) HasTableDataChanges(tableID uint64) bool {
	_, changed := store.dirtyMemo.tableChanges[tableID]
	return changed
}

// GetTableDirtyPoints returns touched segments and blocks in the txn.
func (store *txnStore) GetTableDirtyPoints(tableID uint64) dirtySet {
	dirtiesMap := store.dirtyMemo.tableChanges[tableID]
	return dirtiesMap
}

func (store *txnStore) HasCatalogChanges() bool {
	return store.dirtyMemo.catalogChanged
}
