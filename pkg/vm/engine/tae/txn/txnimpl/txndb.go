// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type txnDB struct {
	store       *txnStore
	tables      map[uint64]*txnTable
	mu          sync.RWMutex
	entry       *catalog.DBEntry
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	ddlCSN      uint32
	idx         int
}

func newTxnDB(store *txnStore, entry *catalog.DBEntry) *txnDB {
	db := &txnDB{
		store:  store,
		tables: make(map[uint64]*txnTable),
		entry:  entry,
	}
	return db
}

func (db *txnDB) SetCreateEntry(e txnif.TxnEntry) error {
	if db.createEntry != nil {
		panic("logic error")
	}
	db.store.IncreateWriteCnt()
	db.store.txn.GetMemo().AddCatalogChange()
	db.createEntry = e
	return nil
}

func (db *txnDB) SetDropEntry(e txnif.TxnEntry) error {
	if db.dropEntry != nil {
		panic("logic error")
	}
	db.store.IncreateWriteCnt()
	db.store.txn.GetMemo().AddCatalogChange()
	db.dropEntry = e
	return nil
}

func (db *txnDB) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readedObjects, readedTombstone []*common.ID) (err error) {
	table, err := db.getOrSetTable(tableId)
	if err != nil {
		return
	}
	return table.LogTxnEntry(entry, readedObjects, readedTombstone)
}

func (db *txnDB) Close() error {
	var err error
	for _, table := range db.tables {
		if err = table.Close(); err != nil {
			break
		}
	}
	db.tables = nil
	db.createEntry = nil
	db.dropEntry = nil
	return err
}

func (db *txnDB) BatchDedup(id uint64, pk containers.Vector) (err error) {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return moerr.NewNotFoundNoCtx()
	}

	return table.DoBatchDedup(pk)
}

func (db *txnDB) Append(ctx context.Context, id uint64, bat *containers.Batch) error {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return moerr.NewNotFoundNoCtx()
	}
	return table.Append(ctx, bat)
}

func (db *txnDB) AddObjsWithMetaLoc(
	ctx context.Context,
	tid uint64,
	stats containers.Vector) error {
	table, err := db.getOrSetTable(tid)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return moerr.NewNotFoundNoCtx()
	}
	return table.AddObjsWithMetaLoc(ctx, stats)
}

// func (db *txnDB) DeleteOne(table *txnTable, id *common.ID, row uint32, dt handle.DeleteType) (err error) {
// 	changed, nid, nrow, err := table.TransferDeleteIntent(id, row)
// 	if err != nil {
// 		return err
// 	}
// 	if !changed {
// 		return table.RangeDelete(id, row, row, dt)
// 	}
// 	return table.RangeDelete(nid, nrow, nrow, dt)
// }

func (db *txnDB) RangeDelete(
	id *common.ID, start, end uint32,
	pkVec containers.Vector, dt handle.DeleteType,
) (err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return moerr.NewNotFoundNoCtx()
	}
	return table.RangeDelete(id, start, end, pkVec, dt)
}

func (db *txnDB) TryDeleteByDeltaloc(
	id *common.ID, deltaloc objectio.Location,
) (ok bool, err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		return false, moerr.NewNotFoundNoCtx()
	}
	return table.TryDeleteByDeltaloc(id, deltaloc)
}

func (db *txnDB) GetByFilter(ctx context.Context, tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	table, err := db.getOrSetTable(tid)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	return table.GetByFilter(ctx, filter)
}

func (db *txnDB) GetValue(id *common.ID, row uint32, colIdx uint16, skipCheckDelete bool) (v any, isNull bool, err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	return table.GetValue(context.Background(), id, row, colIdx, skipCheckDelete)
}

func (db *txnDB) CreateRelation(def any) (relation handle.Relation, err error) {
	schema := def.(*catalog.Schema)
	var factory catalog.TableDataFactory
	if db.store.dataFactory != nil {
		factory = db.store.dataFactory.MakeTableFactory()
	}
	meta, err := db.entry.CreateTableEntry(schema, db.store.txn, factory)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(table)
	table.SetCreateEntry(meta)
	return
}

func (db *txnDB) CreateRelationWithTableId(tableId uint64, def any) (relation handle.Relation, err error) {
	schema := def.(*catalog.Schema)
	var factory catalog.TableDataFactory
	if db.store.dataFactory != nil {
		factory = db.store.dataFactory.MakeTableFactory()
	}
	meta, err := db.entry.CreateTableEntryWithTableId(schema, db.store.txn, factory, tableId)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(table)
	table.SetCreateEntry(meta)
	return
}

func (db *txnDB) DropRelationByName(name string) (relation handle.Relation, err error) {
	hasNewTxnEntry, meta, err := db.entry.DropTableEntry(name, db.store.txn)
	if err != nil {
		return nil, err
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(table)
	if hasNewTxnEntry {
		err = table.SetDropEntry(meta)
	}
	return
}

func (db *txnDB) DropRelationByID(id uint64) (relation handle.Relation, err error) {
	hasNewTxnEntry, meta, err := db.entry.DropTableEntryByID(id, db.store.txn)
	if err != nil {
		return nil, err
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(table)
	if hasNewTxnEntry {
		err = table.SetDropEntry(meta)
	}
	return
}

func (db *txnDB) UnsafeGetRelation(id uint64) (relation handle.Relation, err error) {
	meta, err := db.entry.GetTableEntryByID(id)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(table)
	return
}

func (db *txnDB) GetRelationByName(name string) (relation handle.Relation, err error) {
	meta, err := db.entry.TxnGetTableEntryByName(name, db.store.txn)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(table)
	return
}

func (db *txnDB) GetRelationByID(id uint64) (relation handle.Relation, err error) {
	meta, err := db.entry.TxnGetTableEntryByID(id, db.store.txn)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(table)
	return
}

func (db *txnDB) GetObject(id *common.ID, isTombstone bool) (obj handle.Object, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetObject(id.ObjectID(), isTombstone)
}

func (db *txnDB) CreateObject(tid uint64, isTombstone bool) (obj handle.Object, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateObject(isTombstone)
}
func (db *txnDB) CreateNonAppendableObject(tid uint64, opt *objectio.CreateObjOpt, isTombstone bool) (obj handle.Object, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	opt.WithIsTombstone(isTombstone)
	return table.CreateNonAppendableObject(opt)
}

func (db *txnDB) UpdateObjectStats(id *common.ID, stats *objectio.ObjectStats, isTombstone bool) error {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	table.UpdateObjectStats(id, stats, isTombstone)
	return nil
}
func (db *txnDB) getOrSetTable(id uint64) (table *txnTable, err error) {
	db.mu.RLock()
	table = db.tables[id]
	db.mu.RUnlock()
	if table != nil {
		return
	}
	var entry *catalog.TableEntry
	if entry, err = db.entry.GetTableEntryByID(id); err != nil {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	table = db.tables[id]
	if table != nil {
		return
	}
	if db.store.warChecker == nil {
		db.store.warChecker = newWarChecker(db.store.txn, db.store.catalog)
	}
	table, err = newTxnTable(db.store, entry)
	if err != nil {
		return
	}
	table.idx = len(db.tables)
	db.tables[id] = table
	return
}

func (db *txnDB) SoftDeleteObject(id *common.ID, isTombstone bool) (err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteObject(id.ObjectID(), isTombstone)
}
func (db *txnDB) NeedRollback() bool {
	return db.createEntry != nil && db.dropEntry != nil
}
func (db *txnDB) ApplyRollback() (err error) {
	if db.createEntry != nil {
		if err = db.createEntry.ApplyRollback(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.ApplyRollback(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.ApplyRollback(); err != nil {
			return
		}
	}
	return
}

func (db *txnDB) WaitPrepared() (err error) {
	for _, table := range db.tables {
		table.WaitSynced()
	}
	return
}

func (db *txnDB) ApplyCommit() (err error) {
	now := time.Now()
	if db.createEntry != nil {
		if err = db.createEntry.ApplyCommit(db.store.txn.GetID()); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.ApplyCommit(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.ApplyCommit(db.store.txn.GetID()); err != nil {
			return
		}
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("Txn-%X ApplyCommit Takes %s", db.store.txn.GetID(), time.Since(now))
	})
	return
}

func (db *txnDB) Freeze() (err error) {
	for _, table := range db.tables {
		if table.NeedRollback() {
			if err = table.PrepareRollback(); err != nil {
				return
			}
			delete(db.tables, table.GetID())
		}
	}
	for _, table := range db.tables {
		if err = table.PrePreareTransfer(txnif.FreezePhase, table.store.rt.Now()); err != nil {
			return
		}
	}
	return
}

func (db *txnDB) PrePrepare(ctx context.Context) (err error) {
	for _, table := range db.tables {
		if err = table.PrePreareTransfer(txnif.PrePreparePhase, table.store.rt.Now()); err != nil {
			return
		}
	}

	now := time.Now()
	for _, table := range db.tables {
		if err = table.PrePrepareDedup(ctx, true); err != nil {
			return
		}
		if err = table.PrePrepareDedup(ctx, false); err != nil {
			return
		}
	}
	v2.TxnTNPrePrepareDeduplicateDurationHistogram.Observe(time.Since(now).Seconds())

	for _, table := range db.tables {
		if err = table.PrePrepare(); err != nil {
			return
		}
	}
	return
}

func (db *txnDB) PrepareCommit() (err error) {
	now := time.Now()
	if db.createEntry != nil {
		if err = db.createEntry.PrepareCommit(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.PrepareCommit(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.PrepareCommit(); err != nil {
			return
		}
	}

	common.DoIfDebugEnabled(func() {
		logutil.Debugf("Txn-%X PrepareCommit Takes %s", db.store.txn.GetID(), time.Since(now))
	})

	return
}

func (db *txnDB) PreApplyCommit() (err error) {
	for _, table := range db.tables {
		// table.ApplyAppend()
		if err = table.PreApplyCommit(); err != nil {
			return
		}
	}
	return
}

func (db *txnDB) CollectCmd(cmdMgr *commandManager) (err error) {
	if db.createEntry != nil {
		csn := cmdMgr.GetCSN()
		entry := db.createEntry
		cmd, err := entry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		cmdMgr.AddCmd(cmd)
		db.ddlCSN = csn
	}
	tables := make([]*txnTable, len(db.tables))
	for _, table := range db.tables {
		tables[table.idx] = table
	}
	for _, table := range tables {
		if err = table.CollectCmd(cmdMgr); err != nil {
			return
		}
	}
	if db.dropEntry != nil {
		csn := cmdMgr.GetCSN()
		cmd, err := db.dropEntry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		cmdMgr.AddCmd(cmd)
		db.ddlCSN = csn
	}
	return
}

func (db *txnDB) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// TODO
}

func (db *txnDB) PrepareRollback() error {
	var err error
	if db.createEntry != nil {
		if err := db.createEntry.PrepareRollback(); err != nil {
			return err
		}
	}
	for _, table := range db.tables {
		if err = table.PrepareRollback(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err := db.dropEntry.PrepareRollback(); err != nil {
			return err
		}
	}

	return err
}

func (db *txnDB) CleanUp() {
	for _, tbl := range db.tables {
		tbl.CleanUp()
	}
}

func (db *txnDB) FillInWorkspaceDeletes(id *common.ID, deletes **nulls.Nulls) error {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	return table.FillInWorkspaceDeletes(id.BlockID, deletes)
}

func (db *txnDB) IsDeletedInWorkSpace(id *common.ID, row uint32) (bool, error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return false, err
	}
	return table.IsDeletedInWorkSpace(id.BlockID, row)
}
