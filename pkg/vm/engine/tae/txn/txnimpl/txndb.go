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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type txnDB struct {
	store       *txnStore
	tables      map[uint64]*txnTable
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
	db.createEntry = e
	return nil
}

func (db *txnDB) SetDropEntry(e txnif.TxnEntry) error {
	if db.dropEntry != nil {
		panic("logic error")
	}
	db.store.IncreateWriteCnt()
	db.dropEntry = e
	return nil
}

func (db *txnDB) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	table, err := db.getOrSetTable(tableId)
	if err != nil {
		return
	}
	return table.LogTxnEntry(entry, readed)
}

func (db *txnDB) LogSegmentID(tid, sid uint64) {
	table, _ := db.getOrSetTable(tid)
	table.LogSegmentID(sid)
}

func (db *txnDB) LogBlockID(tid, bid uint64) {
	table, _ := db.getOrSetTable(tid)
	table.LogBlockID(bid)
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

func (db *txnDB) BatchDedup(id uint64, pks ...containers.Vector) (err error) {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return data.ErrNotFound
	}

	return table.DoBatchDedup(pks...)
}

func (db *txnDB) Append(id uint64, bat *containers.Batch) error {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return data.ErrNotFound
	}
	return table.Append(bat)
}

func (db *txnDB) RangeDelete(id *common.ID, start, end uint32, dt handle.DeleteType) (err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return data.ErrNotFound
	}
	return table.RangeDelete(id, start, end, dt)
}

func (db *txnDB) GetByFilter(tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	table, err := db.getOrSetTable(tid)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = data.ErrNotFound
		return
	}
	return table.GetByFilter(filter)
}

func (db *txnDB) GetValue(id *common.ID, row uint32, colIdx uint16) (v any, err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = data.ErrNotFound
		return
	}
	return table.GetValue(id, row, colIdx)
}

func (db *txnDB) Update(id *common.ID, row uint32, colIdx uint16, v any) (err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return data.ErrNotFound
	}
	return table.Update(id, row, colIdx, v)
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

func (db *txnDB) DropRelationByName(name string) (relation handle.Relation, err error) {
	meta, err := db.entry.DropTableEntry(name, db.store.txn)
	if err != nil {
		return nil, err
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(table)
	err = table.SetDropEntry(meta)
	return
}

func (db *txnDB) GetRelationByName(name string) (relation handle.Relation, err error) {
	meta, err := db.entry.GetTableEntry(name, db.store.txn)
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

func (db *txnDB) GetSegment(id *common.ID) (seg handle.Segment, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetSegment(id.SegmentID)
}

func (db *txnDB) CreateSegment(tid uint64) (seg handle.Segment, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateSegment()
}

func (db *txnDB) CreateNonAppendableSegment(tid uint64) (seg handle.Segment, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateNonAppendableSegment()
}

func (db *txnDB) getOrSetTable(id uint64) (table *txnTable, err error) {
	table = db.tables[id]
	if table == nil {
		var entry *catalog.TableEntry
		if entry, err = db.entry.GetTableEntryByID(id); err != nil {
			return
		}
		if db.store.warChecker == nil {
			db.store.warChecker = newWarChecker(db.store.txn, db.store.catalog)
		}
		table = newTxnTable(db.store, entry)
		table.idx = len(db.tables)
		db.tables[id] = table
	}
	return
}

func (db *txnDB) CreateNonAppendableBlock(id *common.ID) (blk handle.Block, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.CreateNonAppendableBlock(id.SegmentID)
}

func (db *txnDB) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetBlock(id)
}

func (db *txnDB) CreateBlock(tid, sid uint64) (blk handle.Block, err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateBlock(sid)
}

func (db *txnDB) SoftDeleteBlock(id *common.ID) (err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteBlock(id)
}

func (db *txnDB) SoftDeleteSegment(id *common.ID) (err error) {
	var table *txnTable
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteSegment(id.SegmentID)
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

func (db *txnDB) ApplyCommit() (err error) {
	now := time.Now()
	for _, table := range db.tables {
		table.WaitSynced()
	}
	if db.createEntry != nil {
		if err = db.createEntry.ApplyCommit(db.store.cmdMgr.MakeLogIndex(db.ddlCSN)); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.ApplyCommit(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.ApplyCommit(db.store.cmdMgr.MakeLogIndex(db.ddlCSN)); err != nil {
			return
		}
	}
	logutil.Debugf("Txn-%d ApplyCommit Takes %s", db.store.txn.GetID(), time.Since(now))
	return
}

func (db *txnDB) PreCommit() (err error) {
	for _, table := range db.tables {
		if err = table.PreCommitDedup(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.PreCommit(); err != nil {
			panic(err)
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

	logutil.Debugf("Txn-%d PrepareCommit Takes %s", db.store.txn.GetID(), time.Since(now))

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
		if db.dropEntry != nil {
			entry = db.createEntry.(*catalog.DBEntry).CloneCreateEntry()
		}
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
