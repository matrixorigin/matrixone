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

package catalog

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
const (
	SnapshotAttr_TID            = "table_id"
	SnapshotAttr_DBID           = "db_id"
	ObjectAttr_ID               = "id"
	ObjectAttr_CreateAt         = "create_at"
	ObjectAttr_SegNode          = "seg_node"
	SnapshotAttr_BlockMaxRow    = "block_max_row"
	SnapshotAttr_ObjectMaxBlock = "Object_max_block"
	SnapshotAttr_SchemaExtra    = "schema_extra"
	AccountIDDbNameTblName      = "account_id_db_name_tbl_name"
	AccountIDDbName             = "account_id_db_name"
	ObjectAttr_ObjectStats      = "object_stats"
	ObjectAttr_State            = "state"
	ObjectAttr_Sorted           = "sorted"
	EntryNode_CreateAt          = "create_at"
	EntryNode_DeleteAt          = "delete_at"
)

type DataFactory interface {
	MakeTableFactory() TableDataFactory
	MakeObjectFactory() ObjectDataFactory
	MakeTombstoneFactory() TombstoneFactory
}

type Catalog struct {
	*IDAlloctor
	*sync.RWMutex

	usageMemo any
	entries   map[uint64]*common.GenericDLNode[*DBEntry]
	nameNodes map[string]*nodeList[*DBEntry]
	link      *common.GenericSortedDList[*DBEntry]
	nodesMu   sync.RWMutex
	gcTS      types.TS
}

func MockCatalog() *Catalog {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList((*DBEntry).Less),
	}
	catalog.InitSystemDB()
	return catalog
}

func OpenCatalog(usageMemo any) (*Catalog, error) {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList((*DBEntry).Less),
		usageMemo:  usageMemo,
	}
	catalog.InitSystemDB()
	return catalog, nil
}

//#region Catalog Manipulation

func genDBFullName(tenantID uint32, name string) string {
	if name == pkgcatalog.MO_CATALOG {
		tenantID = 0
	}
	return fmt.Sprintf("%d-%s", tenantID, name)
}

func (catalog *Catalog) SetUsageMemo(memo any) {
	catalog.usageMemo = memo
}

func (catalog *Catalog) GetUsageMemo() any {
	return catalog.usageMemo
}

func (catalog *Catalog) InitSystemDB() {
	sysDB := NewSystemDBEntry(catalog)
	dbTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_DATABASE_ID, SystemDBSchema)
	tableTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_TABLES_ID, SystemTableSchema)
	columnTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_COLUMNS_ID, SystemColumnSchema)
	err := sysDB.AddEntryLocked(dbTables, nil, false)
	if err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(tableTables, nil, false); err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(columnTables, nil, false); err != nil {
		panic(err)
	}
	if err = catalog.AddEntryLocked(sysDB, nil, false); err != nil {
		panic(err)
	}
}

func (catalog *Catalog) GCByTS(ctx context.Context, ts types.TS) {
	logutil.Infof("GC Catalog %v", ts.ToString())
	processor := LoopProcessor{}
	processor.DatabaseFn = func(d *DBEntry) error {
		needGC := d.DeleteBefore(ts)
		if needGC {
			catalog.RemoveDBEntry(d)
		}
		return nil
	}
	processor.TableFn = func(te *TableEntry) error {
		needGC := te.DeleteBefore(ts)
		if needGC {
			db := te.db
			db.RemoveEntry(te)
		}
		return nil
	}
	processor.ObjectFn = func(se *ObjectEntry) error {
		se.RLock()
		needGC := se.DeleteBeforeLocked(ts) && !se.InMemoryDeletesExistedLocked()
		se.RUnlock()
		if needGC {
			tbl := se.table
			tbl.RemoveEntry(se)
		}
		return nil
	}
	processor.TombstoneFn = func(t data.Tombstone) error {
		obj := t.GetObject().(*ObjectEntry)
		obj.RLock()
		needGC := obj.DeleteBeforeLocked(ts) && !obj.InMemoryDeletesExistedLocked()
		obj.RUnlock()
		if needGC {
			tbl := obj.table
			tbl.GCTombstone(obj.ID)
		}
		return nil
	}
	err := catalog.RecurLoop(&processor)
	if err != nil {
		panic(err)
	}
	catalog.gcTS = ts
}
func (catalog *Catalog) CheckMetadata() {
	logutil.Infof("[MetadataCheck] Start")
	p := &LoopProcessor{}
	p.ObjectFn = catalog.checkObject
	p.TombstoneFn = catalog.checkTombstone
	catalog.RecurLoop(p)
}
func (catalog *Catalog) checkTombstone(t data.Tombstone) error {
	obj := t.GetObject().(*ObjectEntry)
	_, err := obj.GetTable().GetObjectByID(&obj.ID)
	if err != nil {
		logutil.Infof("[MetadataCheck] tombstone and object doesn't match, err %v, obj %v, tombstone %v",
			err,
			obj.PPString(3, 0, ""),
			t.StringLocked(3, 0, ""))
	}
	return nil
}
func (catalog *Catalog) checkObject(o *ObjectEntry) error {
	o.RLock()
	defer o.RUnlock()
	if o.Depth() > 2 {
		logutil.Infof("[MetadataCheck] object mvcc link is too long, depth %d, obj %v", o.Depth(), o.PPStringLocked(3, 0, ""))
	}
	if o.IsAppendable() && o.HasDropCommittedLocked() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Infof("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Infof("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Infof("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !catalog.gcTS.IsEmpty() {
		if o.HasDropCommittedLocked() && o.DeleteBeforeLocked(catalog.gcTS) && !o.InMemoryDeletesExistedLocked() {
			logutil.Infof("[MetadataCheck] object should not exist, gcTS %v, obj %v", catalog.gcTS.ToString(), o.PPStringLocked(3, 0, ""))
		}
	}

	duration := time.Minute * 10
	ts := types.BuildTS(time.Now().UTC().UnixNano()-duration.Nanoseconds(), 0)
	if o.HasDropCommittedLocked() && o.DeleteBeforeLocked(ts) {
		if o.InMemoryDeletesExistedLocked() {
			logutil.Infof("[MetadataCheck] object has in memory deletes %v after deleted, obj %v, tombstone %v",
				duration,
				o.PPStringLocked(3, 0, ""),
				o.GetTable().TryGetTombstone(o.ID).StringLocked(3, 0, ""))
		}
	}
	return nil
}

func (catalog *Catalog) Close() error {
	return nil
}

func (catalog *Catalog) GetItemNodeByIDLocked(id uint64) *common.GenericDLNode[*DBEntry] {
	return catalog.entries[id]
}

func (catalog *Catalog) GetDatabaseByID(id uint64) (db *DBEntry, err error) {
	catalog.RLock()
	defer catalog.RUnlock()
	node := catalog.entries[id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	db = node.GetPayload()
	return
}

func (catalog *Catalog) AddEntryLocked(database *DBEntry, txn txnif.TxnReader, skipDedup bool) error {
	nn := catalog.nameNodes[database.GetFullName()]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.ID] = n

		nn := newNodeList(catalog.GetItemNodeByIDLocked,
			dbVisibilityFn[*DBEntry],
			&catalog.nodesMu,
			database.name)
		catalog.nameNodes[database.GetFullName()] = nn

		nn.CreateNode(database.ID)
	} else {
		node := nn.GetNode()
		if !skipDedup {
			record := node.GetPayload()
			err := record.PrepareAdd(txn)
			if err != nil {
				return err
			}
		}
		n := catalog.link.Insert(database)
		catalog.entries[database.ID] = n
		nn.CreateNode(database.ID)
	}
	return nil
}

func (catalog *Catalog) TxnGetDBEntryByName(name string, txn txnif.AsyncTxn) (*DBEntry, error) {
	catalog.RLock()
	defer catalog.RUnlock()
	fullName := genDBFullName(txn.GetTenantID(), name)
	node := catalog.nameNodes[fullName]
	if node == nil {
		return nil, moerr.NewBadDBNoCtx(name)
	}
	n, err := node.TxnGetNodeLocked(txn, "")
	if err != nil {
		return nil, err
	}
	return n.GetPayload(), nil
}

func (catalog *Catalog) TxnGetDBEntryByID(id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	dbEntry, err := catalog.GetDatabaseByID(id)
	if err != nil {
		return nil, err
	}
	visiable, dropped := dbEntry.GetVisibility(txn)
	if !visiable || dropped {
		return nil, moerr.GetOkExpectedEOB()
	}
	return dbEntry, nil
}

// RemoveDBEntry removes a database entry from the catalog physically, triggered by GC Task
func (catalog *Catalog) RemoveDBEntry(database *DBEntry) error {
	if database.IsSystemDB() {
		logutil.Warnf("system db cannot be removed")
		return moerr.NewTAEErrorNoCtx("not permitted")
	}
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(database.String()))
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.ID]; !ok {
		return moerr.NewBadDBNoCtx(database.GetName())
	} else {
		nn := catalog.nameNodes[database.GetFullName()]
		nn.DeleteNode(database.ID)
		catalog.link.Delete(n)
		if nn.Length() == 0 {
			delete(catalog.nameNodes, database.GetFullName())
		}
		delete(catalog.entries, database.ID)
	}
	return nil
}

// DropDBEntry attach a drop mvvc node the entry.
func (catalog *Catalog) DropDBEntry(entry *DBEntry, txn txnif.AsyncTxn) (isNewMVCCNode bool, err error) {
	if entry.IsSystemDB() {
		return false, moerr.NewTAEErrorNoCtx("not permitted")
	}
	entry.Lock()
	defer entry.Unlock()
	isNewMVCCNode, err = entry.DropEntryLocked(txn)
	return
}

func (catalog *Catalog) DropDBEntryByName(
	name string,
	txn txnif.AsyncTxn) (isNewMVCCNode bool, deleted *DBEntry, err error) {
	deleted, err = catalog.TxnGetDBEntryByName(name, txn)
	if err != nil {
		return
	}
	isNewMVCCNode, err = catalog.DropDBEntry(deleted, txn)
	return
}

func (catalog *Catalog) DropDBEntryByID(id uint64, txn txnif.AsyncTxn) (isNewMVCCNode bool, deleted *DBEntry, err error) {
	deleted, err = catalog.TxnGetDBEntryByID(id, txn)
	if err != nil {
		return
	}
	isNewMVCCNode, err = catalog.DropDBEntry(deleted, txn)
	return
}

func (catalog *Catalog) CreateDBEntry(name, createSql, datTyp string, txn txnif.AsyncTxn) (*DBEntry, error) {
	id := catalog.NextDB()
	return catalog.CreateDBEntryWithID(name, createSql, datTyp, id, txn)
}

func (catalog *Catalog) CreateDBEntryWithID(name, createSql, datTyp string, id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	defer catalog.Unlock()
	if _, exist := catalog.entries[id]; exist {
		return nil, moerr.GetOkExpectedDup()
	}
	entry := NewDBEntryWithID(catalog, name, createSql, datTyp, id, txn)
	err = catalog.AddEntryLocked(entry, txn, false)

	return entry, err
}

//#endregion

//#region - Utils for Catalog

func (catalog *Catalog) MakeDBIt(reverse bool) *common.GenericSortedDListIt[*DBEntry] {
	catalog.RLock()
	defer catalog.RUnlock()
	return common.NewGenericSortedDListIt(catalog.RWMutex, catalog.link, reverse)
}

func (catalog *Catalog) SimplePPString(level common.PPLevel) string {
	return catalog.PPString(level, 0, "")
}

func (catalog *Catalog) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	cnt := 0
	it := catalog.MakeDBIt(true)
	for ; it.Valid(); it.Next() {
		cnt++
		entry := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(entry.PPString(level, depth+1, ""))
	}

	var w2 bytes.Buffer
	_, _ = w2.WriteString(fmt.Sprintf("CATALOG[CNT=%d]", cnt))
	_, _ = w2.WriteString(w.String())
	return w2.String()
}

func (catalog *Catalog) RecurLoop(processor Processor) (err error) {
	dbIt := catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		dbEntry := dbIt.Get().GetPayload()
		if err = processor.OnDatabase(dbEntry); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				continue
			}
			break
		}
		if err = dbEntry.RecurLoop(processor); err != nil {
			return
		}
		if err = processor.OnPostDatabase(dbEntry); err != nil {
			break
		}
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}

//#endregion
