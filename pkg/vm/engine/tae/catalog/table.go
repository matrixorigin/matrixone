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
	"math"
	"strings"
	"sync/atomic"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
)

type TableDataFactory = func(meta *TableEntry) data.Table

func tableVisibilityFn[T *TableEntry](n *common.GenericDLNode[*TableEntry], txn txnif.TxnReader) (visible, dropped bool, name string) {
	table := n.GetPayload()
	visible, dropped, name = table.GetVisibilityAndName(txn)
	return
}

type TableEntry struct {
	*BaseEntryImpl[*TableMVCCNode]
	*TableNode
	Stats     *common.TableCompactStat
	ID        uint64
	db        *DBEntry
	tableData data.Table
	rows      atomic.Uint64

	// fullname is format as 'tenantID-tableName', the tenantID prefix is only used 'mo_catalog' database
	fullName string

	dataObjects      *ObjectList
	tombstoneObjects *ObjectList
}

func genTblFullName(tenantID uint32, name string) string {
	if name == pkgcatalog.MO_DATABASE || name == pkgcatalog.MO_TABLES || name == pkgcatalog.MO_COLUMNS {
		tenantID = 0
	}
	return fmt.Sprintf("%d-%s", tenantID, name)
}

func NewTableEntry(db *DBEntry, schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory) *TableEntry {
	id := db.catalog.NextTable()
	return NewTableEntryWithTableId(db, schema, txnCtx, dataFactory, id)
}

func NewTableEntryWithTableId(db *DBEntry, schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory, tableId uint64) *TableEntry {
	if txnCtx != nil {
		// Only in unit test, txnCtx can be nil
		schema.AcInfo.TenantID = txnCtx.GetTenantID()
		schema.AcInfo.UserID, schema.AcInfo.RoleID = txnCtx.GetUserAndRoleID()
	}
	schema.AcInfo.CreateAt = types.CurrentTimestamp()
	e := &TableEntry{
		ID: tableId,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		db:               db,
		TableNode:        &TableNode{},
		Stats:            common.NewTableCompactStat(),
		dataObjects:      NewObjectList(false),
		tombstoneObjects: NewObjectList(true),
	}
	e.TableNode.schema.Store(schema)
	if dataFactory != nil {
		e.tableData = dataFactory(e)
	}
	e.CreateWithTxnAndSchema(txnCtx, schema)
	return e
}

func NewSystemTableEntry(db *DBEntry, id uint64, schema *Schema) *TableEntry {
	e := NewReplayTableEntry()
	e.ID = id
	e.db = db

	e.TableNode.schema.Store(schema)
	e.CreateWithTSLocked(types.SystemDBTS, &TableMVCCNode{Schema: schema})

	if DefaultTableDataFactory != nil {
		e.tableData = DefaultTableDataFactory(e)
	}
	return e
}

func NewReplayTableEntry() *TableEntry {
	e := &TableEntry{
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		dataObjects:      NewObjectList(false),
		tombstoneObjects: NewObjectList(true),
		Stats:            common.NewTableCompactStat(),
		TableNode:        &TableNode{},
	}
	return e
}

func MockStaloneTableEntry(id uint64, schema *Schema) *TableEntry {
	node := &TableNode{}
	node.schema.Store(schema)
	return &TableEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		TableNode:        node,
		dataObjects:      NewObjectList(false),
		tombstoneObjects: NewObjectList(true),
		Stats:            common.NewTableCompactStat(),
	}
}

func (entry *TableEntry) GetID() uint64 { return entry.ID }
func (entry *TableEntry) IsVirtual() bool {
	if !entry.db.IsSystemDB() {
		return false
	}
	name := entry.GetLastestSchemaLocked(false).Name
	return name == pkgcatalog.MO_DATABASE ||
		name == pkgcatalog.MO_TABLES ||
		name == pkgcatalog.MO_COLUMNS
}

func (entry *TableEntry) GetRows() uint64 {
	return entry.rows.Load()
}

func (entry *TableEntry) AddRows(delta uint64) uint64 {
	return entry.rows.Add(delta)
}

func (entry *TableEntry) RemoveRows(delta uint64) uint64 {
	return entry.rows.Add(^(delta - 1))
}
func (entry *TableEntry) GetObjectByID(id *types.Objectid, isTombstone bool) (obj *ObjectEntry, err error) {
	if isTombstone {
		return entry.getTombstoneObjectByID(id)
	} else {
		return entry.getDataObjectByID(id)
	}
}
func (entry *TableEntry) getDataObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	return entry.dataObjects.GetObjectByID(id)
}
func (entry *TableEntry) getTombstoneObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	return entry.tombstoneObjects.GetObjectByID(id)
}

func (entry *TableEntry) MakeTombstoneObjectIt() btree.IterG[*ObjectEntry] {
	return entry.tombstoneObjects.tree.Load().Iter()
}

func (entry *TableEntry) MakeDataObjectIt() btree.IterG[*ObjectEntry] {
	return entry.dataObjects.tree.Load().Iter()
}

func (entry *TableEntry) MakeObjectIt(isTombstone bool) btree.IterG[*ObjectEntry] {
	if isTombstone {
		return entry.MakeTombstoneObjectIt()
	}
	return entry.MakeDataObjectIt()
}

func (entry *TableEntry) CreateObject(
	txn txnif.AsyncTxn,
	opts *objectio.CreateObjOpt,
	dataFactory ObjectDataFactory,
) (created *ObjectEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	created = NewObjectEntry(entry, txn, *opts.Stats, dataFactory, opts.IsTombstone)
	entry.AddEntryLocked(created)
	return
}

func (entry *TableEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Table
	entry.RLock()
	defer entry.RUnlock()
	return newTableCmd(id, cmdType, entry), nil
}
func (entry *TableEntry) AddEntryLocked(objectEntry *ObjectEntry) {
	if objectEntry.IsTombstone {
		entry.tombstoneObjects.Set(objectEntry, true)
	} else {
		entry.dataObjects.Set(objectEntry, true)
	}
}

func (entry *TableEntry) deleteEntryLocked(objectEntry *ObjectEntry) error {
	if objectEntry.IsTombstone {
		entry.tombstoneObjects.deleteEntryLocked(objectEntry.SortHint)
		return nil
	}
	entry.dataObjects.deleteEntryLocked(objectEntry.SortHint)
	return nil
}

// GetLastestSchemaLocked returns the latest committed schema with entry Not locked
func (entry *TableEntry) GetLastestSchemaLocked(isTombstone bool) *Schema {
	if isTombstone {
		tombstoneSchema := entry.tombstoneSchema.Load()
		if tombstoneSchema == nil {
			entry.tombstoneSchema.Store(GetTombstoneSchema(entry.schema.Load()))
			tombstoneSchema = entry.tombstoneSchema.Load()
		}
		return tombstoneSchema
	}
	return entry.schema.Load()
}

// GetLastestSchema returns the latest committed schema with entry locked
func (entry *TableEntry) GetLastestSchema(isTombstone bool) *Schema {
	entry.RLock()
	defer entry.RUnlock()

	return entry.GetLastestSchemaLocked(isTombstone)
}

// GetVisibleSchema returns committed schema visible at the given txn
func (entry *TableEntry) GetVisibleSchema(txn txnif.TxnReader, isTombstone bool) (schema *Schema) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.GetVisibleNodeLocked(txn)
	if node != nil {
		if isTombstone {
			return node.BaseNode.GetTombstoneSchema()
		}
		return node.BaseNode.Schema
	}
	return nil
}

func (entry *TableEntry) GetVersionSchema(ver uint32) *Schema {
	entry.RLock()
	defer entry.RUnlock()
	var ret *Schema
	entry.LoopChainLocked(func(m *MVCCNode[*TableMVCCNode]) bool {
		if cur := m.BaseNode.Schema.Version; cur > ver {
			return true
		} else if cur == ver {
			ret = m.BaseNode.Schema
		}
		return false
	})
	return ret
}

func (entry *TableEntry) GetColDefs() []*ColDef {
	return entry.GetLastestSchemaLocked(false).ColDefs
}

func (entry *TableEntry) GetFullName() string {
	if len(entry.fullName) == 0 {
		schema := entry.GetLastestSchemaLocked(false)
		entry.fullName = genTblFullName(schema.AcInfo.TenantID, schema.Name)
	}
	return entry.fullName
}

func (entry *TableEntry) GetDB() *DBEntry {
	return entry.db
}

func (entry *TableEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	it := entry.MakeObjectIt(false)
	defer it.Release()
	for it.Next() {
		objectEntry := it.Item()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(objectEntry.PPString(level, depth+1, prefix))
	}
	if level > common.PPL2 {
		it := entry.MakeTombstoneObjectIt()
		defer it.Release()
		for it.Next() {
			objectEntry := it.Item()
			_ = w.WriteByte('\n')
			_, _ = w.WriteString(objectEntry.PPString(level, depth+1, prefix))
		}
	}
	return w.String()
}

type TableStat struct {
	ObjectCnt int
	Loaded    int
	Rows      int
	OSize     int
	Csize     int
}

func (entry *TableEntry) ObjectStats(level common.PPLevel, start, end int) (stat TableStat, w bytes.Buffer) {

	it := entry.MakeDataObjectIt()
	defer it.Release()
	zonemapKind := common.ZonemapPrintKindNormal
	if schema := entry.GetLastestSchemaLocked(false); schema.HasSortKey() && strings.HasPrefix(schema.GetSingleSortKey().Name, "__") {
		zonemapKind = common.ZonemapPrintKindCompose
	}

	if level == common.PPL3 { // some magic, do not ask why
		zonemapKind = common.ZonemapPrintKindHex
	}

	scanCount := 0
	needCount := end - start
	if needCount < 0 {
		needCount = math.MaxInt
	}

	for it.Next() {
		objectEntry := it.Item()
		if !objectEntry.IsActive() {
			continue
		}
		scanCount++
		if scanCount <= start {
			continue
		}
		if needCount <= 0 {
			break
		}
		needCount--
		stat.ObjectCnt += 1
		if objectEntry.GetLoaded() {
			stat.Loaded += 1
			stat.Rows += int(objectEntry.GetRows())
			stat.OSize += int(objectEntry.GetOriginSize())
			stat.Csize += int(objectEntry.GetCompSize())
		}
		if level > common.PPL0 {
			_ = w.WriteByte('\n')
			_, _ = w.WriteString(objectEntry.ID().String())
			_ = w.WriteByte('\n')
			_, _ = w.WriteString("    ")
			_, _ = w.WriteString(objectEntry.StatsString(zonemapKind))
		}
		if w.Len() > 8*common.Const1MBytes {
			w.WriteString("\n...(truncated for too long, more than 8 MB)")
			break
		}
	}
	if level > common.PPL0 && stat.ObjectCnt > 0 {
		w.WriteByte('\n')
	}
	return
}

func (entry *TableEntry) ObjectStatsString(level common.PPLevel, start, end int) string {
	stat, detail := entry.ObjectStats(level, start, end)

	var avgCsize, avgRow, avgOsize int
	if stat.Loaded > 0 {
		avgRow = stat.Rows / stat.Loaded
		avgOsize = stat.OSize / stat.Loaded
		avgCsize = stat.Csize / stat.Loaded
	}

	summary := fmt.Sprintf(
		"summary: %d total, %d unknown, avgRow %d, avgOsize %s, avgCsize %v\n"+
			"Update History:\n  rows %v\n  dels %v ",
		stat.ObjectCnt, stat.ObjectCnt-stat.Loaded, avgRow, common.HumanReadableBytes(avgOsize), common.HumanReadableBytes(avgCsize),
		entry.Stats.RowCnt.String(), entry.Stats.RowDel.String(),
	)
	detail.WriteString(summary)
	return detail.String()
}

func (entry *TableEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *TableEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLockedWithLevel(level)
}
func (entry *TableEntry) StringLockedWithLevel(level common.PPLevel) string {
	name := entry.GetLastestSchemaLocked(false).Name
	if level <= common.PPL1 {
		return fmt.Sprintf("TBL[%d][name=%s][C@%s,D@%s]",
			entry.ID, name, entry.GetCreatedAtLocked().ToString(), entry.GetDeleteAtLocked().ToString())
	}
	return fmt.Sprintf("TBL%s[name=%s, id=%d]", entry.BaseEntryImpl.StringLocked(), name, entry.ID)
}

func (entry *TableEntry) StringLocked() string {
	return entry.StringLockedWithLevel(common.PPL1)
}

func (entry *TableEntry) GetCatalog() *Catalog { return entry.db.catalog }

func (entry *TableEntry) GetTableData() data.Table { return entry.tableData }

func (entry *TableEntry) LastAppendableObject(isTombstone bool) (obj *ObjectEntry) {
	it := entry.MakeObjectIt(isTombstone)
	defer it.Release()
	for ok := it.Last(); ok; ok = it.Prev() {
		itObj := it.Item()
		dropped := itObj.HasDropCommitted()
		if itObj.IsAppendable() && !dropped {
			obj = itObj
			break
		}
	}
	return obj
}

func (entry *TableEntry) AsCommonID() *common.ID {
	return &common.ID{
		DbID:    entry.GetDB().ID,
		TableID: entry.ID,
	}
}

func (entry *TableEntry) RecurLoop(processor Processor) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
	}()
	objIt := entry.MakeDataObjectIt()
	defer objIt.Release()
	for objIt.Next() {
		objectEntry := objIt.Item()
		if err := processor.OnObject(objectEntry); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				continue
			}
			return err
		}
		if err := processor.OnPostObject(objectEntry); err != nil {
			return err
		}
	}
	objIt = entry.MakeTombstoneObjectIt()
	for objIt.Next() {
		objectEntry := objIt.Item()
		if err := processor.OnTombstone(objectEntry); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				objIt.Next()
				continue
			}
			return err
		}
		if err := processor.OnPostObject(objectEntry); err != nil {
			return err
		}
	}
	return
}

func (entry *TableEntry) DropObjectEntry(id *types.Objectid, txn txnif.AsyncTxn, isTombstone bool) (deleted *ObjectEntry, err error) {
	objects := entry.dataObjects
	if isTombstone {
		objects = entry.tombstoneObjects
	}
	obj, isNewNode, err := objects.DropObjectByID(id, txn)
	if err == nil && isNewNode {
		deleted = obj
	}
	return
}
func (entry *TableEntry) getObjectList(isTombstone bool) *ObjectList {
	if isTombstone {
		return entry.tombstoneObjects
	}
	return entry.dataObjects
}
func (entry *TableEntry) RemoveEntry(objectEntry *ObjectEntry) (err error) {
	logutil.Debug("[Catalog]", common.OperationField("remove"),
		common.OperandField(objectEntry.String()))
	// objectEntry.Close()
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(objectEntry)
}

func (entry *TableEntry) PrepareRollback() (err error) {
	// Safety: in commit queue, that's ok without lock
	t := entry.GetLatestNodeLocked()
	if schema := t.BaseNode.Schema; schema.Extra.OldName != "" {
		fullname := genTblFullName(schema.AcInfo.TenantID, schema.Name)
		entry.GetDB().RollbackRenameTable(fullname, entry.ID)
	}
	var isEmpty bool
	isEmpty, err = entry.BaseEntryImpl.PrepareRollback()
	if err != nil {
		return
	}
	if isEmpty {
		err = entry.GetDB().RemoveEntry(entry)
		if err != nil {
			return
		}
	}
	return
}

/*
s: start
p: prepare
c: commit

	         	    old schema  <- | -> new schema
	        					   |
		  s------------------p-----c         AlterColumn Txn

Append Txn:

	          s------------p----c               Yes
	              s-------------p--------c      Yes
	s-----------------------p---------c         Yes
	           s----------------------p         No, schema at s is not same with schema at p
*/
func (entry *TableEntry) ApplyCommit(id string) (err error) {
	err = entry.BaseEntryImpl.ApplyCommit(id)
	if err != nil {
		return
	}
	// It is not wanted that a block spans across different schemas
	if entry.isColumnChangedInSchema() {
		entry.FreezeAppend()
	}
	entry.RLock()
	schema := entry.GetLatestNodeLocked().BaseNode.Schema
	entry.RUnlock()
	// update the shortcut to the lastest schema
	entry.TableNode.schema.Store(schema)
	return
}

// hasColumnChangedSchema checks if add or drop columns on previous schema
func (entry *TableEntry) isColumnChangedInSchema() bool {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.GetLatestNodeLocked()
	toCommitted := node.BaseNode.Schema
	ver := toCommitted.Version
	// skip create table
	if ver == 0 {
		return false
	}
	return toCommitted.Extra.ColumnChanged
}

func (entry *TableEntry) FreezeAppend() {
	obj := entry.LastAppendableObject(false)
	if obj == nil {
		// nothing to freeze
		return
	}
	obj.GetObjectData().FreezeAppend()
}

// IsActive is coarse API: no consistency check
func (entry *TableEntry) IsActive() bool {
	db := entry.GetDB()
	if !db.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

// GetTerminationTS is coarse API: no consistency check
func (entry *TableEntry) GetTerminationTS() (ts types.TS, terminated bool) {
	dbEntry := entry.GetDB()

	terminated, ts = dbEntry.TryGetTerminatedTS(true)

	return
}

func (entry *TableEntry) AlterTable(ctx context.Context, txn txnif.TxnReader, req *apipb.AlterTableReq) (isNewNode bool, newSchema *Schema, err error) {
	entry.Lock()
	defer entry.Unlock()
	needWait, txnToWait := entry.NeedWaitCommittingLocked(txn.GetStartTS())
	if needWait {
		entry.Unlock()
		txnToWait.GetTxnState(true)
		entry.Lock()
	}
	err = entry.CheckConflictLocked(txn)
	if err != nil {
		return
	}
	var node *MVCCNode[*TableMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNodeLocked(txn)

	newSchema = node.BaseNode.Schema
	if isNewNode {
		// Extra info(except seqnnum etc.) is meaningful to the previous version schema
		// reset in new Schema
		var hints []apipb.MergeHint
		copy(hints, newSchema.Extra.Hints)
		newSchema.Extra = &apipb.SchemaExtra{
			NextColSeqnum:     newSchema.Extra.NextColSeqnum,
			MinOsizeQuailifed: newSchema.Extra.MinOsizeQuailifed,
			MaxObjOnerun:      newSchema.Extra.MaxObjOnerun,
			MaxOsizeMergedObj: newSchema.Extra.MaxOsizeMergedObj,
			MinCnMergeSize:    newSchema.Extra.MinCnMergeSize,
			Hints:             hints,
		}

	}
	if err = newSchema.ApplyAlterTable(req); err != nil {
		return
	}
	if isNewNode {
		node.BaseNode.Schema.Version += 1
	}
	return
}

func (entry *TableEntry) CreateWithTxnAndSchema(txn txnif.AsyncTxn, schema *Schema) {
	node := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode: &TableMVCCNode{
			Schema: schema,
		},
	}
	entry.InsertLocked(node)
}

func (entry *TableEntry) GetVisibilityAndName(txn txnif.TxnReader) (visible, dropped bool, name string) {
	entry.RLock()
	defer entry.RUnlock()
	needWait, txnToWait := entry.NeedWaitCommittingLocked(txn.GetStartTS())
	if needWait {
		entry.RUnlock()
		txnToWait.GetTxnState(true)
		entry.RLock()
	}
	un := entry.GetVisibleNodeLocked(txn)
	if un == nil {
		return
	}
	visible = true
	if un.IsSameTxn(txn) {
		dropped = un.HasDropIntent()
	} else {
		dropped = un.HasDropCommitted()
	}
	name = un.BaseNode.Schema.Name
	return
}

// only for test
func MockTableEntryWithDB(dbEntry *DBEntry, tblId uint64) *TableEntry {
	entry := NewReplayTableEntry()
	entry.TableNode = &TableNode{}
	entry.TableNode.schema.Store(NewEmptySchema("test"))
	entry.ID = tblId
	entry.db = dbEntry
	return entry
}
