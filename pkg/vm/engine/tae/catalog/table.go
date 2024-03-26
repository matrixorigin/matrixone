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
	Stats   *common.TableCompactStat
	ID      uint64
	db      *DBEntry
	entries map[types.Objectid]*common.GenericDLNode[*ObjectEntry]
	//link.head and link.tail is nil when create tableEntry object.
	link      *common.GenericSortedDList[*ObjectEntry]
	tableData data.Table
	rows      atomic.Uint64
	// used for the next flush table tail.
	DeletedDirties []*ObjectEntry
	// fullname is format as 'tenantID-tableName', the tenantID prefix is only used 'mo_catalog' database
	fullName string

	deleteList *btree.BTreeG[DeleteEntry]
}

type DeleteEntry struct {
	ObjectID objectio.ObjectId
	data.Tombstone
}

func (d DeleteEntry) Less(o DeleteEntry) bool {
	return bytes.Compare(d.ObjectID[:], o.ObjectID[:]) < 0
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
	opts := btree.Options{
		Degree: 4,
	}
	e := &TableEntry{
		ID: tableId,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		db:         db,
		TableNode:  &TableNode{},
		link:       common.NewGenericSortedDList((*ObjectEntry).Less),
		entries:    make(map[types.Objectid]*common.GenericDLNode[*ObjectEntry]),
		deleteList: btree.NewBTreeGOptions(DeleteEntry.Less, opts),
		Stats:      common.NewTableCompactStat(),
	}
	e.TableNode.schema.Store(schema)
	if dataFactory != nil {
		e.tableData = dataFactory(e)
	}
	e.CreateWithTxnAndSchema(txnCtx, schema)
	return e
}

func NewSystemTableEntry(db *DBEntry, id uint64, schema *Schema) *TableEntry {
	opts := btree.Options{
		Degree: 4,
	}
	e := &TableEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		db:         db,
		TableNode:  &TableNode{},
		link:       common.NewGenericSortedDList((*ObjectEntry).Less),
		entries:    make(map[types.Objectid]*common.GenericDLNode[*ObjectEntry]),
		deleteList: btree.NewBTreeGOptions(DeleteEntry.Less, opts),
		Stats:      common.NewTableCompactStat(),
	}
	e.TableNode.schema.Store(schema)
	e.CreateWithTS(types.SystemDBTS, &TableMVCCNode{Schema: schema})
	var sid types.Uuid
	if schema.Name == SystemTableSchema.Name {
		sid = SystemObject_Table_ID
	} else if schema.Name == SystemDBSchema.Name {
		sid = SystemObject_DB_ID
	} else if schema.Name == SystemColumnSchema.Name {
		sid = SystemObject_Columns_ID
	} else {
		panic("not supported")
	}
	objectEntry := NewSysObjectEntry(e, sid)
	e.AddEntryLocked(objectEntry)
	return e
}

func NewReplayTableEntry() *TableEntry {
	opts := btree.Options{
		Degree: 4,
	}
	e := &TableEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		link:       common.NewGenericSortedDList((*ObjectEntry).Less),
		entries:    make(map[types.Objectid]*common.GenericDLNode[*ObjectEntry]),
		deleteList: btree.NewBTreeGOptions(DeleteEntry.Less, opts),
		Stats:      common.NewTableCompactStat(),
	}
	return e
}

func MockStaloneTableEntry(id uint64, schema *Schema) *TableEntry {
	node := &TableNode{}
	node.schema.Store(schema)
	opts := btree.Options{
		Degree: 4,
	}
	return &TableEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		TableNode:  node,
		link:       common.NewGenericSortedDList((*ObjectEntry).Less),
		entries:    make(map[types.Objectid]*common.GenericDLNode[*ObjectEntry]),
		deleteList: btree.NewBTreeGOptions(DeleteEntry.Less, opts),
		Stats:      common.NewTableCompactStat(),
	}
}
func (entry *TableEntry) GetDeleteList() *btree.BTreeG[DeleteEntry] {
	return entry.deleteList.Copy()
}
func (entry *TableEntry) TryGetTombstone(oid objectio.ObjectId) data.Tombstone {
	pivot := DeleteEntry{ObjectID: oid}
	tombstone, ok := entry.deleteList.Get(pivot)
	if !ok {
		return nil
	}
	return tombstone.Tombstone
}

func (entry *TableEntry) GetOrCreateTombstone(obj *ObjectEntry, factory TombstoneFactory) data.Tombstone {
	pivot := DeleteEntry{ObjectID: obj.ID}
	delete, ok := entry.deleteList.Get(pivot)
	if ok {
		return delete.Tombstone
	}
	pivot.Tombstone = factory(obj)
	entry.deleteList.Set(pivot)
	return pivot.Tombstone
}

func (entry *TableEntry) GetID() uint64 { return entry.ID }
func (entry *TableEntry) IsVirtual() bool {
	if !entry.db.IsSystemDB() {
		return false
	}
	name := entry.GetLastestSchemaLocked().Name
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

func (entry *TableEntry) GetObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.entries[*id]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return node.GetPayload(), nil
}
func (entry *TableEntry) GetObjectsByID(id *types.Segmentid) (obj []*ObjectEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	for nodeID, node := range entry.entries {
		if nodeID.Segment().Eq(*id) {
			if obj == nil {
				obj = make([]*ObjectEntry, 0)
			}
			obj = append(obj, node.GetPayload())
		}
	}
	if obj == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return obj, nil
}

func (entry *TableEntry) MakeObjectIt(reverse bool) *common.GenericSortedDListIt[*ObjectEntry] {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewGenericSortedDListIt(entry.RWMutex, entry.link, reverse)
}

func (entry *TableEntry) CreateObject(
	txn txnif.AsyncTxn,
	state EntryState,
	opts *objectio.CreateObjOpt,
	dataFactory ObjectDataFactory,
) (created *ObjectEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	var id *objectio.ObjectId
	if opts != nil && opts.Id != nil {
		id = opts.Id
	} else {
		id = objectio.NewObjectid()
	}
	created = NewObjectEntry(entry, id, txn, state, dataFactory)
	entry.AddEntryLocked(created)
	return
}

func (entry *TableEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Table
	entry.RLock()
	defer entry.RUnlock()
	return newTableCmd(id, cmdType, entry), nil
}

func (entry *TableEntry) Set1PC() {
	entry.GetLatestNodeLocked().Set1PC()
}
func (entry *TableEntry) Is1PC() bool {
	return entry.GetLatestNodeLocked().Is1PC()
}
func (entry *TableEntry) AddEntryLocked(objectEntry *ObjectEntry) {
	n := entry.link.Insert(objectEntry)
	entry.entries[objectEntry.ID] = n
}

func (entry *TableEntry) deleteEntryLocked(objectEntry *ObjectEntry) error {
	if n, ok := entry.entries[objectEntry.ID]; !ok {
		return moerr.GetOkExpectedEOB()
	} else {
		entry.link.Delete(n)
		delete(entry.entries, objectEntry.ID)
	}
	return nil
}

// GetLastestSchemaLocked returns the latest committed schema with entry Not locked
func (entry *TableEntry) GetLastestSchemaLocked() *Schema {
	return entry.schema.Load()
}

// GetLastestSchema returns the latest committed schema with entry locked
func (entry *TableEntry) GetLastestSchema() *Schema {
	entry.Lock()
	defer entry.Unlock()

	return entry.schema.Load()
}

// GetVisibleSchema returns committed schema visible at the given txn
func (entry *TableEntry) GetVisibleSchema(txn txnif.TxnReader) *Schema {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.GetVisibleNode(txn)
	if node != nil {
		return node.BaseNode.Schema
	}
	return nil
}

func (entry *TableEntry) GetVersionSchema(ver uint32) *Schema {
	entry.RLock()
	defer entry.RUnlock()
	var ret *Schema
	entry.LoopChain(func(m *MVCCNode[*TableMVCCNode]) bool {
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
	return entry.GetLastestSchemaLocked().ColDefs
}

func (entry *TableEntry) GetFullName() string {
	if len(entry.fullName) == 0 {
		schema := entry.GetLastestSchemaLocked()
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
	it := entry.MakeObjectIt(true)
	for it.Valid() {
		objectEntry := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(objectEntry.PPString(level, depth+1, prefix))
		it.Next()
	}
	if level > common.PPL2 {
		_ = w.WriteByte('\n')
		it2 := entry.deleteList.Copy().Iter()
		for it2.Next() {
			w.WriteString(common.RepeatStr("\t", depth+1))
			w.WriteString(prefix)
			objID := it2.Item().ObjectID
			w.WriteString(fmt.Sprintf("Tombstone[%s]\n", objID.String()))
			it2.Item().GetObject().(*ObjectEntry).RLock()
			w.WriteString(it2.Item().StringLocked(level, depth+1, prefix))
			it2.Item().GetObject().(*ObjectEntry).RUnlock()
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

	it := entry.MakeObjectIt(true)
	zonemapKind := common.ZonemapPrintKindNormal
	if schema := entry.GetLastestSchemaLocked(); schema.HasSortKey() && strings.HasPrefix(schema.GetSingleSortKey().Name, "__") {
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

	for ; it.Valid(); it.Next() {
		objectEntry := it.Get().GetPayload()
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
			_, _ = w.WriteString(objectEntry.ID.String())
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
	name := entry.GetLastestSchemaLocked().Name
	if level <= common.PPL1 {
		return fmt.Sprintf("TBL[%d][name=%s][C@%s,D@%s]",
			entry.ID, name, entry.GetCreatedAtLocked().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("TBL%s[name=%s, id=%d]", entry.BaseEntryImpl.StringLocked(), name, entry.ID)
}

func (entry *TableEntry) StringLocked() string {
	return entry.StringLockedWithLevel(common.PPL1)
}

func (entry *TableEntry) GetCatalog() *Catalog { return entry.db.catalog }

func (entry *TableEntry) GetTableData() data.Table { return entry.tableData }

func (entry *TableEntry) LastAppendableObject() (obj *ObjectEntry) {
	it := entry.MakeObjectIt(false)
	for it.Valid() {
		itObj := it.Get().GetPayload()
		dropped := itObj.HasDropCommitted()
		if itObj.IsAppendable() && !dropped {
			obj = itObj
			break
		}
		it.Next()
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
	objIt := entry.MakeObjectIt(true)
	for objIt.Valid() {
		objectEntry := objIt.Get().GetPayload()
		if err := processor.OnObject(objectEntry); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				objIt.Next()
				continue
			}
			return err
		}
		if err := processor.OnPostObject(objectEntry); err != nil {
			return err
		}
		objIt.Next()
	}
	tombstones := entry.deleteList.Copy().Items()
	for _, deletes := range tombstones {
		err = processor.OnTombstone(deletes)
		if err != nil {
			return
		}
	}
	return
}

func (entry *TableEntry) DropObjectEntry(id *types.Objectid, txn txnif.AsyncTxn) (deleted *ObjectEntry, err error) {
	obj, err := entry.GetObjectByID(id)
	if err != nil {
		return
	}
	obj.Lock()
	defer obj.Unlock()
	needWait, waitTxn := obj.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		obj.Unlock()
		waitTxn.GetTxnState(true)
		obj.Lock()
	}
	var isNewNode bool
	isNewNode, err = obj.DropEntryLocked(txn)
	if err == nil && isNewNode {
		deleted = obj
	}
	return
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
func (entry *TableEntry) ApplyCommit() (err error) {
	err = entry.BaseEntryImpl.ApplyCommit()
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
	obj := entry.LastAppendableObject()
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

	dbEntry.RLock()
	terminated, ts = dbEntry.TryGetTerminatedTS(true)
	dbEntry.RUnlock()

	return
}

func (entry *TableEntry) AlterTable(ctx context.Context, txn txnif.TxnReader, req *apipb.AlterTableReq) (isNewNode bool, newSchema *Schema, err error) {
	entry.Lock()
	defer entry.Unlock()
	needWait, txnToWait := entry.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		entry.Unlock()
		txnToWait.GetTxnState(true)
		entry.Lock()
	}
	err = entry.CheckConflict(txn)
	if err != nil {
		return
	}
	var node *MVCCNode[*TableMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNode(txn)

	newSchema = node.BaseNode.Schema
	if isNewNode {
		// Extra info(except seqnnum etc.) is meaningful to the previous version schema
		// reset in new Schema
		var hints []apipb.MergeHint
		copy(hints, newSchema.Extra.Hints)
		newSchema.Extra = &apipb.SchemaExtra{
			NextColSeqnum:    newSchema.Extra.NextColSeqnum,
			MinRowsQuailifed: newSchema.Extra.MinRowsQuailifed,
			MaxObjOnerun:     newSchema.Extra.MaxObjOnerun,
			MaxRowsMergedObj: newSchema.Extra.MaxRowsMergedObj,
			Hints:            hints,
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
	entry.Insert(node)
}

func (entry *TableEntry) GetVisibilityAndName(txn txnif.TxnReader) (visible, dropped bool, name string) {
	entry.RLock()
	defer entry.RUnlock()
	needWait, txnToWait := entry.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		entry.RUnlock()
		txnToWait.GetTxnState(true)
		entry.RLock()
	}
	un := entry.GetVisibleNode(txn)
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
