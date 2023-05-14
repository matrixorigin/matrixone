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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
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
	ID      uint64
	db      *DBEntry
	entries map[types.Uuid]*common.GenericDLNode[*SegmentEntry]
	//link.head and link.tail is nil when create tableEntry object.
	link      *common.GenericSortedDList[*SegmentEntry]
	tableData data.Table
	rows      atomic.Uint64
	// fullname is format as 'tenantID-tableName', the tenantID prefix is only used 'mo_catalog' database
	fullName string
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
		db:        db,
		TableNode: &TableNode{},
		link:      common.NewGenericSortedDList((*SegmentEntry).Less),
		entries:   make(map[types.Uuid]*common.GenericDLNode[*SegmentEntry]),
	}
	e.TableNode.schema.Store(schema)
	if dataFactory != nil {
		e.tableData = dataFactory(e)
	}
	e.CreateWithTxnAndSchema(txnCtx, schema)
	return e
}

func NewSystemTableEntry(db *DBEntry, id uint64, schema *Schema) *TableEntry {
	e := &TableEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		db:        db,
		TableNode: &TableNode{},
		link:      common.NewGenericSortedDList((*SegmentEntry).Less),
		entries:   make(map[types.Uuid]*common.GenericDLNode[*SegmentEntry]),
	}
	e.TableNode.schema.Store(schema)
	e.CreateWithTS(types.SystemDBTS, &TableMVCCNode{Schema: schema})
	var sid types.Uuid
	if schema.Name == SystemTableSchema.Name {
		sid = SystemSegment_Table_ID
	} else if schema.Name == SystemDBSchema.Name {
		sid = SystemSegment_DB_ID
	} else if schema.Name == SystemColumnSchema.Name {
		sid = SystemSegment_Columns_ID
	} else {
		panic("not supported")
	}
	segment := NewSysSegmentEntry(e, sid)
	e.AddEntryLocked(segment)
	return e
}

func NewReplayTableEntry() *TableEntry {
	e := &TableEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *TableMVCCNode { return &TableMVCCNode{} }),
		link:    common.NewGenericSortedDList((*SegmentEntry).Less),
		entries: make(map[types.Uuid]*common.GenericDLNode[*SegmentEntry]),
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
		TableNode: node,
		link:      common.NewGenericSortedDList((*SegmentEntry).Less),
		entries:   make(map[types.Uuid]*common.GenericDLNode[*SegmentEntry]),
	}
}
func (entry *TableEntry) GetID() uint64 { return entry.ID }
func (entry *TableEntry) IsVirtual() bool {
	if !entry.db.IsSystemDB() {
		return false
	}
	name := entry.GetLastestSchema().Name
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

func (entry *TableEntry) GetSegmentByID(id *types.Segmentid) (seg *SegmentEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.entries[*id]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return node.GetPayload(), nil
}

func (entry *TableEntry) MakeSegmentIt(reverse bool) *common.GenericSortedDListIt[*SegmentEntry] {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewGenericSortedDListIt(entry.RWMutex, entry.link, reverse)
}

func (entry *TableEntry) CreateSegment(
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory SegmentDataFactory,
	opts *objectio.CreateSegOpt) (created *SegmentEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	var id *objectio.Segmentid
	if opts != nil && opts.Id != nil {
		id = opts.Id
	} else {
		id = objectio.NewSegmentid()
	}
	created = NewSegmentEntry(entry, id, txn, state, dataFactory)
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
func (entry *TableEntry) AddEntryLocked(segment *SegmentEntry) {
	n := entry.link.Insert(segment)
	entry.entries[segment.ID] = n
}

func (entry *TableEntry) deleteEntryLocked(segment *SegmentEntry) error {
	if n, ok := entry.entries[segment.ID]; !ok {
		return moerr.GetOkExpectedEOB()
	} else {
		entry.link.Delete(n)
		delete(entry.entries, segment.ID)
	}
	return nil
}

// GetLastestSchema returns the latest committed schema
func (entry *TableEntry) GetLastestSchema() *Schema {
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
	return entry.GetLastestSchema().ColDefs
}

func (entry *TableEntry) GetFullName() string {
	if len(entry.fullName) == 0 {
		schema := entry.GetLastestSchema()
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
	it := entry.MakeSegmentIt(true)
	for it.Valid() {
		segment := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(segment.PPString(level, depth+1, prefix))
		it.Next()
	}
	return w.String()
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
	name := entry.GetLastestSchema().Name
	if level <= common.PPL1 {
		return fmt.Sprintf("TBL[%d][name=%s][C@%s,D@%s]",
			entry.ID, name, entry.GetCreatedAt().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("TBL%s[name=%s]", entry.BaseEntryImpl.StringLocked(), name)
}

func (entry *TableEntry) StringLocked() string {
	return entry.StringLockedWithLevel(common.PPL1)
}

func (entry *TableEntry) GetCatalog() *Catalog { return entry.db.catalog }

func (entry *TableEntry) GetTableData() data.Table { return entry.tableData }

func (entry *TableEntry) LastAppendableSegmemt() (seg *SegmentEntry) {
	it := entry.MakeSegmentIt(false)
	for it.Valid() {
		itSeg := it.Get().GetPayload()
		dropped := itSeg.HasDropCommitted()
		if itSeg.IsAppendable() && !dropped {
			seg = itSeg
			break
		}
		it.Next()
	}
	return seg
}

func (entry *TableEntry) LastNonAppendableSegmemt() (seg *SegmentEntry) {
	it := entry.MakeSegmentIt(false)
	for it.Valid() {
		itSeg := it.Get().GetPayload()
		dropped := itSeg.HasDropCommitted()
		if !itSeg.IsAppendable() && !dropped {
			seg = itSeg
			break
		}
		it.Next()
	}
	return seg
}

func (entry *TableEntry) AsCommonID() *common.ID {
	return &common.ID{
		DbID:    entry.GetDB().ID,
		TableID: entry.ID,
	}
}

func (entry *TableEntry) RecurLoop(processor Processor) (err error) {
	segIt := entry.MakeSegmentIt(true)
	for segIt.Valid() {
		segment := segIt.Get().GetPayload()
		if err = processor.OnSegment(segment); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				segIt.Next()
				continue
			}
			break
		}
		blkIt := segment.MakeBlockIt(true)
		for blkIt.Valid() {
			block := blkIt.Get().GetPayload()
			if err = processor.OnBlock(block); err != nil {
				if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
					err = nil
					blkIt.Next()
					continue
				}
				break
			}
			blkIt.Next()
		}
		if err = processor.OnPostSegment(segment); err != nil {
			break
		}
		segIt.Next()
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}

func (entry *TableEntry) DropSegmentEntry(id *types.Segmentid, txn txnif.AsyncTxn) (deleted *SegmentEntry, err error) {
	seg, err := entry.GetSegmentByID(id)
	if err != nil {
		return
	}
	seg.Lock()
	defer seg.Unlock()
	needWait, waitTxn := seg.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		seg.Unlock()
		waitTxn.GetTxnState(true)
		seg.Lock()
	}
	var isNewNode bool
	isNewNode, err = seg.DropEntryLocked(txn)
	if err == nil && isNewNode {
		deleted = seg
	}
	return
}

func (entry *TableEntry) RemoveEntry(segment *SegmentEntry) (err error) {
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(segment.String()))
	// segment.Close()
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(segment)
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
func (entry *TableEntry) ApplyCommit(index *wal.Index) (err error) {
	err = entry.BaseEntryImpl.ApplyCommit(index)
	if err != nil {
		return
	}
	// It is not wanted that a block spans across different schemas
	if entry.isColumnChangedInSchema() {
		entry.FreezeAppend()
	}
	// update the shortcut to the lastest schema
	entry.TableNode.schema.Store(entry.GetLatestNodeLocked().BaseNode.Schema)
	return
}

// hasColumnChangedSchema checks if add or drop columns on previous schema
func (entry *TableEntry) isColumnChangedInSchema() bool {
	// in commit queue, it is safe
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
	seg := entry.LastAppendableSegmemt()
	if seg == nil {
		// nothing to freeze
		return
	}
	blk := seg.LastAppendableBlock()
	if blk == nil {
		// nothing to freeze
		return
	}
	blk.GetBlockData().FreezeAppend()
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
		// Extra info(except seqnnum) is meaningful to the previous version schema
		// reset in new Schema
		newSchema.Extra = &apipb.SchemaExtra{
			NextColSeqnum: newSchema.Extra.NextColSeqnum,
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
