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
	"fmt"
	"io"
	"sort"
	"sync"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type accessInfo struct {
	TenantID, UserID, RoleID uint32
	CreateAt                 types.Timestamp
}

const (
	AccessInfoSize int64 = int64(unsafe.Sizeof(accessInfo{}))
)

func EncodeAccessInfo(ai *accessInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(ai)), AccessInfoSize)
}

func (ai *accessInfo) WriteTo(w io.Writer) (n int64, err error) {
	w.Write(EncodeAccessInfo(ai))
	return AccessInfoSize, nil
}

func (ai *accessInfo) ReadFrom(r io.Reader) (n int64, err error) {
	r.Read(EncodeAccessInfo(ai))
	return AccessInfoSize, nil
}

func dbVisibilityFn[T *DBEntry](n *common.GenericDLNode[*DBEntry], txn txnif.TxnReader) (visible, dropped bool, name string) {
	db := n.GetPayload()
	visible, dropped = db.GetVisibility(txn)
	return
}

type DBEntry struct {
	ID uint64
	*BaseEntryImpl[*EmptyMVCCNode]
	catalog *Catalog
	*DBNode
	fullName string
	isSys    bool

	entries map[uint64]*common.GenericDLNode[*TableEntry]
	// nameNodes[ABC] is a linked list of all table entries had been once named as ABC
	nameNodes map[string]*nodeList[*TableEntry]
	link      *common.GenericSortedDList[*TableEntry]

	nodesMu sync.RWMutex
}

func (entry *TableEntry) Less(b *TableEntry) int {
	return CompareUint64(entry.ID, b.ID)
}

func NewDBEntryWithID(catalog *Catalog, name string, createSql, datTyp string, id uint64, txn txnif.AsyncTxn) *DBEntry {
	e := &DBEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *EmptyMVCCNode { return &EmptyMVCCNode{} }),
		catalog: catalog,
		DBNode: &DBNode{
			name:      name,
			createSql: createSql,
			datType:   datTyp,
		},
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList((*TableEntry).Less),
	}
	if txn != nil {
		// Only in unit test, txn can be nil
		e.acInfo.TenantID = txn.GetTenantID()
		e.acInfo.UserID, e.acInfo.RoleID = txn.GetUserAndRoleID()
	}
	e.CreateWithTxnLocked(txn, &EmptyMVCCNode{})
	e.acInfo.CreateAt = types.CurrentTimestamp()
	return e
}

func NewSystemDBEntry(catalog *Catalog) *DBEntry {
	entry := NewReplayDBEntry()
	entry.isSys = true
	entry.ID = pkgcatalog.MO_CATALOG_ID
	entry.catalog = catalog
	entry.DBNode = &DBNode{
		name:      pkgcatalog.MO_CATALOG,
		createSql: "create database " + pkgcatalog.MO_CATALOG,
	}
	entry.CreateWithTSLocked(types.SystemDBTS, &EmptyMVCCNode{})
	return entry
}

func NewReplayDBEntry() *DBEntry {
	entry := &DBEntry{
		BaseEntryImpl: NewBaseEntry(func() *EmptyMVCCNode { return &EmptyMVCCNode{} }),
		entries:       make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes:     make(map[string]*nodeList[*TableEntry]),
		link:          common.NewGenericSortedDList((*TableEntry).Less),
	}
	return entry
}

func (e *DBEntry) GetID() uint64    { return e.ID }
func (e *DBEntry) IsSystemDB() bool { return e.isSys }
func (e *DBEntry) CoarseTableCnt() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.entries)
}

func (e *DBEntry) Less(b *DBEntry) int {
	return CompareUint64(e.ID, b.ID)
}

func (e *DBEntry) GetTenantID() uint32          { return e.acInfo.TenantID }
func (e *DBEntry) GetUserID() uint32            { return e.acInfo.UserID }
func (e *DBEntry) GetRoleID() uint32            { return e.acInfo.RoleID }
func (e *DBEntry) GetCreateAt() types.Timestamp { return e.acInfo.CreateAt }
func (e *DBEntry) GetName() string              { return e.name }
func (e *DBEntry) GetCreateSql() string         { return e.createSql }
func (e *DBEntry) IsSubscription() bool {
	return e.datType == pkgcatalog.SystemDBTypeSubscription
}
func (e *DBEntry) GetDatType() string { return e.datType }
func (e *DBEntry) GetFullName() string {
	if len(e.fullName) == 0 {
		e.fullName = genDBFullName(e.acInfo.TenantID, e.name)
	}
	return e.fullName
}

func (e *DBEntry) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *DBEntry) StringLocked() string {
	return e.StringWithlevelLocked(common.PPL1)
}

func (e *DBEntry) StringWithLevel(level common.PPLevel) string {
	e.RLock()
	defer e.RUnlock()
	return e.StringWithlevelLocked(level)
}

func (e *DBEntry) StringWithlevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("DB[%d][name=%s][C@%s,D@%s]",
			e.ID, e.GetFullName(), e.GetCreatedAtLocked().ToString(), e.GetDeleteAtLocked().ToString())
	}
	return fmt.Sprintf("DB%s[name=%s, id=%d]", e.BaseEntryImpl.StringLocked(), e.GetFullName(), e.ID)
}

func (e *DBEntry) MakeTableIt(reverse bool) *common.GenericSortedDListIt[*TableEntry] {
	e.RLock()
	defer e.RUnlock()
	return common.NewGenericSortedDListIt(e.RWMutex, e.link, reverse)
}

func (e *DBEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	it := e.MakeTableIt(true)
	for it.Valid() {
		table := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(table.PPString(level, depth+1, ""))
		it.Next()
	}
	return w.String()
}
func (e *DBEntry) AsCommonID() *common.ID {
	return &common.ID{
		DbID: e.ID,
	}
}
func (e *DBEntry) GetObjectEntryByID(id *common.ID, isTombstone bool) (obj *ObjectEntry, err error) {
	e.RLock()
	table, err := e.GetTableEntryByID(id.TableID)
	e.RUnlock()
	if err != nil {
		return
	}
	obj, err = table.GetObjectByID(id.ObjectID(), isTombstone)
	return
}

func (e *DBEntry) GetItemNodeByIDLocked(id uint64) *common.GenericDLNode[*TableEntry] {
	return e.entries[id]
}

func (e *DBEntry) GetTableEntryByID(id uint64) (table *TableEntry, err error) {
	e.RLock()
	defer e.RUnlock()
	node := e.entries[id]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	table = node.GetPayload()
	return
}

func (e *DBEntry) txnGetNodeByName(
	tenantID uint32,
	name string,
	txn txnif.TxnReader) (*common.GenericDLNode[*TableEntry], error) {
	e.RLock()
	defer e.RUnlock()
	fullName := genTblFullName(tenantID, name)
	node := e.nameNodes[fullName]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return node.TxnGetNodeLocked(txn, name)
}

func (e *DBEntry) TxnGetTableEntryByName(name string, txn txnif.AsyncTxn) (entry *TableEntry, err error) {
	n, err := e.txnGetNodeByName(txn.GetTenantID(), name, txn)
	if err != nil {
		return
	}
	entry = n.GetPayload()
	return
}

func (e *DBEntry) GetTableEntryByName(
	tenantID uint32,
	name string,
	txn txnif.TxnReader) (entry *TableEntry, err error) {
	n, err := e.txnGetNodeByName(tenantID, name, txn)
	if err != nil {
		return
	}
	entry = n.GetPayload()
	return
}

func (e *DBEntry) TxnGetTableEntryByID(id uint64, txn txnif.AsyncTxn) (entry *TableEntry, err error) {
	entry, err = e.GetTableEntryByID(id)
	if err != nil {
		return
	}
	//check whether visible and dropped.
	visible, dropped := entry.GetVisibility(txn)
	if !visible || dropped {
		return nil, moerr.GetOkExpectedEOB()
	}
	return
}

// Catalog entry is dropped in following steps:
// 1. Locate the record by timestamp
// 2. Check conflication.
// 2.1 Wait for the related txn if need.
// 2.2 w-w conflict when 1. there's an active txn; or
//  2. the CommitTS of the latest related txn is larger than StartTS of write txn
//
// 3. Check duplicate/not found.
// If the entry has already been dropped, return ErrNotFound.
func (e *DBEntry) DropTableEntry(name string, txn txnif.AsyncTxn) (newEntry bool, deleted *TableEntry, err error) {
	tn, err := e.txnGetNodeByName(txn.GetTenantID(), name, txn)
	if err != nil {
		return
	}
	entry := tn.GetPayload()
	entry.Lock()
	defer entry.Unlock()
	newEntry, err = entry.DropEntryLocked(txn)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) DropTableEntryByID(id uint64, txn txnif.AsyncTxn) (newEntry bool, deleted *TableEntry, err error) {
	entry, err := e.GetTableEntryByID(id)
	if err != nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()
	newEntry, err = entry.DropEntryLocked(txn)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txn txnif.AsyncTxn, dataFactory TableDataFactory) (created *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	created = NewTableEntry(e, schema, txn, dataFactory)
	err = e.AddEntryLocked(created, txn, false)

	return created, err
}

func (e *DBEntry) CreateTableEntryWithTableId(schema *Schema, txn txnif.AsyncTxn, dataFactory TableDataFactory, tableId uint64) (created *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	if tableId < pkgcatalog.MO_RESERVED_MAX {
		return nil, moerr.NewInternalErrorNoCtxf("reserved table ID %d", tableId)
	}
	//Deduplicate for tableId
	if _, exist := e.entries[tableId]; exist {
		return nil, moerr.GetOkExpectedDup()
	}
	created = NewTableEntryWithTableId(e, schema, txn, dataFactory, tableId)
	err = e.AddEntryLocked(created, txn, false)

	return created, err
}

// For test only
func (e *DBEntry) PrettyNameIndex() string {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("[%d]NameIndex:\n", len(e.nameNodes)))
	// iterate all nodes in nameNodes, collect node ids to a string
	ids := make([]uint64, 0)
	// sort e.nameNodes by name
	names := make([]string, 0, len(e.nameNodes))
	for name := range e.nameNodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		node := e.nameNodes[name]
		ids = ids[:0]
		node.ForEachNodes(func(nn *nameNode[*TableEntry]) bool {
			ids = append(ids, nn.id)
			return true
		})
		buf.WriteString(fmt.Sprintf("  %s: %v\n", name, ids))
	}
	return buf.String()
}

func (e *DBEntry) RenameTableInTxn(old, new string, tid uint64, tenantID uint32, txn txnif.TxnReader, first bool) error {
	e.Lock()
	defer e.Unlock()
	// if alter again and again in the same txn, previous temp name should be deleted.
	if !first {
		e.removeNameIndexLocked(genTblFullName(tenantID, old), tid)
	}

	newFullName := genTblFullName(tenantID, new)
	if err := e.checkAddNameConflictLocked(new, tid, e.nameNodes[newFullName], txn); err != nil {
		return err
	}
	// make sure every name node has up to one table id. check case alter A -> B -> A
	e.removeNameIndexLocked(newFullName, tid)
	// add to the head of linked list
	e.addNameIndexLocked(newFullName, tid)

	return nil
}

func (e *DBEntry) addNameIndexLocked(fullname string, tid uint64) {
	node := e.nameNodes[fullname]
	if node == nil {
		nn := newNodeList(e.GetItemNodeByIDLocked,
			tableVisibilityFn[*TableEntry],
			&e.nodesMu,
			fullname)
		e.nameNodes[fullname] = nn
		nn.CreateNode(tid)
	} else {
		node.CreateNode(tid)
	}
}

func (e *DBEntry) removeNameIndexLocked(fullname string, tid uint64) {
	nn := e.nameNodes[fullname]
	if nn == nil {
		return
	}
	if _, empty := nn.DeleteNode(tid); empty {
		delete(e.nameNodes, fullname)
	}
}

func (e *DBEntry) RollbackRenameTable(fullname string, tid uint64) {
	e.Lock()
	defer e.Unlock()
	e.removeNameIndexLocked(fullname, tid)
}

func (e *DBEntry) RemoveEntry(table *TableEntry) (err error) {
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(table.String()))
	e.Lock()
	defer e.Unlock()
	if n, ok := e.entries[table.ID]; !ok {
		return moerr.GetOkExpectedEOB()
	} else {
		table.RLock()
		defer table.RUnlock()
		prevname := ""
		// clean all name because RemoveEntry can be called by GC、
		table.LoopChainLocked(func(m *MVCCNode[*TableMVCCNode]) bool {
			if prevname == m.BaseNode.Schema.Name {
				return true
			}
			prevname = m.BaseNode.Schema.Name
			tenantID := m.BaseNode.Schema.AcInfo.TenantID
			fullname := genTblFullName(tenantID, prevname)
			nn := e.nameNodes[fullname]
			if nn == nil {
				return true
			}
			nn.DeleteNode(table.ID)
			if nn.Length() == 0 {
				delete(e.nameNodes, fullname)
			}
			return true
		})

		// When Rollback, the last mvcc has already removed
		fullname := table.GetFullName()
		nn := e.nameNodes[fullname]
		if nn != nil {
			nn.DeleteNode(table.ID)
			if nn.Length() == 0 {
				delete(e.nameNodes, fullname)
			}
		}
		e.link.Delete(n)
		delete(e.entries, table.ID)
	}
	return
}

// Catalog entry is created in following steps:
// 1. Locate the record. Creating always gets the latest DBEntry.
// 2.1 If there doesn't exist a DBEntry, add new entry and return.
// 2.2 If there exists a DBEntry:
// 2.2.1 Check conflication.
//  1. Wait for the related txn if need.
//  2. w-w conflict when: there's an active txn; or
//     he CommitTS of the latest related txn is larger than StartTS of write txn
//
// 2.2.2 Check duplicate/not found.
// If the entry hasn't been dropped, return ErrDuplicate.
func (e *DBEntry) AddEntryLocked(table *TableEntry, txn txnif.TxnReader, skipDedup bool) (err error) {
	fullName := table.GetFullName()
	nn := e.nameNodes[fullName]
	if nn == nil {
		n := e.link.Insert(table)
		e.entries[table.ID] = n

		nn := newNodeList(e.GetItemNodeByIDLocked,
			tableVisibilityFn[*TableEntry],
			&e.nodesMu,
			fullName)
		e.nameNodes[fullName] = nn

		nn.CreateNode(table.ID)
	} else {
		if !skipDedup {
			name := table.GetLastestSchemaLocked(false).Name
			err = e.checkAddNameConflictLocked(name, table.ID, nn, txn)
			if err != nil {
				return
			}
		}
		n := e.link.Insert(table)
		e.entries[table.ID] = n
		nn.CreateNode(table.ID)
	}
	return
}

func (e *DBEntry) checkAddNameConflictLocked(name string, tid uint64, nn *nodeList[*TableEntry], txn txnif.TxnReader) (err error) {
	if nn == nil {
		return nil
	}
	node := nn.GetNode()
	if node == nil {
		return nil
	}
	// check ww conflict
	tbl := nn.GetNode().GetPayload()
	// skip the same table entry
	if tbl.ID == tid {
		return nil
	}
	if err = tbl.ConflictCheck(txn); err != nil {
		return
	}
	// check name dup
	if txn == nil {
		// replay checkpoint
		return nil
	}
	if existEntry, _ := nn.TxnGetNodeLocked(txn, name); existEntry != nil {
		return moerr.GetOkExpectedDup()
	}
	return nil
}

func (e *DBEntry) MakeCommand(id uint32) (txnif.TxnCmd, error) {
	cmdType := IOET_WALTxnCommand_Database
	e.RLock()
	defer e.RUnlock()
	return newDBCmd(id, cmdType, e), nil
}

func (e *DBEntry) GetCatalog() *Catalog { return e.catalog }

func (e *DBEntry) RecurLoop(processor Processor) (err error) {
	tableIt := e.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload()
		if err = processor.OnTable(table); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				tableIt.Next()
				continue
			}
			break
		}
		if err = table.RecurLoop(processor); err != nil {
			return
		}
		if err = processor.OnPostTable(table); err != nil {
			break
		}
		tableIt.Next()
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}

func (e *DBEntry) PrepareRollback() (err error) {
	var isEmpty bool
	if isEmpty, err = e.BaseEntryImpl.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
		if err = e.catalog.RemoveDBEntry(e); err != nil {
			return
		}
	}
	return
}

// IsActive is coarse API: no consistency check
func (e *DBEntry) IsActive() bool {
	return !e.HasDropCommitted()
}

// only for test
func MockDBEntryWithAccInfo(accId uint64, dbId uint64) *DBEntry {
	entry := &DBEntry{
		ID: dbId,
	}

	entry.DBNode = &DBNode{}
	entry.DBNode.acInfo.TenantID = uint32(accId)

	return entry
}
