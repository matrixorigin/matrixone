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
	"cmp"
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

	nodesMu    sync.RWMutex
	nameOrder  uint64 // protected by the DB lock
	renameUndo map[uint64]*tableRenameUndo
}

type renameNameMutation struct {
	name     string
	node     *nameNode[*TableEntry]
	inserted bool
}

type tableRenameUndo struct {
	txnID     string
	mutations []renameNameMutation
}

func (entry *TableEntry) Less(b *TableEntry) int {
	return cmp.Compare(entry.ID, b.ID)
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
	return cmp.Compare(e.ID, b.ID)
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
	fullName := genTblFullName(tenantID, name)
	for {
		e.RLock()
		node, waitTxn := e.tryGetTableNodeByNameLocked(e.nameNodes[fullName], txn, name, 0)
		e.RUnlock()
		if waitTxn != nil {
			waitTxn.GetTxnState(true)
			continue
		}
		if node == nil {
			return nil, moerr.GetOkExpectedEOB()
		}
		return node, nil
	}
}

// Caller holds the DB lock. No callback may wait while nodesMu is held.
// excludedID models an intermediate binding removed by a same-name rename.
func (e *DBEntry) tryGetTableNodeByNameLocked(list *nodeList[*TableEntry], txn txnif.TxnReader, name string, excludedID uint64) (node *common.GenericDLNode[*TableEntry], waitTxn txnif.TxnReader) {
	if list == nil {
		return
	}
	list.ForEachNodes(func(binding *nameNode[*TableEntry]) bool {
		if excludedID != 0 && binding.id == excludedID {
			return true
		}
		candidate := binding.GetNode()
		visible, dropped, visibleName, wait := candidate.GetPayload().tryVisibilityAndName(txn)
		if wait != nil {
			waitTxn = wait
			return false
		}
		if visible && !dropped && visibleName == name {
			node = candidate
			return false
		}
		return true
	})
	return
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
	if !visible {
		entry.RLock()
		visible = entry.sessionTemporarySchemaLocked(txn) != nil
		entry.RUnlock()
	}
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
	return e.renameTableInTxn(old, new, tid, tenantID, txn, first, txn != nil)
}

// Replay applies MVCC commit directly, so it must not retain live undo state.
func (e *DBEntry) replayRenameTable(old, new string, tid uint64, tenantID uint32, txn txnif.TxnReader, first bool) error {
	return e.renameTableInTxn(old, new, tid, tenantID, txn, first, false)
}

func (e *DBEntry) renameTableInTxn(old, new string, tid uint64, tenantID uint32, txn txnif.TxnReader, first, record bool) error {
	e.Lock()
	defer e.Unlock()
	original := e.entries[tid]
	var undo *tableRenameUndo
	if record {
		if e.renameUndo == nil {
			e.renameUndo = make(map[uint64]*tableRenameUndo)
		}
		undo = e.renameUndo[tid]
		if undo == nil {
			undo = &tableRenameUndo{txnID: txn.GetID()}
			e.renameUndo[tid] = undo
		} else if undo.txnID != txn.GetID() {
			return moerr.NewInternalErrorNoCtx("rename undo belongs to another transaction")
		}
	}
	oldFullName := genTblFullName(tenantID, old)
	newFullName := genTblFullName(tenantID, new)
	excludedID := uint64(0)
	if !first && oldFullName == newFullName {
		excludedID = tid
	}
	admissionErr := e.checkAddNameConflictLocked(new, tid, newFullName, txn, excludedID)
	// Admission may release the DB lock to wait. Revalidate runtime ownership
	// before using the journal or applying any name-index mutation.
	if e.entries[tid] != original {
		return moerr.GetOkExpectedEOB()
	}
	if record {
		undo = e.renameUndo[tid]
		if undo == nil || undo.txnID != txn.GetID() {
			return moerr.NewInternalErrorNoCtx("rename undo ownership changed during admission")
		}
	}
	remove := func(name string) {
		if undo == nil {
			e.removeNameIndexLocked(name, tid)
			return
		}
		if list := e.nameNodes[name]; list != nil {
			_, empty := list.deleteNode(tid, nil, func(node *nameNode[*TableEntry]) {
				undo.mutations = append(undo.mutations, renameNameMutation{name: name, node: node})
			})
			if empty {
				delete(e.nameNodes, name)
			}
		}
	}
	// if alter again and again in the same txn, previous temp name should be deleted.
	if !first {
		remove(oldFullName)
	}

	if admissionErr != nil {
		return admissionErr
	}
	// make sure every name node has up to one table id. check case alter A -> B -> A
	remove(newFullName)
	// add to the head of linked list
	e.nameOrder++
	e.tableNameListLocked(newFullName).createOrderedNode(tid, e.nameOrder, func(node *nameNode[*TableEntry]) {
		if undo != nil {
			undo.mutations = append(undo.mutations, renameNameMutation{name: newFullName, node: node, inserted: true})
		}
	})

	return nil
}

func (e *DBEntry) tableNameListLocked(fullname string) *nodeList[*TableEntry] {
	node := e.nameNodes[fullname]
	if node == nil {
		nn := newNodeList(e.GetItemNodeByIDLocked,
			tableVisibilityFn[*TableEntry],
			&e.nodesMu,
			fullname)
		e.nameNodes[fullname] = nn
		node = nn
	}
	return node
}

func (e *DBEntry) addNameIndexLocked(fullname string, tid uint64) {
	e.nameOrder++
	e.tableNameListLocked(fullname).createOrderedNode(tid, e.nameOrder, nil)
}

// Caller holds the DB lock throughout undo and the following MVCC rollback.
func (e *DBEntry) undoRenameLocked(tid uint64, txnID string) (bool, error) {
	undo := e.renameUndo[tid]
	if undo == nil {
		return false, nil
	}
	if undo.txnID != txnID {
		return true, moerr.NewInternalErrorNoCtx("rename rollback transaction mismatch")
	}
	for len(undo.mutations) > 0 {
		i := len(undo.mutations) - 1
		mutation := undo.mutations[i]
		if mutation.inserted {
			if list := e.nameNodes[mutation.name]; list != nil {
				if _, empty := list.deleteNode(tid, mutation.node, nil); empty {
					delete(e.nameNodes, mutation.name)
				}
			}
		} else if err := e.tableNameListLocked(mutation.name).restoreNode(mutation.node); err != nil {
			return true, err
		}
		undo.mutations[i] = renameNameMutation{}
		undo.mutations = undo.mutations[:i]
	}
	return true, nil
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
		delete(e.renameUndo, table.ID)
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
	if !skipDedup {
		name := table.GetLastestSchemaLocked(false).Name
		if err = e.checkAddNameConflictLocked(name, table.ID, fullName, txn, 0); err != nil {
			return
		}
	}
	// A foreign admission can install this explicit ID while our name check
	// releases the DB lock. Never overwrite its entries slot after wakeup.
	if _, exists := e.entries[table.ID]; exists {
		return moerr.GetOkExpectedDup()
	}
	n := e.link.Insert(table)
	e.entries[table.ID] = n
	e.addNameIndexLocked(fullName, table.ID)
	return
}

// Entered and returned with DB write lock held. A wait temporarily releases
// that lock; callers must not retain a table/list lock or mutate bindings first.
func (e *DBEntry) checkAddNameConflictLocked(name string, tid uint64, fullName string, txn txnif.TxnReader, excludedID uint64) error {
	for {
		waitTxn, err := e.tryAddNameConflictLocked(name, tid, e.nameNodes[fullName], txn, excludedID)
		if waitTxn == nil {
			return err
		}
		e.Unlock()
		waitTxn.GetTxnState(true)
		e.Lock()
	}
}

func (e *DBEntry) tryAddNameConflictLocked(name string, tid uint64, nn *nodeList[*TableEntry], txn txnif.TxnReader, excludedID uint64) (waitTxn txnif.TxnReader, err error) {
	if nn == nil {
		return
	}
	var node *common.GenericDLNode[*TableEntry]
	nn.ForEachNodes(func(binding *nameNode[*TableEntry]) bool {
		if excludedID != 0 && binding.id == excludedID {
			return true
		}
		node = binding.GetNode()
		return false
	})
	if node == nil {
		return
	}
	// check ww conflict
	tbl := node.GetPayload()
	// skip the same table entry
	if tbl.ID == tid {
		return
	}
	if txn != nil {
		tbl.RLock()
		needWait, wait := tbl.NeedWaitCommittingLocked(txn.GetStartTS())
		if needWait {
			waitTxn = wait
		} else {
			err = tbl.CheckConflictLocked(txn)
		}
		tbl.RUnlock()
		if waitTxn != nil || err != nil {
			return
		}
	}
	// check name dup
	if txn == nil {
		// replay checkpoint
		return
	}
	// A visible historical binding can be the rename target itself after its
	// provisional schema update. Distinct admitted owners precede it in this
	// list; do not continue into older history and introduce new waits.
	existEntry, wait := e.tryGetTableNodeByNameLocked(nn, txn, name, excludedID)
	if wait != nil {
		return wait, nil
	}
	if existEntry != nil && existEntry.GetPayload().ID != tid {
		err = moerr.GetOkExpectedDup()
	}
	return
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
	entry.BaseEntryImpl = NewBaseEntry(func() *EmptyMVCCNode { return &EmptyMVCCNode{} })

	return entry
}

// for test
func (e *DBEntry) TestSetCatalog(catalog *Catalog) {
	e.catalog = catalog
}
