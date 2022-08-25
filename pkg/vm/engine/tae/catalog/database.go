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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type accessInfo struct {
	TenantID, UserID, RoleID uint32
	CreateAt                 types.Timestamp
}

func (ai *accessInfo) WriteTo(w io.Writer) (n int64, err error) {
	for _, id := range []uint32{ai.TenantID, ai.UserID, ai.RoleID} {
		if err = binary.Write(w, binary.BigEndian, id); err != nil {
			return
		}
	}
	if err = binary.Write(w, binary.BigEndian, int64(ai.CreateAt)); err != nil {
		return
	}
	return 20, nil
}

func (ai *accessInfo) ReadFrom(r io.Reader) (n int64, err error) {
	for _, idPtr := range []*uint32{&ai.TenantID, &ai.UserID, &ai.RoleID} {
		if err = binary.Read(r, binary.BigEndian, idPtr); err != nil {
			return
		}
	}
	at := int64(0)
	if err = binary.Read(r, binary.BigEndian, &at); err != nil {
		return
	}
	ai.CreateAt = types.Timestamp(at)
	return 20, nil
}

type DBEntry struct {
	*BaseEntry
	catalog  *Catalog
	acInfo   accessInfo
	name     string
	fullName string
	isSys    bool

	entries   map[uint64]*common.DLNode
	nameNodes map[string]*nodeList
	link      *common.SortedDList

	nodesMu sync.RWMutex
}

func NewDBEntry(catalog *Catalog, name string, txnCtx txnif.AsyncTxn) *DBEntry {
	id := catalog.NextDB()

	e := &DBEntry{
		BaseEntry: NewBaseEntry(id),
		catalog:   catalog,
		name:      name,
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.SortedDList),
	}
	if txnCtx != nil {
		// Only in unit test, txnCtx can be nil
		e.acInfo.TenantID = txnCtx.GetTenantID()
		e.acInfo.UserID, e.acInfo.RoleID = txnCtx.GetUserAndRoleID()
	}
	e.CreateWithTxn(txnCtx)
	e.acInfo.CreateAt = types.CurrentTimestamp()
	return e
}

func NewSystemDBEntry(catalog *Catalog) *DBEntry {
	id := SystemDBID
	entry := &DBEntry{
		BaseEntry: NewBaseEntry(id),
		catalog:   catalog,
		name:      SystemDBName,
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.SortedDList),
		isSys:     true,
	}
	entry.CreateWithTS(types.SystemDBTS)
	return entry
}

func NewReplayDBEntry() *DBEntry {
	entry := &DBEntry{
		BaseEntry: NewReplayBaseEntry(),
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.SortedDList),
	}
	return entry
}

func (e *DBEntry) IsSystemDB() bool { return e.isSys }
func (e *DBEntry) CoarseTableCnt() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.entries)
}

func (e *DBEntry) Compare(o common.NodePayload) int {
	oe := o.(*DBEntry).BaseEntry
	return e.DoCompre(oe)
}

func (e *DBEntry) GetTenantID() uint32          { return e.acInfo.TenantID }
func (e *DBEntry) GetUserID() uint32            { return e.acInfo.UserID }
func (e *DBEntry) GetRoleID() uint32            { return e.acInfo.RoleID }
func (e *DBEntry) GetCreateAt() types.Timestamp { return e.acInfo.CreateAt }
func (e *DBEntry) GetName() string              { return e.name }
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
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntry.StringLocked(), e.GetFullName())
}

func (e *DBEntry) MakeTableIt(reverse bool) *common.SortedDListIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewSortedDListIt(e.RWMutex, e.link, reverse)
}

func (e *DBEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.String()))
	if level == common.PPL0 {
		return w.String()
	}
	it := e.MakeTableIt(true)
	for it.Valid() {
		table := it.Get().GetPayload().(*TableEntry)
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(table.PPString(level, depth+1, ""))
		it.Next()
	}
	return w.String()
}

func (e *DBEntry) GetBlockEntryByID(id *common.ID) (blk *BlockEntry, err error) {
	e.RLock()
	table, err := e.GetTableEntryByID(id.TableID)
	e.RUnlock()
	if err != nil {
		return
	}
	seg, err := table.GetSegmentByID(id.SegmentID)
	if err != nil {
		return
	}
	blk, err = seg.GetBlockEntryByID(id.BlockID)
	return
}

func (e *DBEntry) GetTableEntryByID(id uint64) (table *TableEntry, err error) {
	e.RLock()
	defer e.RUnlock()
	node := e.entries[id]
	if node == nil {
		return nil, ErrNotFound
	}
	table = node.GetPayload().(*TableEntry)
	return
}

func (e *DBEntry) txnGetNodeByName(name string, txnCtx txnif.AsyncTxn) (*common.DLNode, error) {
	e.RLock()
	defer e.RUnlock()
	node := e.nameNodes[name]
	if node == nil {
		return nil, ErrNotFound
	}
	return node.TxnGetTableNodeLocked(txnCtx)
}

func (e *DBEntry) GetTableEntry(name string, txnCtx txnif.AsyncTxn) (entry *TableEntry, err error) {
	n, err := e.txnGetNodeByName(name, txnCtx)
	if err != nil {
		return
	}
	entry = n.GetPayload().(*TableEntry)
	return
}

func (e *DBEntry) DropTableEntry(name string, txnCtx txnif.AsyncTxn) (deleted *TableEntry, err error) {
	dn, err := e.txnGetNodeByName(name, txnCtx)
	if err != nil {
		return
	}
	entry := dn.GetPayload().(*TableEntry)
	entry.Lock()
	defer entry.Unlock()
	needWait, txn := entry.NeedWaitCommitting(txnCtx.GetStartTS())
	if needWait {
		entry.Unlock()
		txn.GetTxnState(true)
		entry.Lock()
	}
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory) (created *TableEntry, err error) {
	e.Lock()
	created = NewTableEntry(e, schema, txnCtx, dataFactory)
	err = e.AddEntryLocked(created, txnCtx)
	e.Unlock()

	return created, err
}

func (e *DBEntry) RemoveEntry(table *TableEntry) (err error) {
	defer func() {
		if err == nil {
			e.catalog.AddTableCnt(-1)
			e.catalog.AddColumnCnt(-1 * len(table.schema.ColDefs))
		}
	}()
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(table.String()))
	e.Lock()
	defer e.Unlock()
	if n, ok := e.entries[table.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := e.nameNodes[table.GetSchema().Name]
		nn.DeleteNode(table.GetID())
		e.link.Delete(n)
		if nn.Length() == 0 {
			delete(e.nameNodes, table.GetSchema().Name)
		}
		delete(e.entries, table.GetID())
	}
	return
}

func (e *DBEntry) AddEntryLocked(table *TableEntry, txn txnif.AsyncTxn) (err error) {
	defer func() {
		if err == nil {
			e.catalog.AddTableCnt(1)
			e.catalog.AddColumnCnt(len(table.schema.ColDefs))
		}
	}()
	nn := e.nameNodes[table.schema.Name]
	if nn == nil {
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n

		nn := newNodeList(e, &e.nodesMu, table.schema.Name)
		e.nameNodes[table.schema.Name] = nn
		nn.CreateNode(table.GetID())
	} else {
		node := nn.GetTableNode()
		record := node.GetPayload().(*TableEntry)
		err = record.PrepareAdd(txn)
		if err != nil {
			return
		}
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return
}

func (e *DBEntry) MakeCommand(id uint32) (txnif.TxnCmd, error) {
	cmdType := CmdUpdateDatabase
	e.RLock()
	defer e.RUnlock()
	return newDBCmd(id, cmdType, e), nil
}

func (e *DBEntry) GetCatalog() *Catalog { return e.catalog }

func (e *DBEntry) RecurLoop(processor Processor) (err error) {
	tableIt := e.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload().(*TableEntry)
		if err = processor.OnTable(table); err != nil {
			if err == ErrStopCurrRecur {
				err = nil
				tableIt.Next()
				continue
			}
			break
		}
		if err = table.RecurLoop(processor); err != nil {
			return
		}
		tableIt.Next()
	}
	if err == ErrStopCurrRecur {
		err = nil
	}
	return err
}

func (e *DBEntry) PrepareRollback() (err error) {
	var isEmpty bool
	if isEmpty, err = e.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
		if err = e.catalog.RemoveEntry(e); err != nil {
			return
		}
	}
	return
}

func (e *DBEntry) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = e.BaseEntry.WriteAllTo(w); err != nil {
		return
	}
	x, err := e.acInfo.WriteTo(w)
	if err != nil {
		return
	}
	n += x
	if err = binary.Write(w, binary.BigEndian, uint16(len(e.name))); err != nil {
		return
	}
	var sn int
	sn, err = w.Write([]byte(e.name))
	n += int64(sn) + 2
	return
}

func (e *DBEntry) ReadFrom(r io.Reader) (n int64, err error) {
	if n, err = e.BaseEntry.ReadAllFrom(r); err != nil {
		return
	}
	x, err := e.acInfo.ReadFrom(r)
	if err != nil {
		return
	}
	n += x
	size := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}
	n += 2
	buf := make([]byte, size)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(size)
	e.name = string(buf)
	return
}

func (e *DBEntry) MakeLogEntry() *EntryCommand {
	return newDBCmd(0, CmdLogDatabase, e)
}

func (e *DBEntry) GetCheckpointItems(start, end types.TS) CheckpointItems {
	ret := e.CloneCommittedInRange(start, end)
	if ret == nil {
		return nil
	}
	return &DBEntry{
		BaseEntry: ret,
		acInfo:    e.acInfo,
		name:      e.name,
		catalog:   e.catalog,
	}
}

func (e *DBEntry) CloneCreateEntry() *DBEntry {
	return &DBEntry{
		acInfo:    e.acInfo,
		BaseEntry: e.BaseEntry.CloneCreateEntry(),
		name:      e.name,
	}
}

// IsActive is coarse API: no consistency check
func (e *DBEntry) IsActive() bool {
	e.RLock()
	defer e.RUnlock()
	dropped := e.IsDroppedCommitted()
	return !dropped
}
