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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type DBEntry struct {
	// *BaseEntry
	*BaseEntry
	catalog *Catalog
	name    string
	isSys   bool

	entries   map[uint64]*common.DLNode
	nameNodes map[string]*nodeList
	link      *common.Link

	nodesMu sync.RWMutex
}

func NewDBEntry(catalog *Catalog, name string, txnCtx txnif.AsyncTxn) *DBEntry {
	id := catalog.NextDB()
	e := &DBEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
				Txn:    txnCtx,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		catalog:   catalog,
		name:      name,
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.Link),
	}
	return e
}

func NewSystemDBEntry(catalog *Catalog) *DBEntry {
	id := SystemDBID
	entry := &DBEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
			},
			RWMutex:  new(sync.RWMutex),
			ID:       id,
			CreateAt: 1,
		},
		catalog:   catalog,
		name:      SystemDBName,
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.Link),
		isSys:     true,
	}
	return entry
}

func NewReplayDBEntry() *DBEntry {
	entry := &DBEntry{
		BaseEntry: new(BaseEntry),
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.Link),
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

func (e *DBEntry) GetName() string { return e.name }

func (e *DBEntry) String() string {
	e.RLock()
	defer e.RUnlock()
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntry.String(), e.name)
}

func (e *DBEntry) StringLocked() string {
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntry.String(), e.name)
}

func (e *DBEntry) MakeTableIt(reverse bool) *common.LinkIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewLinkIt(e.RWMutex, e.link, reverse)
}

func (e *DBEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.String())
	if level == common.PPL0 {
		return s
	}
	var body string
	it := e.MakeTableIt(true)
	for it.Valid() {
		table := it.Get().GetPayload().(*TableEntry)
		if len(body) == 0 {
			body = table.PPString(level, depth+1, "")
		} else {
			body = fmt.Sprintf("%s\n%s", body, table.PPString(level, depth+1, ""))
		}
		it.Next()
	}

	if len(body) == 0 {
		return s
	}
	return fmt.Sprintf("%s\n%s", s, body)
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

func (e *DBEntry) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) (*common.DLNode, error) {
	node := e.nameNodes[name]
	if node == nil {
		return nil, ErrNotFound
	}
	return node.TxnGetTableNodeLocked(txnCtx)
}

func (e *DBEntry) GetTableEntry(name string, txnCtx txnif.AsyncTxn) (entry *TableEntry, err error) {
	e.RLock()
	n, err := e.txnGetNodeByNameLocked(name, txnCtx)
	e.RUnlock()
	if err != nil {
		return
	}
	entry = n.GetPayload().(*TableEntry)
	return
}

func (e *DBEntry) DropTableEntry(name string, txnCtx txnif.AsyncTxn) (deleted *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	dn, err := e.txnGetNodeByNameLocked(name, txnCtx)
	if err != nil {
		return
	}
	entry := dn.GetPayload().(*TableEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory) (created *TableEntry, err error) {
	e.Lock()
	created = NewTableEntry(e, schema, txnCtx, dataFactory)
	err = e.AddEntryLocked(created)
	e.Unlock()

	return created, err
}

func (e *DBEntry) RemoveEntry(table *TableEntry) (err error) {
	logutil.Infof("Removing: %s", table.String())
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

func (e *DBEntry) AddEntryLocked(table *TableEntry) error {
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
		record.RLock()
		err := record.PrepareWrite(table.GetTxn(), record.RWMutex)
		if err != nil {
			record.RUnlock()
			return err
		}
		if record.HasActiveTxn() {
			if !record.IsDroppedUncommitted() {
				record.RUnlock()
				return ErrDuplicate
			}
		} else if !record.HasDropped() {
			record.RUnlock()
			return ErrDuplicate
		}
		record.RUnlock()
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return nil
}

func (e *DBEntry) MakeCommand(id uint32) (txnif.TxnCmd, error) {
	cmdType := CmdCreateDatabase
	e.RLock()
	defer e.RUnlock()
	if e.CurrOp == OpSoftDelete {
		cmdType = CmdDropDatabase
	}
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
	e.RLock()
	currOp := e.CurrOp
	e.RUnlock()
	if currOp == OpCreate {
		err = e.catalog.RemoveEntry(e)
	}
	if err = e.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	return
}

func (entry *DBEntry) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint16(len(entry.name))); err != nil {
		return
	}
	var sn int
	sn, err = w.Write([]byte(entry.name))
	n += int64(sn) + 2
	return
}

func (entry *DBEntry) ReadFrom(r io.Reader) (n int64, err error) {
	if n, err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
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
	entry.name = string(buf)
	return
}

func (entry *DBEntry) MakeLogEntry() *EntryCommand {
	return newDBCmd(0, CmdLogDatabase, entry)
}

func (entry *DBEntry) Clone() CheckpointItem {
	cloned := &DBEntry{
		BaseEntry: entry.BaseEntry.Clone(),
		name:      entry.name,
	}
	return cloned
}

func (entry *DBEntry) CloneCreate() CheckpointItem {
	cloned := &DBEntry{
		BaseEntry: entry.BaseEntry.CloneCreate(),
		name:      entry.name,
	}
	return cloned
}

// Coarse API: no consistency check
func (entry *DBEntry) IsActive() bool {
	entry.RLock()
	dropped := entry.IsDroppedCommitted()
	entry.RUnlock()
	return !dropped
}
