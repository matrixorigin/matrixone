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
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type TableDataFactory = func(meta *TableEntry) data.Table

type TableEntry struct {
	*BaseEntry
	db        *DBEntry
	schema    *Schema
	entries   map[uint64]*common.DLNode
	link      *common.Link
	tableData data.Table
}

func NewTableEntry(db *DBEntry, schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory) *TableEntry {
	id := db.catalog.NextTable()
	e := &TableEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txnCtx,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		db:      db,
		schema:  schema,
		link:    new(common.Link),
		entries: make(map[uint64]*common.DLNode),
	}
	if dataFactory != nil {
		e.tableData = dataFactory(e)
	}
	return e
}

func MockStaloneTableEntry(id uint64, schema *Schema) *TableEntry {
	return &TableEntry{
		BaseEntry: &BaseEntry{
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		schema:  schema,
		link:    new(common.Link),
		entries: make(map[uint64]*common.DLNode),
	}
}

func (entry *TableEntry) GetSegmentByID(id uint64) (seg *SegmentEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.entries[id]
	if node == nil {
		return nil, ErrNotFound
	}
	return node.GetPayload().(*SegmentEntry), nil
}

func (entry *TableEntry) MakeSegmentIt(reverse bool) *common.LinkIt {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewLinkIt(entry.RWMutex, entry.link, reverse)
}

func (entry *TableEntry) CreateSegment(txn txnif.AsyncTxn, state EntryState, dataFactory SegmentDataFactory) (created *SegmentEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	created = NewSegmentEntry(entry, txn, state, dataFactory)
	entry.addEntryLocked(created)
	return
}

func (entry *TableEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdCreateTable
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropTable
	}
	return newTableCmd(id, cmdType, entry), nil
}

func (entry *TableEntry) addEntryLocked(segment *SegmentEntry) {
	n := entry.link.Insert(segment)
	entry.entries[segment.GetID()] = n
}

func (entry *TableEntry) deleteEntryLocked(segment *SegmentEntry) error {
	if n, ok := entry.entries[segment.GetID()]; !ok {
		return ErrNotFound
	} else {
		entry.link.Delete(n)
	}
	return nil
}

func (entry *TableEntry) GetSchema() *Schema {
	return entry.schema
}

func (entry *TableEntry) Compare(o common.NodePayload) int {
	oe := o.(*TableEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *TableEntry) GetDB() *DBEntry {
	return entry.db
}

func (entry *TableEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.String())
	if level == common.PPL0 {
		return s
	}
	var body string
	it := entry.MakeSegmentIt(true)
	for it.Valid() {
		segment := it.Get().GetPayload().(*SegmentEntry)
		if len(body) == 0 {
			body = segment.PPString(level, depth+1, prefix)
		} else {
			body = fmt.Sprintf("%s\n%s", body, segment.PPString(level, depth+1, prefix))
		}
		it.Next()
	}
	if len(body) == 0 {
		return s
	}
	return fmt.Sprintf("%s\n%s", s, body)
}

func (entry *TableEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *TableEntry) StringLocked() string {
	return fmt.Sprintf("TABLE%s[name=%s]", entry.BaseEntry.String(), entry.schema.Name)
}

func (entry *TableEntry) GetCatalog() *Catalog { return entry.db.catalog }

func (entry *TableEntry) GetTableData() data.Table { return entry.tableData }

func (entry *TableEntry) LastAppendableSegmemt() (seg *SegmentEntry) {
	it := entry.MakeSegmentIt(false)
	for it.Valid() {
		itSeg := it.Get().GetPayload().(*SegmentEntry)
		if itSeg.IsAppendable() {
			seg = itSeg
			break
		}
		it.Next()
	}
	return seg
}

func (entry *TableEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID: entry.GetID(),
	}
}

func (entry *TableEntry) RecurLoop(processor Processor) (err error) {
	segIt := entry.MakeSegmentIt(true)
	for segIt.Valid() {
		segment := segIt.Get().GetPayload().(*SegmentEntry)
		if err = processor.OnSegment(segment); err != nil {
			if err == ErrStopCurrRecur {
				err = nil
				continue
			}
			break
		}
		blkIt := segment.MakeBlockIt(true)
		for blkIt.Valid() {
			block := blkIt.Get().GetPayload().(*BlockEntry)
			if err = processor.OnBlock(block); err != nil {
				return
			}
			blkIt.Next()
		}
		if err = processor.OnPostSegment(segment); err != nil {
			break
		}
		segIt.Next()
	}
	if err == ErrStopCurrRecur {
		err = nil
	}
	return err
}

func (entry *TableEntry) DropSegmentEntry(id uint64, txn txnif.AsyncTxn) (deleted *SegmentEntry, err error) {
	seg, err := entry.GetSegmentByID(id)
	if err != nil {
		return
	}
	seg.Lock()
	defer seg.Unlock()
	err = seg.DropEntryLocked(txn)
	if err == nil {
		deleted = seg
	}
	return
}

func (entry *TableEntry) RemoveEntry(segment *SegmentEntry) (err error) {
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(segment)
}

func (entry *TableEntry) PrepareRollback() (err error) {
	entry.RLock()
	currOp := entry.CurrOp
	entry.RUnlock()
	if currOp == OpCreate {
		err = entry.GetDB().RemoveEntry(entry)
	}
	if err = entry.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	return
}

func (entry *TableEntry) WriteTo(w io.Writer) (err error) {
	if err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	buf, err := entry.schema.Marshal()
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func (entry *TableEntry) ReadFrom(r io.Reader) (err error) {
	if err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
	if entry.schema == nil {
		entry.schema = NewEmptySchema("")
	}
	return entry.schema.ReadFrom(r)
}

func (entry *TableEntry) MakeLogEntry() *EntryCommand {
	return newTableCmd(0, CmdLogTable, entry)
}

func (entry *TableEntry) Clone() CheckpointItem {
	cloned := &TableEntry{
		BaseEntry: entry.BaseEntry.Clone(),
		schema:    entry.schema,
		db:        entry.db,
	}
	return cloned
}

func (entry *TableEntry) CloneCreate() CheckpointItem {
	cloned := &TableEntry{
		BaseEntry: entry.BaseEntry.CloneCreate(),
		schema:    entry.schema,
		db:        entry.db,
	}
	return cloned
}
