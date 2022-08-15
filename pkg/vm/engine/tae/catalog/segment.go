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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type SegmentDataFactory = func(meta *SegmentEntry) data.Segment

type SegmentEntry struct {
	*BaseEntry
	table   *TableEntry
	entries map[uint64]*common.DLNode
	link    *common.Link
	state   EntryState
	segData data.Segment
}

func NewSegmentEntry(table *TableEntry, txn txnif.AsyncTxn, state EntryState, dataFactory SegmentDataFactory) *SegmentEntry {
	id := table.GetDB().catalog.NextSegment()
	e := &SegmentEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		table:   table,
		link:    new(common.Link),
		entries: make(map[uint64]*common.DLNode),
		state:   state,
	}
	if dataFactory != nil {
		e.segData = dataFactory(e)
	}
	return e
}

func NewReplaySegmentEntry() *SegmentEntry {
	e := &SegmentEntry{
		BaseEntry: new(BaseEntry),
		link:      new(common.Link),
		entries:   make(map[uint64]*common.DLNode),
	}
	return e
}

func NewStandaloneSegment(table *TableEntry, id uint64, ts types.TS) *SegmentEntry {
	e := &SegmentEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
			},
			RWMutex:  new(sync.RWMutex),
			ID:       id,
			CreateAt: ts,
		},
		table:   table,
		link:    new(common.Link),
		entries: make(map[uint64]*common.DLNode),
		state:   ES_Appendable,
	}
	return e
}

func NewSysSegmentEntry(table *TableEntry, id uint64) *SegmentEntry {
	e := &SegmentEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
			},
			RWMutex:  new(sync.RWMutex),
			ID:       id,
			CreateAt: types.SystemDBTS,
		},
		table:   table,
		link:    new(common.Link),
		entries: make(map[uint64]*common.DLNode),
		state:   ES_Appendable,
	}
	var bid uint64
	if table.schema.Name == SystemTableSchema.Name {
		bid = SystemBlock_Table_ID
	} else if table.schema.Name == SystemDBSchema.Name {
		bid = SystemBlock_DB_ID
	} else if table.schema.Name == SystemColumnSchema.Name {
		bid = SystemBlock_Columns_ID
	} else {
		panic("not supported")
	}
	block := NewSysBlockEntry(e, bid)
	e.AddEntryLocked(block)
	return e
}

func (entry *SegmentEntry) GetBlockEntryByID(id uint64) (blk *BlockEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	return entry.GetBlockEntryByIDLocked(id)
}

func (entry *SegmentEntry) GetBlockEntryByIDLocked(id uint64) (blk *BlockEntry, err error) {
	node := entry.entries[id]
	if node == nil {
		err = ErrNotFound
		return
	}
	blk = node.GetPayload().(*BlockEntry)
	return
}

func (entry *SegmentEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdCreateSegment
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropSegment
	}
	return newSegmentCmd(id, cmdType, entry), nil
}

func (entry *SegmentEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.String()))
	if level == common.PPL0 {
		return w.String()
	}
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload().(*BlockEntry)
		block.RLock()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(block.PPString(level, depth+1, prefix))
		block.RUnlock()
		it.Next()
	}
	return w.String()
}

func (entry *SegmentEntry) StringLocked() string {
	return fmt.Sprintf("[%s]SEGMENT%s", entry.state.Repr(), entry.BaseEntry.String())
}

func (entry *SegmentEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s]SEGMENT[%s]", entry.state.Repr(), id.String())
}

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *SegmentEntry) BlockCnt() int {
	return len(entry.entries)
}

func (entry *SegmentEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *SegmentEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *SegmentEntry) Compare(o common.NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *SegmentEntry) GetAppendableBlockCnt() int {
	cnt := 0
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		if it.Get().GetPayload().(*BlockEntry).IsAppendable() {
			cnt++
		}
		it.Next()
	}
	return cnt
}

func (entry *SegmentEntry) LastAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload().(*BlockEntry)
		if itBlk.IsAppendable() {
			blk = itBlk
			break
		}
		it.Next()
	}
	return blk
}

func (entry *SegmentEntry) CreateBlock(txn txnif.AsyncTxn, state EntryState, dataFactory BlockDataFactory) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	created = NewBlockEntry(entry, txn, state, dataFactory)
	entry.AddEntryLocked(created)
	return
}

func (entry *SegmentEntry) DropBlockEntry(id uint64, txn txnif.AsyncTxn) (deleted *BlockEntry, err error) {
	blk, err := entry.GetBlockEntryByID(id)
	if err != nil {
		return
	}
	blk.Lock()
	defer blk.Unlock()
	err = blk.DropEntryLocked(txn)
	if err == nil {
		deleted = blk
	}
	return
}

func (entry *SegmentEntry) MakeBlockIt(reverse bool) *common.LinkIt {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewLinkIt(entry.RWMutex, entry.link, reverse)
}

func (entry *SegmentEntry) AddEntryLocked(block *BlockEntry) {
	n := entry.link.Insert(block)
	entry.entries[block.GetID()] = n
}

func (entry *SegmentEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetTable().GetID(),
		SegmentID: entry.GetID(),
	}
}

func (entry *SegmentEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *SegmentEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeSegmentFactory()
	entry.segData = dataFactory(entry)
}
func (entry *SegmentEntry) GetSegmentData() data.Segment { return entry.segData }

func (entry *SegmentEntry) deleteEntryLocked(block *BlockEntry) error {
	if n, ok := entry.entries[block.GetID()]; !ok {
		return ErrNotFound
	} else {
		entry.link.Delete(n)
		delete(entry.entries, block.GetID())
	}
	return nil
}

func (entry *SegmentEntry) RemoveEntry(block *BlockEntry) (err error) {
	logutil.Debug("[Catalog]", common.OperationField("remove"),
		common.OperandField(block.String()))
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(block)
}

func (entry *SegmentEntry) PrepareRollback() (err error) {
	entry.RLock()
	currOp := entry.CurrOp
	logutil.Infof("PrepareRollback %s", entry.StringLocked())
	entry.RUnlock()
	if currOp == OpCreate {
		if err = entry.GetTable().RemoveEntry(entry); err != nil {
			return
		}
		//TODO: maybe scheduled?
		// entry.GetCatalog().GetScheduler().ScheduleScopedFn(nil, tasks.IOTask, entry.AsCommonID(), entry.DestroyData)
		if err = entry.DestroyData(); err != nil {
			logutil.Fatalf("Cannot destroy uncommitted segment [%s] data: %v", entry.Repr(), err)
			return
		}
	}
	if err = entry.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	return
}

func (entry *SegmentEntry) WriteTo(w io.Writer) (n int64, err error) {
	sn := int64(0)
	if sn, err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, entry.state); err != nil {
		return
	}
	n = sn + 1
	return
}

func (entry *SegmentEntry) ReadFrom(r io.Reader) (n int64, err error) {
	if n, err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
	err = binary.Read(r, binary.BigEndian, &entry.state)
	n += 1
	return
}

func (entry *SegmentEntry) MakeLogEntry() *EntryCommand {
	return newSegmentCmd(0, CmdLogSegment, entry)
}

func (entry *SegmentEntry) Clone() CheckpointItem {
	cloned := &SegmentEntry{
		BaseEntry: entry.BaseEntry.Clone(),
		state:     entry.state,
		table:     entry.table,
	}
	return cloned
}

func (entry *SegmentEntry) CloneCreate() CheckpointItem {
	cloned := &SegmentEntry{
		BaseEntry: entry.BaseEntry.CloneCreate(),
		state:     entry.state,
		table:     entry.table,
	}
	return cloned
}

func (entry *SegmentEntry) GetScheduler() tasks.TaskScheduler {
	return entry.GetTable().GetCatalog().GetScheduler()
}

func (entry *SegmentEntry) CollectBlockEntries(commitFilter func(be *BaseEntry) bool, blockFilter func(be *BlockEntry) bool) []*BlockEntry {
	blks := make([]*BlockEntry, 0)
	blkIt := entry.MakeBlockIt(true)
	for blkIt.Valid() {
		blk := blkIt.Get().GetPayload().(*BlockEntry)
		blk.RLock()
		if commitFilter != nil && blockFilter != nil {
			if commitFilter(blk.BaseEntry) && blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if blockFilter != nil {
			if blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if commitFilter != nil {
			if commitFilter(blk.BaseEntry) {
				blks = append(blks, blk)
			}
		}
		blk.RUnlock()
		blkIt.Next()
	}
	return blks
}

func (entry *SegmentEntry) DestroyData() (err error) {
	if entry.segData != nil {
		err = entry.segData.Destroy()
	}
	return
}

// IsActive is coarse API: no consistency check
func (entry *SegmentEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	entry.RLock()
	dropped := entry.IsDroppedCommitted()
	entry.RUnlock()
	return !dropped
}

func (entry *SegmentEntry) TreeMaxDropCommitEntry() *BaseEntry {
	table := entry.GetTable()
	db := table.GetDB()
	if db.IsDroppedCommitted() {
		return db.BaseEntry
	}
	if table.IsDroppedCommitted() {
		return table.BaseEntry
	}
	if entry.IsDroppedCommitted() {
		return entry.BaseEntry
	}
	return nil
}
