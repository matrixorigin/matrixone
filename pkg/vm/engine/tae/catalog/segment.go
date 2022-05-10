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
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringLocked())
	if level == common.PPL0 {
		return s
	}
	var body string
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload().(*BlockEntry)
		block.RLock()
		if len(body) == 0 {
			body = block.PPString(level, depth+1, prefix)
		} else {
			body = fmt.Sprintf("%s\n%s", body, block.PPString(level, depth+1, prefix))
		}
		block.RUnlock()
		it.Next()
	}
	if len(body) == 0 {
		return s
	}
	return fmt.Sprintf("%s\n%s", s, body)
}

func (entry *SegmentEntry) StringLocked() string {
	return fmt.Sprintf("[%s]SEGMENT%s", entry.state.Repr(), entry.BaseEntry.String())
}

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
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
	entry.addEntryLocked(created)
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

func (entry *SegmentEntry) addEntryLocked(block *BlockEntry) {
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

func (entry *SegmentEntry) GetSegmentData() data.Segment { return entry.segData }

func (entry *SegmentEntry) deleteEntryLocked(block *BlockEntry) error {
	if n, ok := entry.entries[block.GetID()]; !ok {
		return ErrNotFound
	} else {
		entry.link.Delete(n)
	}
	return nil
}

func (entry *SegmentEntry) RemoveEntry(block *BlockEntry) (err error) {
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(block)
}

func (entry *SegmentEntry) PrepareRollback() (err error) {
	entry.RLock()
	currOp := entry.CurrOp
	entry.RUnlock()
	if currOp == OpCreate {
		err = entry.GetTable().RemoveEntry(entry)
	}
	if err = entry.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	return
}

func (entry *SegmentEntry) WriteTo(w io.Writer) (err error) {
	if err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, entry.state); err != nil {
		return
	}
	return
}

func (entry *SegmentEntry) ReadFrom(r io.Reader) (err error) {
	if err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
	return binary.Read(r, binary.BigEndian, &entry.state)
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
