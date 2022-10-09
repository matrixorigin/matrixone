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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type SegmentDataFactory = func(meta *SegmentEntry) data.Segment

func compareSegmentFn(a, b *SegmentEntry) int {
	return a.MetaBaseEntry.DoCompre(b.MetaBaseEntry)
}

type SegmentEntry struct {
	*MetaBaseEntry
	table   *TableEntry
	entries map[uint64]*common.GenericDLNode[*BlockEntry]
	link    *common.GenericSortedDList[*BlockEntry]
	state   EntryState
	segData data.Segment
}

func NewSegmentEntry(table *TableEntry, txn txnif.AsyncTxn, state EntryState, dataFactory SegmentDataFactory) *SegmentEntry {
	id := table.GetDB().catalog.NextSegment()
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(id),
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[uint64]*common.GenericDLNode[*BlockEntry]),
		state:         state,
	}
	e.CreateWithTxn(txn)
	if dataFactory != nil {
		e.segData = dataFactory(e)
	}
	return e
}

func NewReplaySegmentEntry() *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewReplayMetaBaseEntry(),
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[uint64]*common.GenericDLNode[*BlockEntry]),
	}
	return e
}

func NewStandaloneSegment(table *TableEntry, id uint64, ts types.TS) *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(id),
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[uint64]*common.GenericDLNode[*BlockEntry]),
		state:         ES_Appendable,
	}
	e.CreateWithTS(ts)
	return e
}

func NewSysSegmentEntry(table *TableEntry, id uint64) *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(id),
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[uint64]*common.GenericDLNode[*BlockEntry]),
		state:         ES_Appendable,
	}
	e.CreateWithTS(types.SystemDBTS)
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

// XXX API like this, why do we need the error?   Isn't blk is nil enough?
func (entry *SegmentEntry) GetBlockEntryByIDLocked(id uint64) (blk *BlockEntry, err error) {
	node := entry.entries[id]
	if node == nil {
		err = moerr.NewNotFound()
		return
	}
	blk = node.GetPayload()
	return
}

func (entry *SegmentEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdUpdateSegment
	entry.RLock()
	defer entry.RUnlock()
	return newSegmentCmd(id, cmdType, entry), nil
}

func (entry *SegmentEntry) Set1PC() {
	entry.GetNodeLocked().Set1PC()
}
func (entry *SegmentEntry) Is1PC() bool {
	return entry.GetNodeLocked().Is1PC()
}
func (entry *SegmentEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload()
		block.RLock()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(block.PPString(level, depth+1, prefix))
		block.RUnlock()
		it.Next()
	}
	return w.String()
}

func (entry *SegmentEntry) StringLocked() string {
	return entry.StringWithLevelLocked(common.PPL1)
}

func (entry *SegmentEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s]SEG[%s]", entry.state.Repr(), id.String())
}

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *SegmentEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringWithLevelLocked(level)
}

func (entry *SegmentEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s]SEG[%d][C@%s,D@%s]",
			entry.state.Repr(), entry.ID, entry.GetCreatedAt().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s]SEG%s", entry.state.Repr(), entry.MetaBaseEntry.StringLocked())
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

func (entry *SegmentEntry) GetAppendableBlockCnt() int {
	cnt := 0
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		if it.Get().GetPayload().IsAppendable() {
			cnt++
		}
		it.Next()
	}
	return cnt
}
func (entry *SegmentEntry) GetAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload()
		if itBlk.IsAppendable() {
			blk = itBlk
			break
		}
		it.Next()
	}
	return
}
func (entry *SegmentEntry) LastAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload()
		dropped := itBlk.HasDropped()
		if itBlk.IsAppendable() && !dropped {
			blk = itBlk
			break
		}
		it.Next()
	}
	return
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
	needWait, waitTxn := blk.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		blk.Unlock()
		waitTxn.GetTxnState(true)
		blk.Lock()
	}
	var isNewNode bool
	isNewNode, err = blk.DropEntryLocked(txn)
	if err == nil && isNewNode {
		deleted = blk
	}
	return
}

func (entry *SegmentEntry) MakeBlockIt(reverse bool) *common.GenericSortedDListIt[*BlockEntry] {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewGenericSortedDListIt(entry.RWMutex, entry.link, reverse)
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
		return moerr.NewNotFound()
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
	var isEmpty bool
	if isEmpty, err = entry.MetaBaseEntry.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
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
	return
}

func (entry *SegmentEntry) WriteTo(w io.Writer) (n int64, err error) {
	sn := int64(0)
	if sn, err = entry.MetaBaseEntry.WriteAllTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, entry.state); err != nil {
		return
	}
	n = sn + 1
	return
}

func (entry *SegmentEntry) ReadFrom(r io.Reader) (n int64, err error) {
	if n, err = entry.MetaBaseEntry.ReadAllFrom(r); err != nil {
		return
	}
	err = binary.Read(r, binary.BigEndian, &entry.state)
	n += 1
	return
}

func (entry *SegmentEntry) MakeLogEntry() *EntryCommand {
	return newSegmentCmd(0, CmdLogSegment, entry)
}

func (entry *SegmentEntry) GetCheckpointItems(start, end types.TS) CheckpointItems {
	ret := entry.CloneCommittedInRange(start, end)
	if ret == nil {
		return nil
	}
	return &SegmentEntry{
		MetaBaseEntry: ret.(*MetaBaseEntry),
		state:         entry.state,
		table:         entry.table,
	}
}

func (entry *SegmentEntry) GetScheduler() tasks.TaskScheduler {
	return entry.GetTable().GetCatalog().GetScheduler()
}

func (entry *SegmentEntry) CollectBlockEntries(commitFilter func(be *MetaBaseEntry) bool, blockFilter func(be *BlockEntry) bool) []*BlockEntry {
	blks := make([]*BlockEntry, 0)
	blkIt := entry.MakeBlockIt(true)
	for blkIt.Valid() {
		blk := blkIt.Get().GetPayload()
		blk.RLock()
		if commitFilter != nil && blockFilter != nil {
			if commitFilter(blk.MetaBaseEntry) && blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if blockFilter != nil {
			if blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if commitFilter != nil {
			if commitFilter(blk.MetaBaseEntry) {
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

func (entry *SegmentEntry) TreeMaxDropCommitEntry() BaseEntry {
	table := entry.GetTable()
	db := table.GetDB()
	if db.IsDroppedCommitted() {
		return db.DBBaseEntry
	}
	if table.IsDroppedCommitted() {
		return table.TableBaseEntry
	}
	if entry.IsDroppedCommitted() {
		return entry.MetaBaseEntry
	}
	return nil
}
