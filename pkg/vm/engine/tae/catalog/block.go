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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type BlockDataFactory = func(meta *BlockEntry) data.Block

func compareBlockFn(a, b *BlockEntry) int {
	return a.ID.Compare(b.ID)
}

type BlockEntry struct {
	*BaseEntryImpl[*MetadataMVCCNode]
	segment *SegmentEntry
	*BlockNode
	ID      types.Blockid
	blkData data.Block
}

func NewReplayBlockEntry() *BlockEntry {
	return &BlockEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} },
		),
	}
}

func NewBlockEntry(segment *SegmentEntry, id types.Blockid, txn txnif.AsyncTxn, state EntryState, dataFactory BlockDataFactory) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: state,
		},
	}
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	e.BaseEntryImpl.CreateWithTxn(txn, &MetadataMVCCNode{})
	return e
}

func NewBlockEntryWithMeta(
	segment *SegmentEntry,
	id types.Blockid,
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory BlockDataFactory,
	metaLoc string,
	deltaLoc string) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: state,
		},
	}
	e.CreateWithTxnAndMeta(txn, metaLoc, deltaLoc)
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	return e
}

func NewStandaloneBlock(segment *SegmentEntry, id types.Blockid, ts types.TS) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: ES_Appendable,
		},
	}
	e.BaseEntryImpl.CreateWithTS(ts, &MetadataMVCCNode{})
	return e
}

func NewStandaloneBlockWithLoc(
	segment *SegmentEntry,
	id types.Blockid,
	ts types.TS,
	metaLoc string,
	delLoc string) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: ES_Appendable,
		},
	}
	e.CreateWithLoc(ts, metaLoc, delLoc)
	return e
}

func NewSysBlockEntry(segment *SegmentEntry, id types.Blockid) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: ES_Appendable,
		},
	}
	e.BaseEntryImpl.CreateWithTS(types.SystemDBTS, &MetadataMVCCNode{})
	return e
}

func (entry *BlockEntry) GetCatalog() *Catalog { return entry.segment.table.db.catalog }

func (entry *BlockEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *BlockEntry) GetSegment() *SegmentEntry {
	return entry.segment
}

func (entry *BlockEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdUpdateBlock
	entry.RLock()
	defer entry.RUnlock()
	return newBlockCmd(id, cmdType, entry), nil
}

func (entry *BlockEntry) Set1PC() {
	entry.GetLatestNodeLocked().Set1PC()
}
func (entry *BlockEntry) Is1PC() bool {
	return entry.GetLatestNodeLocked().Is1PC()
}
func (entry *BlockEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevelLocked(level))
	return s
}

func (entry *BlockEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s]BLK[%s]", entry.state.Repr(), id.String())
}

func (entry *BlockEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *BlockEntry) StringLocked() string {
	return fmt.Sprintf("[%s]BLK%s", entry.state.Repr(), entry.BaseEntryImpl.StringLocked())
}

func (entry *BlockEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringWithLevelLocked(level)
}

func (entry *BlockEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s]BLK[%s][C@%s,D@%s]",
			entry.state.Repr(), entry.ID.ShortString(), entry.GetCreatedAt().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s]BLK[%s]%s", entry.state.Repr(), entry.ID.String(), entry.BaseEntryImpl.StringLocked())
}

func (entry *BlockEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetSegment().GetTable().ID,
		SegmentID: entry.GetSegment().ID,
		BlockID:   entry.ID,
	}
}

func (entry *BlockEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeBlockFactory()
	entry.blkData = dataFactory(entry)
}
func (entry *BlockEntry) GetBlockData() data.Block { return entry.blkData }
func (entry *BlockEntry) GetSchema() *Schema       { return entry.GetSegment().GetTable().GetSchema() }
func (entry *BlockEntry) PrepareRollback() (err error) {
	var empty bool
	empty, err = entry.BaseEntryImpl.PrepareRollback()
	if err != nil {
		panic(err)
	}
	if empty {
		if err = entry.GetSegment().RemoveEntry(entry); err != nil {
			return
		}
	}
	return
}

func (entry *BlockEntry) MakeKey() []byte {
	prefix := entry.ID // copy id
	return prefix[:]
}

// PrepareCompact is performance insensitive
// a block can be compacted:
// 1. no uncommited node
// 2. at least one committed node
// 3. not compacted
func (entry *BlockEntry) PrepareCompact() bool {
	entry.RLock()
	defer entry.RUnlock()
	if entry.HasUncommittedNode() {
		return false
	}
	if !entry.HasCommittedNode() {
		return false
	}
	if entry.HasDropCommittedLocked() {
		return false
	}
	return true
}

// IsActive is coarse API: no consistency check
func (entry *BlockEntry) IsActive() bool {
	segment := entry.GetSegment()
	if !segment.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

// GetTerminationTS is coarse API: no consistency check
func (entry *BlockEntry) GetTerminationTS() (ts types.TS, terminated bool) {
	segmentEntry := entry.GetSegment()
	tableEntry := segmentEntry.GetTable()
	dbEntry := tableEntry.GetDB()

	dbEntry.RLock()
	terminated, ts = dbEntry.TryGetTerminatedTS(true)
	if terminated {
		dbEntry.RUnlock()
		return
	}
	dbEntry.RUnlock()

	tableEntry.RLock()
	terminated, ts = tableEntry.TryGetTerminatedTS(true)
	if terminated {
		tableEntry.RUnlock()
		return
	}
	tableEntry.RUnlock()

	// segmentEntry.RLock()
	// terminated,ts = segmentEntry.TryGetTerminatedTS(true)
	// segmentEntry.RUnlock()
	return
}

func (entry *BlockEntry) HasPersistedData() bool {
	return entry.GetMetaLoc() != ""
}
func (entry *BlockEntry) GetMetaLoc() string {
	entry.RLock()
	defer entry.RUnlock()
	if entry.GetLatestNodeLocked() == nil {
		return ""
	}
	str := entry.GetLatestNodeLocked().BaseNode.MetaLoc
	return str
}
func (entry *BlockEntry) HasPersistedDeltaData() bool {
	return entry.GetDeltaLoc() != ""
}
func (entry *BlockEntry) GetDeltaLoc() string {
	entry.RLock()
	defer entry.RUnlock()
	if entry.GetLatestNodeLocked() == nil {
		return ""
	}
	str := entry.GetLatestNodeLocked().BaseNode.DeltaLoc
	return str
}

func (entry *BlockEntry) GetVisibleMetaLoc(txn txnif.TxnReader) string {
	entry.RLock()
	defer entry.RUnlock()
	str := entry.GetVisibleNode(txn).BaseNode.MetaLoc
	return str
}
func (entry *BlockEntry) GetVisibleDeltaLoc(txn txnif.TxnReader) string {
	entry.RLock()
	defer entry.RUnlock()
	str := entry.GetVisibleNode(txn).BaseNode.DeltaLoc
	return str
}

func (entry *BlockEntry) CreateWithLoc(ts types.TS, metaLoc string, deltaLoc string) {
	baseNode := &MetadataMVCCNode{
		MetaLoc:  metaLoc,
		DeltaLoc: deltaLoc,
	}
	node := &MVCCNode[*MetadataMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
		BaseNode:    baseNode.CloneAll(),
	}
	entry.Insert(node)
}

func (entry *BlockEntry) CreateWithTxnAndMeta(txn txnif.AsyncTxn, metaLoc string, deltaLoc string) {
	baseNode := &MetadataMVCCNode{
		MetaLoc:  metaLoc,
		DeltaLoc: deltaLoc,
	}
	node := &MVCCNode[*MetadataMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode:    baseNode.CloneAll(),
	}
	entry.Insert(node)
}
func (entry *BlockEntry) UpdateMetaLoc(txn txnif.TxnReader, metaloc string) (isNewNode bool, err error) {
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
	baseNode := &MetadataMVCCNode{
		MetaLoc: metaloc,
	}
	var node *MVCCNode[*MetadataMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNode(txn)
	node.BaseNode.Update(baseNode)
	return
}

func (entry *BlockEntry) UpdateDeltaLoc(txn txnif.TxnReader, deltaloc string) (isNewNode bool, err error) {
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
	baseNode := &MetadataMVCCNode{
		DeltaLoc: deltaloc,
	}
	var node *MVCCNode[*MetadataMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNode(txn)
	node.BaseNode.Update(baseNode)
	return
}
