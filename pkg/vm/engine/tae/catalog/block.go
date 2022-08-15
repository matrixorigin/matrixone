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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type BlockDataFactory = func(meta *BlockEntry) data.Block

type BlockEntry struct {
	*BaseEntry
	segment *SegmentEntry
	state   EntryState
	blkData data.Block
}

func NewReplayBlockEntry() *BlockEntry {
	return &BlockEntry{
		BaseEntry: new(BaseEntry),
	}
}

func NewBlockEntry(segment *SegmentEntry, txn txnif.AsyncTxn, state EntryState, dataFactory BlockDataFactory) *BlockEntry {
	id := segment.GetTable().GetDB().catalog.NextBlock()
	e := &BlockEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		segment: segment,
		state:   state,
	}
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	return e
}

func NewStandaloneBlock(segment *SegmentEntry, id uint64, ts types.TS) *BlockEntry {
	e := &BlockEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
			},
			RWMutex:  new(sync.RWMutex),
			ID:       id,
			CreateAt: ts,
		},
		segment: segment,
		state:   ES_Appendable,
	}
	return e
}

func NewSysBlockEntry(segment *SegmentEntry, id uint64) *BlockEntry {
	e := &BlockEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
			},
			RWMutex:  new(sync.RWMutex),
			ID:       id,
			CreateAt: types.SystemDBTS,
		},
		segment: segment,
		state:   ES_Appendable,
	}
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
	cmdType := CmdCreateBlock
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropBlock
	}
	return newBlockCmd(id, cmdType, entry), nil
}

func (entry *BlockEntry) Compare(o common.NodePayload) int {
	oe := o.(*BlockEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *BlockEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringLocked())
	return s
}

func (entry *BlockEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s]BLOCK[%s]", entry.state.Repr(), id.String())
}

func (entry *BlockEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *BlockEntry) StringLocked() string {
	return fmt.Sprintf("[%s]BLOCK%s", entry.state.Repr(), entry.BaseEntry.String())
}

func (entry *BlockEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetSegment().GetTable().GetID(),
		SegmentID: entry.GetSegment().GetID(),
		BlockID:   entry.GetID(),
	}
}

func (entry *BlockEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeBlockFactory(entry.segment.GetSegmentData().GetSegmentFile())
	entry.blkData = dataFactory(entry)
}
func (entry *BlockEntry) GetBlockData() data.Block { return entry.blkData }
func (entry *BlockEntry) GetSchema() *Schema       { return entry.GetSegment().GetTable().GetSchema() }
func (entry *BlockEntry) GetFileTs() (types.TS, error) {
	return entry.GetBlockData().GetBlockFile().ReadTS()
}
func (entry *BlockEntry) PrepareRollback() (err error) {
	entry.RLock()
	currOp := entry.CurrOp
	entry.RUnlock()
	if currOp == OpCreate {
		if err = entry.GetSegment().RemoveEntry(entry); err != nil {
			return
		}
	}
	if err = entry.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	return
}

func (entry *BlockEntry) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, entry.state); err != nil {
		return
	}
	n += 1
	return
}

func (entry *BlockEntry) ReadFrom(r io.Reader) (n int64, err error) {
	if n, err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
	err = binary.Read(r, binary.BigEndian, &entry.state)
	n += 1
	return
}

func (entry *BlockEntry) MakeLogEntry() *EntryCommand {
	return newBlockCmd(0, CmdLogBlock, entry)
}

func (entry *BlockEntry) Clone() CheckpointItem {
	cloned := &BlockEntry{
		BaseEntry: entry.BaseEntry.Clone(),
		state:     entry.state,
		segment:   entry.segment,
	}
	return cloned
}

func (entry *BlockEntry) CloneCreate() CheckpointItem {
	cloned := &BlockEntry{
		BaseEntry: entry.BaseEntry.CloneCreate(),
		state:     entry.state,
		segment:   entry.segment,
	}
	return cloned
}

func (entry *BlockEntry) DestroyData() (err error) {
	if entry.blkData == nil {
		return
	}
	return entry.blkData.Destroy()
}

func (entry *BlockEntry) MakeKey() []byte {
	return model.EncodeBlockKeyPrefix(entry.segment.ID, entry.ID)
}

// IsActive is coarse API: no consistency check
func (entry *BlockEntry) IsActive() bool {
	segment := entry.GetSegment()
	if !segment.IsActive() {
		return false
	}
	entry.RLock()
	dropped := entry.IsDroppedCommitted()
	entry.RUnlock()
	return !dropped
}

// GetTerminationTS is coarse API: no consistency check
func (entry *BlockEntry) GetTerminationTS() (ts types.TS, terminated bool) {
	segmentEntry := entry.GetSegment()
	tableEntry := segmentEntry.GetTable()
	dbEntry := tableEntry.GetDB()

	dbEntry.RLock()
	terminated = dbEntry.IsDroppedCommitted()
	if terminated {
		ts = dbEntry.DeleteAt
	}
	dbEntry.RUnlock()
	if terminated {
		return
	}

	tableEntry.RLock()
	terminated = tableEntry.IsDroppedCommitted()
	if terminated {
		ts = tableEntry.DeleteAt
	}
	tableEntry.RUnlock()
	return
	// segmentEntry.RLock()
	// terminated = segmentEntry.IsDroppedCommitted()
	// if terminated {
	// 	ts = segmentEntry.DeleteAt
	// }
	// segmentEntry.RUnlock()
	// if terminated {
	// 	return
	// }
}
