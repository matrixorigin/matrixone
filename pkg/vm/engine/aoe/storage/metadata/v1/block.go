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

package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
)

func EstimateColumnBlockSize(colIdx int, meta *Block) uint64 {
	switch meta.Segment.Table.Schema.ColDefs[colIdx].Type.Oid {
	case types.T_json, types.T_char, types.T_varchar:
		return meta.Segment.Table.Conf.BlockMaxRows * 2 * 4
	default:
		return meta.Segment.Table.Conf.BlockMaxRows * uint64(meta.Segment.Table.Schema.ColDefs[colIdx].Type.Size)
	}
}

func EstimateBlockSize(meta *Block) uint64 {
	size := uint64(0)
	for colIdx, _ := range meta.Segment.Table.Schema.ColDefs {
		size += EstimateColumnBlockSize(colIdx, meta)
	}
	return size
}

func NewBlock(id uint64, segment *Segment) *Block {
	blk := &Block{
		ID:          id,
		TimeStamp:   *NewTimeStamp(),
		MaxRowCount: segment.Table.Conf.BlockMaxRows,
		Segment:     segment,
	}
	return blk
}

// GetReplayIndex returns the clone of replay index for this block if exists.
func (blk *Block) GetReplayIndex() *LogIndex {
	if blk.Index == nil {
		return nil
	}
	ctx := &LogIndex{
		ID:       blk.Index.ID,
		Start:    blk.Index.Start,
		Count:    blk.Index.Count,
		Capacity: blk.Index.Capacity,
	}
	return ctx
}

// GetAppliedIndex returns ID of the applied index (previous index or the current
// index if it's applied) and true if exists, otherwise returns 0 and false.
func (blk *Block) GetAppliedIndex() (uint64, bool) {
	blk.RLock()
	defer blk.RUnlock()
	if blk.Index != nil && blk.Index.IsApplied() {
		return blk.Index.ID, true
	}

	if blk.PrevIndex != nil {
		return blk.PrevIndex.ID, true
	}

	return 0, false
}

func (blk *Block) GetID() uint64 {
	return blk.ID
}

func (blk *Block) TryUpgrade() bool {
	blk.Lock()
	defer blk.Unlock()
	if blk.Count == blk.MaxRowCount {
		blk.DataState = FULL
		return true
	}
	return false
}

func (blk *Block) GetSegmentID() uint64 {
	return blk.Segment.ID
}

func (blk *Block) GetCount() uint64 {
	return atomic.LoadUint64(&blk.Count)
}

func (blk *Block) AddCount(n uint64) (uint64, error) {
	curCnt := blk.GetCount()
	if curCnt+n > blk.Segment.Table.Conf.BlockMaxRows {
		return 0, errors.New(fmt.Sprintf("block row count %d > block max rows %d", curCnt+n, blk.Segment.Table.Conf.BlockMaxRows))
	}
	for !atomic.CompareAndSwapUint64(&blk.Count, curCnt, curCnt+n) {
		curCnt = blk.GetCount()
		if curCnt+n > blk.Segment.Table.Conf.BlockMaxRows {
			return 0, errors.New(fmt.Sprintf("block row count %d > block max rows %d", curCnt+n, blk.Segment.Table.Conf.BlockMaxRows))
		}
	}
	return curCnt+n, nil
}

// SetIndex changes the current index to previous index if exists, and
// sets the current index to idx.
func (blk *Block) SetIndex(idx LogIndex) error {
	blk.Lock()
	defer blk.Unlock()
	if blk.Index != nil {
		if !blk.Index.IsApplied() {
			return errors.New(fmt.Sprintf("block already has applied index: %d", blk.Index.ID))
		}
		blk.PrevIndex = blk.Index
		blk.Index = &idx
	} else {
		if blk.PrevIndex != nil {
			return errors.New(fmt.Sprintf("block has no index but has prev index: %d", blk.PrevIndex.ID))
		}
		blk.Index = &idx
	}
	return nil
}

func (blk *Block) String() string {
	s := fmt.Sprintf("Blk(%d-%d-%d)(DataState=%d)", blk.Segment.Table.ID, blk.Segment.ID, blk.ID, blk.DataState)
	if blk.IsDeleted(NowMicro()) {
		s += "[D]"
	}
	if blk.Count == blk.MaxRowCount {
		s += "[F]"
	}
	return s
}

func (blk *Block) IsFull() bool {
	return blk.Count == blk.MaxRowCount
}
// SetCount sets blk row count to count, changing its
// DataState if needed.
func (blk *Block) SetCount(count uint64) error {
	blk.Lock()
	defer blk.Unlock()
	if count > blk.MaxRowCount {
		return errors.New("SetCount exceeds max limit")
	}
	if count <= blk.Count {
		return errors.New("SetCount cannot set smaller count")
	}
	blk.Count = count
	if count < blk.MaxRowCount {
		blk.DataState = PARTIAL
	} else {
		blk.DataState = FULL
	}
	return nil
}

// Update upgrades the current blk to target block if possible.
func (blk *Block) Update(target *Block) error {
	blk.Lock()
	defer blk.Unlock()
	if blk.ID != target.ID || blk.Segment.ID != target.Segment.ID || blk.Segment.Table.ID != target.Segment.Table.ID {
		return errors.New("block, segment, table id not matched")
	}

	if blk.MaxRowCount != target.MaxRowCount {
		return errors.New("update block MaxRowCount not matched")
	}

	if blk.DataState > target.DataState {
		return errors.New(fmt.Sprintf("Cannot Update block from DataState %d to %d", blk.DataState, target.DataState))
	}

	if blk.Count > target.Count {
		return errors.New(fmt.Sprintf("Cannot Update block from Count %d to %d", blk.Count, target.Count))
	}
	target.copyNoLock(blk)
	blk.Segment.Table.UpdateVersion()

	return nil
}

// AsCommonID generates the unique commonID for the block.
func (blk *Block) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   blk.Segment.Table.ID,
		SegmentID: blk.Segment.ID,
		BlockID:   blk.ID,
	}
}

func (blk *Block) Marshal() ([]byte, error) {
	return json.Marshal(blk)
}

func (blk *Block) Copy() *Block {
	blk.RLock()
	defer blk.RUnlock()
	var newBlk *Block
	newBlk = blk.copyNoLock(newBlk)
	return newBlk
}

func (blk *Block) copyNoLock(newBlk *Block) *Block {
	if newBlk == nil {
		newBlk = NewBlock(blk.ID, blk.Segment)
	}
	newBlk.Segment = blk.Segment
	newBlk.ID = blk.ID
	newBlk.TimeStamp = blk.TimeStamp
	newBlk.MaxRowCount = blk.MaxRowCount
	newBlk.BoundSate = blk.BoundSate
	newBlk.Count = blk.Count
	newBlk.Index = blk.Index
	newBlk.PrevIndex = blk.PrevIndex
	newBlk.DataState = blk.DataState

	return newBlk
}
