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
	// "matrixone/pkg/logutil"
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

func (blk *Block) AddCount(n uint64) uint64 {
	newCnt := atomic.AddUint64(&blk.Count, n)
	if newCnt > blk.Segment.Table.Conf.BlockMaxRows {
		panic("logic error")
	}
	return newCnt
}

func (blk *Block) SetIndex(idx LogIndex) {
	blk.Lock()
	defer blk.Unlock()
	if blk.Index != nil {
		if !blk.Index.IsApplied() {
			panic("logic error")
		}
		blk.PrevIndex = blk.Index
		blk.Index = &idx
	} else {
		if blk.PrevIndex != nil {
			panic("logic error")
		}
		blk.Index = &idx
	}
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
	if count == 0 {
		blk.DataState = EMPTY
	} else if count < blk.MaxRowCount {
		blk.DataState = PARTIAL
	} else {
		blk.DataState = FULL
	}
	return nil
}

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
	var new_blk *Block
	new_blk = blk.copyNoLock(new_blk)
	return new_blk
}

func (blk *Block) copyNoLock(new_blk *Block) *Block {
	if new_blk == nil {
		new_blk = NewBlock(blk.ID, blk.Segment)
	}
	new_blk.Segment = blk.Segment
	new_blk.ID = blk.ID
	new_blk.TimeStamp = blk.TimeStamp
	new_blk.MaxRowCount = blk.MaxRowCount
	new_blk.BoundSate = blk.BoundSate
	new_blk.Count = blk.Count
	new_blk.Index = blk.Index
	new_blk.PrevIndex = blk.PrevIndex
	new_blk.DataState = blk.DataState

	return new_blk
}
