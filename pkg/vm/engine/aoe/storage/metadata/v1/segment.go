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

	"matrixone/pkg/vm/engine/aoe/storage/common"
)

func NewSegment(table *Table, id uint64) *Segment {
	seg := &Segment{
		ID:            id,
		Table:         table,
		Blocks:        make([]*Block, 0),
		IdMap:         make(map[uint64]int),
		TimeStamp:     *NewTimeStamp(),
		MaxBlockCount: table.Conf.SegmentMaxBlocks,
	}
	return seg
}

func (seg *Segment) GetTableID() uint64 {
	return seg.Table.ID
}

func (seg *Segment) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   seg.Table.ID,
		SegmentID: seg.ID,
	}
}

func (seg *Segment) GetID() uint64 {
	return seg.ID
}

// BlockIDList returns the ID list of blocks lived in args[0], which is a timestamp.
func (seg *Segment) BlockIDList(args ...interface{}) []uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0].(int64)
	}
	ids := make([]uint64, 0)
	seg.RLock()
	defer seg.RUnlock()
	for _, blk := range seg.Blocks {
		if !blk.Select(ts) {
			continue
		}
		ids = append(ids, blk.ID)
	}
	return ids
}

// BlockIDs returns the ID map of blocks lived in args[0], which is a timestamp.
func (seg *Segment) BlockIDs(args ...interface{}) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0].(int64)
	}
	ids := make(map[uint64]uint64)
	seg.RLock()
	defer seg.RUnlock()
	for _, blk := range seg.Blocks {
		if !blk.Select(ts) {
			continue
		}
		ids[blk.ID] = blk.ID
	}
	return ids
}

func (seg *Segment) HasUncommitted() bool {
	if seg.DataState >= CLOSED {
		return false
	}
	if seg.DataState < FULL {
		return true
	}
	for _, blk := range seg.Blocks {
		if blk.DataState != FULL {
			return true
		}
	}
	return false
}

func (seg *Segment) GetActiveBlk() *Block {
	if seg.ActiveBlk >= len(seg.Blocks) {
		return nil
	}
	return seg.Blocks[seg.ActiveBlk]
}

func (seg *Segment) NextActiveBlk() *Block {
	var blk *Block
	if seg.ActiveBlk >= len(seg.Blocks)-1 {
		seg.ActiveBlk++
		return blk
	}
	blk = seg.Blocks[seg.ActiveBlk]
	seg.ActiveBlk++
	return blk
}

func (seg *Segment) Marshal() ([]byte, error) {
	return json.Marshal(seg)
}

// CreateBlock generates a new block id with its Sequence and
// returns a new block meta with this id.
func (seg *Segment) CreateBlock() (blk *Block, err error) {
	blk = NewBlock(seg.Table.Info.Sequence.GetBlockID(), seg)
	return blk, err
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%d-%d) [blkPos=%d][State=%d]", seg.Table.ID, seg.ID, seg.ActiveBlk, seg.DataState)
	s += "["
	pos := 0
	for _, blk := range seg.Blocks {
		if pos != 0 {
			s += "<-->"
		}
		s += blk.String()
		pos++
	}
	s += "]"
	return s
}

func (seg *Segment) cloneBlockNoLock(id uint64, ctx CopyCtx) (blk *Block, err error) {
	idx, ok := seg.IdMap[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	blk = seg.Blocks[idx].Copy()
	if !ctx.Attached {
		err = blk.Detach()
	}
	return blk, err
}

// CloneBlock returns the clone of the block if exists, whose block id is id.
func (seg *Segment) CloneBlock(id uint64, ctx CopyCtx) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	return seg.cloneBlockNoLock(id, ctx)
}

func (seg *Segment) ReferenceBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	idx, ok := seg.IdMap[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	return seg.Blocks[idx], nil
}

// RegisterBlock registers a block via an existing block meta.
func (seg *Segment) RegisterBlock(blk *Block) error {
	if blk.Segment.Table.ID != seg.Table.ID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", seg.Table.ID, blk.Segment.Table.ID))
	}
	if blk.GetSegmentID() != seg.GetID() {
		return errors.New(fmt.Sprintf("segment id mismatch %d:%d", seg.GetID(), blk.GetSegmentID()))
	}
	seg.Lock()
	defer seg.Unlock()

	err := blk.Attach()
	if err != nil {
		return err
	}
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		return errors.New(fmt.Sprintf("Cannot add block into full segment %d", seg.ID))
	}
	_, ok := seg.IdMap[blk.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate block %d found in segment %d", blk.GetID(), seg.ID))
	}
	seg.IdMap[blk.GetID()] = len(seg.Blocks)
	seg.Blocks = append(seg.Blocks, blk)
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		if blk.IsFull() {
			seg.DataState = CLOSED
		} else {
			seg.DataState = FULL
		}
	} else {
		seg.DataState = PARTIAL
	}
	seg.Table.Lock()
	seg.Table.UpdateVersion()
	seg.Table.Unlock()
	return nil
}

func (seg *Segment) TryClose() bool {
	seg.Lock()
	defer seg.Unlock()
	if seg.DataState == CLOSED || seg.DataState == SORTED {
		return true
	}
	if seg.DataState == FULL || len(seg.Blocks) == int(seg.MaxBlockCount) {
		for _, blk := range seg.Blocks {
			if !blk.IsFull() {
				return false
			}
		}
		seg.DataState = CLOSED
		return true
	}
	return false
}

func (seg *Segment) TrySorted() {
	seg.Lock()
	defer seg.Unlock()
	if seg.DataState == SORTED {
		return
	}
	if seg.DataState != CLOSED {
		panic("logic error")
	}
	seg.DataState = SORTED
}

func (seg *Segment) GetMaxBlkID() uint64 {
	blkId := uint64(0)
	for bid := range seg.IdMap {
		if bid > blkId {
			blkId = bid
		}
	}

	return blkId
}

func (seg *Segment) ReplayState() {
	if seg.DataState >= CLOSED {
		return
	}
	if len(seg.Blocks) == 0 {
		seg.DataState = EMPTY
		return
	}
	fullBlkCnt := 0
	for _, blk := range seg.Blocks {
		if blk.DataState == FULL {
			fullBlkCnt++
		}
	}
	if fullBlkCnt == 0 {
		seg.DataState = EMPTY
	} else if fullBlkCnt < int(seg.Table.Conf.SegmentMaxBlocks) {
		seg.DataState = PARTIAL
	} else {
		seg.DataState = CLOSED
	}
}

func (seg *Segment) Copy(ctx CopyCtx) *Segment {
	if ctx.Ts == 0 {
		ctx.Ts = NowMicro()
	}
	seg.RLock()
	defer seg.RUnlock()
	newSeg := NewSegment(seg.Table, seg.ID)
	newSeg.TimeStamp = seg.TimeStamp
	newSeg.MaxBlockCount = seg.MaxBlockCount
	newSeg.DataState = seg.DataState
	newSeg.BoundSate = seg.BoundSate
	for _, v := range seg.Blocks {
		if !v.Select(ctx.Ts) {
			continue
		}
		blk, _ := seg.cloneBlockNoLock(v.ID, ctx)
		newSeg.IdMap[v.GetID()] = len(newSeg.Blocks)
		newSeg.Blocks = append(newSeg.Blocks, blk)
	}

	return newSeg
}
