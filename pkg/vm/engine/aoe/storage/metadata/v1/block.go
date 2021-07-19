package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

const (
	BLOCK_ROW_COUNT = 16
)

func NewBlock(id uint64, segment *Segment) *Block {
	blk := &Block{
		ID:          id,
		TimeStamp:   *NewTimeStamp(),
		MaxRowCount: segment.Table.Conf.BlockMaxRows,
		Segment:     segment,
	}
	return blk
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
	s := fmt.Sprintf("Blk(%d-%d-%d)(%d)", blk.Segment.Table.ID, blk.Segment.ID, blk.ID, blk.BoundSate)
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
