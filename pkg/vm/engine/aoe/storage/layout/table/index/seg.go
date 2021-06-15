package index

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type SegmentType uint8

const (
	UnsortedSegment SegmentType = iota
	SortedSegment
)

type SegmentHolder struct {
	ID   uint64
	self struct {
		sync.RWMutex
		Indexes map[string]Index
	}
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	Type SegmentType
}

func NewSegmentHolder(id uint64, segType SegmentType) *SegmentHolder {
	holder := &SegmentHolder{ID: id, Type: segType}
	holder.tree.Blocks = make([]*BlockHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.self.Indexes = make(map[string]Index)
	return holder
}

func (holder *SegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%d]>[Ty=%d](Cnt=%d)", holder.ID, holder.Type, holder.tree.BlockCnt)
	return s
}

func (holder *SegmentHolder) GetBlock(id uint64) (blk *BlockHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	blk = holder.tree.Blocks[idx]
	holder.tree.RUnlock()
	return blk
}

func (holder *SegmentHolder) AddBlock(blk *BlockHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[blk.ID]
	if ok {
		panic(fmt.Sprintf("Duplicate blk %d for seg %d", blk.ID, holder.ID))
	}
	holder.tree.IdMap[blk.ID] = len(holder.tree.Blocks)
	holder.tree.Blocks = append(holder.tree.Blocks, blk)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(1))
}

func (holder *SegmentHolder) DropSegment(id uint64) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified blk %d not found in seg %d", id, holder.ID))
	}
	dropped := holder.tree.Blocks[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Blocks = append(holder.tree.Blocks[:idx], holder.tree.Blocks[idx+1:]...)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(-1))
	return dropped
}

func (holder *SegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

func (holder *SegmentHolder) UpgradeBlock(id uint64, blkType BlockType) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Blocks[idx]
	if stale.Type >= blkType {
		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blkType))
	}
	newBlk := NewBlockHolder(id, blkType)
	holder.tree.Blocks[idx] = newBlk
	return newBlk
}
