package index

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type TableHolder struct {
	tree struct {
		sync.RWMutex
		Segments   []*SegmentHolder
		IdMap      map[uint64]int
		SegmentCnt int64
	}
}

func NewTableHolder() *TableHolder {
	holder := &TableHolder{}
	holder.tree.Segments = make([]*SegmentHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	return holder
}

func (holder *TableHolder) AddSegment(seg *SegmentHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[seg.ID]
	if ok {
		panic(fmt.Sprintf("Duplicate seg %d", seg.ID))
	}
	holder.tree.IdMap[seg.ID] = len(holder.tree.Segments)
	holder.tree.Segments = append(holder.tree.Segments, seg)
	atomic.AddInt64(&holder.tree.SegmentCnt, int64(1))
}

func (holder *TableHolder) DropSegment(id uint64) *SegmentHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified seg %d not found", id))
	}
	dropped := holder.tree.Segments[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Segments = append(holder.tree.Segments[:idx], holder.tree.Segments[idx+1:]...)
	atomic.AddInt64(&holder.tree.SegmentCnt, int64(-1))
	return dropped
}

func (holder *TableHolder) GetSegmentCount() int64 {
	return atomic.LoadInt64(&holder.tree.SegmentCnt)
}

func (holder *TableHolder) GetSegment(id uint64) (seg *SegmentHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	seg = holder.tree.Segments[idx]
	holder.tree.RUnlock()
	return seg
}
