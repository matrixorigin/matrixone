package index

import (
	"fmt"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
)

type TableHolder struct {
	ID     uint64
	BufMgr mgrif.IBufferManager
	tree   struct {
		sync.RWMutex
		Segments   []*SegmentHolder
		IdMap      map[uint64]int
		SegmentCnt int64
	}
}

func NewTableHolder(bufMgr mgrif.IBufferManager, id uint64) *TableHolder {
	holder := &TableHolder{ID: id, BufMgr: bufMgr}
	holder.tree.Segments = make([]*SegmentHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	return holder
}

func (holder *TableHolder) AddSegment(seg *SegmentHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[seg.ID.SegmentID]
	if ok {
		panic(fmt.Sprintf("Duplicate seg %s", seg.ID.SegmentString()))
	}
	holder.tree.IdMap[seg.ID.SegmentID] = len(holder.tree.Segments)
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

func (holder *TableHolder) String() string {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	s := fmt.Sprintf("<IndexTableHolder[%d]>[Cnt=%d]", holder.ID, holder.tree.SegmentCnt)
	for _, seg := range holder.tree.Segments {
		s = fmt.Sprintf("%s\n\t%s", s, seg.stringNoLock())
	}
	return s
}

func (holder *TableHolder) UpgradeSegment(id uint64, segType base.SegmentType) *SegmentHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified seg %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Segments[idx]
	if stale.Type >= segType {
		panic(fmt.Sprintf("Cannot upgrade segment %d, type %d", id, segType))
	}
	newSeg := NewSegmentHolder(holder.BufMgr, stale.ID, segType)
	holder.tree.Segments[idx] = newSeg
	return newSeg
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
