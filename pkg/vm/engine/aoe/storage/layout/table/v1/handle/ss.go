package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"sync"
	"sync/atomic"
)

type CloseSegmentItCB func(dbi.ISegmentIt)

var (
	_ dbi.ISnapshot = (*Snapshot)(nil)
)

const (
	Active int32 = iota
	Closing
	Closed
)

type Snapshot struct {
	TableData iface.ITableData
	Attr      []int
	Ids       []uint64
	ScanAll   bool
	State     int32
	tree      struct {
		sync.RWMutex
		Segments map[uint64]*Segment
	}
	trace struct {
		sync.RWMutex
		Iterators map[dbi.ISegmentIt]bool
	}
	ActiveIters int32
}

func NewSnapshot(ids []uint64, attrs []int, td iface.ITableData) *Snapshot {
	ss := &Snapshot{
		Ids:       append([]uint64(nil), ids...),
		Attr:      attrs,
		TableData: td,
		State:     Active,
	}
	ss.trace.Iterators = make(map[dbi.ISegmentIt]bool)
	ss.tree.Segments = make(map[uint64]*Segment)

	return ss
}

func NewEmptySnapshot() *Snapshot {
	ss := new(Snapshot)
	ss.trace.Iterators = make(map[dbi.ISegmentIt]bool)
	ss.tree.Segments = make(map[uint64]*Segment)
	return ss
}

func NewLinkAllSnapshot(attrs []int, td iface.ITableData) *Snapshot {
	ss := &Snapshot{
		ScanAll:   true,
		Attr:      attrs,
		TableData: td,
		State:     Active,
	}
	return ss
}

func (ss *Snapshot) GetSegment(id uint64) dbi.ISegment {
	if ss.TableData != nil {
		ss.tree.RLock()
		seg := ss.tree.Segments[id]
		if seg != nil {
			ss.tree.RUnlock()
			return seg
		}
		ss.tree.RUnlock()
		ss.tree.Lock()
		seg = ss.tree.Segments[id]
		if seg != nil {
			ss.tree.Unlock()
			return seg
		}
		seg = &Segment{
			Data: ss.TableData.StrongRefSegment(id),
			Attr: ss.Attr,
		}
		ss.tree.Segments[id] = seg
		ss.tree.Unlock()
		return seg
	}
	return nil
}

func (ss *Snapshot) SegmentIds() []uint64 {
	if ss.ScanAll {
		return ss.TableData.SegmentIds()
	}
	return ss.Ids
}

func (ss *Snapshot) getState() int32 {
	return atomic.LoadInt32(&ss.State)
}

func (ss *Snapshot) Close() error {
	if atomic.LoadInt32(&ss.State) > Active {
		return nil
	}
	if atomic.CompareAndSwapInt32(&ss.State, Active, Closing) {
		ss.trace.Lock()
		// for it := range ss.trace.Iterators {
		// 	it.Close()
		// }
		// for it := range ss.trace.Iterators {
		// 	delete(ss.trace.Iterators, it)
		// }
		ss.tree.Lock()
		for _, seg := range ss.tree.Segments {
			seg.Data.Unref()
		}
		ss.tree.Unlock()
		ss.trace.Unlock()
		if ss.TableData != nil {
			ss.TableData.Unref()
			ss.TableData = nil
		}
		atomic.StoreInt32(&ss.State, Closed)
	}
	return nil
}

func (ss *Snapshot) removeIt(it dbi.ISegmentIt) {
	atomic.AddInt32(&ss.ActiveIters, int32(-1))
}

func (ss *Snapshot) NewIt() dbi.ISegmentIt {
	if ss.getState() > Active {
		panic("logic error")
	}
	var it dbi.ISegmentIt
	if ss.ScanAll {
		it = NewSegmentLinkIt(ss)
	} else {
		it = NewSegmentIt(ss)
	}

	// ss.trace.Lock()
	// ss.trace.Iterators[it] = true
	// ss.trace.Unlock()
	atomic.AddInt32(&ss.ActiveIters, int32(1))
	return it
}
