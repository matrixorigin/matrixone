package handle

import (
	hif "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"sync"
	"sync/atomic"
)

type CloseSegmentItCB func(hif.ISegmentIt)

var (
	EmptySnapshot = &Snapshot{}
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
	dynamic   struct {
		sync.RWMutex
		Iterators map[hif.ISegmentIt]bool
	}
}

func NewSnapshot(ids []uint64, attrs []int, td iface.ITableData) *Snapshot {
	ss := &Snapshot{
		Ids:       append([]uint64(nil), ids...),
		Attr:      attrs,
		TableData: td,
		State:     Active,
	}
	ss.dynamic.Iterators = make(map[hif.ISegmentIt]bool)

	return ss
}

func NewEmptySnapshot() *Snapshot {
	ss := new(Snapshot)
	ss.dynamic.Iterators = make(map[hif.ISegmentIt]bool)
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

func (ss *Snapshot) GetState() int32 {
	return atomic.LoadInt32(&ss.State)
}

func (ss *Snapshot) Close() error {
	if atomic.LoadInt32(&ss.State) > Active {
		return nil
	}
	if atomic.CompareAndSwapInt32(&ss.State, Active, Closing) {
		ss.dynamic.Lock()
		for it := range ss.dynamic.Iterators {
			it.Close()
		}
		for it := range ss.dynamic.Iterators {
			delete(ss.dynamic.Iterators, it)
		}
		ss.dynamic.Unlock()
		if ss.TableData != nil {
			// TODO: Implement TableData ref logic
			// ss.TableData.Unref()
			ss.TableData = nil
		}
		atomic.StoreInt32(&ss.State, Closed)
	}
	return nil
}

func (ss *Snapshot) removeIt(it hif.ISegmentIt) {
	// ss.dynamic.Lock()
	// delete(ss.dynamic.Iterators, it)
	// ss.dynamic.Unlock()
}

func (ss *Snapshot) NewSegmentIt() hif.ISegmentIt {
	if ss.GetState() > Active {
		panic("logic error")
	}
	var it hif.ISegmentIt
	if ss.ScanAll {
	} else {
		it = NewSegmentIt(ss)
	}

	ss.dynamic.Lock()
	ss.dynamic.Iterators[it] = true
	ss.dynamic.Unlock()
	return it
}
