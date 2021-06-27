package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle/base"
	// log "github.com/sirupsen/logrus"
)

var (
	_ base.ISegmentIterator = (*SegmentIt)(nil)
)

var EmptySegmentIt = &SegmentIt{}

type SegmentIt struct {
	Handle *SegmentsHandle
	Pos    int
}

func (ssit *SegmentIt) Valid() bool {
	if ssit.Handle == nil {
		return false
	}
	if ssit.Pos >= len(ssit.Handle.IDS) {
		return false
	}
	return true
}

func (ssit *SegmentIt) Next() {
	ssit.Pos++
}

func (ssit *SegmentIt) Close() error {
	// if ssit.Handle != nil {
	// 	ssit.Handle.Close
	// }
	return nil
}

func (ssit *SegmentIt) GetSegmentHandle() base.ISegmentHandle {
	h := segHandlePool.Get().(*SegmentHandle)
	h.ID = ssit.Handle.IDS[ssit.Pos]
	for idx := range ssit.Handle.ColIdxes {
		colData := ssit.Handle.TableData.GetCollumn(idx)
		h.Cols = append(h.Cols, colData.GetSegment(h.ID))
	}
	h.IndexHolder = h.Cols[0].GetIndexHolder()
	return h
}
