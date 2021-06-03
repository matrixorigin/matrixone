package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
)

var (
	_ base.ISegmentIterator = (*SegmentIt)(nil)
)

type SegmentIt struct {
	Handle *SegmentsHandle
	Pos    int
}

func (ssit *SegmentIt) Valid() bool {
	if ssit.Pos >= len(ssit.Handle.IDS) {
		return false
	}
	return true
}

func (ssit *SegmentIt) Next() {
	ssit.Pos++
}

func (ssit *SegmentIt) Close() error {
	return nil
}

func (ssit *SegmentIt) GetSegmentHandle() base.ISegmentHandle {
	h := &SegmentHandle{
		ID:   ssit.Handle.IDS[ssit.Pos],
		Cols: make([]col.IColumnSegment, 0),
	}
	for idx := range ssit.Handle.ColIdxes {
		colData := ssit.Handle.TableData.GetCollumn(idx)
		h.Cols = append(h.Cols, colData.GetSegment(h.ID))
	}
	return h
}
