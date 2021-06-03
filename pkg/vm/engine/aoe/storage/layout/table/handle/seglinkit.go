package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
	// log "github.com/sirupsen/logrus"
)

var (
	_ base.ISegmentIterator = (*SegmentIt)(nil)
)

type SegmentLinkIt struct {
	Cols []col.IColumnSegment
}

func (it *SegmentLinkIt) Valid() bool {
	if it == nil {
		return false
	}
	if len(it.Cols) == 0 {
		return false
	}
	return true
}

func (it *SegmentLinkIt) Next() {
	newCols := make([]col.IColumnSegment, 0)
	for _, colSeg := range it.Cols {
		newCol := colSeg.GetNext()
		if newCol == nil {
			it.Cols = nil
			return
		}
		newCols = append(newCols, newCol)
	}
	it.Cols = newCols
}

func (it *SegmentLinkIt) GetSegmentHandle() base.ISegmentHandle {
	h := &SegmentHandle{
		ID:   it.Cols[0].GetID(),
		Cols: it.Cols,
	}
	return h
}
