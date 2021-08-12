package db

import (
	"errors"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
)

type SegmentSparseFilter struct {
	segment *Segment
}

func NewSegmentSparseFilter(s *Segment) *SegmentSparseFilter {
	return &SegmentSparseFilter{segment: s}
}

func (f *SegmentSparseFilter) Eq(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	ctx := index.FilterCtx{
		Op:      index.OpEq,
		Val:     val,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return []string{}, nil
	}
	blkCnt := len(f.segment.Blocks())
	var res []string
	for idx := 0; idx < blkCnt; idx++ {
		ctx.BoolRes = false
		strID := f.segment.Blocks()[idx]
		blkID := f.segment.Data.BlockIds()[idx]
		blk := f.segment.Data.StrongRefBlock(blkID)
		err = blk.GetIndexHolder().EvalFilter(colIdx, &ctx)
		if err != nil {
			return nil, err
		}
		if ctx.BoolRes {
			res = append(res, strID)
		}
	}

	return res, nil
}

func (f *SegmentSparseFilter) Ne(attr string, val interface{}) ([]string, error) {
	panic("implement me")
}

func (f *SegmentSparseFilter) Lt(attr string, val interface{}) ([]string, error) {
	panic("implement me")
}

func (f *SegmentSparseFilter) Le(attr string, val interface{}) ([]string, error) {
	panic("implement me")
}

func (f *SegmentSparseFilter) Gt(attr string, val interface{}) ([]string, error) {
	panic("implement me")
}

func (f *SegmentSparseFilter) Ge(attr string, val interface{}) ([]string, error) {
	panic("implement me")
}

func (f *SegmentSparseFilter) Btw(attr string, minv interface{}, maxv interface{}) ([]string, error) {
	panic("implement me")
}

