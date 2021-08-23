package db

import (
	"errors"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
)

type SegmentFilter struct {
	segment *Segment
}

func NewSegmentFilter(s *Segment) engine.Filter {
	return &SegmentFilter{segment: s}
}

func (f *SegmentFilter) Eq(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpEq,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Ne(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpNe,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Lt(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpLt,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Le(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpLe,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Gt(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpGt,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Ge(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpGe,
		Val:     val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}

func (f *SegmentFilter) Btw(attr string, minv interface{}, maxv interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New("attr not found")
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:      index.OpIn,
		ValMin:     minv,
		ValMax: maxv,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := bmRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}


