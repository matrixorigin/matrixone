// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"matrixone/pkg/vm/engine"
)

// SegmentSparseFilter provides segment-level & sparse interfaces with bitmap
// support. (e.g. Eq(string, interface{}) ([]string, error)
// where inputs are column name and value, returns a string array telling
// which blocks *might* have the same value.)
type SegmentSparseFilter struct {
	segment *Segment
}

func (s2 SegmentSparseFilter) Eq(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Ne(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Lt(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Le(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Gt(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Ge(s string, i interface{}) (engine.Reader, error) {
	panic("implement me")
}

func (s2 SegmentSparseFilter) Btw(s string, i interface{}, i2 interface{}) (engine.Reader, error) {
	panic("implement me")
}

func NewSegmentSparseFilter(s *Segment) engine.SparseFilter {
	return &SegmentSparseFilter{segment: s}
}

/*func (f *SegmentSparseFilter) Eq(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpEq,
		Val:     val,
		BMRes: roaring.NewBitmap(),
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
	blkMin, blkMax, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		//log.Info(blkMin[idx], " ", blkMax[idx], " ", val)
		if compare(blkMin[idx], val, typ) > 0 || compare(blkMax[idx], val, typ) < 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Ne(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	blkCnt := len(f.segment.Blocks())
	var res []string
	blkMin, blkMax, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMin[idx], val, typ) <= 0 && compare(blkMax[idx], val, typ) >= 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Lt(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpLt,
		Val:     val,
		BMRes: roaring.NewBitmap(),
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
	blkMin, _, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMin[idx], val, typ) >= 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Le(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpLe,
		Val:     val,
		BMRes: roaring.NewBitmap(),
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
	blkMin, _, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMin[idx], val, typ) > 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Gt(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpGt,
		Val:     val,
		BMRes: roaring.NewBitmap(),
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
	_, blkMax, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMax[idx], val, typ) <= 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Ge(attr string, val interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpGe,
		Val:     val,
		BMRes: roaring.NewBitmap(),
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
	_, blkMax, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMax[idx], val, typ) < 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func (f *SegmentSparseFilter) Btw(attr string, minv interface{}, maxv interface{}) ([]string, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	ctx := index.FilterCtx{
		Op:      index.OpIn,
		ValMin: minv,
		ValMax: maxv,
		BMRes: roaring.NewBitmap(),
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
	blkMin, blkMax, err := f.segment.Data.GetIndexHolder().CollectMinMax(colIdx)
	if err != nil {
		return nil, err
	}
	for idx := 0; idx < blkCnt; idx++ {
		strID := f.segment.Blocks()[idx]
		typ := f.segment.Data.GetMeta().Table.Schema.ColDefs[colIdx].Type
		if compare(blkMin[idx], minv, typ) > 0 || compare(blkMax[idx], maxv, typ) < 0 {
			continue
		}
		res = append(res, strID)
	}

	return res, nil
}

func compare(val1, val2 interface{}, typ types.Type) int {
	switch typ.Oid {
	case types.T_int8:
		return int(val1.(int8) - val2.(int8))
	case types.T_int16:
		return int(val1.(int16) - val2.(int16))
	case types.T_int32:
		return int(val1.(int32) - val2.(int32))
	case types.T_int64:
		return int(val1.(int64) - val2.(int64))
	case types.T_uint8:
		return int(val1.(uint8)) - int(val2.(uint8))
	case types.T_uint16:
		return int(val1.(uint16)) - int(val2.(uint16))
	case types.T_uint32:
		return int(val1.(uint32)) - int(val2.(uint32))
	case types.T_uint64:
		return int(val1.(uint64)) - int(val2.(uint64))
	case types.T_float32:
		return int(val1.(float32) - val2.(float32))
	case types.T_float64:
		return int(val1.(float64) - val2.(float64))
	case types.T_char, types.T_json, types.T_varchar:
		return bytes.Compare(val1.([]byte), val2.([]byte))
	case types.T_datetime:
		return int(val1.(types.Datetime) - val2.(types.Datetime))
	case types.T_date:
		return int(val1.(types.Date) - val2.(types.Date))
	}
	panic("unsupported")
}*/


