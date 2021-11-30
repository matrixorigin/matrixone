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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
)

// SegmentFilter provides segment-level & dense interfaces with bitmap
// support. (e.g. Eq(string, interface{}) (*roaring.Bitmap, error)
// where inputs are column name and value, returns a bitmap telling
// which rows have the same value.)
type SegmentFilter struct {
	segment *Segment
}

func NewSegmentFilter(s *Segment) engine.Filter {
	return &SegmentFilter{segment: s}
}

func (f *SegmentFilter) Eq(attr string, val interface{}) (*roaring64.Bitmap, error) {
	colIdx := f.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpEq,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpNe,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpLt,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpLe,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpGt,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:    index.OpGe,
		Val:   val,
		BMRes: bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
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
		return nil, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	bmRes := roaring.NewBitmap()
	bmRes.AddRange(0, uint64(f.segment.Rows()))
	ctx := index.FilterCtx{
		Op:     index.OpIn,
		ValMin: minv,
		ValMax: maxv,
		BMRes:  bmRes,
	}
	err := f.segment.Data.GetIndexHolder().EvalFilter(colIdx, &ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.BoolRes {
		return roaring64.NewBitmap(), nil
	}
	buf, err := ctx.BMRes.ToBase64()
	if err != nil {
		return nil, err
	}
	ret := roaring64.NewBitmap()
	_, err = ret.FromBase64(buf)
	return ret, err
}
