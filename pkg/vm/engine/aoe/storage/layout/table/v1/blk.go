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

package table

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ro "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/col"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/wrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type block struct {
	common.BaseMvcc
	baseBlock
	data struct {
		cols  []col.IColumnBlock
		sizes []uint64
	}
}

func newBlock(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	blk := &block{
		baseBlock: *newBaseBlock(host, meta),
	}
	blk.data.cols = make([]col.IColumnBlock, 0)
	blk.OnZeroCB = blk.close
	blk.Ref()

	if err := blk.initColumns(); err != nil {
		return nil, err
	}

	blk.GetObject = func() interface{} { return blk }
	blk.Pin = func(o interface{}) { o.(iface.IBlock).Ref() }
	blk.Unpin = func(o interface{}) { o.(iface.IBlock).Unref() }

	if blk.indexholder != nil {
		blk.indexholder.Init(blk.GetSegmentFile())
	}
	return blk, nil
}

func (blk *block) initColumns() error {
	blk.data.sizes = make([]uint64, 0)
	for idx := range blk.meta.Segment.Table.Schema.ColDefs {
		blk.Ref()
		colBlk := col.NewStdColumnBlock(blk, idx)
		blk.data.cols = append(blk.data.cols, colBlk)
		if blk.typ >= base.PERSISTENT_BLK {
			blk.data.sizes = append(blk.data.sizes, colBlk.Size())
		}
	}
	return nil
}

func (blk *block) close() {
	blk.baseBlock.release()
	for _, colBlk := range blk.data.cols {
		colBlk.Unref()
	}
	blk.OnVersionStale()
}

func (blk *block) Size(attr string) uint64 {
	idx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	if idx < 0 {
		panic(fmt.Sprintf("Specified attr %s not found", attr))
	}
	if !blk.IsMutable() {
		return blk.data.sizes[idx]
	}
	return blk.data.cols[idx].Size()
}

func (blk *block) cloneWithUpgradeColumns(upgraded *block) {
	for idx, colBlk := range blk.data.cols {
		upgraded.Ref()
		upgradedCol := colBlk.CloneWithUpgrade(upgraded)
		upgraded.data.cols[idx] = upgradedCol
		upgraded.data.sizes = append(upgraded.data.sizes, upgradedCol.Size())
	}
}

func (blk *block) CloneWithUpgrade(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	defer host.Unref()
	newBase, err := blk.upgrade(host, meta)
	if err != nil {
		return nil, err
	}

	upgraded := &block{
		baseBlock: *newBase,
	}

	upgraded.data.cols = make([]col.IColumnBlock, len(blk.data.cols))
	blk.cloneWithUpgradeColumns(upgraded)

	upgraded.OnZeroCB = upgraded.close

	upgraded.Ref()

	upgraded.GetObject = func() interface{} { return upgraded }
	upgraded.Pin = func(o interface{}) { o.(iface.IBlock).Ref() }
	upgraded.Unpin = func(o interface{}) { o.(iface.IBlock).Unref() }

	if upgraded.indexholder != nil {
		upgraded.indexholder.Init(upgraded.GetSegmentFile())
	}

	return upgraded, nil
}

func (blk *block) String() string {
	s := fmt.Sprintf("<Blk[%d]>(ColBlk=%d)(RefCount=%d)", blk.meta.Id, len(blk.data.cols), blk.RefCount())
	// for _, colBlk := range blk.data.cols {
	// 	s = fmt.Sprintf("%s\n\t%s", s, colBlk.String())
	// }
	return s
}

func (blk *block) GetVectorWrapper(attrid int) (*vector.VectorWrapper, error) {
	vec, err := blk.data.cols[attrid].LoadVectorWrapper()
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (blk *block) GetVectorCopy(attr string, compressed, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return nil, fmt.Errorf("column %s not found", attr)
	}
	vec, err := blk.data.cols[colIdx].ForceLoad(compressed, deCompressed)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (blk *block) Prefetch(attr string) error {
	colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return fmt.Errorf("column %s not found", attr)
	}
	return blk.data.cols[colIdx].Prefetch()
}

func (blk *block) GetFullBatch() batch.IBatch {
	vecs := make([]vector.IVector, len(blk.data.cols))
	attrs := make([]int, len(blk.data.cols))
	for idx, colBlk := range blk.data.cols {
		vecs[idx] = colBlk.GetVector()
		attrs[idx] = colBlk.GetColIdx()
	}
	blk.Ref()
	bat, err := wrapper.NewBatch(blk, attrs, vecs)
	if err != nil {
		// TODO: returns error
		panic(err)
	}
	return bat
}

func (blk *block) GetBatch(attrs []int) dbi.IBatchReader {
	// TODO: check attrs validity
	vecs := make([]vector.IVector, len(attrs))
	clonedAttrs := make([]int, len(attrs))
	for idx, attr := range attrs {
		clonedAttrs[idx] = attr
		vecs[idx] = blk.data.cols[attr].GetVector()
	}
	blk.Ref()
	bat, err := wrapper.NewBatch(blk, attrs, vecs)
	if err != nil {
		// TODO: returns error
		panic(err)
	}
	return bat.(dbi.IBatchReader)
}

func (blk *block) Sum(colIdx int, filter *roaring64.Bitmap) (int64, uint64) {
	vec, err := blk.GetVectorWrapper(colIdx)
	if err != nil {
		panic(err)
	}
	defer common.GPool.Free(vec.MNode)
	cnt := filter.GetCardinality()
	sum := int64(0)
	rows := filter.ToArray()
	for _, row := range rows {
		if nulls.Contains(vec.Nsp, row) {
			continue
		}
		idx := int(row)
		val, err := vec.GetValue(idx)
		if err != nil {
			panic(err)
		}
		switch blk.meta.Segment.Table.Schema.ColDefs[colIdx].Type.Oid {
		case types.T_int8:
			sum += int64(val.(int8))
		case types.T_int16:
			sum += int64(val.(int16))
		case types.T_int32:
			sum += int64(val.(int32))
		case types.T_int64:
			sum += val.(int64)
		case types.T_uint8:
			sum += int64(val.(uint8))
		case types.T_uint16:
			sum += int64(val.(uint16))
		case types.T_uint32:
			sum += int64(val.(uint32))
		case types.T_uint64:
			sum += int64(val.(uint64))
		case types.T_float32:
			sum += int64(val.(float32))
		case types.T_float64:
			sum += int64(val.(float64))
		case types.T_date:
			sum += int64(val.(types.Date))
		case types.T_datetime:
			sum += int64(val.(types.Datetime))
		}
	}
	return sum, cnt
}

func (blk *block) Max(colIdx int, filter *roaring64.Bitmap) interface{} {
	vec, err := blk.GetVectorWrapper(colIdx)
	if err != nil {
		panic(err)
	}
	defer common.GPool.Free(vec.MNode)
	rows := filter.ToArray()
	var max interface{}
	flag := true
	for _, row := range rows {
		if nulls.Contains(vec.Nsp, row) {
			continue
		}
		idx := int(row)
		if flag {
			max, err = vec.GetValue(idx)
			if err != nil {
				panic(err)
			}
			flag = false
		}
		val, err := vec.GetValue(idx)
		if err != nil {
			panic(err)
		}
		if common.CompareInterface(val, max) > 0 {
			max = val
		}
	}
	return max
}

func (blk *block) Min(colIdx int, filter *roaring64.Bitmap) interface{} {
	vec, err := blk.GetVectorWrapper(colIdx)
	if err != nil {
		panic(err)
	}
	defer common.GPool.Free(vec.MNode)
	rows := filter.ToArray()
	var min interface{}
	flag := true
	for _, row := range rows {
		if nulls.Contains(vec.Nsp, row) {
			continue
		}
		idx := int(row)
		if flag {
			min, err = vec.GetValue(idx)
			if err != nil {
				panic(err)
			}
			flag = false
		}
		val, err := vec.GetValue(idx)
		if err != nil {
			panic(err)
		}
		if common.CompareInterface(min, val) > 0 {
			min = val
		}
	}
	return min
}

func (blk *block) Count(colIdx int, filter *roaring64.Bitmap) uint64 {
	vec, err := blk.GetVectorWrapper(colIdx)
	if err != nil {
		panic(err)
	}
	defer common.GPool.Free(vec.MNode)
	rows := filter.ToArray()
	cnt := uint64(0)
	for _, row := range rows {
		if nulls.Contains(vec.Nsp, row) {
			continue
		}
	}
	return cnt
}

func (blk *block) NullCount(colIdx int, filter *roaring64.Bitmap) uint64 {
	vec, err := blk.GetVectorWrapper(colIdx)
	if err != nil {
		panic(err)
	}
	defer common.GPool.Free(vec.MNode)
	cnt := uint64(0)
	rows := filter.ToArray()
	for _, row := range rows {
		if nulls.Contains(vec.Nsp, row) {
			cnt++
		}
	}
	return cnt
}

func (blk *block) Eq(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Ne(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Ge(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Le(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Gt(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Lt(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	return nil
}

func (blk *block) Btw(colIdx int, offset uint64, min, max interface{}) *roaring.Bitmap {
	return nil
}
