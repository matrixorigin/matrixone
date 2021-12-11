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

package table

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"runtime"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	fb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/wrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type tblock struct {
	common.BaseMvcc
	baseBlock
	node       mb.IMutableBlock
	nodeMgr    bb.INodeManager
	coarseSize map[string]uint64
}

func newTBlock(host iface.ISegment, meta *metadata.Block, factory fb.NodeFactory, mockSize *mb.MockSize) (*tblock, error) {
	blk := &tblock{
		baseBlock:  *newBaseBlock(host, meta),
		node:       factory.CreateNode(host.GetSegmentFile(), meta, mockSize).(mb.IMutableBlock),
		nodeMgr:    factory.GetManager(),
		coarseSize: make(map[string]uint64),
	}
	for i, colDef := range meta.Segment.Table.Schema.ColDefs {
		blk.coarseSize[colDef.Name] = metadata.EstimateColumnBlockSize(i, meta)
	}
	blk.GetObject = func() interface{} { return blk }
	blk.Pin = func(o interface{}) { o.(iface.IBlock).Ref() }
	blk.Unpin = func(o interface{}) { o.(iface.IBlock).Unref() }

	blk.OnZeroCB = blk.close
	blk.Ref()
	return blk, nil
}

func (blk *tblock) close() {
	if blk.meta.Segment.Table.IsDeleted() || blk.meta.Segment.Table.Database.IsDeleted() {
		snip := blk.meta.ConsumeSnippet(true)
		blk.meta.Segment.Table.Database.Catalog.IndexWal.Checkpoint(snip)
	}
	blk.baseBlock.release()
	blk.node.SetStale()
	blk.node.Close()
	blk.OnVersionStale()
}

func (blk *tblock) getHandle() bb.INodeHandle {
	h := blk.nodeMgr.Pin(blk.node)
	for h == nil {
		runtime.Gosched()
		h = blk.nodeMgr.Pin(blk.node)
	}
	return h
}

func (blk *tblock) WithPinedContext(fn func(mb.IMutableBlock) error) error {
	h := blk.getHandle()
	err := fn(blk.node)
	h.Close()
	return err
}

func (blk *tblock) MakeHandle() bb.INodeHandle {
	return blk.getHandle()
}

func (blk *tblock) ProcessData(fn func(batch.IBatch) error) error {
	h := blk.getHandle()
	data := blk.node.GetData()
	err := fn(data)
	h.Close()
	return err
}

func (blk *tblock) Size(attr string) uint64 {
	return blk.coarseSize[attr]
}

func (blk *tblock) CloneWithUpgrade(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	defer host.Unref()
	return newBlock(host, meta)
}

func (blk *tblock) String() string {
	s := fmt.Sprintf("<TBlk[%d]>(Refs=%d)", blk.meta.Id, blk.RefCount())
	return s
}

func (blk *tblock) GetVectorWrapper(attrid int) (*vector.VectorWrapper, error) {
	panic("not implemented")
}

func (blk *tblock) getVectorCopyFactory(attr string, compressed, deCompressed *bytes.Buffer) func(batch.IBatch) (*gvec.Vector, error) {
	return func(bat batch.IBatch) (*gvec.Vector, error) {
		colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
		vec, err := bat.GetVectorByAttr(colIdx)
		if err != nil {
			return nil, err
		}
		raw := vec.GetLatestView()
		return raw.CopyToVectorWithBuffer(compressed, deCompressed)
	}
}

func (blk *tblock) GetVectorCopy(attr string, compressed, deCompressed *bytes.Buffer) (*gvec.Vector, error) {
	fn := blk.getVectorCopyFactory(attr, compressed, deCompressed)
	h := blk.getHandle()
	data := blk.node.GetData()
	v, err := fn(data)
	h.Close()
	return v, err
}

func (blk *tblock) Prefetch(attr string) error {
	return nil
}

func (blk *tblock) GetFullBatch() batch.IBatch {
	panic("not supported")
}

func (blk *tblock) GetBatch(attrids []int) dbi.IBatchReader {
	h := blk.getHandle()
	data := blk.node.GetData()
	attrs := make([]int, len(attrids))
	vecs := make([]vector.IVector, len(attrids))
	var err error
	for idx, attr := range attrids {
		attrs[idx] = attr
		vecs[idx], err = data.GetVectorByAttr(attr)
		if err != nil {
			// TODO: returns error
			panic(err)
		}
	}
	wrapped, err := batch.NewBatch(attrs, vecs)
	if err != nil {
		// TODO: returns error
		panic(err)
	}
	return wrapper.NewBatch2(h, wrapped)
}

func (blk *tblock) Sum(colIdx int, filter *roaring64.Bitmap) (int64, uint64) {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	cnt := uint64(0)
	sum := int64(0)
	rows := filter.ToArray()
	for _, row := range rows {
		idx := int(row)
		if idx >= vec.Length() {
			continue
		}
		if ok, err := vec.IsNull(idx); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		cnt++
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
			sum += int64(val.(int64))
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

func (blk *tblock) Max(colIdx int, filter *roaring64.Bitmap) interface{} {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	rows := filter.ToArray()
	var max interface{}
	flag := true
	for _, row := range rows {
		idx := int(row)
		if idx >= vec.Length() {
			continue
		}
		if ok, err := vec.IsNull(idx); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
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

func (blk *tblock) Min(colIdx int, filter *roaring64.Bitmap) interface{} {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	rows := filter.ToArray()
	var min interface{}
	flag := true
	for _, row := range rows {
		idx := int(row)
		if idx >= vec.Length() {
			continue
		}
		if ok, err := vec.IsNull(idx); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
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

func (blk *tblock) Count(colIdx int, filter *roaring64.Bitmap) uint64 {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	rows := filter.ToArray()
	cnt := uint64(0)
	for _, row := range rows {
		idx := int(row)
		if idx >= vec.Length() {
			continue
		}
		if ok, err := vec.IsNull(idx); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		cnt++
	}
	return cnt
}

func (blk *tblock) NullCount(colIdx int, filter *roaring64.Bitmap) uint64 {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	rows := filter.ToArray()
	cnt := uint64(0)
	for _, row := range rows {
		idx := int(row)
		if idx >= vec.Length() {
			continue
		}
		if ok, err := vec.IsNull(idx); err != nil {
			panic(err)
		} else {
			if !ok {
				continue
			}
		}
		cnt++
	}
	return cnt
}

func (blk *tblock) Eq(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(val, v) == 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Ne(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(val, v) != 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Ge(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(v, val) >= 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Le(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(v, val) <= 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Gt(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(v, val) > 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Lt(colIdx int, offset uint64, val interface{}) *roaring.Bitmap {
	vec, err := blk.node.GetData().GetVectorByAttr(colIdx)
	if err != nil {
		panic(err)
	}
	length := vec.Length()
	res := roaring.NewBitmap()
	for i := 0; i < length; i++ {
		v, err := vec.GetValue(i)
		if err != nil {
			panic(err)
		}
		if ok, err := vec.IsNull(i); err != nil {
			panic(err)
		} else {
			if ok {
				continue
			}
		}
		if common.CompareInterface(v, val) < 0 {
			res.Add(uint32(offset + uint64(i)))
		}
	}
	return res
}

func (blk *tblock) Btw(colIdx int, offset uint64, min, max interface{}) *roaring.Bitmap {
	res := blk.Ge(colIdx, offset, min)
	res.And(blk.Le(colIdx, offset, max))
	return res
}

