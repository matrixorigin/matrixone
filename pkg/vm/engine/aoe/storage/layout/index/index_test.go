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

package index

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Unused
// var (
// 	workDir = "/tmp/index_test"
// )

func TestSegment(t *testing.T) {
	//segType := base.UNSORTED_SEG
	segID := common.ID{}
	bufMgr := bmgr.MockBufMgr(1000)
	//segHolder := newSegmentHolder(bufMgr, segID, segType, nil)
	segHolder := newUnsortedSegmentHolder(bufMgr, segID, nil)
	assert.Equal(t, int32(0), segHolder.GetBlockCount())

	blk0Id := segID
	blk0Holder := newBlockIndexHolder(bufMgr, blk0Id, base.TRANSIENT_BLK, nil)
	blk1Id := blk0Id
	blk1Id.BlockID++
	blk1Holder := newBlockIndexHolder(bufMgr, blk1Id, base.TRANSIENT_BLK, nil)

	blk0 := segHolder.StrongRefBlock(blk0Id.BlockID)
	assert.Nil(t, blk0)
	segHolder.(*unsortedSegmentHolder).addBlock(blk0Holder)
	blk0 = segHolder.StrongRefBlock(blk0Id.BlockID)
	assert.NotNil(t, blk0)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	segHolder.(*unsortedSegmentHolder).addBlock(blk1Holder)
	assert.Equal(t, int32(2), segHolder.GetBlockCount())

	dropped := segHolder.DropBlock(blk0Id.BlockID)
	assert.Equal(t, blk0Id, dropped.ID)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	blk0 = segHolder.StrongRefBlock(blk0Id.BlockID)
	assert.Nil(t, blk0)
}

func TestTable(t *testing.T) {
	bufMgr := bmgr.MockBufMgr(1000)
	tableHolder := NewTableHolder(bufMgr, uint64(0))
	assert.Equal(t, int64(0), tableHolder.GetSegmentCount())

	//segType := base.UNSORTED_SEG
	seg0Id := common.ID{}
	//seg0Holder := newSegmentHolder(bufMgr, seg0Id, segType, nil)
	seg0Holder := newUnsortedSegmentHolder(bufMgr, seg0Id, nil)
	seg1Id := seg0Id
	seg1Id.SegmentID++
	//seg1Holder := newSegmentHolder(bufMgr, seg1Id, segType, nil)
	seg1Holder := newUnsortedSegmentHolder(bufMgr, seg1Id, nil)

	seg0 := tableHolder.StrongRefSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
	tableHolder.addSegment(seg0Holder)
	seg0 = tableHolder.StrongRefSegment(seg0Id.SegmentID)
	assert.NotNil(t, seg0)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	tableHolder.addSegment(seg1Holder)
	assert.Equal(t, int64(2), tableHolder.GetSegmentCount())

	dropped := tableHolder.DropSegment(seg0Id.SegmentID)
	assert.Equal(t, seg0Id, dropped.GetID())
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	seg0 = tableHolder.StrongRefSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
}

func TestStrBsi(t *testing.T) {
	tp := types.Type{
		Oid:   types.T_char,
		Size:  8,
		Width: 8,
	}
	col := int16(20)
	bsiIdx := NewStringBsiIndex(tp, col)
	xs := [][]byte{
		[]byte("asdf"), []byte("a"), []byte("asd"), []byte("as"), []byte("bat"), []byte("basket"), []byte("asd"), []byte("a"),
	}
	for i, x := range xs {
		err := bsiIdx.Set(uint64(i), x)
		assert.Nil(t, err)
	}

	for i, x := range xs {
		v, ok := bsiIdx.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err := bsiIdx.Eq([]byte("asd"), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())

	buf, err := bsiIdx.Marshal()
	assert.Nil(t, err)

	bsiIdx2 := NewStringBsiIndex(tp, 0)
	err = bsiIdx2.Unmarshal(buf)
	assert.Nil(t, err)
	assert.Equal(t, col, bsiIdx2.Col)

	for i, x := range xs {
		v, ok := bsiIdx2.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err = bsiIdx2.Eq([]byte("asd"), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())

	fname := "/tmp/xxstringbsi"
	os.Remove(fname)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()

	capacity, err := bsiIdx.WriteTo(f)
	assert.Nil(t, err)

	f, err = os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
	assert.Nil(t, err)
	defer f.Close()

	stat, _ := f.Stat()
	vf := common.NewMemFile(stat.Size())
	node := NewStringBsiEmptyNode(vf, false, nil)
	_, err = node.ReadFrom(f)
	assert.Nil(t, err)
	t.Log(capacity)

	bsiIdx3 := node.(*StringBsiIndex)
	assert.Equal(t, col, bsiIdx3.Col)

	for i, x := range xs {
		v, ok := bsiIdx3.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err = bsiIdx3.Eq([]byte("asd"), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())
}

func TestNumBsi(t *testing.T) {
	tp := types.Type{
		Oid:  types.T_int32,
		Size: 4,
	}
	bitSize := 30
	col := int16(10)
	bsiIdx := NewNumericBsiIndex(tp, bitSize, col)

	xs := []int32{
		10, 3, -7, 9, 0, 1, 9, -8, 2, -1, 12, -35435, 6545654, 2332, 2,
	}

	for i, x := range xs {
		err := bsiIdx.Set(uint64(i), x)
		assert.Nil(t, err)
	}

	for i, x := range xs {
		v, ok := bsiIdx.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err := bsiIdx.Eq(int32(9), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())

	buf, err := bsiIdx.Marshal()
	assert.Nil(t, err)

	bsiIdx2 := NewNumericBsiIndex(tp, 0, 0)
	err = bsiIdx2.Unmarshal(buf)
	assert.Nil(t, err)
	assert.Equal(t, col, bsiIdx2.Col)

	for i, x := range xs {
		v, ok := bsiIdx2.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err = bsiIdx2.Eq(int32(9), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())

	fname := "/tmp/xxnumericbsi"
	os.Remove(fname)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()

	capacity, err := bsiIdx.WriteTo(f)
	assert.Nil(t, err)

	f, err = os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
	assert.Nil(t, err)
	defer f.Close()

	stat, _ := f.Stat()
	vf := common.NewMemFile(stat.Size())
	node := NewNumericBsiEmptyNode(vf, false, nil)
	_, err = node.ReadFrom(f)
	assert.Nil(t, err)
	t.Log(capacity)

	bsiIdx3 := node.(*NumericBsiIndex)
	assert.Equal(t, col, bsiIdx3.Col)

	for i, x := range xs {
		v, ok := bsiIdx3.Get(uint64(i))
		assert.True(t, ok)
		assert.Equal(t, x, v)
	}
	res, err = bsiIdx3.Eq(int32(9), nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), res.GetCardinality())
}

func TestZM(t *testing.T) {
	int32zm := NewBlockZoneMap(types.Type{Oid: types.T_int32, Size: 4}, int32(10), int32(100), int16(0))
	ctx := NewFilterCtx(OpEq)
	ctx.Val = int32(9)
	err := ctx.Eval(int32zm)
	assert.Nil(t, err)
	assert.False(t, ctx.BoolRes)
	ctx.Reset()
	ctx.Val = int32(10)
	ctx.Op = OpEq
	err = ctx.Eval(int32zm)
	assert.Nil(t, err)
	assert.True(t, ctx.BoolRes)
	ctx.Val = int32(100)
	ctx.Op = OpEq
	err = ctx.Eval(int32zm)
	assert.Nil(t, err)
	assert.True(t, ctx.BoolRes)
	ctx.Val = int32(101)
	ctx.Op = OpEq
	err = ctx.Eval(int32zm)
	assert.Nil(t, err)
	assert.False(t, ctx.BoolRes)
}

func TestRefs1(t *testing.T) {
	capacity := uint64(1000)
	bufMgr := bmgr.MockBufMgr(capacity)
	id := common.ID{}
	tblHolder := NewTableHolder(bufMgr, id.TableID)
	released := false
	cb := func(interface{}) {
		released = true
	}
	seg0IndexHolder := tblHolder.RegisterSegment(id, base.UNSORTED_SEG, cb)
	assert.Equal(t, int64(2), seg0IndexHolder.RefCount())

	seg0Ref := tblHolder.StrongRefSegment(id.SegmentID)
	assert.Equal(t, int64(3), seg0Ref.RefCount())
	assert.Equal(t, int64(3), seg0IndexHolder.RefCount())
	assert.False(t, released)

	droppedSeg := tblHolder.DropSegment(id.SegmentID)
	assert.Equal(t, int64(3), seg0Ref.RefCount())
	assert.Equal(t, int64(3), droppedSeg.RefCount())
	assert.Equal(t, int64(3), seg0IndexHolder.RefCount())

	droppedSeg.Unref()
	assert.Equal(t, int64(2), seg0Ref.RefCount())
	assert.Equal(t, int64(2), droppedSeg.RefCount())
	assert.Equal(t, int64(2), seg0IndexHolder.RefCount())
	assert.False(t, released)

	seg0IndexHolder.Unref()
	assert.Equal(t, int64(1), seg0Ref.RefCount())
	assert.Equal(t, int64(1), droppedSeg.RefCount())
	assert.Equal(t, int64(1), seg0IndexHolder.RefCount())
	assert.False(t, released)

	seg0Ref.Unref()
	assert.Equal(t, int64(0), droppedSeg.RefCount())
	assert.Equal(t, int64(0), seg0IndexHolder.RefCount())
	assert.True(t, released)
}

func TestRefs2(t *testing.T) {
	capacity := uint64(1000)
	bufMgr := bmgr.MockBufMgr(capacity)
	id := common.ID{}
	tblHolder := NewTableHolder(bufMgr, id.TableID)
	released := false
	cb := func(interface{}) {
		released = true
	}
	seg0IndexHolder := tblHolder.RegisterSegment(id, base.UNSORTED_SEG, nil)
	blk0 := seg0IndexHolder.RegisterBlock(id, base.TRANSIENT_BLK, cb)
	assert.Equal(t, int64(2), blk0.RefCount())
	assert.False(t, released)

	blk0Ref := seg0IndexHolder.StrongRefBlock(id.BlockID)
	assert.Equal(t, int64(3), blk0Ref.RefCount())
	assert.False(t, released)

	seg0IndexHolder.Unref()

	blk0.Unref()
	assert.Equal(t, int64(2), blk0.RefCount())
	assert.False(t, released)

	droppedSeg := tblHolder.DropSegment(id.SegmentID)
	droppedSeg.Unref()
	assert.Equal(t, int64(1), blk0.RefCount())
	assert.False(t, released)

	blk0Ref.Unref()
	assert.Equal(t, int64(0), blk0.RefCount())
	assert.True(t, released)
}
