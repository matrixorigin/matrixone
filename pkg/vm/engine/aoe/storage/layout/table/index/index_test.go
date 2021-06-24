package index

import (
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/types"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"testing"
)

var (
	workDir = "/tmp/index_test"
)

func TestSegment(t *testing.T) {
	segType := base.UNSORTED_SEG
	segID := common.ID{}
	bufMgr := bmgr.MockBufMgr(1000)
	segHolder := newSegmentHolder(bufMgr, segID, segType, nil)
	assert.Equal(t, int32(0), segHolder.GetBlockCount())

	blk0Id := segID
	blk0Holder := newBlockHolder(bufMgr, blk0Id, base.TRANSIENT_BLK, nil)
	blk1Id := blk0Id
	blk1Id.BlockID++
	blk1Holder := newBlockHolder(bufMgr, blk1Id, base.TRANSIENT_BLK, nil)

	blk0 := segHolder.GetBlock(blk0Id.BlockID)
	assert.Nil(t, blk0)
	segHolder.addBlock(blk0Holder)
	blk0 = segHolder.GetBlock(blk0Id.BlockID)
	assert.NotNil(t, blk0)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	segHolder.addBlock(blk1Holder)
	assert.Equal(t, int32(2), segHolder.GetBlockCount())

	dropped := segHolder.DropBlock(blk0Id.BlockID)
	assert.Equal(t, blk0Id, dropped.ID)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	blk0 = segHolder.GetBlock(blk0Id.BlockID)
	assert.Nil(t, blk0)
}

func TestTable(t *testing.T) {
	bufMgr := bmgr.MockBufMgr(1000)
	tableHolder := NewTableHolder(bufMgr, uint64(0))
	assert.Equal(t, int64(0), tableHolder.GetSegmentCount())

	segType := base.UNSORTED_SEG
	seg0Id := common.ID{}
	seg0Holder := newSegmentHolder(bufMgr, seg0Id, segType, nil)
	seg1Id := seg0Id
	seg1Id.SegmentID++
	seg1Holder := newSegmentHolder(bufMgr, seg1Id, segType, nil)

	seg0 := tableHolder.GetSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
	tableHolder.addSegment(seg0Holder)
	seg0 = tableHolder.GetSegment(seg0Id.SegmentID)
	assert.NotNil(t, seg0)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	tableHolder.addSegment(seg1Holder)
	assert.Equal(t, int64(2), tableHolder.GetSegmentCount())

	dropped := tableHolder.DropSegment(seg0Id.SegmentID)
	assert.Equal(t, seg0Id, dropped.ID)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	seg0 = tableHolder.GetSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
}

func TestIndex(t *testing.T) {
	int32zm := NewZoneMap(types.Type{Oid: types.T_int32, Size: 4}, int32(10), int32(100), int16(0))
	ctx := NewFilterCtx(OpEq)
	ctx.Val = int32(9)
	ctx.Eval(int32zm)
	assert.False(t, ctx.BoolRes)
	ctx.Reset()
	ctx.Val = int32(10)
	ctx.Op = OpEq
	ctx.Eval(int32zm)
	assert.True(t, ctx.BoolRes)
	ctx.Val = int32(100)
	ctx.Op = OpEq
	ctx.Eval(int32zm)
	assert.True(t, ctx.BoolRes)
	ctx.Val = int32(101)
	ctx.Op = OpEq
	ctx.Eval(int32zm)
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

	seg0Ref := tblHolder.GetSegment(id.SegmentID)
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

	blk0Ref := seg0IndexHolder.GetBlock(id.BlockID)
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
