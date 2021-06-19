package index

import (
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/types"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"testing"
)

func TestSegment(t *testing.T) {
	segType := base.UNSORTED_SEG
	segID := common.ID{}
	bufMgr := bmgr.MockBufMgr(1000)
	segHolder := NewSegmentHolder(bufMgr, segID, segType)
	assert.Equal(t, int32(0), segHolder.GetBlockCount())

	blk0Id := segID
	blk0Holder := newBlockHolder(bufMgr, blk0Id, base.TRANSIENT_BLK)
	blk1Id := blk0Id
	blk1Id.BlockID++
	blk1Holder := newBlockHolder(bufMgr, blk1Id, base.TRANSIENT_BLK)

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
	seg0Holder := NewSegmentHolder(bufMgr, seg0Id, segType)
	seg1Id := seg0Id
	seg1Id.SegmentID++
	seg1Holder := NewSegmentHolder(bufMgr, seg1Id, segType)

	seg0 := tableHolder.GetSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
	tableHolder.AddSegment(seg0Holder)
	seg0 = tableHolder.GetSegment(seg0Id.SegmentID)
	assert.NotNil(t, seg0)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	tableHolder.AddSegment(seg1Holder)
	assert.Equal(t, int64(2), tableHolder.GetSegmentCount())

	dropped := tableHolder.DropSegment(seg0Id.SegmentID)
	assert.Equal(t, seg0Id, dropped.ID)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	seg0 = tableHolder.GetSegment(seg0Id.SegmentID)
	assert.Nil(t, seg0)
}

func TestIndex(t *testing.T) {
	int32zm := NewZoneMap(types.Type{Oid: types.T_int32, Size: 4}, int32(10), int32(100), int16(0))
	assert.False(t, int32zm.Eq(int32(9)))
	assert.True(t, int32zm.Eq(int32(10)))
	assert.True(t, int32zm.Eq(int32(100)))
	assert.False(t, int32zm.Eq(int32(101)))
}
