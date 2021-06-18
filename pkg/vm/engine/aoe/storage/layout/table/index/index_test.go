package index

import (
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"testing"
	// "matrixone/pkg/vm/engine/aoe/storage/common"
)

func TestSegment(t *testing.T) {
	segType := base.UNSORTED_SEG
	segHolder := NewSegmentHolder(uint64(0), segType)
	assert.Equal(t, int32(0), segHolder.GetBlockCount())

	blk0Id := uint64(0)
	blk0Holder := NewBlockHolder(blk0Id, base.TRANSIENT_BLK)
	blk1Id := uint64(1)
	blk1Holder := NewBlockHolder(blk1Id, base.TRANSIENT_BLK)

	blk0 := segHolder.GetBlock(blk0Id)
	assert.Nil(t, blk0)
	segHolder.AddBlock(blk0Holder)
	blk0 = segHolder.GetBlock(blk0Id)
	assert.NotNil(t, blk0)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	segHolder.AddBlock(blk1Holder)
	assert.Equal(t, int32(2), segHolder.GetBlockCount())

	dropped := segHolder.DropSegment(blk0Id)
	assert.Equal(t, blk0Id, dropped.ID)
	assert.Equal(t, int32(1), segHolder.GetBlockCount())
	blk0 = segHolder.GetBlock(blk0Id)
	assert.Nil(t, blk0)
}

func TestTable(t *testing.T) {
	tableHolder := NewTableHolder(uint64(0))
	assert.Equal(t, int64(0), tableHolder.GetSegmentCount())

	segType := base.UNSORTED_SEG
	seg0Id := uint64(0)
	seg0Holder := NewSegmentHolder(seg0Id, segType)
	seg1Id := uint64(1)
	seg1Holder := NewSegmentHolder(seg1Id, segType)

	seg0 := tableHolder.GetSegment(seg0Id)
	assert.Nil(t, seg0)
	tableHolder.AddSegment(seg0Holder)
	seg0 = tableHolder.GetSegment(seg0Id)
	assert.NotNil(t, seg0)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	tableHolder.AddSegment(seg1Holder)
	assert.Equal(t, int64(2), tableHolder.GetSegmentCount())

	dropped := tableHolder.DropSegment(seg0Id)
	assert.Equal(t, seg0Id, dropped.ID)
	assert.Equal(t, int64(1), tableHolder.GetSegmentCount())
	seg0 = tableHolder.GetSegment(seg0Id)
	assert.Nil(t, seg0)
}

func TestIndex(t *testing.T) {
	int32zm := NewZoneMap(types.Type{Oid: types.T_int32, Size: 4}, int32(10), int32(100), int16(0))
	assert.False(t, int32zm.Eq(int32(9)))
	assert.True(t, int32zm.Eq(int32(10)))
	assert.True(t, int32zm.Eq(int32(100)))
	assert.False(t, int32zm.Eq(int32(101)))
}
