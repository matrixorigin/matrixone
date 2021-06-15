package index

import (
	// "matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSegment(t *testing.T) {
	segHolder := NewSegmentHolder(uint64(0))
	assert.Equal(t, int32(0), segHolder.GetBlockCount())

	blk0Id := uint64(0)
	blk0Holder := NewBlockHolder(blk0Id)
	blk1Id := uint64(1)
	blk1Holder := NewBlockHolder(blk1Id)

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

	seg0Id := uint64(0)
	seg0Holder := NewSegmentHolder(seg0Id)
	seg1Id := uint64(1)
	seg1Holder := NewSegmentHolder(seg1Id)

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
