package model

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func TestPrepareHiddenData(t *testing.T) {
	typ := types.Type_DECIMAL128.ToType()
	id := common.ID{
		TableID:   1,
		SegmentID: 2,
		BlockID:   3,
	}
	prefix := EncodeBlockKeyPrefix(id.SegmentID, id.BlockID)
	data, err := PrepareHiddenData(typ, prefix, 0, 20)
	assert.NoError(t, err)
	assert.Equal(t, 20, data.Length())
	for i := 0; i < data.Length(); i++ {
		v := data.Get(i)
		sid, bid, off := DecodeHiddenKey(types.EncodeFixed(v.(types.Decimal128)))
		assert.Equal(t, sid, id.SegmentID)
		assert.Equal(t, bid, id.BlockID)
		assert.Equal(t, off, uint32(i))
	}
}
