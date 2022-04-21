package compute

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/stretchr/testify/assert"
)

func TestApplyUpdateToIVector(t *testing.T) {
	typ := types.Type{
		Oid:   types.T_int32,
		Size:  4,
		Width: 32,
	}
	vec := vector.MockVector(typ, 10)

	mask := roaring.NewBitmap()
	vals := make(map[uint32]interface{})
	mask.Add(3)
	vals[3] = int32(87)
	mask.Add(9)
	vals[9] = int32(99)
	vec2 := ApplyUpdateToIVector(vec, mask, vals)

	val, err := vec2.GetValue(3)
	assert.Nil(t, err)
	assert.Equal(t, int32(87), val)
	val, err = vec2.GetValue(4)
	assert.Nil(t, err)
	assert.Equal(t, int32(4), val)
	val, err = vec2.GetValue(9)
	assert.Nil(t, err)
	assert.Equal(t, int32(99), val)
}
func TestApplyUpdateToIVector2(t *testing.T) {
	typ := types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	vec := vector.MockVector(typ, 10)

	mask := roaring.NewBitmap()
	vals := make(map[uint32]interface{})
	mask.Add(3)
	vals[3] = []byte("TestApplyUpdateToIVector3")
	mask.Add(9)
	vals[9] = []byte("TestApplyUpdateToIVector9")
	vec2 := ApplyUpdateToIVector(vec, mask, vals)

	val, err := vec2.GetValue(3)
	assert.Nil(t, err)
	assert.Equal(t, []byte("TestApplyUpdateToIVector3"), val)
	val, err = vec2.GetValue(4)
	assert.Nil(t, err)
	assert.Equal(t, []byte("str4"), val)
	val, err = vec2.GetValue(9)
	assert.Nil(t, err)
	assert.Equal(t, []byte("TestApplyUpdateToIVector9"), val)
	t.Logf("%v",vec2)
}
