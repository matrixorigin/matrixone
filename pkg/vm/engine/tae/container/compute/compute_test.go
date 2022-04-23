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
	t.Logf("%v", vec2)
}

func TestShuffleByDeletes(t *testing.T) {
	origMask := roaring.New()
	origVals := make(map[uint32]interface{})
	origMask.Add(1)
	origVals[1] = 1
	origMask.Add(10)
	origVals[10] = 10
	origMask.Add(20)
	origVals[20] = 20
	origMask.Add(30)
	origVals[30] = 30

	deletes := roaring.New()
	deletes.Add(0)
	deletes.Add(8)
	deletes.Add(22)

	destMask, destVals, destDelets := ShuffleByDeletes(origMask, origVals, deletes)
	t.Log(destMask.String())
	t.Log(destVals)
	t.Log(destDelets.String())
	assert.True(t, destMask.Contains(0))
	assert.True(t, destMask.Contains(8))
	assert.True(t, destMask.Contains(18))
	assert.True(t, destMask.Contains(27))
}
