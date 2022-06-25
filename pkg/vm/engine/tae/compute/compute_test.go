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

package compute

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func TestUpdateVector(t *testing.T) {
	colTypes := types.MockColTypes(17)
	check := func(typ types.Type) {
		vec := MockVec(typ, 10, 0)
		mask := roaring.BitmapOf(4)
		vals := map[uint32]any{4: types.Null{}}
		vec = ApplyUpdateToVector(vec, mask, vals)
		assert.Equal(t, 10, LengthOfMoVector(vec))
		assert.True(t, vec.Nsp.Np.Contains(uint64(4)))
		t.Log(vec.String())

		v := GetValue(vec, 8)
		vals[4] = v
		vec = ApplyUpdateToVector(vec, mask, vals)
		assert.Equal(t, 10, LengthOfMoVector(vec))
		assert.True(t, vec.Nsp.Np.IsEmpty())
		t.Log(vec.String())
	}
	for _, typ := range colTypes {
		check(typ)
	}
}

func TestApplyUpdateToIVector(t *testing.T) {
	typ := types.Type_INT32.ToType()
	vec := vector.MockVector(typ, 10)

	mask := roaring.NewBitmap()
	vals := make(map[uint32]any)
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
		Oid:   types.Type_VARCHAR,
		Size:  24,
		Width: 100,
	}
	vec := vector.MockVector(typ, 10)

	mask := roaring.NewBitmap()
	vals := make(map[uint32]any)
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
	origVals := make(map[uint32]any)
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

func TestCheckRowExists(t *testing.T) {
	typ := types.Type{
		Oid:   types.Type_INT32,
		Size:  4,
		Width: 32,
	}
	vec := MockVec(typ, 100, 0)
	_, exist := GetOffsetByVal(vec, int32(55), nil)
	require.True(t, exist)
	_, exist = GetOffsetByVal(vec, int32(0), nil)
	require.True(t, exist)
	_, exist = GetOffsetByVal(vec, int32(99), nil)
	require.True(t, exist)

	_, exist = GetOffsetByVal(vec, int32(-1), nil)
	require.False(t, exist)
	_, exist = GetOffsetByVal(vec, int32(100), nil)
	require.False(t, exist)
	_, exist = GetOffsetByVal(vec, int32(114514), nil)
	require.False(t, exist)

	dels := roaring.NewBitmap()
	dels.Add(uint32(55))
	_, exist = GetOffsetByVal(vec, int32(55), dels)
	require.False(t, exist)
}

func TestAppendNull(t *testing.T) {
	colTypes := types.MockColTypes(17)
	check := func(typ types.Type) {
		vec := MockVec(typ, 10, 0)
		AppendValue(vec, types.Null{})
		assert.Equal(t, 11, LengthOfMoVector(vec))
		assert.True(t, vec.Nsp.Np.Contains(uint64(10)))
		t.Log(vec.String())
	}
	for _, typ := range colTypes {
		check(typ)
	}
}
