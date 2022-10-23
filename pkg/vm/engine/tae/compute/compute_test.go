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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
)

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

	destDelets := ShuffleByDeletes(deletes, deletes)
	t.Log(destDelets.String())
}

func TestCheckRowExists(t *testing.T) {
	typ := types.T_int32.ToType()
	vec := containers.MockVector2(typ, 100, 0)
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
		vec := containers.MockVector2(typ, 10, 0)
		defer vec.Close()
		vec.Append(types.Null{})
		assert.Equal(t, 11, vec.Length())
		assert.True(t, vec.IsNull(10))
		t.Log(vec.String())
	}
	for _, typ := range colTypes {
		check(typ)
	}
}

func TestBinarySearch(t *testing.T) {
	slice := []int{1, 2, 6, 9, 12}
	pos := BinarySearch(slice, 3)
	assert.Equal(t, -1, pos)
	pos = BinarySearch(slice, 6)
	assert.Equal(t, 2, pos)
}
