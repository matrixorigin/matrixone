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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestFindFirstIndex(t *testing.T) {
	testCases := []struct {
		items  []int32
		target int32
		result int
	}{
		{[]int32{1, 2, 2, 3, 3}, 3, 3},
		{[]int32{1, 2, 3, 3, 5}, 3, 2},
		{[]int32{1, 2, 2, 2, 2}, 3, -1},
	}

	t.Run("test orderedFindFirstIndexInSortedSlice", func(t *testing.T) {
		for i, testCase := range testCases {
			result := OrderedFindFirstIndexInSortedSlice(testCase.target, testCase.items)
			require.Equal(t, testCase.result, result, "test OrderedFindFirstIndexInSortedSlice at cases[%d], get result is different with expected", i)
		}
	})
}

// test FindFirstIndexInSortedVarlenVector
func TestFindFirstIndexInSortedVarlenVector(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType())
	err := AppendStringList(v1, []string{"a", "b", "b", "c", "c"}, nil, mp)
	require.NoError(t, err)
	defer v1.Free(mp)
	v2 := NewVec(types.T_char.ToType())
	err = AppendStringList(v2, []string{"a", "b", "c", "c", "e"}, nil, mp)
	require.NoError(t, err)
	defer v2.Free(mp)
	v3 := NewVec(types.T_char.ToType())
	err = AppendStringList(v3, []string{"a", "b", "b", "b", "b"}, nil, mp)
	require.NoError(t, err)
	defer v3.Free(mp)

	testCases := []struct {
		items  *Vector
		target string
		result int
	}{
		{v1, "c", 3},
		{v2, "c", 2},
		{v3, "c", -1},
	}

	t.Run("test FindFirstIndexInSortedVarlenVector", func(t *testing.T) {
		for i, testCase := range testCases {
			result := FindFirstIndexInSortedVarlenVector(testCase.items, []byte(testCase.target))
			require.Equal(t, testCase.result, result, "test FindFirstIndexInSortedVarlenVector at cases[%d], get result is different with expected", i)
		}
	})
}

func TestCollectOffsetsByPrefixEqFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType())
	defer v1.Free(mp)

	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1221"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)

	prefix1 := []byte("01")
	prefix2 := []byte("12")
	prefix3 := []byte("14")
	prefix4 := []byte("113")

	fn1 := CollectOffsetsByPrefixEqFactory(prefix1)
	fn2 := CollectOffsetsByPrefixEqFactory(prefix2)
	fn3 := CollectOffsetsByPrefixEqFactory(prefix3)
	fn4 := CollectOffsetsByPrefixEqFactory(prefix4)
	off1 := fn1(v1)
	off2 := fn2(v1)
	off3 := fn3(v1)
	off4 := fn4(v1)
	require.Equal(t, 0, len(off1))
	require.Equal(t, []int64{2, 3, 4}, off2)
	require.Equal(t, 0, len(off3))
	require.Equal(t, 0, len(off4))
}

func TestCollectOffsetsByPrefixBetweenFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType())
	defer v1.Free(mp)

	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1221"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)

	left1 := []byte("11")
	right1 := []byte("12")
	left2 := []byte("113")
	right2 := []byte("124")

	fn1 := CollectOffsetsByPrefixBetweenFactory(left1, right1)
	fn2 := CollectOffsetsByPrefixBetweenFactory(left2, right2)
	off1 := fn1(v1)
	off2 := fn2(v1)

	require.Equal(t, []int64{0, 1, 2, 3, 4}, off1)
	require.Equal(t, []int64{2, 3, 4}, off2)
}

func TestCollectOffsetsByPrefixInRangeFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType())
	defer v1.Free(mp)

	// Sorted data: keys with prefixes "11", "11", "12", "12", "12", "13"
	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1221"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)

	lb := []byte("11")
	ub := []byte("12")

	// hint=0: [lb, ub] — both closed
	fn0 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, fn0(v1))

	// hint=1: (lb, ub] — left open
	fn1 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 1)
	require.Equal(t, []int64{2, 3, 4}, fn1(v1))

	// hint=2: [lb, ub) — right open
	fn2 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 2)
	require.Equal(t, []int64{0, 1}, fn2(v1))

	// hint=3: (lb, ub) — both open
	fn3 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 3)
	require.Nil(t, fn3(v1))

	// Asymmetric bounds: short prefix fence as UB
	lb2 := []byte("1121")
	ub2 := []byte("12")
	fn4 := CollectOffsetsByPrefixInRangeFactory(lb2, ub2, 1) // (lb2, ub2]
	require.Equal(t, []int64{2, 3, 4}, fn4(v1))

	// Empty vector
	v2 := NewVec(types.T_char.ToType())
	defer v2.Free(mp)
	fn5 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Nil(t, fn5(v2))
}

func TestLinearCollectOffsetsByPrefixInRangeFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType())
	defer v1.Free(mp)

	// Unsorted data
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)

	lb := []byte("11")
	ub := []byte("12")

	// hint=0: [lb, ub]
	fn0 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Equal(t, []int64{0, 1, 3, 4}, fn0(v1))

	// hint=1: (lb, ub]
	fn1 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 1)
	require.Equal(t, []int64{0, 4}, fn1(v1))

	// hint=2: [lb, ub)
	fn2 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 2)
	require.Equal(t, []int64{1, 3}, fn2(v1))

	// hint=3: (lb, ub)
	fn3 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 3)
	require.Nil(t, fn3(v1))
}
