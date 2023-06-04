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
