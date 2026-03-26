// Copyright 2022 Matrix Origin
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

package mergeutil

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSortColumnsByIndex(t *testing.T) {
	mp := mpool.MustNewZero()

	const (
		vecNum = 3
		vecLen = 50
	)

	var vecs = make([]*vector.Vector, vecNum)
	for i := 0; i < vecNum; i++ {
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_int32.ToType())
		}

		for j := 0; j < vecLen; j++ {
			x := rand.Int32N(10000)
			err := vector.AppendFixed[int32](vecs[i], x, false, mp)
			require.NoError(t, err)
		}
	}

	for i := 0; i < vecNum; i++ {
		err := SortColumnsByIndex(vecs, i, mp)
		require.NoError(t, err)

		for j := 0; j < vecNum; j++ {
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[j])
			if j == i {
				require.True(t, slices.IsSorted(vals))
				require.True(t, vecs[j].GetSorted())
			} else {
				require.False(t, slices.IsSorted(vals))
				require.False(t, vecs[j].GetSorted())
			}
		}
	}
}

func TestSortColumnsByIndexWithBuf(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("basic reuse across rounds", func(t *testing.T) {
		const (
			vecNum = 3
			vecLen = 50
		)
		var sortBuf []int64
		for round := 0; round < 3; round++ {
			vecs := make([]*vector.Vector, vecNum)
			for i := 0; i < vecNum; i++ {
				vecs[i] = vector.NewVec(types.T_int32.ToType())
				for j := 0; j < vecLen; j++ {
					require.NoError(t, vector.AppendFixed[int32](vecs[i], rand.Int32N(10000), false, mp))
				}
			}
			require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &sortBuf))
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[0])
			require.True(t, slices.IsSorted(vals))
			require.True(t, vecs[0].GetSorted())
		}
		require.GreaterOrEqual(t, cap(sortBuf), 50)
	})

	t.Run("nil buf works like no reuse", func(t *testing.T) {
		vecs := []*vector.Vector{vector.NewVec(types.T_int64.ToType())}
		for i := 0; i < 20; i++ {
			require.NoError(t, vector.AppendFixed[int64](vecs[0], int64(20-i), false, mp))
		}
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, nil))
		vals := vector.MustFixedColNoTypeCheck[int64](vecs[0])
		require.True(t, slices.IsSorted(vals))
	})

	t.Run("single element", func(t *testing.T) {
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		require.NoError(t, vector.AppendFixed[int32](vecs[0], int32(42), false, mp))
		var buf []int64
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &buf))
		require.Equal(t, int32(42), vector.MustFixedColNoTypeCheck[int32](vecs[0])[0])
		require.GreaterOrEqual(t, cap(buf), 1)
	})

	t.Run("buffer grows when vector is larger", func(t *testing.T) {
		var buf []int64 = make([]int64, 4) // deliberately small
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		for i := 0; i < 100; i++ {
			require.NoError(t, vector.AppendFixed[int32](vecs[0], rand.Int32N(1000), false, mp))
		}
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &buf))
		require.GreaterOrEqual(t, cap(buf), 100) // grew to fit
		require.True(t, slices.IsSorted(vector.MustFixedColNoTypeCheck[int32](vecs[0])))
	})

	t.Run("buffer not reallocated when capacity sufficient", func(t *testing.T) {
		const rows = 50
		var buf []int64 = make([]int64, rows)
		ptr0 := &buf[0]
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		for i := 0; i < rows; i++ {
			require.NoError(t, vector.AppendFixed[int32](vecs[0], rand.Int32N(1000), false, mp))
		}
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &buf))
		require.Equal(t, ptr0, &buf[0]) // same backing array
	})
}
