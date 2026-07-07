// Copyright 2023 Matrix Origin
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

package compare

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestArrayCompareNarrow covers the bf16 / f16 / int8 / uint8 switch arms of
// arrayCompare.Compare, which testutil.NewVector cannot build (it only supports
// float32/float64 arrays). Two rows where the first sorts before the second let
// us assert asc/desc symmetry and the equal-element zero case.
func TestArrayCompareNarrow(t *testing.T) {
	mp := mpool.MustNewZero()

	buildBF16 := func() (a, b *vector.Vector) {
		return newArrayVec(t, mp, types.T_array_bf16,
			[][]types.BF16{{types.BF16FromFloat32(1), types.BF16FromFloat32(2)}}),
			newArrayVec(t, mp, types.T_array_bf16,
				[][]types.BF16{{types.BF16FromFloat32(1), types.BF16FromFloat32(3)}})
	}
	buildF16 := func() (a, b *vector.Vector) {
		return newArrayVec(t, mp, types.T_array_float16,
			[][]types.Float16{{types.Float16FromFloat32(1), types.Float16FromFloat32(2)}}),
			newArrayVec(t, mp, types.T_array_float16,
				[][]types.Float16{{types.Float16FromFloat32(1), types.Float16FromFloat32(3)}})
	}
	buildI8 := func() (a, b *vector.Vector) {
		return newArrayVec(t, mp, types.T_array_int8, [][]int8{{1, 2}}),
			newArrayVec(t, mp, types.T_array_int8, [][]int8{{1, 3}})
	}
	buildU8 := func() (a, b *vector.Vector) {
		return newArrayVec(t, mp, types.T_array_uint8, [][]uint8{{1, 2}}),
			newArrayVec(t, mp, types.T_array_uint8, [][]uint8{{1, 3}})
	}

	for _, tc := range []struct {
		name  string
		build func() (a, b *vector.Vector)
	}{
		{"bf16", buildBF16},
		{"f16", buildF16},
		{"int8", buildI8},
		{"uint8", buildU8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi := tc.build()
			defer lo.Free(mp)
			defer hi.Free(mp)

			// Ascending: lo < hi -> negative; hi > lo -> positive; equal -> 0.
			asc := New(*lo.GetType(), false, false)
			asc.Set(0, lo)
			asc.Set(1, hi)
			require.Negative(t, asc.Compare(0, 1, 0, 0), "asc lo<hi")
			require.Positive(t, asc.Compare(1, 0, 0, 0), "asc hi>lo")
			require.Zero(t, asc.Compare(0, 0, 0, 0), "asc equal")

			// Descending flips the sign of the strict comparisons.
			desc := New(*lo.GetType(), true, false)
			desc.Set(0, lo)
			desc.Set(1, hi)
			require.Positive(t, desc.Compare(0, 1, 0, 0), "desc lo<hi")
			require.Zero(t, desc.Compare(0, 0, 0, 0), "desc equal")
		})
	}
}

func newArrayVec[T types.ArrayElement](t *testing.T, mp *mpool.MPool, oid types.T, rows [][]T) *vector.Vector {
	v := vector.NewVec(types.New(oid, types.MaxArrayDimension, 0))
	for _, r := range rows {
		require.NoError(t, vector.AppendBytes(v, types.ArrayToBytes(r), false, mp))
	}
	return v
}
