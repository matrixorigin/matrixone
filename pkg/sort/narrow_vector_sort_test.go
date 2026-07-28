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

package sort

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// ORDER BY on a vecbf16/vecf16/vecint8/vecuint8 column. Sort dispatches per
// element type, and the narrow arms route through arrayElementCompare (the
// float32 bridge) rather than types.ArrayCompare, which is constrained to
// RealNumbers and would not compile for int8/uint8.
//
// The bf16/f16 cases are the ones worth having: a naive sort over the raw
// 2-byte storage words orders NEGATIVE values backwards, because the sign bit
// makes the unsigned word larger. Every fixture below therefore contains a
// negative value, so a regression to raw-word ordering fails here instead of
// silently returning rows in the wrong order.
func TestSortNarrowVectorColumns(t *testing.T) {
	mp := mpool.MustNewZero()

	f32 := func(v float32) types.BF16 { return types.BF16FromFloat32(v) }
	f16 := func(v float32) types.Float16 { return types.Float16FromFloat32(v) }

	for _, tc := range []struct {
		name string
		typ  types.Type
		fill func(*vector.Vector)
		// expected row order, ascending
		asc []int64
	}{
		{
			name: "bf16",
			typ:  types.T_array_bf16.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[types.BF16](v, [][]types.BF16{
					{f32(10), f32(0), f32(0)},
					{f32(-5), f32(0), f32(0)},
					{f32(3), f32(0), f32(0)},
				}, nil, mp))
			},
			asc: []int64{1, 2, 0}, // -5, 3, 10
		},
		{
			name: "f16",
			typ:  types.T_array_float16.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[types.Float16](v, [][]types.Float16{
					{f16(10), f16(0), f16(0)},
					{f16(-5), f16(0), f16(0)},
					{f16(3), f16(0), f16(0)},
				}, nil, mp))
			},
			asc: []int64{1, 2, 0},
		},
		{
			name: "int8",
			typ:  types.T_array_int8.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[int8](v, [][]int8{
					{10, 0, 0},
					{-128, 0, 0},
					{3, 0, 0},
				}, nil, mp))
			},
			asc: []int64{1, 2, 0}, // -128, 3, 10
		},
		{
			name: "uint8",
			typ:  types.T_array_uint8.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[uint8](v, [][]uint8{
					{10, 0, 0},
					{255, 0, 0},
					{3, 0, 0},
				}, nil, mp))
			},
			// 255 must sort LAST, not first: uint8 has no sign bit, so a shared
			// int8 path would read it as -1.
			asc: []int64{2, 0, 1}, // 3, 10, 255
		},
	} {
		t.Run(tc.name+"/asc", func(t *testing.T) {
			vec := vector.NewVec(tc.typ)
			defer vec.Free(mp)
			tc.fill(vec)

			os := []int64{0, 1, 2}
			Sort(false, false, false, os, vec)
			require.Equal(t, tc.asc, os)
		})

		t.Run(tc.name+"/desc", func(t *testing.T) {
			vec := vector.NewVec(tc.typ)
			defer vec.Free(mp)
			tc.fill(vec)

			want := []int64{tc.asc[2], tc.asc[1], tc.asc[0]}
			os := []int64{0, 1, 2}
			Sort(true, false, false, os, vec)
			require.Equal(t, want, os)
		})
	}
}
