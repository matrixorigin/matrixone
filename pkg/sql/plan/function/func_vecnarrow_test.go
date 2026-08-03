// Copyright 2021 - 2024 Matrix Origin
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

package function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// l2 of [1,2,3] vs [4,6,8]: sqrt(9+16+25)=sqrt(50)=7.0710678118654755
// (the distance is computed in float64; the framework's InEpsilonF64 1e-9
// tolerance absorbs cross-platform variance).
func TestL2DistanceNarrowArray(t *testing.T) {
	proc := testutil.NewProcess(t)

	// int8: exact integer values, so distance matches float reference exactly.
	t.Run("int8", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{{4, 6, 8}}, []bool{false}),
			},
			NewFunctionTestResult(types.T_float64.ToType(), false, []float64{7.0710678118654755}, []bool{false}),
			L2DistanceArrayViaF32[int8])
		s, info := tc.Run()
		require.True(t, s, info)
	})

	// uint8: exact unsigned integer values, distance matches the float reference.
	t.Run("uint8", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{{4, 6, 8}}, []bool{false}),
			},
			NewFunctionTestResult(types.T_float64.ToType(), false, []float64{7.0710678118654755}, []bool{false}),
			L2DistanceArrayViaF32[uint8])
		s, info := tc.Run()
		require.True(t, s, info)
	})

	// bf16: small integers are exactly representable in bf16, so still exact.
	t.Run("bf16", func(t *testing.T) {
		mk := func(vs ...float32) []types.BF16 {
			out := make([]types.BF16, len(vs))
			for i, v := range vs {
				out[i] = types.BF16FromFloat32(v)
			}
			return out
		}
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{mk(1, 2, 3)}, []bool{false}),
				NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{mk(4, 6, 8)}, []bool{false}),
			},
			NewFunctionTestResult(types.T_float64.ToType(), false, []float64{7.0710678118654755}, []bool{false}),
			L2DistanceArrayViaF32[types.BF16])
		s, info := tc.Run()
		require.True(t, s, info)
	})

	// float16: same exact small integers.
	t.Run("f16", func(t *testing.T) {
		mk := func(vs ...float32) []types.Float16 {
			out := make([]types.Float16, len(vs))
			for i, v := range vs {
				out[i] = types.Float16FromFloat32(v)
			}
			return out
		}
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{mk(1, 2, 3)}, []bool{false}),
				NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{mk(4, 6, 8)}, []bool{false}),
			},
			NewFunctionTestResult(types.T_float64.ToType(), false, []float64{7.0710678118654755}, []bool{false}),
			L2DistanceArrayViaF32[types.Float16])
		s, info := tc.Run()
		require.True(t, s, info)
	})
}

// Sanity: inner_product of [1,2,3]·[4,5,6] = 4+10+18 = 32.
func TestInnerProductNarrowArray(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{{1, 2, 3}}, []bool{false}),
			NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{{4, 5, 6}}, []bool{false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false, []float64{-32}, []bool{false}),
		InnerProductArrayViaF32[int8])
	s, info := tc.Run()
	require.True(t, s, fmt.Sprintf("inner_product int8: %s", info))

	// uint8 sibling: same dot product over unsigned values.
	tc = NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{{1, 2, 3}}, []bool{false}),
			NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{{4, 5, 6}}, []bool{false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false, []float64{-32}, []bool{false}),
		InnerProductArrayViaF32[uint8])
	s, info = tc.Run()
	require.True(t, s, fmt.Sprintf("inner_product uint8: %s", info))
}
