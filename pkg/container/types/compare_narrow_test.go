// Copyright 2026 Matrix Origin
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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestArrayElementCompareLength covers the length-tiebreak branches of
// ArrayElementCompare (unequal-length arrays) that the value-ordering test in
// float16_test.go does not exercise.
func TestArrayElementCompareLength(t *testing.T) {
	require.Equal(t, 0, ArrayElementCompare([]int8{1, 2}, []int8{1, 2}))
	require.Equal(t, -1, ArrayElementCompare([]int8{1}, []int8{1, 2}))
	require.Equal(t, 1, ArrayElementCompare([]int8{1, 2}, []int8{1}))
	require.Equal(t, -1, ArrayElementCompare([]uint8{1, 2}, []uint8{1, 3}))
	f16a := Float32ToFloat16Slice([]float32{1, 2})
	f16b := Float32ToFloat16Slice([]float32{1, 2, 3})
	require.Equal(t, -1, ArrayElementCompare(f16a, f16b))
}

// TestCompareArrayElementFromBytes covers the bytes-level narrow-vector comparator
// (bf16/f16/int8/uint8), including the desc (descending) flip.
func TestCompareArrayElementFromBytes(t *testing.T) {
	x := ArrayToBytes[int8]([]int8{1, 2, 3})
	y := ArrayToBytes[int8]([]int8{1, 2, 4})
	require.Equal(t, -1, CompareArrayElementFromBytes[int8](x, y, false))
	require.Equal(t, 1, CompareArrayElementFromBytes[int8](x, y, true)) // desc flips the order

	fx := ArrayToBytes(Float32ToFloat16Slice([]float32{1, 2}))
	fy := ArrayToBytes(Float32ToFloat16Slice([]float32{1, 2}))
	require.Equal(t, 0, CompareArrayElementFromBytes[Float16](fx, fy, false))

	bx := ArrayToBytes(Float32ToBF16Slice([]float32{1}))
	by := ArrayToBytes(Float32ToBF16Slice([]float32{2}))
	require.Equal(t, -1, CompareArrayElementFromBytes[BF16](bx, by, false))

	ux := ArrayToBytes[uint8]([]uint8{9})
	uy := ArrayToBytes[uint8]([]uint8{1})
	require.Equal(t, 1, CompareArrayElementFromBytes[uint8](ux, uy, false))
}
