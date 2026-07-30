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

// CompareValue's type switch listed []float32/[]float64 only, so a narrow
// vector slice fell through to the "unsupported" tail — reached from the data
// branch compare path, where the same value as vecf32 compared fine.
func TestCompareValueNarrowVectorSlices(t *testing.T) {
	bf := Float32ToBF16Slice
	f16 := Float32ToFloat16Slice

	require.Equal(t, 0, CompareValue(bf([]float32{1, 2}), bf([]float32{1, 2})))
	require.True(t, CompareValue(bf([]float32{1, 2}), bf([]float32{1, 3})) < 0)
	require.True(t, CompareValue(bf([]float32{1, 3}), bf([]float32{1, 2})) > 0)

	require.Equal(t, 0, CompareValue(f16([]float32{1, 2}), f16([]float32{1, 2})))
	require.True(t, CompareValue(f16([]float32{1, 2}), f16([]float32{1, 3})) < 0)

	// int8 must order by sign, not by raw byte value: -1 is 0xff as a byte.
	require.True(t, CompareValue([]int8{-1}, []int8{1}) < 0)
	require.Equal(t, 0, CompareValue([]int8{-1, 2}, []int8{-1, 2}))

	// []uint8 IS []byte in Go, so it is handled by the []byte arm. Assert the
	// result is still correct unsigned ordering — 200 must sort after 100.
	require.True(t, CompareValue([]uint8{100}, []uint8{200}) < 0)
	require.Equal(t, 0, CompareValue([]uint8{1, 255}, []uint8{1, 255}))
}
