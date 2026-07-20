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

// TestArrayElementCompareZeroAlloc guards that comparing two 768-dimensional
// narrow vectors performs no heap allocation. ArrayElementCompare is called once
// per comparison in the sort / merge-top / scalar-compare / vector min-max hot
// paths, so a per-call []float32 bridge (the previous implementation) added
// ~6KB and 2 allocs per comparison — turning a 100k-row ORDER BY (~N·log2 N
// comparisons) into roughly 10GB of transient garbage. This is the regression
// guard against that bridge returning.
func TestArrayElementCompareZeroAlloc(t *testing.T) {
	const dim = 768
	f32a := make([]float32, dim)
	f32b := make([]float32, dim)
	for i := range f32a {
		f32a[i] = float32(i)
		f32b[i] = float32(i)
	}
	// Differ only in the last element: the worst case that forces the comparator
	// to scan every element instead of short-circuiting early.
	f32b[dim-1] = float32(dim)

	f16a := Float32ToFloat16Slice(f32a)
	f16b := Float32ToFloat16Slice(f32b)
	bf16a := Float32ToBF16Slice(f32a)
	bf16b := Float32ToBF16Slice(f32b)
	i8a := make([]int8, dim)
	i8b := make([]int8, dim)
	i8b[dim-1] = 1
	u8a := make([]uint8, dim)
	u8b := make([]uint8, dim)
	u8b[dim-1] = 1

	cases := []struct {
		name string
		fn   func()
	}{
		{"float32", func() { ArrayElementCompare(f32a, f32b) }},
		{"float16", func() { ArrayElementCompare(f16a, f16b) }},
		{"bf16", func() { ArrayElementCompare(bf16a, bf16b) }},
		{"int8", func() { ArrayElementCompare(i8a, i8b) }},
		{"uint8", func() { ArrayElementCompare(u8a, u8b) }},
	}
	for _, c := range cases {
		allocs := testing.AllocsPerRun(100, c.fn)
		require.Zerof(t, allocs, "%s: ArrayElementCompare must be allocation-free, got %v allocs/op", c.name, allocs)
	}
}

// BenchmarkArrayElementCompare reports B/op and allocs/op for a 768-dim compare
// so the allocation cost of this hot-path comparator stays visible in CI output.
func BenchmarkArrayElementCompare(b *testing.B) {
	const dim = 768
	src := make([]float32, dim)
	for i := range src {
		src[i] = float32(i)
	}
	x := Float32ToFloat16Slice(src)
	y := Float32ToFloat16Slice(src)
	y[dim-1] = Float16FromFloat32(float32(dim))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ArrayElementCompare(x, y)
	}
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
