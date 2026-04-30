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

package simdkernels

import "math/bits"

// Decimal128 element-wise negate / absolute value on slices of uint64 with
// the matrixone Decimal128 layout (lo, hi pair per element). The src and
// dst slices both have length 2*N. dst may alias src.
//
// Both ops use 128-bit two's complement (~x + 1). Negate is unconditional;
// Abs is conditional on the sign bit of the high word. MinInt128 wraps to
// itself, matching the scalar SQL semantics in arith_decimal_fast.go.
//
// The exported variables are dispatchers; their default values are the
// scalar reference implementations and may be replaced at init time on
// amd64 when AVX2 / AVX-512 are detected (see d128_negabs_simd_amd64.go).

var (
	D128Negate func(src, dst []uint64) = scalarD128Negate
	D128Abs    func(src, dst []uint64) = scalarD128Abs
)

func scalarD128Negate(src, dst []uint64) {
	n := len(dst) / 2
	if len(src) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo := ^src[j]
		hi := ^src[j+1]
		var c uint64
		lo, c = bits.Add64(lo, 1, 0)
		hi, _ = bits.Add64(hi, 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}

func scalarD128Abs(src, dst []uint64) {
	n := len(dst) / 2
	if len(src) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, hi := src[j], src[j+1]
		sign := uint64(int64(hi) >> 63) // 0 or all-ones
		lo ^= sign
		hi ^= sign
		var c uint64
		lo, c = bits.Add64(lo, sign&1, 0)
		hi, _ = bits.Add64(hi, 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}
