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

// Decimal256 element-wise negate / absolute value on slices of uint64 with
// the matrixone Decimal256 layout (4 uint64 per element, low to high). The
// src and dst slices both have length 4*N. dst may alias src.
//
// Both ops use 256-bit two's complement (~x + 1). Negate is unconditional;
// Abs is conditional on the sign bit of the topmost word. MinInt256 wraps to
// itself, matching the scalar SQL semantics in arith_decimal_fast.go.

var (
	D256Negate func(src, dst []uint64) = scalarD256Negate
	D256Abs    func(src, dst []uint64) = scalarD256Abs
)

func scalarD256Negate(src, dst []uint64) {
	n := len(dst) / 4
	if len(src) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0 := ^src[j]
		w1 := ^src[j+1]
		w2 := ^src[j+2]
		w3 := ^src[j+3]
		var c uint64
		w0, c = bits.Add64(w0, 1, 0)
		w1, c = bits.Add64(w1, 0, c)
		w2, c = bits.Add64(w2, 0, c)
		w3, _ = bits.Add64(w3, 0, c)
		dst[j] = w0
		dst[j+1] = w1
		dst[j+2] = w2
		dst[j+3] = w3
	}
}

func scalarD256Abs(src, dst []uint64) {
	n := len(dst) / 4
	if len(src) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, w1, w2, w3 := src[j], src[j+1], src[j+2], src[j+3]
		sign := uint64(int64(w3) >> 63) // 0 or all-ones
		w0 ^= sign
		w1 ^= sign
		w2 ^= sign
		w3 ^= sign
		var c uint64
		w0, c = bits.Add64(w0, sign&1, 0)
		w1, c = bits.Add64(w1, 0, c)
		w2, c = bits.Add64(w2, 0, c)
		w3, _ = bits.Add64(w3, 0, c)
		dst[j] = w0
		dst[j+1] = w1
		dst[j+2] = w2
		dst[j+3] = w3
	}
}
