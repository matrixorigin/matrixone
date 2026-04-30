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

// Decimal64 × Decimal64 → Decimal128 on slices.
//
// Inputs a, b are int64-typed (length n). Output r holds Decimal128 values
// stored as interleaved (lo, hi) uint64 pairs, length 2n. The kernel matches
// the semantics of d64MulInline in arith_decimal_fast.go: per element compute
// the signed 128-bit product of a[i] * b[i] via abs-mul-conditional-negate.
//
// The product of two int64s fits in 128 bits, so no overflow is possible at
// this step — the API has no Checked variant.
//
// scaleAdj is an output-scale adjustment in [-18, 0]. When non-zero the
// kernel divides the 128-bit product by 10^|scaleAdj| with half-up rounding
// (matching d128DivPow10Once). The scalar fallback reuses the same divisor
// per element; the SIMD path computes the product with SIMD and then defers
// the divide to a per-element scalar helper (the divide-by-constant primitive
// will be SIMD-vectorized in a separate task).

var (
	D64MulNoBroadcast func(a, b, r []uint64, scaleAdj int32) = scalarD64MulNoBroadcast
)

// pow10 mirrors types.Pow10 for 0..18. Duplicated here to keep simdkernels
// free of a dependency cycle on container/types.
var pow10Table = [19]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
}

func scalarD64MulNoBroadcast(a, b, r []uint64, scaleAdj int32) {
	n := len(a)
	if len(b) < n || len(r) < 2*n {
		return
	}
	if scaleAdj == 0 {
		for i := 0; i < n; i++ {
			lo, hi := d64MulOne(a[i], b[i])
			r[2*i] = lo
			r[2*i+1] = hi
		}
		return
	}
	d := pow10Table[-scaleAdj]
	half := (d + 1) >> 1
	for i := 0; i < n; i++ {
		lo, hi := d64MulOne(a[i], b[i])
		lo, hi = d128DivConst(lo, hi, d, half)
		r[2*i] = lo
		r[2*i+1] = hi
	}
}

// d64MulOne computes the signed 128-bit product (lo, hi) of two int64-typed
// uint64s, exactly mirroring d64MulInline in arith_decimal_fast.go.
func d64MulOne(av, bv uint64) (lo, hi uint64) {
	xi, yi := int64(av), int64(bv)
	mx, my := xi>>63, yi>>63
	ax, ay := uint64((xi^mx)-mx), uint64((yi^my)-my)
	hi, lo = bits.Mul64(ax, ay)
	nm := uint64((xi ^ yi) >> 63) // 0xFF..FF iff signs differ
	lo ^= nm
	hi ^= nm
	var c uint64
	lo, c = bits.Add64(lo, 0, nm&1)
	hi, _ = bits.Add64(hi, 0, c)
	return lo, hi
}

// d128DivConst divides a signed 128-bit value (lo, hi) by a positive
// constant d (≤ 10^18), with half-up rounding. Mirrors d128ScaleDownPow10
// + d128DivPow10Once in arith_decimal_fast.go (sign extraction → unsigned
// divide → re-apply sign).
func d128DivConst(lo, hi, d, half uint64) (uint64, uint64) {
	// Branchless abs of (lo, hi) when interpreted as int128.
	// sign = top bit of hi as a 0/-1 mask.
	sign := uint64(int64(hi) >> 63)
	// Negate iff sign == -1: (lo, hi) := -(lo, hi).
	lo ^= sign
	hi ^= sign
	var c uint64
	lo, c = bits.Add64(lo, 0, sign&1)
	hi, _ = bits.Add64(hi, 0, c)

	// Unsigned 128 ÷ 64 with half-up rounding (matches d128DivPow10Once).
	var rem uint64
	hi, rem = bits.Div64(0, hi, d)
	lo, rem = bits.Div64(rem, lo, d)
	_, borrow := bits.Sub64(rem, half, 0)
	round := 1 - borrow
	lo, c = bits.Add64(lo, round, 0)
	hi += c

	// Re-apply sign: if sign == -1, negate the quotient.
	lo ^= sign
	hi ^= sign
	lo, c = bits.Add64(lo, 0, sign&1)
	hi, _ = bits.Add64(hi, 0, c)
	return lo, hi
}
