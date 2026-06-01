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

// Decimal64 add/sub on slices, treating elements as signed int64.
//
// Two variants per operator:
//   *Unchecked — wraps on overflow, no detection. Use when caller can prove
//                no overflow (e.g., max(p1,p2)+1 ≤ 18 for d_T(p,s) operands).
//   *Checked   — returns the index of the first element that overflows, or
//                -1 if none did. Vector loop accumulates an overflow mask
//                and pays for it whether or not overflow occurs.
//
// The exported variables are function-pointer dispatchers: their values are
// the scalar reference implementations by default and may be replaced at
// init time on amd64 when AVX2 is available (see d64_addsub_simd_amd64.go).

var (
	D64AddUnchecked func(a, b, r []uint64)     = scalarD64AddUnchecked
	D64SubUnchecked func(a, b, r []uint64)     = scalarD64SubUnchecked
	D64AddChecked   func(a, b, r []uint64) int = scalarD64AddChecked
	D64SubChecked   func(a, b, r []uint64) int = scalarD64SubChecked

	// Scalar-broadcast variants. Use when one operand is a constant /
	// bound parameter / single-row literal — i.e. the SQL frontend's
	// (column op constant) and (constant op column) shapes.
	//
	// For Add (commutative) one entry covers both sides.
	// For Sub:
	//   D64SubScalar  → v[i] - s  (column - constant)
	//   D64ScalarSub  → s - v[i]  (constant - column)
	D64AddScalarUnchecked func(s uint64, v, r []uint64)              = scalarD64AddScalarUnchecked
	D64AddScalarChecked   func(s uint64, v, r []uint64) int          = scalarD64AddScalarChecked
	D64SubScalarUnchecked func(v []uint64, s uint64, r []uint64)     = scalarD64SubScalarUnchecked
	D64SubScalarChecked   func(v []uint64, s uint64, r []uint64) int = scalarD64SubScalarChecked
	D64ScalarSubUnchecked func(s uint64, v, r []uint64)              = scalarD64ScalarSubUnchecked
	D64ScalarSubChecked   func(s uint64, v, r []uint64) int          = scalarD64ScalarSubChecked

	// D64SumReduceToD128 sums a slice of Decimal64 values (signed) and
	// returns the 128-bit signed total as (lo, hi). Wraps mod 2^128.
	// Caller is responsible for ensuring the true sum fits in 128 bits
	// (always true for any plausible Decimal64 batch since |val| < 10^18).
	D64SumReduceToD128 func(v []uint64) (lo, hi uint64) = scalarD64SumReduceToD128
)

func scalarD64AddUnchecked(a, b, r []uint64) {
	n := len(r)
	if len(a) < n || len(b) < n {
		return
	}
	for i := 0; i < n; i++ {
		r[i] = a[i] + b[i]
	}
}

func scalarD64SubUnchecked(a, b, r []uint64) {
	n := len(r)
	if len(a) < n || len(b) < n {
		return
	}
	for i := 0; i < n; i++ {
		r[i] = a[i] - b[i]
	}
}

func scalarD64AddChecked(a, b, r []uint64) int {
	n := len(r)
	if len(a) < n || len(b) < n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai + bi
		r[i] = uint64(ri)
		if first < 0 && (ai^ri)&^(ai^bi) < 0 {
			first = i
		}
	}
	return first
}

func scalarD64SubChecked(a, b, r []uint64) int {
	n := len(r)
	if len(a) < n || len(b) < n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai - bi
		r[i] = uint64(ri)
		if first < 0 && (ai^ri)&(ai^bi) < 0 {
			first = i
		}
	}
	return first
}

// d64FirstOverflow rescans [0, end) for the first overflow index. Used by
// SIMD checked variants when their accumulated mask reports overflow but
// the scalar tail did not see one (so the offender is in the vector range).
func d64FirstOverflow(a, b []uint64, end int, sub bool) int {
	if sub {
		for i := 0; i < end; i++ {
			ai, bi := int64(a[i]), int64(b[i])
			ri := ai - bi
			if (ai^ri)&(ai^bi) < 0 {
				return i
			}
		}
		return -1
	}
	for i := 0; i < end; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai + bi
		if (ai^ri)&^(ai^bi) < 0 {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Scalar-broadcast reference implementations.
// ---------------------------------------------------------------------------

func scalarD64AddScalarUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if len(v) < n {
		return
	}
	for i := 0; i < n; i++ {
		r[i] = s + v[i]
	}
}

func scalarD64SubScalarUnchecked(v []uint64, s uint64, r []uint64) {
	n := len(r)
	if len(v) < n {
		return
	}
	for i := 0; i < n; i++ {
		r[i] = v[i] - s
	}
}

func scalarD64ScalarSubUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if len(v) < n {
		return
	}
	for i := 0; i < n; i++ {
		r[i] = s - v[i]
	}
}

func scalarD64AddScalarChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if len(v) < n {
		return -1
	}
	si := int64(s)
	first := -1
	for i := 0; i < n; i++ {
		vi := int64(v[i])
		ri := si + vi
		r[i] = uint64(ri)
		if first < 0 && (si^ri)&^(si^vi) < 0 {
			first = i
		}
	}
	return first
}

func scalarD64SubScalarChecked(v []uint64, s uint64, r []uint64) int {
	n := len(r)
	if len(v) < n {
		return -1
	}
	si := int64(s)
	first := -1
	for i := 0; i < n; i++ {
		vi := int64(v[i])
		ri := vi - si
		r[i] = uint64(ri)
		if first < 0 && (vi^ri)&(vi^si) < 0 {
			first = i
		}
	}
	return first
}

func scalarD64ScalarSubChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if len(v) < n {
		return -1
	}
	si := int64(s)
	first := -1
	for i := 0; i < n; i++ {
		vi := int64(v[i])
		ri := si - vi
		r[i] = uint64(ri)
		if first < 0 && (si^ri)&(si^vi) < 0 {
			first = i
		}
	}
	return first
}

// d64ScalarFirstOverflow rescans [0, end) for the first overflow index in
// scalar-broadcast operations. kind selects the operation:
//
//	0 = s + v[i]   (AddScalar)
//	1 = v[i] - s   (SubScalar)
//	2 = s - v[i]   (ScalarSub)
func d64ScalarFirstOverflow(s uint64, v []uint64, end int, kind int) int {
	si := int64(s)
	switch kind {
	case 0:
		for i := 0; i < end; i++ {
			vi := int64(v[i])
			ri := si + vi
			if (si^ri)&^(si^vi) < 0 {
				return i
			}
		}
	case 1:
		for i := 0; i < end; i++ {
			vi := int64(v[i])
			ri := vi - si
			if (vi^ri)&(vi^si) < 0 {
				return i
			}
		}
	case 2:
		for i := 0; i < end; i++ {
			vi := int64(v[i])
			ri := si - vi
			if (si^ri)&(si^vi) < 0 {
				return i
			}
		}
	}
	return -1
}

func scalarD64SumReduceToD128(v []uint64) (lo, hi uint64) {
	for i := 0; i < len(v); i++ {
		x := int64(v[i])
		sx := uint64(x >> 63)
		var c uint64
		lo, c = bits.Add64(lo, uint64(x), 0)
		hi, _ = bits.Add64(hi, sx, c)
	}
	return
}
