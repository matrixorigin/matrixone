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

package vector

import "math"

// minMaxUnrollRows is the column length from which the unrolled min/max
// kernels are used. Below it the plain loop is as fast.
const minMaxUnrollRows = 32

// integerT is every fixed-size type that orders like its integer
// representation, including the temporal and enum types built on integers.
type integerT interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// IntegerGetMinAndMax returns the min and max of an integer-backed vector
// that has at least one non-NULL row.
func IntegerGetMinAndMax[T integerT](vec *Vector) (minv, maxv T) {
	if vec.class == FLAT && vec.length >= minMaxUnrollRows && !vec.HasNull() {
		return integerMinMaxNoNull(MustFixedColNoTypeCheck[T](vec))
	}
	return OrderedGetMinAndMax[T](vec)
}

// integerMinMaxNoNull scans a column of at least four values. Four
// independent accumulators remove the serial min/max dependency without
// changing integer ordering.
func integerMinMaxNoNull[T integerT](col []T) (T, T) {
	lo0, hi0 := col[0], col[0]
	lo1, hi1 := col[1], col[1]
	lo2, hi2 := col[2], col[2]
	lo3, hi3 := col[3], col[3]
	col = col[4:]
	for len(col) >= 4 {
		a, b, c, d := col[0], col[1], col[2], col[3]
		lo0, hi0 = min(lo0, a), max(hi0, a)
		lo1, hi1 = min(lo1, b), max(hi1, b)
		lo2, hi2 = min(lo2, c), max(hi2, c)
		lo3, hi3 = min(lo3, d), max(hi3, d)
		col = col[4:]
	}
	lo, hi := min(lo0, lo1, lo2, lo3), max(hi0, hi1, hi2, hi3)
	for _, value := range col {
		lo, hi = min(lo, value), max(hi, value)
	}
	return lo, hi
}

// floatMinMaxNoNull scans a column without NULLs, skipping NaN like
// FloatGetMinAndMax: comparisons with NaN are false, so NaN never replaces an
// accumulator. ok is false when every value is NaN. When +0 and -0 tie for a
// bound either may be returned; they are equal as values.
func floatMinMaxNoNull[T ~float32 | ~float64](col []T) (lo, hi T, ok bool) {
	inf, negInf := T(math.Inf(1)), T(math.Inf(-1))
	lo0, lo1, lo2, lo3 := inf, inf, inf, inf
	hi0, hi1, hi2, hi3 := negInf, negInf, negInf, negInf
	for len(col) >= 4 {
		a, b, c, d := col[0], col[1], col[2], col[3]
		if a < lo0 {
			lo0 = a
		}
		if a > hi0 {
			hi0 = a
		}
		if b < lo1 {
			lo1 = b
		}
		if b > hi1 {
			hi1 = b
		}
		if c < lo2 {
			lo2 = c
		}
		if c > hi2 {
			hi2 = c
		}
		if d < lo3 {
			lo3 = d
		}
		if d > hi3 {
			hi3 = d
		}
		col = col[4:]
	}
	lo, hi = lo0, hi0
	for _, l := range [...]T{lo1, lo2, lo3} {
		if l < lo {
			lo = l
		}
	}
	for _, h := range [...]T{hi1, hi2, hi3} {
		if h > hi {
			hi = h
		}
	}
	for _, value := range col {
		if value < lo {
			lo = value
		}
		if value > hi {
			hi = value
		}
	}
	return lo, hi, lo <= hi
}
