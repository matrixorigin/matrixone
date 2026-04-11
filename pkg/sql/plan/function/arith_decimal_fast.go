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

package function

// Specialized batch kernels for all Decimal64/128/256 arithmetic operations.
//
// Each kernel processes entire vectors (slices) in a tight loop, eliminating
// per-element GetValue/type-assertion/closure-call overhead.
//
// Layout:
//   - Multiply:    d64Mul, d128Mul, d256Mul
//   - Add/Sub:     d64Add/d64Sub, d128Add/d128Sub, d256Add/d256Sub
//   - Division:    d64Div, d128Div, d256Div
//   - Modulo:      d64Mod, d128Mod, d256Mod
//   - Integer Div: d64IntDiv, d128IntDiv, d256IntDiv (DIV operator)

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func operandsAt[T any](v1, v2 []T, idx int) (T, T) {
	// Branchless: mask is 0 when len==1 (scalar), -1 when len>1 (vector).
	// idx & 0 = 0 (always v[0]), idx & -1 = idx (v[idx]).
	return v1[idx&-min(1, len(v1)-1)], v2[idx&-min(1, len(v2)-1)]
}

// ---- Branchless sign helpers ----

// d128Abs makes a signed D128 absolute in-place, returns the sign bit (0 or 1).
func d128Abs(x *types.Decimal128) uint64 {
	sign := x.B64_127 >> 63
	negMask := -sign
	x.B0_63 ^= negMask
	x.B64_127 ^= negMask
	var c uint64
	x.B0_63, c = bits.Add64(x.B0_63, sign, 0)
	x.B64_127, _ = bits.Add64(x.B64_127, 0, c)
	return sign
}

// d128Negate conditionally negates a D128 in-place (two's complement).
// sign=0: no-op. sign=1: negate.
func d128Negate(x *types.Decimal128, sign uint64) {
	negMask := -sign
	x.B0_63 ^= negMask
	x.B64_127 ^= negMask
	var c uint64
	x.B0_63, c = bits.Add64(x.B0_63, sign, 0)
	x.B64_127, _ = bits.Add64(x.B64_127, 0, c)
}

// d256Abs makes a signed D256 absolute in-place, returns the sign bit (0 or 1).
func d256Abs(x *types.Decimal256) uint64 {
	sign := x.B192_255 >> 63
	negMask := -sign
	x.B0_63 ^= negMask
	x.B64_127 ^= negMask
	x.B128_191 ^= negMask
	x.B192_255 ^= negMask
	var c uint64
	x.B0_63, c = bits.Add64(x.B0_63, sign, 0)
	x.B64_127, c = bits.Add64(x.B64_127, 0, c)
	x.B128_191, c = bits.Add64(x.B128_191, 0, c)
	x.B192_255, _ = bits.Add64(x.B192_255, 0, c)
	return sign
}

// d256Negate conditionally negates a D256 in-place (two's complement).
// sign=0: no-op. sign=1: negate.
func d256Negate(x *types.Decimal256, sign uint64) {
	negMask := -sign
	x.B0_63 ^= negMask
	x.B64_127 ^= negMask
	x.B128_191 ^= negMask
	x.B192_255 ^= negMask
	var c uint64
	x.B0_63, c = bits.Add64(x.B0_63, sign, 0)
	x.B64_127, c = bits.Add64(x.B64_127, 0, c)
	x.B128_191, c = bits.Add64(x.B128_191, 0, c)
	x.B192_255, _ = bits.Add64(x.B192_255, 0, c)
}

func d128IsZero(d types.Decimal128) bool { return d.B0_63 == 0 && d.B64_127 == 0 }
func d256IsZero(d types.Decimal256) bool {
	return d.B0_63 == 0 && d.B64_127 == 0 && d.B128_191 == 0 && d.B192_255 == 0
}

// ---- Decimal128 add/sub ----

// d128Mul is the batch kernel for Decimal128 multiplication. It inlines a fast
// multiply path for D64-origin values and falls back to MulInplace for
// genuinely large Decimal128 values.
func d128Add(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d128AddSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d128AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal128 Add overflow: %s+%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

// d128Sub is the batch kernel for Decimal128 subtraction.
// Same-scale: direct bits.Sub64 without overflow checking.
// Different-scale: scales operand(s) first, then uses bits.Sub64 loop.

func d128AddSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls) int {
	bmp := rsnull.GetBitmap()
	len1 := len(v1)
	len2 := len(v2)
	noNull := rsnull.IsEmpty()

	var carry uint64

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, v2[i].B64_127, carry)
				if signX == v2[i].B64_127>>63 && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, v2[i].B64_127, carry)
				if signX == v2[i].B64_127>>63 && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := a.B64_127 >> 63
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i].B0_63, carry = bits.Add64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(a.B64_127, v2[i].B64_127, carry)
				if signA == v2[i].B64_127>>63 && signA != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, carry = bits.Add64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(a.B64_127, v2[i].B64_127, carry)
				if signA == v2[i].B64_127>>63 && signA != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	} else {
		b := v2[0]
		signB := b.B64_127 >> 63
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, b.B64_127, carry)
				if v1[i].B64_127>>63 == signB && v1[i].B64_127>>63 != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, b.B64_127, carry)
				if v1[i].B64_127>>63 == signB && v1[i].B64_127>>63 != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	}
	return -1
}

func d128AddDiffScale(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1 := len(v1)
	len2 := len(v2)

	// Addition is commutative: normalize to (const, vec) or (vec, vec).
	if len1 == 1 {
		if scale1 < scale2 {
			a := v1[0]
			if !d128ScaleUp(&a, scale2-scale1) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", v1[0].Format(scale2-scale1))
			}
			return d128AddSameScale([]types.Decimal128{a}, v2, rs, rsnull), nil
		}
		if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return -1, err
		}
		return d128AddSameScale(rs[:len2], []types.Decimal128{v1[0]}, rs, rsnull), nil
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b := v2[0]
			if !d128ScaleUp(&b, scale1-scale2) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", v2[0].Format(scale1-scale2))
			}
			return d128AddSameScale(v1, []types.Decimal128{b}, rs, rsnull), nil
		}
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d128AddSameScale(rs[:len1], []types.Decimal128{v2[0]}, rs, rsnull), nil
	}

	// vector-vector: scale the lower-scale one into rs, then add.
	if scale1 < scale2 {
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d128AddSameScale(rs[:len1], v2, rs, rsnull), nil
	}
	if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return -1, err
	}
	return d128AddSameScale(v1, rs[:len2], rs, rsnull), nil
}

// d128SubDiffScale handles Decimal128 subtraction (v1 - v2) when scales differ.

func d128Sub(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d128SubSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d128SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal128 Sub overflow: %s-%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

// d128AddDiffScale handles Decimal128 addition when scales differ.
// For const operands: scale the constant once, then same-scale bits.Add64.
// For vector-vector: scale lower-scale vector into rs, then bits.Add64 with other.

func d128SubSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls) int {
	bmp := rsnull.GetBitmap()
	len1 := len(v1)
	len2 := len(v2)
	noNull := rsnull.IsEmpty()

	var borrow uint64

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, v2[i].B64_127, borrow)
				if signX != v2[i].B64_127>>63 && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, v2[i].B64_127, borrow)
				if signX != v2[i].B64_127>>63 && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := a.B64_127 >> 63
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i].B0_63, borrow = bits.Sub64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(a.B64_127, v2[i].B64_127, borrow)
				if signA != v2[i].B64_127>>63 && signA != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, borrow = bits.Sub64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(a.B64_127, v2[i].B64_127, borrow)
				if signA != v2[i].B64_127>>63 && signA != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	} else {
		b := v2[0]
		signB := b.B64_127 >> 63
		if noNull {
			for i := 0; i < len1; i++ {
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, b.B64_127, borrow)
				if signX != signB && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				signX := v1[i].B64_127 >> 63
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, b.B64_127, borrow)
				if signX != signB && signX != rs[i].B64_127>>63 {
					return i
				}
			}
		}
	}
	return -1
}

func d128SubDiffScale(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1 := len(v1)
	len2 := len(v2)

	if len1 == 1 {
		// const - vector
		if scale1 < scale2 {
			a := v1[0]
			if !d128ScaleUp(&a, scale2-scale1) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", v1[0].Format(scale2-scale1))
			}
			return d128SubSameScale([]types.Decimal128{a}, v2, rs, rsnull), nil
		}
		// scale1 > scale2: scale v2 into rs, then const - rs[i]
		if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return -1, err
		}
		return d128SubSameScale([]types.Decimal128{v1[0]}, rs[:len2], rs, rsnull), nil
	}

	if len2 == 1 {
		// vector - const
		if scale2 < scale1 {
			b := v2[0]
			if !d128ScaleUp(&b, scale1-scale2) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", v2[0].Format(scale1-scale2))
			}
			return d128SubSameScale(v1, []types.Decimal128{b}, rs, rsnull), nil
		}
		// scale2 > scale1: scale v1 into rs, then rs[i] - const
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d128SubSameScale(rs[:len1], []types.Decimal128{v2[0]}, rs, rsnull), nil
	}

	// vector-vector: scale the lower-scale operand into rs.
	if scale1 < scale2 {
		// scale v1 into rs, then rs[i] - v2[i]
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d128SubSameScale(rs[:len1], v2, rs, rsnull), nil
	}
	// scale v2 into rs, then v1[i] - rs[i]
	if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return -1, err
	}
	return d128SubSameScale(v1, rs[:len2], rs, rsnull), nil
}

// d128ScaleIntoRs scales vec[i] * 10^scaleDiff into rs[i], respecting nulls.
// scaleDiff is always positive (multiply up). Values are signed D128.

func d128MulPow10(x *types.Decimal128, n int32) bool {
	if n <= 19 {
		return d128Mul1Limb(x, types.Pow10[n])
	}
	if n > 38 {
		return false
	}
	return d128Mul1Limb(x, types.Pow10[19]) && d128Mul1Limb(x, types.Pow10[n-19])
}

// d128DivPow10 divides unsigned D128 x by 10^n in-place with round-half-up.
// n must be in [1, 38].
func d128DivPow10(x *types.Decimal128, n int32) {
	if n <= 19 {
		d128DivPow10Once(x, types.Pow10[n])
		return
	}
	d128DivPow10Once(x, types.Pow10[19])
	d128DivPow10Once(x, types.Pow10[n-19])
}

// d128ScaleUp scales a signed D128 up by 10^n. Branchless sign handling, in-place.
// Returns false on overflow (x may be corrupted; callers use original for error msg).
func d128ScaleUp(x *types.Decimal128, n int32) bool {
	sign := d128Abs(x)
	ok := d128MulPow10(x, n)
	d128Negate(x, sign)
	return ok
}

// d128ScaleDown divides a signed D128 by 10^n with round-half-up. Branchless, in-place.
func d128ScaleDown(x *types.Decimal128, n int32) {
	sign := d128Abs(x)
	d128DivPow10(x, n)
	d128Negate(x, sign)
}

// d256Mul1Limb multiplies unsigned D256 x by a single 64-bit factor p in-place.
// Returns false on overflow (unsigned result must fit in 255 bits for signed restore).
func d128ScaleIntoRs(vec, rs []types.Decimal128, n int, scaleDiff int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	if rsnull.IsEmpty() {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			if !d128ScaleUp(&rs[i], scaleDiff) {
				return moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", vec[i].Format(scaleDiff))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			if bmp.Contains(uint64(i)) {
				continue
			}
			if !d128ScaleUp(&rs[i], scaleDiff) {
				return moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", vec[i].Format(scaleDiff))
			}
		}
	}
	return nil
}

// ---- Decimal128 multiply ----

func d128Mul(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	var scale int32 = 12
	if scale1 > scale {
		scale = scale1
	}
	if scale2 > scale {
		scale = scale2
	}
	if scale1+scale2 < scale {
		scale = scale1 + scale2
	}
	scaleAdj := scale - scale1 - scale2

	len1, len2 := len(v1), len(v2)

	if len1 == len2 {
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				if err := d128MulInline(&v1[i], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128MulInline(&v1[i], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else if len1 == 1 {
		if rsnull.IsEmpty() {
			for i := 0; i < len2; i++ {
				if err := d128MulInline(&v1[0], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128MulInline(&v1[0], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else {
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				if err := d128MulInline(&v1[i], &v2[0], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128MulInline(&v1[i], &v2[0], &rs[i], scaleAdj, scale1, scale2); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// d128MulInline multiplies two Decimal128 values, writing the result to dst.
// Uses the fast int64 path when both operands fit in int64, falls back to
// MulInplace otherwise. Pointer arguments avoid return-value copy overhead.

func d128MulInline(x, y, dst *types.Decimal128, scaleAdj, scale1, scale2 int32) error {
	xhi, yhi := x.B64_127, y.B64_127
	// Fast path: both values fit in int64 range [-2^63, 2^63-1].
	// Sign-extension check: if B0_63 interpreted as int64 sign-extends to B64_127,
	// the full 128-bit value is representable as int64.
	if uint64(int64(x.B0_63)>>63) == xhi && uint64(int64(y.B0_63)>>63) == yhi {
		neg := (xhi ^ yhi) >> 63
		// Get absolute values of int64 representation.
		xi := int64(x.B0_63)
		yi := int64(y.B0_63)
		ax := uint64((xi ^ (xi >> 63)) - (xi >> 63)) // branchless abs
		ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
		hi, lo := bits.Mul64(ax, ay)
		dst.B0_63 = lo
		dst.B64_127 = hi
		if scaleAdj != 0 {
			// scaleAdj is always negative here (divide by 10^|scaleAdj|).
			d128DivPow10(dst, -scaleAdj)
		}
		// Branchless conditional negate (two's complement: XOR + add carry).
		d128Negate(dst, neg)
		return nil
	}

	// Slow path: genuinely large Decimal128 values — inline MulInplace.
	ax := *x
	signx := d128Abs(&ax)
	ay := *y
	signy := d128Abs(&ay)
	neg := signx ^ signy

	// Try 128×128→128 unsigned multiply (inline Mul128InPlace).
	// result = ax.lo*ay.lo + cross*2^64 where cross = ax.lo*ay.hi or ax.hi*ay.lo.
	if ax.B64_127 == 0 || ay.B64_127 == 0 {
		zhi, zlo := bits.Mul64(ax.B0_63, ay.B0_63)
		var crossHi, crossLo uint64
		if ax.B64_127 == 0 {
			crossHi, crossLo = bits.Mul64(ax.B0_63, ay.B64_127)
		} else {
			crossHi, crossLo = bits.Mul64(ax.B64_127, ay.B0_63)
		}
		rhi, carry := bits.Add64(zhi, crossLo, 0)
		if (crossHi | carry | (rhi >> 63)) == 0 {
			dst.B0_63 = zlo
			dst.B64_127 = rhi
			if scaleAdj != 0 {
				d128DivPow10(dst, -scaleAdj)
			}
			d128Negate(dst, neg)
			return nil
		}
	}

	// D256 fallback: multiply in 256-bit, scale down, truncate.
	x2 := types.Decimal256{B0_63: ax.B0_63, B64_127: ax.B64_127}
	y2 := types.Decimal256{B0_63: ay.B0_63, B64_127: ay.B64_127}
	x2, _ = x2.Mul256(y2)
	d256DivPow10(&x2, -scaleAdj)
	if x2.B128_191 != 0 || x2.B192_255 != 0 || x2.B64_127>>63 != 0 {
		return moerr.NewInvalidInputNoCtxf("Decimal128 Mul overflow: %s*%s", x.Format(scale1), y.Format(scale2))
	}
	dst.B0_63 = x2.B0_63
	dst.B64_127 = x2.B64_127
	d128Negate(dst, neg)
	return nil
}

// ---- Decimal128 division ----

// d128DivKernel returns a batch division kernel for Decimal128 inputs.
// The shouldError flag controls division-by-zero behavior:
//   - false: mark result as NULL (SQL standard / MySQL default)
//   - true: return error (strict mode)
func d128DivKernel(shouldError bool) func(v1, v2 []types.Decimal128, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2 []types.Decimal128, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d128Div(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

// d64DivKernel returns a batch division kernel for Decimal64 → Decimal128.

func d128Div(v1, v2 []types.Decimal128, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	bmp := rsnull.GetBitmap()
	// Compute result scale once (same logic as Decimal128.Div).
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	// Pre-compute scale factor for the fast inline path.
	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	len1, len2 := len(v1), len(v2)

	if len1 == len2 {
		// vec / vec
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				if err := d128DivOneDispatch(v1[i], v2[i], &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128DivOneDispatch(v1[i], v2[i], &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else if len1 == 1 {
		// const / vec
		a := v1[0]
		if rsnull.IsEmpty() {
			for i := 0; i < len2; i++ {
				if err := d128DivOneDispatch(a, v2[i], &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128DivOneDispatch(a, v2[i], &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else {
		// vec / const: precompute divisor properties.
		b := v2[0]
		if b.B0_63 == 0 && b.B64_127 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		signyU := b.B64_127 >> 63
		negMaskY := -signyU
		absY := types.Decimal128{B0_63: b.B0_63 ^ negMaskY, B64_127: b.B64_127 ^ negMaskY}
		var cY uint64
		absY.B0_63, cY = bits.Add64(absY.B0_63, signyU, 0)
		absY.B64_127, _ = bits.Add64(absY.B64_127, 0, cY)
		// Use fully-inlined path if divisor fits in 64 bits and scale is small.
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					if !d128DivInline(v1[i], absY64, signyU, scaleFactor, &rs[i]) {
						if err := d128DivOne(v1[i], b, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					if !d128DivInline(v1[i], absY64, signyU, scaleFactor, &rs[i]) {
						if err := d128DivOne(v1[i], b, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				if err := d128DivOne(v1[i], b, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				if err := d128DivOne(v1[i], b, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// d128DivOneDispatch tries the fast inline path first, falls back to d128DivOne.
func d128DivOneDispatch(x, y types.Decimal128, dst *types.Decimal128, scaleAdj int32,
	scaleFactor uint64, canInline bool, rsnull *nulls.Nulls, idx uint64, shouldError bool, scale1, scale2 int32) error {
	// Zero-divisor check.
	if y.B0_63 == 0 && y.B64_127 == 0 {
		if shouldError {
			return moerr.NewDivByZeroNoCtx()
		}
		rsnull.Add(idx)
		return nil
	}
	// Try fast path: small scale, 64-bit divisor.
	if canInline {
		absY := y
		signyU := d128Abs(&absY)
		if absY.B64_127 == 0 {
			if d128DivInline(x, absY.B0_63, signyU, scaleFactor, dst) {
				return nil
			}
		}
	}
	return d128DivOne(x, y, dst, scaleAdj, rsnull, idx, shouldError, scale1, scale2)
}

// d128DivOne computes a single D128 division with scale adjustment.
// Handles all cases including overflow fallback to D256.

func d128DivOne(x, y types.Decimal128, dst *types.Decimal128, scaleAdj int32, rsnull *nulls.Nulls, idx uint64, shouldError bool, scale1, scale2 int32) error {
	if y.B0_63 == 0 && y.B64_127 == 0 {
		if shouldError {
			return moerr.NewDivByZeroNoCtx()
		}
		rsnull.Add(idx)
		return nil
	}

	signxU := d128Abs(&x)
	signyU := d128Abs(&y)
	neg := signxU ^ signyU

	// Inline scale-up: multiply unsigned x by 10^scaleAdj.
	z := x
	if !d128MulPow10(&z, scaleAdj) {
		// Overflow in scale: fall back to D256 division.
		x2 := types.Decimal256{B0_63: x.B0_63, B64_127: x.B64_127}
		y2 := types.Decimal256{B0_63: y.B0_63, B64_127: y.B64_127}
		if !d256MulPow10(&x2, scaleAdj) {
			return moerr.NewInvalidInputNoCtxf("Decimal128 Div overflow: %s/%s", x.Format(scale1), y.Format(scale2))
		}
		x2, divErr := x2.Div256(y2)
		if divErr != nil || x2.B192_255 != 0 || x2.B128_191 != 0 || x2.B64_127>>63 != 0 {
			return moerr.NewInvalidInputNoCtxf("Decimal128 Div overflow: %s/%s", x.Format(scale1), y.Format(scale2))
		}
		z = types.Decimal128{B0_63: x2.B0_63, B64_127: x2.B64_127}
		d128Negate(&z, neg)
		*dst = z
		return nil
	}

	var err error

	z, err = z.Div128(y)
	if err != nil {
		return err
	}
	d128Negate(&z, neg)
	*dst = z
	return nil
}

// d128DivInline is the fast inline path for D128 division.
// Pre-conditions: absY > 0, absY fits in 64 bits (absY64 > 0),
// scaleAdj ≤ 19. x is a signed D128. signy is the sign of the divisor.
// Returns false if the fast path overflows and the caller should fall back.
func d128DivInline(x types.Decimal128, absY64 uint64, signy uint64,
	scaleFactor uint64, dst *types.Decimal128) bool {

	signx := d128Abs(&x)

	// Inline Scale: multiply |x| by scaleFactor (a uint64 power of 10).
	// This is Mul128 with y = {scaleFactor, 0}, but without error allocation.
	var zHi, zLo uint64
	zHi, zLo = bits.Mul64(x.B0_63, scaleFactor)
	if x.B64_127 != 0 {
		crossHi, crossLo := bits.Mul64(x.B64_127, scaleFactor)
		if crossHi != 0 {
			return false // overflow
		}
		var carry uint64
		zHi, carry = bits.Add64(zHi, crossLo, 0)
		if carry != 0 || zHi>>63 != 0 {
			return false // overflow (result must be positive D128)
		}
	}

	// Inline Div128 for 64-bit divisor: two bits.Div64 calls.
	var rem uint64
	zHi, rem = bits.Div64(0, zHi, absY64)
	zLo, rem = bits.Div64(rem, zLo, absY64)
	// Rounding: if remainder*2 >= divisor, round up.
	if rem*2 >= absY64 || rem>>63 != 0 {
		zLo++
		if zLo == 0 {
			zHi++
		}
	}

	// Branchless sign restore.
	z := types.Decimal128{B0_63: zLo, B64_127: zHi}
	d128Negate(&z, signx^signy)
	*dst = z
	return true
}

// ---- Decimal128 modulo ----

func d128ModKernel(shouldError bool) func(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d128Mod(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

func d128Mod(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Same-scale fast path: inline sign handling + 128-bit mod.
	if scale1 == scale2 {
		return d128ModSameScale(v1, v2, rs, rsnull, shouldError, len1, len2, hasNull, bmp)
	}

	// Diff-scale fast path: branchless abs + inline scale + mod.
	diff := scale2 - scale1
	mask := diff >> 31
	scaleAx := diff &^ mask
	scaleAy := (-diff) & mask

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d128IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, ok := d128ModDiffScaleOne(v1[i], v2[i], scaleAx, scaleAy)
			if !ok {
				var err error
				r, _, err = v1[i].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
			}
			rs[i] = r
		}
	} else if len1 == 1 {
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d128IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, ok := d128ModDiffScaleOne(v1[0], v2[i], scaleAx, scaleAy)
			if !ok {
				var err error
				r, _, err = v1[0].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
			}
			rs[i] = r
		}
	} else {
		if d128IsZero(v2[0]) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, ok := d128ModDiffScaleOne(v1[i], v2[0], scaleAx, scaleAy)
			if !ok {
				var err error
				r, _, err = v1[i].Mod(v2[0], scale1, scale2)
				if err != nil {
					return err
				}
			}
			rs[i] = r
		}
	}
	return nil
}

// d128ModSameScale handles same-scale D128 modulo with inline 128-bit mod.
func d128ModSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls, shouldError bool,
	len1, len2 int, hasNull bool, bmp *bitmap.Bitmap) error {

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d128IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			rs[i] = d128ModOne(v1[i], v2[i])
		}
	} else if len1 == 1 {
		x := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d128IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			rs[i] = d128ModOne(x, v2[i])
		}
	} else {
		if d128IsZero(v2[0]) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		y := v2[0]
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			rs[i] = d128ModOne(v1[i], y)
		}
	}
	return nil
}

// d128ModOne computes x mod y for signed Decimal128 values (same scale).
// Result has the sign of x (Go % semantics).
func d128ModOne(x, y types.Decimal128) types.Decimal128 {
	ax := x
	signx := d128Abs(&ax)
	ay := y
	d128Abs(&ay)

	// Inline mod: if ay fits in 64 bits, use efficient 2-div approach.
	var r types.Decimal128
	if ay.B64_127 == 0 {
		_, rhi := bits.Div64(0, ax.B64_127, ay.B0_63)
		_, rlo := bits.Div64(rhi, ax.B0_63, ay.B0_63)
		r = types.Decimal128{B0_63: rlo}
	} else {
		r, _ = ax.Mod128(ay)
	}

	// Restore x's sign.
	d128Negate(&r, signx)
	return r
}

// d128ModDiffScaleOne computes x mod y where scales differ.
// scaleAx and scaleAy are precomputed at batch level: one is scaleDiff, the other 0.
// Branchless abs + inline scale eliminates ~5 branches vs generic .Mod().
// Returns (result, ok). ok=false only on D128 scaling overflow (rare).
func d128ModDiffScaleOne(x, y types.Decimal128, scaleAx, scaleAy int32) (types.Decimal128, bool) {
	ax := x
	signx := d128Abs(&ax)
	ay := y
	d128Abs(&ay)

	if !d128MulPow10(&ax, scaleAx) || !d128MulPow10(&ay, scaleAy) {
		return types.Decimal128{}, false
	}

	var r types.Decimal128
	if ay.B64_127 == 0 {
		_, rhi := bits.Div64(0, ax.B64_127, ay.B0_63)
		_, rlo := bits.Div64(rhi, ax.B0_63, ay.B0_63)
		r = types.Decimal128{B0_63: rlo}
	} else {
		r, _ = ax.Mod128(ay)
	}

	d128Negate(&r, signx)
	return r, true
}

// ---- Decimal128 integer division ----

// d256Add is the batch kernel for Decimal256 addition.
func d128IntDivKernel(proc *process.Process, selectList *FunctionSelectList) func(v1, v2 []types.Decimal128, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	shouldError := checkDivisionByZeroBehavior(proc, selectList)
	return func(v1, v2 []types.Decimal128, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d128IntDiv(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

// d256IntDivKernel returns a closure for Decimal256 → int64 integer division.
func d128IntDiv(v1, v2 []types.Decimal128, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	bmp := rsnull.GetBitmap()
	hasNull := !rsnull.IsEmpty()
	// Compute result scale (same as Decimal128.Div).
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	len1, len2 := len(v1), len(v2)
	for i := 0; i < max(len1, len2); i++ {
		if hasNull && bmp.Contains(uint64(i)) {
			continue
		}
		a, b := operandsAt(v1, v2, i)
		if b.B0_63 == 0 && b.B64_127 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			rsnull.Add(uint64(i))
			continue
		}
		var divResult types.Decimal128
		if err := d128DivOneDispatch(a, b, &divResult, scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
			return err
		}
		// Truncate fractional part.
		if scale > 0 {
			d128ScaleDown(&divResult, scale)
		}
		var err error
		rs[i], err = decimal128ToInt64(divResult)
		if err != nil {
			return err
		}
	}
	return nil
}

// ---- Decimal256 add/sub ----

func d256Mul1Limb(x *types.Decimal256, p uint64) bool {
	h0, l0 := bits.Mul64(x.B0_63, p)
	h1, l1 := bits.Mul64(x.B64_127, p)
	h2, l2 := bits.Mul64(x.B128_191, p)
	h3, l3 := bits.Mul64(x.B192_255, p)
	x.B0_63 = l0
	var c uint64
	x.B64_127, c = bits.Add64(l1, h0, 0)
	x.B128_191, c = bits.Add64(l2, h1, c)
	x.B192_255, c = bits.Add64(l3, h2, c)
	return h3 == 0 && c == 0 && x.B192_255>>63 == 0
}

// d256MulPow10 multiplies unsigned D256 x by 10^n in-place.
// Returns false on overflow. n must be >= 1.
func d256MulPow10(x *types.Decimal256, n int32) bool {
	for n > 19 {
		if !d256Mul1Limb(x, types.Pow10[19]) {
			return false
		}
		n -= 19
	}
	return d256Mul1Limb(x, types.Pow10[n])
}

// d256ScaleUp scales a signed D256 up by 10^n. Branchless sign handling, in-place.
// Returns false on overflow (x may be corrupted; callers use original for error msg).
func d256ScaleUp(x *types.Decimal256, n int32) bool {
	sign := d256Abs(x)
	ok := d256MulPow10(x, n)
	d256Negate(x, sign)
	return ok
}

// d256DivPow10 divides unsigned D256 x by 10^n in-place with round-half-up.
// n must be >= 1. Uses loop for n > 19 (same approach as d256MulPow10).
func d256DivPow10(x *types.Decimal256, n int32) {
	for n > 19 {
		d256DivPow10Once(x, types.Pow10[19])
		n -= 19
	}
	d256DivPow10Once(x, types.Pow10[n])
}

// d256DivPow10Once divides unsigned D256 x by d in-place with round-half-up.
func d256DivPow10Once(x *types.Decimal256, d uint64) {
	var rem uint64
	x.B192_255, rem = bits.Div64(0, x.B192_255, d)
	x.B128_191, rem = bits.Div64(rem, x.B128_191, d)
	x.B64_127, rem = bits.Div64(rem, x.B64_127, d)
	x.B0_63, rem = bits.Div64(rem, x.B0_63, d)
	// Branchless round half-up: round = 1 iff 2*rem >= d.
	dblLo, dblHi := bits.Add64(rem, rem, 0)
	_, borrow := bits.Sub64(dblLo, d, 0)
	round := (1 - borrow) | dblHi
	var c uint64
	x.B0_63, c = bits.Add64(x.B0_63, round, 0)
	x.B64_127, c = bits.Add64(x.B64_127, 0, c)
	x.B128_191, c = bits.Add64(x.B128_191, 0, c)
	x.B192_255, _ = bits.Add64(x.B192_255, 0, c)
}

// d256ScaleDown divides a signed D256 by 10^n with round-half-up. Branchless, in-place.
func d256ScaleDown(x *types.Decimal256, n int32) {
	sign := d256Abs(x)
	d256DivPow10(x, n)
	d256Negate(x, sign)
}

// d64Add is the batch kernel for Decimal64 addition.
func d256Add(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d256AddSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d256AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal256 Add overflow: %s+%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

// d256Sub is the batch kernel for Decimal256 subtraction.
func d256AddSameScale(v1, v2, rs []types.Decimal256, rsnull *nulls.Nulls) int {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			x, y := v1[i], v2[i]
			signX := x.B192_255 >> 63
			signY := y.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(x.B0_63, y.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(x.B64_127, y.B64_127, c)
			rs[i].B128_191, c = bits.Add64(x.B128_191, y.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(x.B192_255, y.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signX == signY && signX != signR {
				return i
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := a.B192_255 >> 63
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := v2[i]
			signY := y.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(a.B0_63, y.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(a.B64_127, y.B64_127, c)
			rs[i].B128_191, c = bits.Add64(a.B128_191, y.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(a.B192_255, y.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signA == signY && signA != signR {
				return i
			}
		}
	} else {
		b := v2[0]
		signB := b.B192_255 >> 63
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			x := v1[i]
			signX := x.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(x.B0_63, b.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(x.B64_127, b.B64_127, c)
			rs[i].B128_191, c = bits.Add64(x.B128_191, b.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(x.B192_255, b.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signX == signB && signX != signR {
				return i
			}
		}
	}
	return -1
}

// d256AddDiffScale handles Decimal256 addition when scales differ.
// Fuses per-element scaling and addition into a single pass to avoid
// writing scaled values to rs and re-reading them.
func d256AddDiffScale(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	if len1 == 1 {
		if scale1 < scale2 {
			a := v1[0]
			if !d256ScaleUp(&a, scale2-scale1) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[0].Format(0))
			}
			return d256AddSameScale([]types.Decimal256{a}, v2, rs, rsnull), nil
		}
		a := v1[0]
		signA := a.B192_255 >> 63
		scaleDiff := scale1 - scale2
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUp(&b, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[i].Format(0))
			}
			signB := b.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(a.B0_63, b.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(a.B64_127, b.B64_127, c)
			rs[i].B128_191, c = bits.Add64(a.B128_191, b.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(a.B192_255, b.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signA == signB && signA != signR {
				return i, nil
			}
		}
		return -1, nil
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b := v2[0]
			if !d256ScaleUp(&b, scale1-scale2) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[0].Format(0))
			}
			return d256AddSameScale(v1, []types.Decimal256{b}, rs, rsnull), nil
		}
		b := v2[0]
		signB := b.B192_255 >> 63
		scaleDiff := scale2 - scale1
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUp(&a, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[i].Format(0))
			}
			signA := a.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(a.B0_63, b.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(a.B64_127, b.B64_127, c)
			rs[i].B128_191, c = bits.Add64(a.B128_191, b.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(a.B192_255, b.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signA == signB && signA != signR {
				return i, nil
			}
		}
		return -1, nil
	}

	// len1 == len2: fuse scale + add per element
	if scale1 < scale2 {
		scaleDiff := scale2 - scale1
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUp(&a, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[i].Format(0))
			}
			y := v2[i]
			signA := a.B192_255 >> 63
			signY := y.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(a.B0_63, y.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(a.B64_127, y.B64_127, c)
			rs[i].B128_191, c = bits.Add64(a.B128_191, y.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(a.B192_255, y.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signA == signY && signA != signR {
				return i, nil
			}
		}
	} else {
		scaleDiff := scale1 - scale2
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUp(&b, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[i].Format(0))
			}
			x := v1[i]
			signX := x.B192_255 >> 63
			signB := b.B192_255 >> 63
			var c uint64
			rs[i].B0_63, c = bits.Add64(x.B0_63, b.B0_63, 0)
			rs[i].B64_127, c = bits.Add64(x.B64_127, b.B64_127, c)
			rs[i].B128_191, c = bits.Add64(x.B128_191, b.B128_191, c)
			rs[i].B192_255, _ = bits.Add64(x.B192_255, b.B192_255, c)
			signR := rs[i].B192_255 >> 63
			if signX == signB && signX != signR {
				return i, nil
			}
		}
	}
	return -1, nil
}

// d256SubSameScale subtracts two Decimal256 vectors with the same scale, inlining
// the 4-limb borrow chain and overflow check to avoid the non-inlinable Sub256 method.
func d256Sub(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d256SubSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d256SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal256 Sub overflow: %s-%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

// d256AddSameScale adds two Decimal256 vectors with the same scale, inlining
// the 4-limb carry chain and overflow check to avoid the non-inlinable Add256 method.
func d256SubSameScale(v1, v2, rs []types.Decimal256, rsnull *nulls.Nulls) int {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			x, y := v1[i], v2[i]
			signX := x.B192_255 >> 63
			signY := y.B192_255 >> 63
			var b uint64
			rs[i].B0_63, b = bits.Sub64(x.B0_63, y.B0_63, 0)
			rs[i].B64_127, b = bits.Sub64(x.B64_127, y.B64_127, b)
			rs[i].B128_191, b = bits.Sub64(x.B128_191, y.B128_191, b)
			rs[i].B192_255, _ = bits.Sub64(x.B192_255, y.B192_255, b)
			signR := rs[i].B192_255 >> 63
			if signX != signY && signX != signR {
				return i
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := a.B192_255 >> 63
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := v2[i]
			signY := y.B192_255 >> 63
			var b uint64
			rs[i].B0_63, b = bits.Sub64(a.B0_63, y.B0_63, 0)
			rs[i].B64_127, b = bits.Sub64(a.B64_127, y.B64_127, b)
			rs[i].B128_191, b = bits.Sub64(a.B128_191, y.B128_191, b)
			rs[i].B192_255, _ = bits.Sub64(a.B192_255, y.B192_255, b)
			signR := rs[i].B192_255 >> 63
			if signA != signY && signA != signR {
				return i
			}
		}
	} else {
		b := v2[0]
		signB := b.B192_255 >> 63
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			x := v1[i]
			signX := x.B192_255 >> 63
			var bw uint64
			rs[i].B0_63, bw = bits.Sub64(x.B0_63, b.B0_63, 0)
			rs[i].B64_127, bw = bits.Sub64(x.B64_127, b.B64_127, bw)
			rs[i].B128_191, bw = bits.Sub64(x.B128_191, b.B128_191, bw)
			rs[i].B192_255, _ = bits.Sub64(x.B192_255, b.B192_255, bw)
			signR := rs[i].B192_255 >> 63
			if signX != signB && signX != signR {
				return i
			}
		}
	}
	return -1
}

// d256SubDiffScale handles Decimal256 subtraction when scales differ.
// Fuses per-element scaling and subtraction into a single pass.
func d256SubDiffScale(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	if len1 == 1 {
		if scale1 < scale2 {
			a := v1[0]
			if !d256ScaleUp(&a, scale2-scale1) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[0].Format(0))
			}
			return d256SubSameScale([]types.Decimal256{a}, v2, rs, rsnull), nil
		}
		a := v1[0]
		signA := a.B192_255 >> 63
		scaleDiff := scale1 - scale2
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUp(&b, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[i].Format(0))
			}
			signB := b.B192_255 >> 63
			var bw uint64
			rs[i].B0_63, bw = bits.Sub64(a.B0_63, b.B0_63, 0)
			rs[i].B64_127, bw = bits.Sub64(a.B64_127, b.B64_127, bw)
			rs[i].B128_191, bw = bits.Sub64(a.B128_191, b.B128_191, bw)
			rs[i].B192_255, _ = bits.Sub64(a.B192_255, b.B192_255, bw)
			signR := rs[i].B192_255 >> 63
			if signA != signB && signA != signR {
				return i, nil
			}
		}
		return -1, nil
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b := v2[0]
			if !d256ScaleUp(&b, scale1-scale2) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[0].Format(0))
			}
			return d256SubSameScale(v1, []types.Decimal256{b}, rs, rsnull), nil
		}
		b := v2[0]
		signB := b.B192_255 >> 63
		scaleDiff := scale2 - scale1
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUp(&a, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[i].Format(0))
			}
			signA := a.B192_255 >> 63
			var bw uint64
			rs[i].B0_63, bw = bits.Sub64(a.B0_63, b.B0_63, 0)
			rs[i].B64_127, bw = bits.Sub64(a.B64_127, b.B64_127, bw)
			rs[i].B128_191, bw = bits.Sub64(a.B128_191, b.B128_191, bw)
			rs[i].B192_255, _ = bits.Sub64(a.B192_255, b.B192_255, bw)
			signR := rs[i].B192_255 >> 63
			if signA != signB && signA != signR {
				return i, nil
			}
		}
		return -1, nil
	}

	// len1 == len2: fuse scale + sub per element
	if scale1 < scale2 {
		scaleDiff := scale2 - scale1
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUp(&a, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v1[i].Format(0))
			}
			y := v2[i]
			signA := a.B192_255 >> 63
			signY := y.B192_255 >> 63
			var bw uint64
			rs[i].B0_63, bw = bits.Sub64(a.B0_63, y.B0_63, 0)
			rs[i].B64_127, bw = bits.Sub64(a.B64_127, y.B64_127, bw)
			rs[i].B128_191, bw = bits.Sub64(a.B128_191, y.B128_191, bw)
			rs[i].B192_255, _ = bits.Sub64(a.B192_255, y.B192_255, bw)
			signR := rs[i].B192_255 >> 63
			if signA != signY && signA != signR {
				return i, nil
			}
		}
	} else {
		scaleDiff := scale1 - scale2
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUp(&b, scaleDiff) {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal256 scale overflow: %s", v2[i].Format(0))
			}
			x := v1[i]
			signX := x.B192_255 >> 63
			signB := b.B192_255 >> 63
			var bw uint64
			rs[i].B0_63, bw = bits.Sub64(x.B0_63, b.B0_63, 0)
			rs[i].B64_127, bw = bits.Sub64(x.B64_127, b.B64_127, bw)
			rs[i].B128_191, bw = bits.Sub64(x.B128_191, b.B128_191, bw)
			rs[i].B192_255, _ = bits.Sub64(x.B192_255, b.B192_255, bw)
			signR := rs[i].B192_255 >> 63
			if signX != signB && signX != signR {
				return i, nil
			}
		}
	}
	return -1, nil
}

// ---- Decimal256 multiply ----

// d256Mul is the batch kernel for Decimal256 multiplication.
// Inlines the schoolbook 4×4 Mul256 cross-product to avoid
// per-element method calls (Mul256 cost 1230 >> inlining budget 80).
func d256Mul(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	desiredScale := int32(12)
	if scale1 > desiredScale {
		desiredScale = scale1
	}
	if scale2 > desiredScale {
		desiredScale = scale2
	}
	if scale1+scale2 < desiredScale {
		desiredScale = scale1 + scale2
	}
	scaleAdj := desiredScale - scale1 - scale2

	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d256MulInline(&v1[i], &v2[i], &rs[i], scale1, scale2); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d256MulInline(&v1[0], &v2[i], &rs[i], scale1, scale2); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d256MulInline(&v1[i], &v2[0], &rs[i], scale1, scale2); err != nil {
				return err
			}
		}
	}
	// Batch-level scale-down: applied once per batch, outside per-element loops.
	if scaleAdj < 0 {
		divN := -scaleAdj
		for i := range rs {
			d256ScaleDown(&rs[i], divN)
		}
	}
	return nil
}

// d256MulInline performs a single D256 multiply with inlined schoolbook 4×4 cross-product.
// Uses a single overflow flag instead of per-branch error returns.
func d256MulInline(x, y *types.Decimal256, dst *types.Decimal256, scale1, scale2 int32) error {
	xc := *x
	signxU := d256Abs(&xc)
	yc := *y
	signyU := d256Abs(&yc)
	x0, x1, x2, x3 := xc.B0_63, xc.B64_127, xc.B128_191, xc.B192_255
	y0, y1, y2, y3 := yc.B0_63, yc.B64_127, yc.B128_191, yc.B192_255

	// Early overflow: if both operands have high limbs set, product can't fit.
	if (x3 != 0 && (y3|y2|y1) != 0) || (x2 != 0 && (y3|y2) != 0) || (x1 != 0 && y3 != 0) {
		return moerr.NewInvalidInputNoCtxf("Decimal256 Mul overflow: %s*%s", x.Format(scale1), y.Format(scale2))
	}

	// Schoolbook 4×4 cross-product. Overflow flag avoids per-step error branches.
	var z0, z1, z2, z3, hi, lo, ca uint64

	z1, z0 = bits.Mul64(x0, y0)

	z2, lo = bits.Mul64(x0, y1)
	z1, ca = bits.Add64(z1, lo, 0)

	hi, lo = bits.Mul64(x1, y0)
	z2, z3 = bits.Add64(z2, hi, ca)
	z1, ca = bits.Add64(z1, lo, 0)

	hi, lo = bits.Mul64(x2, y0)
	z2, ca = bits.Add64(z2, lo, ca)
	z3, ca = bits.Add64(z3, hi, ca)
	ovf := ca

	hi, lo = bits.Mul64(x1, y1)
	z2, ca = bits.Add64(z2, lo, 0)
	z3, ca = bits.Add64(z3, hi, ca)
	ovf |= ca

	hi, lo = bits.Mul64(x0, y2)
	z2, ca = bits.Add64(z2, lo, 0)
	z3, ca = bits.Add64(z3, hi, ca)
	ovf |= ca

	hi, lo = bits.Mul64(x3, y0)
	ovf |= hi
	z3, ca = bits.Add64(z3, lo, 0)
	ovf |= ca

	hi, lo = bits.Mul64(x2, y1)
	ovf |= hi
	z3, ca = bits.Add64(z3, lo, 0)
	ovf |= ca

	hi, lo = bits.Mul64(x1, y2)
	ovf |= hi
	z3, ca = bits.Add64(z3, lo, 0)
	ovf |= ca

	hi, lo = bits.Mul64(x0, y3)
	ovf |= hi
	z3, ca = bits.Add64(z3, lo, 0)
	ovf |= ca

	if ovf != 0 || z3>>63 != 0 {
		return moerr.NewInvalidInputNoCtxf("Decimal256 Mul overflow: %s*%s", x.Format(scale1), y.Format(scale2))
	}

	z := types.Decimal256{B0_63: z0, B64_127: z1, B128_191: z2, B192_255: z3}
	d256Negate(&z, signxU^signyU)
	*dst = z
	return nil
}

// d256DivKernel returns a batch division kernel for Decimal256.

// ---- Decimal256 division ----

func d256DivKernel(shouldError bool) func(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d256Div(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

// d256AllFitD128 checks if ALL elements in a D256 slice fit in a D128 value.
// A D256 fits in D128 when the upper 128 bits are the sign extension of bit 127.
// Returns true if the entire batch can use the D128 fast path.
func d256AllFitD128(vs []types.Decimal256) bool {
	overflow := uint64(0)
	for i := range vs {
		signExt := ^uint64(0) * (vs[i].B64_127 >> 63)
		overflow |= vs[i].B128_191 ^ signExt
		overflow |= vs[i].B192_255 ^ signExt
	}
	return overflow == 0
}

func d256Div(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Pre-compute D128 scale factors for fast path.
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	// Pre-scan: if all elements fit in D128, use the fast D128 division path
	// for the entire batch without per-element fit checks.
	if d256AllFitD128(v1) && d256AllFitD128(v2) {
		return d256DivViaD128(v1, v2, rs, scaleAdj, rsnull, shouldError, scale1, scale2, hasNull, bmp)
	}

	// Slow path: use generic D256 division.
	divGeneric := func(a, b types.Decimal256, dst *types.Decimal256) error {
		r, _, err := a.Div(b, scale1, scale2)
		if err != nil {
			return moerr.NewInvalidInputNoCtxf("Decimal256 Div overflow: %s/%s", a.Format(scale1), b.Format(scale2))
		}
		*dst = r
		return nil
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divGeneric(v1[i], v2[i], &rs[i]); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divGeneric(a, v2[i], &rs[i]); err != nil {
				return err
			}
		}
	} else {
		b := v2[0]
		if d256IsZero(b) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := divGeneric(v1[i], b, &rs[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// d256DivViaD128 runs D256 division entirely through the D128 fast path.
// Called only after d256AllFitD128 confirms all elements fit.
func d256DivViaD128(v1, v2 []types.Decimal256, rs []types.Decimal256, scaleAdj int32, rsnull *nulls.Nulls, shouldError bool, scale1, scale2 int32, hasNull bool, bmp *bitmap.Bitmap) error {
	len1, len2 := len(v1), len(v2)

	d256toD128 := func(d types.Decimal256) types.Decimal128 {
		return types.Decimal128{B0_63: d.B0_63, B64_127: d.B64_127}
	}
	d128toD256 := func(d types.Decimal128) types.Decimal256 {
		signExt := ^uint64(0) * (d.B64_127 >> 63)
		return types.Decimal256{B0_63: d.B0_63, B64_127: d.B64_127, B128_191: signExt, B192_255: signExt}
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if y.B0_63 == 0 && y.B64_127 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			var r128 types.Decimal128
			if err := d128DivOne(d256toD128(v1[i]), y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			rs[i] = d128toD256(r128)
		}
	} else if len1 == 1 {
		x := d256toD128(v1[0])
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if y.B0_63 == 0 && y.B64_127 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			var r128 types.Decimal128
			if err := d128DivOne(x, y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			rs[i] = d128toD256(r128)
		}
	} else {
		y := d256toD128(v2[0])
		if y.B0_63 == 0 && y.B64_127 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			var r128 types.Decimal128
			if err := d128DivOne(d256toD128(v1[i]), y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			rs[i] = d128toD256(r128)
		}
	}
	return nil
}

// ---- Decimal256 modulo ----

func d256ModKernel(shouldError bool) func(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d256Mod(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

func d256Mod(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Pre-scan: if all elements fit in D128, use fast D128 mod path.
	if d256AllFitD128(v1) && d256AllFitD128(v2) {
		return d256ModViaD128(v1, v2, rs, scale1, scale2, rsnull, shouldError, len1, len2, hasNull, bmp)
	}

	// Slow path: generic D256 mod.
	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, _, err := v1[i].Mod(v2[i], scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else if len1 == 1 {
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, _, err := v1[0].Mod(v2[i], scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else {
		if d256IsZero(v2[0]) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, _, err := v1[i].Mod(v2[0], scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	}
	return nil
}

// d256ModViaD128 handles D256 mod via D128 narrowing.
// Unified path for both same-scale and diff-scale: when scales match,
// scaleAx=scaleAy=0 makes d128MulPow10 a no-op, eliminating the branch.
func d256ModViaD128(v1, v2, rs []types.Decimal256, scale1, scale2 int32,
	rsnull *nulls.Nulls, shouldError bool, len1, len2 int, hasNull bool, bmp *bitmap.Bitmap) error {

	d256toD128 := func(d types.Decimal256) types.Decimal128 {
		return types.Decimal128{B0_63: d.B0_63, B64_127: d.B64_127}
	}
	d128toD256 := func(d types.Decimal128) types.Decimal256 {
		signExt := ^uint64(0) * (d.B64_127 >> 63)
		return types.Decimal256{B0_63: d.B0_63, B64_127: d.B64_127, B128_191: signExt, B192_255: signExt}
	}

	diff := scale2 - scale1
	mask := diff >> 31
	scaleAx := diff &^ mask
	scaleAy := (-diff) & mask

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if d128IsZero(y) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, ok := d128ModDiffScaleOne(d256toD128(v1[i]), y, scaleAx, scaleAy)
			if !ok {
				var err error
				rs[i], _, err = v1[i].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
				continue
			}
			rs[i] = d128toD256(r)
		}
	} else if len1 == 1 {
		x := d256toD128(v1[0])
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if d128IsZero(y) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, ok := d128ModDiffScaleOne(x, y, scaleAx, scaleAy)
			if !ok {
				var err error
				rs[i], _, err = v1[0].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
				continue
			}
			rs[i] = d128toD256(r)
		}
	} else {
		y := d256toD128(v2[0])
		if d128IsZero(y) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, ok := d128ModDiffScaleOne(d256toD128(v1[i]), y, scaleAx, scaleAy)
			if !ok {
				var err error
				rs[i], _, err = v1[i].Mod(v2[0], scale1, scale2)
				if err != nil {
					return err
				}
				continue
			}
			rs[i] = d128toD256(r)
		}
	}
	return nil
}

// ---- Decimal256 integer division ----

// Returns int64 results. Uses the same division kernels as / but truncates
// the fractional part and converts to int64.

// d64IntDivKernel returns a closure matching the decimalBatchArith arithFn
// signature for Decimal64 → int64 integer division.
func d256IntDivKernel(proc *process.Process, selectList *FunctionSelectList) func(v1, v2 []types.Decimal256, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	shouldError := checkDivisionByZeroBehavior(proc, selectList)
	return func(v1, v2 []types.Decimal256, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d256IntDiv(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

func d256IntDiv(v1, v2 []types.Decimal256, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Pre-compute D128 scale factors for fast path.
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	// Pre-scan: if all elements fit in D128, use the fast D128 division path.
	if d256AllFitD128(v1) && d256AllFitD128(v2) {
		return d256IntDivViaD128(v1, v2, rs, scale, scaleAdj, rsnull, shouldError, scale1, scale2, hasNull, bmp)
	}

	// Slow path: generic D256 division.
	divGeneric := func(a, b types.Decimal256, dst *int64) error {
		divResult, resultScale, err := a.Div(b, scale1, scale2)
		if err != nil {
			return moerr.NewInvalidInputNoCtxf("Decimal256 Div overflow: %s/%s", a.Format(scale1), b.Format(scale2))
		}
		d256ScaleDown(&divResult, resultScale)
		*dst, err = decimal256ToInt64(divResult)
		return err
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divGeneric(v1[i], v2[i], &rs[i]); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if d256IsZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divGeneric(a, v2[i], &rs[i]); err != nil {
				return err
			}
		}
	} else {
		b := v2[0]
		if d256IsZero(b) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := divGeneric(v1[i], b, &rs[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// d256IntDivViaD128 runs D256 integer division through the D128 fast path.
func d256IntDivViaD128(v1, v2 []types.Decimal256, rs []int64, scale, scaleAdj int32, rsnull *nulls.Nulls, shouldError bool, scale1, scale2 int32, hasNull bool, bmp *bitmap.Bitmap) error {
	len1, len2 := len(v1), len(v2)

	d256toD128 := func(d types.Decimal256) types.Decimal128 {
		return types.Decimal128{B0_63: d.B0_63, B64_127: d.B64_127}
	}

	divOne := func(x, y types.Decimal128, dst *int64, idx uint64) error {
		var divResult types.Decimal128
		if err := d128DivOne(x, y, &divResult, scaleAdj, rsnull, idx, shouldError, scale1, scale2); err != nil {
			return err
		}
		if scale > 0 {
			d128ScaleDown(&divResult, scale)
		}
		var err error
		*dst, err = decimal128ToInt64(divResult)
		return err
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if y.B0_63 == 0 && y.B64_127 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divOne(d256toD128(v1[i]), y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		x := d256toD128(v1[0])
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			y := d256toD128(v2[i])
			if y.B0_63 == 0 && y.B64_127 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			if err := divOne(x, y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	} else {
		y := d256toD128(v2[0])
		if y.B0_63 == 0 && y.B64_127 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := divOne(d256toD128(v1[i]), y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- Decimal64 add/sub ----

// d64Mul is the batch kernel for Decimal64 × Decimal64 → Decimal128.
// len(v1)==1 or len(v2)==1 indicates a broadcast constant.
func d64Add(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d64AddSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d64AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal64 Add overflow: %s+%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

func d64AddSameScale(v1, v2, rs []types.Decimal64, rsnull *nulls.Nulls) int {
	bmp := rsnull.GetBitmap()
	len1, len2 := len(v1), len(v2)
	noNull := rsnull.IsEmpty()

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i] = v1[i] + v2[i]
				signX := uint64(v1[i]) >> 63
				if signX == uint64(v2[i])>>63 && signX != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = v1[i] + v2[i]
				signX := uint64(v1[i]) >> 63
				if signX == uint64(v2[i])>>63 && signX != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := uint64(a) >> 63
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i] = a + v2[i]
				if signA == uint64(v2[i])>>63 && signA != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = a + v2[i]
				if signA == uint64(v2[i])>>63 && signA != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	} else {
		b := v2[0]
		signB := uint64(b) >> 63
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i] = v1[i] + b
				if uint64(v1[i])>>63 == signB && uint64(v1[i])>>63 != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = v1[i] + b
				if uint64(v1[i])>>63 == signB && uint64(v1[i])>>63 != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	}
	return -1
}

// d64AddDiffScale handles Decimal64 addition when scales differ.
// Pre-scales constants or vectors once, then delegates to same-scale loop.

func d64AddDiffScale(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1, len2 := len(v1), len(v2)

	if len1 == 1 {
		if scale1 < scale2 {
			a, ok := d64MulPow10(v1[0], scale2-scale1)
			if !ok {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", v1[0].Format(scale2-scale1))
			}
			return d64AddSameScale([]types.Decimal64{a}, v2, rs, rsnull), nil
		}
		if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return -1, err
		}
		return d64AddSameScale(rs[:len2], []types.Decimal64{v1[0]}, rs, rsnull), nil
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b, ok := d64MulPow10(v2[0], scale1-scale2)
			if !ok {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", v2[0].Format(scale1-scale2))
			}
			return d64AddSameScale(v1, []types.Decimal64{b}, rs, rsnull), nil
		}
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d64AddSameScale(rs[:len1], []types.Decimal64{v2[0]}, rs, rsnull), nil
	}

	// vec-vec: scale the lower-scale one into rs, then same-scale add.
	if scale1 < scale2 {
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d64AddSameScale(rs[:len1], v2, rs, rsnull), nil
	}
	if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return -1, err
	}
	return d64AddSameScale(v1, rs[:len2], rs, rsnull), nil
}

// d64Sub is the batch kernel for Decimal64 subtraction.

func d64Sub(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	var idx int
	var err error
	if scale1 == scale2 {
		idx = d64SubSameScale(v1, v2, rs, rsnull)
	} else {
		idx, err = d64SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
		if err != nil {
			return err
		}
	}
	if idx >= 0 {
		a, b := operandsAt(v1, v2, idx)
		return moerr.NewInvalidInputNoCtxf("Decimal64 Sub overflow: %s-%s", a.Format(scale1), b.Format(scale2))
	}
	return nil
}

func d64SubSameScale(v1, v2, rs []types.Decimal64, rsnull *nulls.Nulls) int {
	bmp := rsnull.GetBitmap()
	len1, len2 := len(v1), len(v2)
	noNull := rsnull.IsEmpty()

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i] = v1[i] - v2[i]
				signX := uint64(v1[i]) >> 63
				if signX != uint64(v2[i])>>63 && signX != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = v1[i] - v2[i]
				signX := uint64(v1[i]) >> 63
				if signX != uint64(v2[i])>>63 && signX != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		signA := uint64(a) >> 63
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i] = a - v2[i]
				if signA != uint64(v2[i])>>63 && signA != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = a - v2[i]
				if signA != uint64(v2[i])>>63 && signA != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	} else {
		b := v2[0]
		signB := uint64(b) >> 63
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i] = v1[i] - b
				if uint64(v1[i])>>63 != signB && uint64(v1[i])>>63 != uint64(rs[i])>>63 {
					return i
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = v1[i] - b
				if uint64(v1[i])>>63 != signB && uint64(v1[i])>>63 != uint64(rs[i])>>63 {
					return i
				}
			}
		}
	}
	return -1
}

// d64SubDiffScale handles Decimal64 subtraction (v1 - v2) when scales differ.
// Pre-scales constants or vectors once, then delegates to same-scale loop.

func d64SubDiffScale(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) (int, error) {
	len1, len2 := len(v1), len(v2)

	if len1 == 1 {
		// const - vector
		if scale1 < scale2 {
			a, ok := d64MulPow10(v1[0], scale2-scale1)
			if !ok {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", v1[0].Format(scale2-scale1))
			}
			return d64SubSameScale([]types.Decimal64{a}, v2, rs, rsnull), nil
		}
		if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return -1, err
		}
		return d64SubSameScale([]types.Decimal64{v1[0]}, rs[:len2], rs, rsnull), nil
	}

	if len2 == 1 {
		// vector - const
		if scale2 < scale1 {
			b, ok := d64MulPow10(v2[0], scale1-scale2)
			if !ok {
				return -1, moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", v2[0].Format(scale1-scale2))
			}
			return d64SubSameScale(v1, []types.Decimal64{b}, rs, rsnull), nil
		}
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d64SubSameScale(rs[:len1], []types.Decimal64{v2[0]}, rs, rsnull), nil
	}

	// vec-vec: scale the lower-scale operand into rs.
	if scale1 < scale2 {
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return -1, err
		}
		return d64SubSameScale(rs[:len1], v2, rs, rsnull), nil
	}
	if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return -1, err
	}
	return d64SubSameScale(v1, rs[:len2], rs, rsnull), nil
}

// d64ScaleIntoRs scales vec[i] by 10^scaleDiff into rs[i], respecting nulls.
// scaleDiff is bounded by Decimal64 max scale (18), so Pow10[scaleDiff] is safe (Pow10 has 20 entries).

// ---- Decimal64 multiply ----

func d64Mul(v1, v2 []types.Decimal64, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	desiredScale := int32(12)
	if scale1 > desiredScale {
		desiredScale = scale1
	}
	if scale2 > desiredScale {
		desiredScale = scale2
	}
	if scale1+scale2 < desiredScale {
		desiredScale = scale1 + scale2
	}
	scaleAdj := desiredScale - scale1 - scale2

	n := len(rs)
	if len(v1) == 1 && len(v2) == 1 {
		r := d64MulInline(v1[0], v2[0])
		if scaleAdj != 0 {
			d128ScaleDown(&r, -scaleAdj)
		}
		for i := 0; i < n; i++ {
			rs[i] = r
		}
		return nil
	}

	if scaleAdj != 0 {
		return d64MulScaled(v1, v2, rs, scaleAdj, rsnull)
	}

	if len(v1) == 1 {
		x := v1[0]
		if rsnull.Any() {
			for i := 0; i < n; i++ {
				if !bmp.Contains(uint64(i)) {
					rs[i] = d64MulInline(x, v2[i])
				}
			}
		} else {
			for i := 0; i < n; i++ {
				rs[i] = d64MulInline(x, v2[i])
			}
		}
		return nil
	}
	if len(v2) == 1 {
		y := v2[0]
		if rsnull.Any() {
			for i := 0; i < n; i++ {
				if !bmp.Contains(uint64(i)) {
					rs[i] = d64MulInline(v1[i], y)
				}
			}
		} else {
			for i := 0; i < n; i++ {
				rs[i] = d64MulInline(v1[i], y)
			}
		}
		return nil
	}
	// Both vectors.
	if rsnull.Any() {
		for i := 0; i < n; i++ {
			if !bmp.Contains(uint64(i)) {
				rs[i] = d64MulInline(v1[i], v2[i])
			}
		}
	} else {
		for i := 0; i < n; i++ {
			rs[i] = d64MulInline(v1[i], v2[i])
		}
	}
	return nil
}

// d64MulScaled handles the rare case where scaleAdj != 0.

func d64MulScaled(v1, v2 []types.Decimal64, rs []types.Decimal128, scaleAdj int32, rsnull *nulls.Nulls) error {
	n := len(rs)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	for i := 0; i < n; i++ {
		if hasNull && bmp.Contains(uint64(i)) {
			continue
		}
		var a, b types.Decimal64
		if len(v1) == 1 {
			a = v1[0]
		} else {
			a = v1[i]
		}
		if len(v2) == 1 {
			b = v2[0]
		} else {
			b = v2[i]
		}
		r := d64MulInline(a, b)
		d128ScaleDown(&r, -scaleAdj)
		rs[i] = r
	}
	return nil
}

// d64MulInline multiplies two Decimal64 values, returning Decimal128.
// Single 64×64→128 bit multiply. No overflow possible. Fully branchless.

func d64MulInline(x, y types.Decimal64) types.Decimal128 {
	xi, yi := int64(x), int64(y)
	mx, my := xi>>63, yi>>63
	ax, ay := uint64((xi^mx)-mx), uint64((yi^my)-my)
	hi, lo := bits.Mul64(ax, ay)
	// Branchless conditional negate (two's complement: XOR + add carry).
	nm := uint64((xi ^ yi) >> 63) // 0xFF..FF if different signs
	lo ^= nm
	hi ^= nm
	var c uint64
	lo, c = bits.Add64(lo, 0, nm&1)
	hi, _ = bits.Add64(hi, 0, c)
	return types.Decimal128{B0_63: lo, B64_127: hi}
}

// ---- Decimal64 division ----

func d64DivKernel(shouldError bool) func(v1, v2 []types.Decimal64, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2 []types.Decimal64, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d64Div(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

func d64Div(v1, v2 []types.Decimal64, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	bmp := rsnull.GetBitmap()
	// Compute result scale once.
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	// D64 division always uses the inline fast path:
	// scaleAdj is always ≤ 19 (max scale1=18, so scaleAdj ≤ 18+18=36? no..
	// Actually scale = min(12, scale1+6, max(scale1)) and scaleAdj = scale - scale1 + scale2.
	// Worst case: scale2=18, scale1=0 → scaleAdj=12+18=30. So may exceed 19.
	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	len1, len2 := len(v1), len(v2)

	// Helper: sign-extend D64 to D128.
	d64toD128 := func(v types.Decimal64) types.Decimal128 {
		x := types.Decimal128{B0_63: uint64(v)}
		if v>>63 != 0 {
			x.B64_127 = ^uint64(0)
		}
		return x
	}

	if len1 == len2 {
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				x := d64toD128(v1[i])
				y := d64toD128(v2[i])
				if err := d128DivOneDispatch(x, y, &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				x := d64toD128(v1[i])
				y := d64toD128(v2[i])
				if err := d128DivOneDispatch(x, y, &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else if len1 == 1 {
		x := d64toD128(v1[0])
		if rsnull.IsEmpty() {
			for i := 0; i < len2; i++ {
				y := d64toD128(v2[i])
				if err := d128DivOneDispatch(x, y, &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				y := d64toD128(v2[i])
				if err := d128DivOneDispatch(x, y, &rs[i], scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	} else {
		y := d64toD128(v2[0])
		// Check constant divisor for zero once.
		if y.B0_63 == 0 && y.B64_127 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		// D64 values always fit in 64 bits of D128, so use fully-inlined path.
		signyU := y.B64_127 >> 63
		negMaskY := -signyU
		absY := types.Decimal128{B0_63: y.B0_63 ^ negMaskY, B64_127: y.B64_127 ^ negMaskY}
		var cY uint64
		absY.B0_63, cY = bits.Add64(absY.B0_63, signyU, 0)
		absY.B64_127, _ = bits.Add64(absY.B64_127, 0, cY)
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					x := d64toD128(v1[i])
					if !d128DivInline(x, absY64, signyU, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, y, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					x := d64toD128(v1[i])
					if !d128DivInline(x, absY64, signyU, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, y, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				x := d64toD128(v1[i])
				if err := d128DivOne(x, y, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				x := d64toD128(v1[i])
				if err := d128DivOne(x, y, &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ---- Decimal64 modulo ----

func d64ModKernel(shouldError bool) func(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	return func(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d64Mod(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

func d64Mod(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Same-scale fast path: Decimal64 is uint64 but represents signed values,
	// so cast to int64 for signed modulo (Go's % preserves dividend sign).
	if scale1 == scale2 {
		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				if v2[i] == 0 {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				rs[i] = types.Decimal64(int64(v1[i]) % int64(v2[i]))
			}
		} else if len1 == 1 {
			x := int64(v1[0])
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				if v2[i] == 0 {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				rs[i] = types.Decimal64(x % int64(v2[i]))
			}
		} else {
			if v2[0] == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				for i := 0; i < len1; i++ {
					rsnull.Add(uint64(i))
				}
				return nil
			}
			y := int64(v2[0])
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				rs[i] = types.Decimal64(int64(v1[i]) % y)
			}
		}
		return nil
	}

	// Diff-scale fast path: widen D64 to D128, branchless abs + inline scale + mod.
	diff := scale2 - scale1
	mask := diff >> 31
	scaleAx := diff &^ mask
	scaleAy := (-diff) & mask

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if v2[i] == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			sx := int64(v1[i])
			x128 := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
			sy := int64(v2[i])
			y128 := types.Decimal128{B0_63: uint64(sy), B64_127: uint64(sy >> 63)}
			r, ok := d128ModDiffScaleOne(x128, y128, scaleAx, scaleAy)
			if !ok {
				r64, _, err := v1[i].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
				rs[i] = r64
				continue
			}
			rs[i] = types.Decimal64(r.B0_63)
		}
	} else if len1 == 1 {
		sx := int64(v1[0])
		x128 := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if v2[i] == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			sy := int64(v2[i])
			y128 := types.Decimal128{B0_63: uint64(sy), B64_127: uint64(sy >> 63)}
			r, ok := d128ModDiffScaleOne(x128, y128, scaleAx, scaleAy)
			if !ok {
				r64, _, err := v1[0].Mod(v2[i], scale1, scale2)
				if err != nil {
					return err
				}
				rs[i] = r64
				continue
			}
			rs[i] = types.Decimal64(r.B0_63)
		}
	} else {
		if v2[0] == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		sy := int64(v2[0])
		y128 := types.Decimal128{B0_63: uint64(sy), B64_127: uint64(sy >> 63)}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			sx := int64(v1[i])
			x128 := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
			r, ok := d128ModDiffScaleOne(x128, y128, scaleAx, scaleAy)
			if !ok {
				r64, _, err := v1[i].Mod(v2[0], scale1, scale2)
				if err != nil {
					return err
				}
				rs[i] = r64
				continue
			}
			rs[i] = types.Decimal64(r.B0_63)
		}
	}
	return nil
}

// ---- Decimal64 integer division ----

// d128Add is the batch kernel for Decimal128 addition.
// Same-scale: direct bits.Add64 without overflow checking.
// Different-scale: scales operand(s) first, then uses bits.Add64 loop.
func d64IntDivKernel(proc *process.Process, selectList *FunctionSelectList) func(v1, v2 []types.Decimal64, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	shouldError := checkDivisionByZeroBehavior(proc, selectList)
	return func(v1, v2 []types.Decimal64, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
		return d64IntDiv(v1, v2, rs, scale1, scale2, rsnull, shouldError)
	}
}

// d128IntDivKernel returns a closure for Decimal128 → int64 integer division.
func d64IntDiv(v1, v2 []types.Decimal64, rs []int64, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	bmp := rsnull.GetBitmap()
	hasNull := !rsnull.IsEmpty()
	// Compute result scale (same as Decimal128.Div).
	scale := int32(12)
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	scaleAdj := scale - scale1 + scale2

	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	d64toD128 := func(v types.Decimal64) types.Decimal128 {
		x := types.Decimal128{B0_63: uint64(v)}
		if v>>63 != 0 {
			x.B64_127 = ^uint64(0)
		}
		return x
	}

	len1, len2 := len(v1), len(v2)
	for i := 0; i < max(len1, len2); i++ {
		if hasNull && bmp.Contains(uint64(i)) {
			continue
		}
		a, b := operandsAt(v1, v2, i)
		if b == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			rsnull.Add(uint64(i))
			continue
		}
		x, y := d64toD128(a), d64toD128(b)
		var divResult types.Decimal128
		if err := d128DivOneDispatch(x, y, &divResult, scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
			return err
		}
		// Truncate fractional part.
		if scale > 0 {
			d128ScaleDown(&divResult, scale)
		}
		var err error
		rs[i], err = decimal128ToInt64(divResult)
		if err != nil {
			return err
		}
	}
	return nil
}

// d128MulPow10 multiplies unsigned D128 x by 10^n in-place.
// Returns false on overflow. n must be >= 1.

// ---- Scale helpers ----

func d64ScaleIntoRs(vec, rs []types.Decimal64, n int, scaleDiff int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	scaleFactor := types.Decimal64(types.Pow10[scaleDiff])
	if rsnull.IsEmpty() {
		for i := 0; i < n; i++ {
			signBit := vec[i] >> 63
			mask := -signBit
			var err error
			rs[i], err = ((vec[i] ^ mask) + signBit).Mul64(scaleFactor)
			if err != nil {
				return err
			}
			rs[i] = (rs[i] ^ mask) + signBit
		}
	} else {
		for i := 0; i < n; i++ {
			if bmp.Contains(uint64(i)) {
				continue
			}
			signBit := vec[i] >> 63
			mask := -signBit
			var err error
			rs[i], err = ((vec[i] ^ mask) + signBit).Mul64(scaleFactor)
			if err != nil {
				return err
			}
			rs[i] = (rs[i] ^ mask) + signBit
		}
	}
	return nil
}

// d64MulPow10 multiplies signed D64 x by 10^n. n must be in [1, 18].
// Returns (result, true) on success, (x, false) on overflow.
func d64MulPow10(x types.Decimal64, n int32) (types.Decimal64, bool) {
	signBit := uint64(x) >> 63
	mask := -signBit
	abs := (uint64(x) ^ mask) + signBit
	hi, lo := bits.Mul64(abs, types.Pow10[n])
	if hi != 0 || lo>>63 != 0 {
		return x, false
	}
	return types.Decimal64((lo ^ mask) + signBit), true
}

// d128Mul1Limb multiplies unsigned D128 x by f in-place. Returns false on overflow.
func d128Mul1Limb(x *types.Decimal128, f uint64) bool {
	hi, lo := bits.Mul64(x.B0_63, f)
	if x.B64_127 != 0 {
		crossHi, crossLo := bits.Mul64(x.B64_127, f)
		if crossHi != 0 {
			return false
		}
		var c uint64
		hi, c = bits.Add64(hi, crossLo, 0)
		if c != 0 || hi>>63 != 0 {
			return false
		}
	}
	x.B0_63 = lo
	x.B64_127 = hi
	return true
}

// d128DivPow10Once divides unsigned D128 x by d in-place with round-half-up.
func d128DivPow10Once(x *types.Decimal128, d uint64) {
	var rem uint64
	x.B64_127, rem = bits.Div64(0, x.B64_127, d)
	x.B0_63, rem = bits.Div64(rem, x.B0_63, d)
	if rem*2 >= d || rem>>63 != 0 {
		x.B0_63++
		if x.B0_63 == 0 {
			x.B64_127++
		}
	}
}
