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
	"math"
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

func d128IsZero(d types.Decimal128) bool { return d.B0_63|d.B64_127 == 0 }
func d256IsZero(d types.Decimal256) bool {
	return d.B0_63|d.B64_127|d.B128_191|d.B192_255 == 0
}

// ---- Decimal128 add/sub ----

// d128Add is the batch kernel for Decimal128 addition. It dispatches to
// same-scale or diff-scale handlers based on the operand scales.
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

// scalePow10Factors returns pre-computed pow10 factors for scaling by 10^n.
// For n ≤ 19: returns (Pow10[n], false, 0). For n > 19: returns (Pow10[19], true, Pow10[n-19]).
func scalePow10Factors(n int32) (pow10a uint64, twoStep bool, pow10b uint64) {
	if n > 19 {
		pow10a = types.Pow10[19]
		pow10b = types.Pow10[n-19]
		twoStep = true
	} else {
		pow10a = types.Pow10[n]
	}
	return
}

func d128MulPow10(x *types.Decimal128, n int32) bool {
	if n <= 19 {
		return d128Mul1Limb(x, types.Pow10[n])
	}
	if n > 38 {
		return false
	}
	return d128Mul1Limb(x, types.Pow10[19]) && d128Mul1Limb(x, types.Pow10[n-19])
}

// d128ScaleUp scales a signed D128 up by 10^n. Branchless sign handling, in-place.
// Returns false on overflow (x may be corrupted; callers use original for error msg).
func d128ScaleUp(x *types.Decimal128, n int32) bool {
	sign := d128Abs(x)
	ok := d128MulPow10(x, n)
	d128Negate(x, sign)
	return ok
}

// d128ScaleUpPow10 scales a signed D128 by pre-computed pow10 factor(s).
// For n ≤ 19: pass (Pow10[n], false, 0). For n > 19: pass (Pow10[19], true, Pow10[n-19]).
// Eliminates d128ScaleUp→d128MulPow10 wrapper chain; d128Abs/d128Mul1Limb/d128Negate inline.
func d128ScaleUpPow10(x *types.Decimal128, pow10a uint64, twoStep bool, pow10b uint64) bool {
	sign := d128Abs(x)
	ok := d128Mul1Limb(x, pow10a)
	if ok && twoStep {
		ok = d128Mul1Limb(x, pow10b)
	}
	d128Negate(x, sign)
	return ok
}

// d128ScaleIntoRs scales vec[i] * 10^scaleDiff into rs[i], respecting nulls.
// Hoists pow10 factor outside the loop so d128Abs/d128Mul1Limb/d128Negate all inline.
func d128ScaleIntoRs(vec, rs []types.Decimal128, n int, scaleDiff int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	pow10a := types.Pow10[scaleDiff]
	var pow10b uint64
	twoStep := scaleDiff > 19
	if twoStep {
		pow10a = types.Pow10[19]
		pow10b = types.Pow10[scaleDiff-19]
	}
	// Prescan: when all values fit in int64 and scaleDiff ≤ 19, replace
	// d128Abs+d128Mul1Limb+d128Negate with branchless abs → bits.Mul64 → d128Negate.
	// Saves the B64_127 branch in d128Mul1Limb and the copy+abs overhead.
	if !twoStep && d128AllFitInt64(vec, n) {
		if rsnull.IsEmpty() {
			for i := 0; i < n; i++ {
				s := vec[i].B64_127 >> 63
				a := (vec[i].B0_63 ^ (-s)) + s
				h, l := bits.Mul64(a, pow10a)
				rs[i] = types.Decimal128{B0_63: l, B64_127: h}
				d128Negate(&rs[i], s)
			}
		} else {
			for i := 0; i < n; i++ {
				rs[i] = vec[i]
				if bmp.Contains(uint64(i)) {
					continue
				}
				s := vec[i].B64_127 >> 63
				a := (vec[i].B0_63 ^ (-s)) + s
				h, l := bits.Mul64(a, pow10a)
				rs[i] = types.Decimal128{B0_63: l, B64_127: h}
				d128Negate(&rs[i], s)
			}
		}
		return nil
	}
	if rsnull.IsEmpty() {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			sign := d128Abs(&rs[i])
			if !d128Mul1Limb(&rs[i], pow10a) || (twoStep && !d128Mul1Limb(&rs[i], pow10b)) {
				return moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", vec[i].Format(scaleDiff))
			}
			d128Negate(&rs[i], sign)
		}
	} else {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			if bmp.Contains(uint64(i)) {
				continue
			}
			sign := d128Abs(&rs[i])
			if !d128Mul1Limb(&rs[i], pow10a) || (twoStep && !d128Mul1Limb(&rs[i], pow10b)) {
				return moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow: %s", vec[i].Format(scaleDiff))
			}
			d128Negate(&rs[i], sign)
		}
	}
	return nil
}

// ---- Decimal128 multiply ----

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

// d128ScaleDown divides a signed D128 by 10^n with round-half-up. Branchless, in-place.
func d128ScaleDown(x *types.Decimal128, n int32) {
	sign := d128Abs(x)
	d128DivPow10(x, n)
	d128Negate(x, sign)
}

// d128ScaleDownPow10 divides signed D128 by pre-computed pow10 factor(s).
// d128Abs/d128DivPow10Once/d128Negate all inline (costs 48, 66, 38).
func d128ScaleDownPow10(x *types.Decimal128, pow10a uint64, twoStep bool, pow10b uint64) {
	sign := d128Abs(x)
	d128DivPow10Once(x, pow10a)
	if twoStep {
		d128DivPow10Once(x, pow10b)
	}
	d128Negate(x, sign)
}

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
	hasNull := !rsnull.IsEmpty()
	needScale := scaleAdj != 0
	negScaleAdj := -scaleAdj

	// Prescan: if all values in both arrays fit in int64, use inline 64-bit multiply
	// loop — avoids the non-inlinable d128MulInline call (cost 941) per element.
	if d128AllFitInt64(v1, len1) && d128AllFitInt64(v2, len2) {
		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := (v1[i].B64_127 ^ v2[i].B64_127) >> 63
				xi, yi := int64(v1[i].B0_63), int64(v2[i].B0_63)
				ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
				ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		} else if len1 == 1 {
			xi := int64(v1[0].B0_63)
			ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
			signx := v1[0].B64_127 >> 63
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := signx ^ (v2[i].B64_127 >> 63)
				yi := int64(v2[i].B0_63)
				ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		} else {
			yi := int64(v2[0].B0_63)
			ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
			signy := v2[0].B64_127 >> 63
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := (v1[i].B64_127 >> 63) ^ signy
				xi := int64(v1[i].B0_63)
				ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		}
		return nil
	}

	// Tier 2: if all |values| fit in uint64 (but not int64), inline multiply
	// using branchless abs on B0_63. Avoids d128MulInline call (cost 941).
	if d128AllAbsFit64(v1, len1) && d128AllAbsFit64(v2, len2) {
		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				signx := v1[i].B64_127 >> 63
				signy := v2[i].B64_127 >> 63
				neg := signx ^ signy
				ax := (v1[i].B0_63 ^ (-signx)) + signx
				ay := (v2[i].B0_63 ^ (-signy)) + signy
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		} else if len1 == 1 {
			signx := v1[0].B64_127 >> 63
			ax := (v1[0].B0_63 ^ (-signx)) + signx
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				signy := v2[i].B64_127 >> 63
				neg := signx ^ signy
				ay := (v2[i].B0_63 ^ (-signy)) + signy
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		} else {
			signy := v2[0].B64_127 >> 63
			ay := (v2[0].B0_63 ^ (-signy)) + signy
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				signx := v1[i].B64_127 >> 63
				neg := signx ^ signy
				ax := (v1[i].B0_63 ^ (-signx)) + signx
				hi, lo := bits.Mul64(ax, ay)
				rs[i].B0_63 = lo
				rs[i].B64_127 = hi
				if needScale {
					d128DivPow10(&rs[i], negScaleAdj)
				}
				d128Negate(&rs[i], neg)
			}
		}
		return nil
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d128MulInline(&v1[i], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d128MulInline(&v1[0], &v2[i], &rs[i], scaleAdj, scale1, scale2); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d128MulInline(&v1[i], &v2[0], &rs[i], scaleAdj, scale1, scale2); err != nil {
				return err
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
		// vec / vec: prescan divisors for 64-bit fit to skip dispatch overhead.
		if canInline && d128AllAbsFit64(v2, len2) {
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					y := v2[i]
					if d128IsZero(y) {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signy := d128Abs(&y)
					if !d128DivInline(v1[i], y.B0_63, signy, scaleFactor, &rs[i]) {
						if err := d128DivOne(v1[i], v2[i], &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					y := v2[i]
					if d128IsZero(y) {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signy := d128Abs(&y)
					if !d128DivInline(v1[i], y.B0_63, signy, scaleFactor, &rs[i]) {
						if err := d128DivOne(v1[i], v2[i], &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
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
		// const / vec: prescan divisors for 64-bit fit.
		a := v1[0]
		if canInline && d128AllAbsFit64(v2, len2) {
			if rsnull.IsEmpty() {
				for i := 0; i < len2; i++ {
					y := v2[i]
					if d128IsZero(y) {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signy := d128Abs(&y)
					if !d128DivInline(a, y.B0_63, signy, scaleFactor, &rs[i]) {
						if err := d128DivOne(a, v2[i], &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len2; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					y := v2[i]
					if d128IsZero(y) {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signy := d128Abs(&y)
					if !d128DivInline(a, y.B0_63, signy, scaleFactor, &rs[i]) {
						if err := d128DivOne(a, v2[i], &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
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
		if d128IsZero(b) {
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
	if d128IsZero(y) {
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
	if d128IsZero(y) {
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
		var carry uint64
		zHi, carry = bits.Add64(zHi, crossLo, 0)
		if crossHi|carry|(zHi>>63) != 0 {
			return false // overflow (result must be positive D128)
		}
	} else if zHi>>63 != 0 {
		return false // overflow (result must be positive D128)
	}

	// Inline Div128 for 64-bit divisor: two bits.Div64 calls.
	var rem uint64
	zHi, rem = bits.Div64(0, zHi, absY64)
	zLo, rem = bits.Div64(rem, zLo, absY64)
	// Branchless round half-up: round = 1 iff rem >= ceil(absY64/2).
	_, borrow := bits.Sub64(rem, (absY64+1)>>1, 0)
	round := 1 - borrow
	zLo, c2 := bits.Add64(zLo, round, 0)
	zHi += c2

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

	// Diff-scale fast path: hoist pow10 factors before the loop so d128Mul1Limb
	// inlines directly (eliminates d128MulPow10 wrapper call per element).
	diff := scale2 - scale1
	scaleX := diff > 0
	mask := diff >> 31
	scaleDiff := (diff ^ mask) - mask
	pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)

	// Dispatch once: pick the scaleX or scaleY variant to eliminate
	// the scaleX branch per element.
	modFn := d128ModDiffScaleYPow10
	if scaleX {
		modFn = d128ModDiffScaleXPow10
	}

	// Prescan: when we scale x (not y), check if all |v2| fit in 64 bits.
	// If so, ay.B64_127==0 is guaranteed after abs → use inline 64-bit mod.
	canInline64 := scaleX && d128AllAbsFit64(v2, len2)

	if len1 == len2 {
		if canInline64 {
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
				signy := v2[i].B64_127 >> 63
				absY64 := (v2[i].B0_63 ^ (-signy)) + signy
				ax := v1[i]
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[i].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				rs[i] = r
			}
			return nil
		}
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
			r, ok := modFn(v1[i], v2[i], pow10a, twoStep, pow10b)
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
		if canInline64 {
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
				signy := v2[i].B64_127 >> 63
				absY64 := (v2[i].B0_63 ^ (-signy)) + signy
				ax := v1[0]
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[0].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				rs[i] = r
			}
			return nil
		}
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
			r, ok := modFn(v1[0], v2[i], pow10a, twoStep, pow10b)
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
		if canInline64 {
			signy := v2[0].B64_127 >> 63
			absY64 := (v2[0].B0_63 ^ (-signy)) + signy
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				ax := v1[i]
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[i].Mod(v2[0], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				rs[i] = r
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, ok := modFn(v1[i], v2[0], pow10a, twoStep, pow10b)
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

// d64AllFitInt32 checks whether every D64 element fits in int32 range [-2^31, 2^31-1].
// Uses OR-reduce: shift the offset value right by 32; non-zero means out of range.
func d64AllFitInt32(vs []types.Decimal64) bool {
	var or uint64
	for _, v := range vs {
		or |= (uint64(v) + 0x80000000) >> 32
	}
	return or == 0
}

// d256AllFitInt64 checks whether every D256 element fits in int64 range.
// True when B64_127, B128_191, B192_255 are all sign extension of B0_63 bit 63.
func d256AllFitInt64(vs []types.Decimal256, n int) bool {
	for i := 0; i < n; i++ {
		signExt := uint64(int64(vs[i].B0_63) >> 63)
		if vs[i].B64_127^signExt|vs[i].B128_191^signExt|vs[i].B192_255^signExt != 0 {
			return false
		}
	}
	return true
}

// d256AllFitInt32 checks whether every D256 element fits in int32 range [-2^31, 2^31-1].
// Uses OR-reduce: upper limbs must be sign extension AND B0_63 must fit in int32.
func d256AllFitInt32(vs []types.Decimal256, n int) bool {
	var or uint64
	for i := 0; i < n; i++ {
		signExt := uint64(int64(vs[i].B0_63) >> 63)
		or |= vs[i].B64_127 ^ signExt | vs[i].B128_191 ^ signExt | vs[i].B192_255 ^ signExt
		or |= (vs[i].B0_63 + 0x80000000) >> 32
	}
	return or == 0
}

// d128AllFitInt64 checks whether every element fits in int64 range [-2^63, 2^63-1].
// True when B64_127 == sign extension of B0_63 for every element.
func d128AllFitInt64(vs []types.Decimal128, n int) bool {
	for i := 0; i < n; i++ {
		if uint64(int64(vs[i].B0_63)>>63)^vs[i].B64_127 != 0 {
			return false
		}
	}
	return true
}

// d128AllAbsFit64 checks whether |v[i]| fits in 64 bits for all elements.
// True when B64_127 is 0 (positive small) or ^0 (negative small) for every element,
// excluding the degenerate {B0_63=0, B64_127=^0} = -2^64 whose abs is 2^64 > uint64 max.
func d128AllAbsFit64(vs []types.Decimal128, n int) bool {
	for i := 0; i < n; i++ {
		h := vs[i].B64_127
		if h+1 > 1 {
			return false // h is neither 0 nor ^0
		}
		if h>>63 != 0 && vs[i].B0_63 == 0 {
			return false // -2^64: abs doesn't fit
		}
	}
	return true
}

// d128ModSameScale handles same-scale D128 modulo with inline 128-bit mod.
func d128ModSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls, shouldError bool,
	len1, len2 int, hasNull bool, bmp *bitmap.Bitmap) error {

	// Pre-scan: if all |divisors| fit in 64 bits, use loop with no ay.B64_127 branch
	// and no d128ModOne function call overhead.
	if d128AllAbsFit64(v2, len2) {
		return d128ModSameScale64(v1, v2, rs, rsnull, shouldError, len1, len2, hasNull, bmp)
	}

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

// d128ModSameScale64 is the specialized same-scale loop for when all |divisors| fit in 64 bits.
// Eliminates the d128ModOne function call and the ay.B64_127 == 0 branch per element.
func d128ModSameScale64(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls, shouldError bool,
	len1, len2 int, hasNull bool, bmp *bitmap.Bitmap) error {

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			ay := v2[i]
			d128Abs(&ay)
			if ay.B0_63 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			ax := v1[i]
			signx := d128Abs(&ax)
			_, rhi := bits.Div64(0, ax.B64_127, ay.B0_63)
			_, rlo := bits.Div64(rhi, ax.B0_63, ay.B0_63)
			rs[i] = types.Decimal128{B0_63: rlo}
			d128Negate(&rs[i], signx)
		}
	} else if len1 == 1 {
		ax0 := v1[0]
		signx0 := d128Abs(&ax0)
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			ay := v2[i]
			d128Abs(&ay)
			if ay.B0_63 == 0 {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			_, rhi := bits.Div64(0, ax0.B64_127, ay.B0_63)
			_, rlo := bits.Div64(rhi, ax0.B0_63, ay.B0_63)
			rs[i] = types.Decimal128{B0_63: rlo}
			d128Negate(&rs[i], signx0)
		}
	} else {
		ay := v2[0]
		d128Abs(&ay)
		if ay.B0_63 == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		d := ay.B0_63
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			ax := v1[i]
			signx := d128Abs(&ax)
			_, rhi := bits.Div64(0, ax.B64_127, d)
			_, rlo := bits.Div64(rhi, ax.B0_63, d)
			rs[i] = types.Decimal128{B0_63: rlo}
			d128Negate(&rs[i], signx)
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

// d128ModDiffScaleXPow10 computes x mod y, scaling x up by pre-computed pow10 factors.
// d128Abs, d128Mul1Limb, d128Negate all inline inside this function.
func d128ModDiffScaleXPow10(x, y types.Decimal128, pow10a uint64, twoStep bool, pow10b uint64) (types.Decimal128, bool) {
	ax := x
	signx := d128Abs(&ax)
	ay := y
	d128Abs(&ay)

	if !d128Mul1Limb(&ax, pow10a) {
		return types.Decimal128{}, false
	}
	if twoStep && !d128Mul1Limb(&ax, pow10b) {
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

// d128ModDiffScaleYPow10 computes x mod y, scaling y up by pre-computed pow10 factors.
func d128ModDiffScaleYPow10(x, y types.Decimal128, pow10a uint64, twoStep bool, pow10b uint64) (types.Decimal128, bool) {
	ax := x
	signx := d128Abs(&ax)
	ay := y
	d128Abs(&ay)

	if !d128Mul1Limb(&ay, pow10a) {
		return types.Decimal128{}, false
	}
	if twoStep && !d128Mul1Limb(&ay, pow10b) {
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

// d128IntDivKernel returns a closure for Decimal128 → int64 integer division.
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

	// Hoist pow10 factors for scale-down outside the loop.
	var sdPow10a uint64
	var sdTwoStep bool
	var sdPow10b uint64
	if scale > 0 {
		sdPow10a, sdTwoStep, sdPow10b = scalePow10Factors(scale)
	}

	// Inline IntDiv element: divide, scale-down, convert to int64.
	intDivElem := func(divResult types.Decimal128) (int64, error) {
		if scale > 0 {
			d128ScaleDownPow10(&divResult, sdPow10a, sdTwoStep, sdPow10b)
		}
		return decimal128ToInt64(divResult)
	}

	len1, len2 := len(v1), len(v2)

	if len1 == len2 {
		if canInline && d128AllAbsFit64(v2, len2) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				y := v2[i]
				if d128IsZero(y) {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				signy := d128Abs(&y)
				var divResult types.Decimal128
				if !d128DivInline(v1[i], y.B0_63, signy, scaleFactor, &divResult) {
					if err := d128DivOne(v1[i], v2[i], &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a, b := operandsAt(v1, v2, i)
			if d128IsZero(b) {
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
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		if canInline && d128AllAbsFit64(v2, len2) {
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				y := v2[i]
				if d128IsZero(y) {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				signy := d128Abs(&y)
				var divResult types.Decimal128
				if !d128DivInline(a, y.B0_63, signy, scaleFactor, &divResult) {
					if err := d128DivOne(a, v2[i], &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
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
			var divResult types.Decimal128
			if err := d128DivOneDispatch(a, v2[i], &divResult, scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
		}
	} else {
		b := v2[0]
		if d128IsZero(b) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		// vec/const: precompute divisor properties for inline path.
		signyU := b.B64_127 >> 63
		negMaskY := -signyU
		absY := types.Decimal128{B0_63: b.B0_63 ^ negMaskY, B64_127: b.B64_127 ^ negMaskY}
		var cY uint64
		absY.B0_63, cY = bits.Add64(absY.B0_63, signyU, 0)
		absY.B64_127, _ = bits.Add64(absY.B64_127, 0, cY)
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				var divResult types.Decimal128
				if !d128DivInline(v1[i], absY64, signyU, scaleFactor, &divResult) {
					if err := d128DivOne(v1[i], b, &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			var divResult types.Decimal128
			if err := d128DivOne(v1[i], b, &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
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
	return (h3 | c | (x.B192_255 >> 63)) == 0
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

// d256ScaleUpPow10 scales a signed D256 by pre-computed pow10 factor(s).
// Eliminates d256ScaleUp→d256MulPow10 wrapper chain; d256Abs/d256Negate inline.
func d256ScaleUpPow10(x *types.Decimal256, pow10a uint64, twoStep bool, pow10b uint64) bool {
	sign := d256Abs(x)
	ok := d256Mul1Limb(x, pow10a)
	if ok && twoStep {
		ok = d256Mul1Limb(x, pow10b)
	}
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

// d256ScaleDownPow10 divides signed D256 by pre-computed pow10 factor(s).
// Eliminates d256ScaleDown→d256DivPow10 wrapper chain; d256Abs/d256Negate inline.
func d256ScaleDownPow10(x *types.Decimal256, pow10a uint64, twoStep bool, pow10b uint64) {
	sign := d256Abs(x)
	d256DivPow10Once(x, pow10a)
	if twoStep {
		d256DivPow10Once(x, pow10b)
	}
	d256Negate(x, sign)
}

// d256Add is the batch kernel for Decimal256 addition.
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

// d256AddSameScale adds two Decimal256 vectors with the same scale, inlining
// the 4-limb carry chain and overflow check to avoid the non-inlinable Add256 method.
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v2, len2) {
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sb := v2[i].B192_255 >> 63
				ab := (v2[i].B0_63 ^ (-sb)) + sb
				bh, bl := bits.Mul64(ab, pow10a)
				b := types.Decimal256{B0_63: bl, B64_127: bh}
				d256Negate(&b, sb)
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
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUpPow10(&b, pow10a, twoStep, pow10b) {
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v1, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sa := v1[i].B192_255 >> 63
				aa := (v1[i].B0_63 ^ (-sa)) + sa
				ah, al := bits.Mul64(aa, pow10a)
				a := types.Decimal256{B0_63: al, B64_127: ah}
				d256Negate(&a, sa)
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
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUpPow10(&a, pow10a, twoStep, pow10b) {
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

	// len1 == len2: fuse scale + add per element.
	// Prescan: when all scaled values fit in int64 and scaleDiff ≤ 19,
	// inline the scale-up as a single bits.Mul64 instead of calling
	// d256ScaleUpPow10 (cost 287) which does 3 wasted Mul64 on zero limbs.
	if scale1 < scale2 {
		scaleDiff := scale2 - scale1
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v1, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sa := v1[i].B192_255 >> 63
				aa := (v1[i].B0_63 ^ (-sa)) + sa
				ah, al := bits.Mul64(aa, pow10a)
				a := types.Decimal256{B0_63: al, B64_127: ah}
				d256Negate(&a, sa)
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
			return -1, nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUpPow10(&a, pow10a, twoStep, pow10b) {
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v2, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sb := v2[i].B192_255 >> 63
				ab := (v2[i].B0_63 ^ (-sb)) + sb
				bh, bl := bits.Mul64(ab, pow10a)
				b := types.Decimal256{B0_63: bl, B64_127: bh}
				d256Negate(&b, sb)
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
			return -1, nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUpPow10(&b, pow10a, twoStep, pow10b) {
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

// d256SubSameScale subtracts two Decimal256 vectors with the same scale, inlining
// the 4-limb borrow chain and overflow check to avoid the non-inlinable Sub256 method.
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v2, len2) {
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sb := v2[i].B192_255 >> 63
				ab := (v2[i].B0_63 ^ (-sb)) + sb
				bh, bl := bits.Mul64(ab, pow10a)
				b := types.Decimal256{B0_63: bl, B64_127: bh}
				d256Negate(&b, sb)
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
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUpPow10(&b, pow10a, twoStep, pow10b) {
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v1, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sa := v1[i].B192_255 >> 63
				aa := (v1[i].B0_63 ^ (-sa)) + sa
				ah, al := bits.Mul64(aa, pow10a)
				a := types.Decimal256{B0_63: al, B64_127: ah}
				d256Negate(&a, sa)
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
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUpPow10(&a, pow10a, twoStep, pow10b) {
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

	// len1 == len2: fuse scale + sub per element.
	// Prescan: when all scaled values fit in int64 and scaleDiff ≤ 19,
	// inline the scale-up as a single bits.Mul64 instead of calling
	// d256ScaleUpPow10 (cost 287) which does 3 wasted Mul64 on zero limbs.
	if scale1 < scale2 {
		scaleDiff := scale2 - scale1
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v1, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sa := v1[i].B192_255 >> 63
				aa := (v1[i].B0_63 ^ (-sa)) + sa
				ah, al := bits.Mul64(aa, pow10a)
				a := types.Decimal256{B0_63: al, B64_127: ah}
				d256Negate(&a, sa)
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
			return -1, nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			a := v1[i]
			if !d256ScaleUpPow10(&a, pow10a, twoStep, pow10b) {
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
		pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)
		if !twoStep && d256AllFitInt64(v2, len1) {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sb := v2[i].B192_255 >> 63
				ab := (v2[i].B0_63 ^ (-sb)) + sb
				bh, bl := bits.Mul64(ab, pow10a)
				b := types.Decimal256{B0_63: bl, B64_127: bh}
				d256Negate(&b, sb)
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
			return -1, nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			b := v2[i]
			if !d256ScaleUpPow10(&b, pow10a, twoStep, pow10b) {
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

	// Tier 1 prescan: if all values fit in int32, products fit in int64 →
	// single 64-bit multiply + single 64÷64 division per scale step.
	// Saves one DIV instruction per element vs the int64 tier.
	if d256AllFitInt32(v1, len1) && d256AllFitInt32(v2, len2) {
		needScale := scaleAdj < 0
		negScaleAdj := -scaleAdj
		pow10a, twoStep, pow10b := scalePow10Factors(negScaleAdj)
		d := pow10a
		halfD := (d + 1) >> 1
		halfB := (pow10b + 1) >> 1
		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := int64(v1[i].B0_63) * int64(v2[i].B0_63)
				if needScale {
					sign := uint64(p >> 63)
					abs := uint64((p ^ int64(sign)) - int64(sign))
					q, rem := bits.Div64(0, abs, d)
					_, borrow := bits.Sub64(rem, halfD, 0)
					q += 1 - borrow
					if twoStep {
						q, rem = bits.Div64(0, q, pow10b)
						_, borrow = bits.Sub64(rem, halfB, 0)
						q += 1 - borrow
					}
					signedQ := int64((q ^ sign) - sign)
					se := uint64(signedQ >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(signedQ), B64_127: se, B128_191: se, B192_255: se}
				} else {
					se := uint64(p >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(p), B64_127: se, B128_191: se, B192_255: se}
				}
			}
		} else if len1 == 1 {
			x := int64(v1[0].B0_63)
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := x * int64(v2[i].B0_63)
				if needScale {
					sign := uint64(p >> 63)
					abs := uint64((p ^ int64(sign)) - int64(sign))
					q, rem := bits.Div64(0, abs, d)
					_, borrow := bits.Sub64(rem, halfD, 0)
					q += 1 - borrow
					if twoStep {
						q, rem = bits.Div64(0, q, pow10b)
						_, borrow = bits.Sub64(rem, halfB, 0)
						q += 1 - borrow
					}
					signedQ := int64((q ^ sign) - sign)
					se := uint64(signedQ >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(signedQ), B64_127: se, B128_191: se, B192_255: se}
				} else {
					se := uint64(p >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(p), B64_127: se, B128_191: se, B192_255: se}
				}
			}
		} else {
			y := int64(v2[0].B0_63)
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := int64(v1[i].B0_63) * y
				if needScale {
					sign := uint64(p >> 63)
					abs := uint64((p ^ int64(sign)) - int64(sign))
					q, rem := bits.Div64(0, abs, d)
					_, borrow := bits.Sub64(rem, halfD, 0)
					q += 1 - borrow
					if twoStep {
						q, rem = bits.Div64(0, q, pow10b)
						_, borrow = bits.Sub64(rem, halfB, 0)
						q += 1 - borrow
					}
					signedQ := int64((q ^ sign) - sign)
					se := uint64(signedQ >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(signedQ), B64_127: se, B128_191: se, B192_255: se}
				} else {
					se := uint64(p >> 63)
					rs[i] = types.Decimal256{B0_63: uint64(p), B64_127: se, B128_191: se, B192_255: se}
				}
			}
		}
		return nil
	}

	// Tier 2 prescan: if all values fit in int64, use inline bits.Mul64 — skips the
	// 10-multiply schoolbook in d256MulInline (cost >> 80, never inlines).
	// When scaleAdj < 0, fuse scale-down into the loop (d128DivPow10 on
	// the unsigned 128-bit product) to avoid a second pass over the array.
	if d256AllFitInt64(v1, len1) && d256AllFitInt64(v2, len2) {
		needScale := scaleAdj < 0
		negScaleAdj := -scaleAdj
		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := (v1[i].B192_255 ^ v2[i].B192_255) >> 63
				xi, yi := int64(v1[i].B0_63), int64(v2[i].B0_63)
				ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
				ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				r := types.Decimal128{B0_63: lo, B64_127: hi}
				if needScale {
					d128DivPow10(&r, negScaleAdj)
				}
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127}
				d256Negate(&rs[i], neg)
			}
		} else if len1 == 1 {
			xi := int64(v1[0].B0_63)
			ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
			signx := v1[0].B192_255 >> 63
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := signx ^ (v2[i].B192_255 >> 63)
				yi := int64(v2[i].B0_63)
				ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				r := types.Decimal128{B0_63: lo, B64_127: hi}
				if needScale {
					d128DivPow10(&r, negScaleAdj)
				}
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127}
				d256Negate(&rs[i], neg)
			}
		} else {
			yi := int64(v2[0].B0_63)
			ay := uint64((yi ^ (yi >> 63)) - (yi >> 63))
			signy := v2[0].B192_255 >> 63
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				neg := (v1[i].B192_255 >> 63) ^ signy
				xi := int64(v1[i].B0_63)
				ax := uint64((xi ^ (xi >> 63)) - (xi >> 63))
				hi, lo := bits.Mul64(ax, ay)
				r := types.Decimal128{B0_63: lo, B64_127: hi}
				if needScale {
					d128DivPow10(&r, negScaleAdj)
				}
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127}
				d256Negate(&rs[i], neg)
			}
		}
		return nil
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
	// Batch-level scale-down for slow path only.
	if scaleAdj < 0 {
		pow10a, twoStep, pow10b := scalePow10Factors(-scaleAdj)
		for i := range rs {
			d256ScaleDownPow10(&rs[i], pow10a, twoStep, pow10b)
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

	// Pre-compute scale factor for inline fast path.
	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	// Branchless prescan: check if all D256 divisors (narrowed to D128) fit in 64 bits.
	d256DivFit64 := func(vs []types.Decimal256) bool {
		var acc uint64
		for i := range vs {
			acc |= vs[i].B64_127 + 1
		}
		return acc <= 1
	}

	if len1 == len2 {
		if canInline && d256DivFit64(v2) {
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
				signy := d128Abs(&y)
				var r128 types.Decimal128
				if !d128DivInline(d256toD128(v1[i]), y.B0_63, signy, scaleFactor, &r128) {
					if err := d128DivOne(d256toD128(v1[i]), d256toD128(v2[i]), &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				rs[i] = d128toD256(r128)
			}
			return nil
		}
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
			var r128 types.Decimal128
			if err := d128DivOne(d256toD128(v1[i]), y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			rs[i] = d128toD256(r128)
		}
	} else if len1 == 1 {
		x := d256toD128(v1[0])
		if canInline && d256DivFit64(v2) {
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
				signy := d128Abs(&y)
				var r128 types.Decimal128
				if !d128DivInline(x, y.B0_63, signy, scaleFactor, &r128) {
					if err := d128DivOne(x, d256toD128(v2[i]), &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				rs[i] = d128toD256(r128)
			}
			return nil
		}
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
			var r128 types.Decimal128
			if err := d128DivOne(x, y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			rs[i] = d128toD256(r128)
		}
	} else {
		y := d256toD128(v2[0])
		if d128IsZero(y) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		// vec/const: pre-compute divisor properties for inline path.
		if canInline {
			absY := y
			signyU := d128Abs(&absY)
			if absY.B64_127 == 0 {
				absY64 := absY.B0_63
				for i := 0; i < len1; i++ {
					if hasNull && bmp.Contains(uint64(i)) {
						continue
					}
					var r128 types.Decimal128
					if !d128DivInline(d256toD128(v1[i]), absY64, signyU, scaleFactor, &r128) {
						if err := d128DivOne(d256toD128(v1[i]), y, &r128, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
					rs[i] = d128toD256(r128)
				}
				return nil
			}
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
	scaleX := diff > 0
	dMask := diff >> 31
	scaleDiff := (diff ^ dMask) - dMask
	pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)

	modFn := d128ModDiffScaleYPow10
	if scaleX {
		modFn = d128ModDiffScaleXPow10
	}

	// Prescan: when y is not scaled up (scaleX scales x, or same-scale scales by 1),
	// check if all |v2| narrowed to D128 fit in 64 bits for inline fast 64-bit mod.
	canInline64 := false
	if scaleX || scaleDiff == 0 {
		var acc uint64
		for i := 0; i < len2; i++ {
			acc |= v2[i].B64_127 + 1
		}
		canInline64 = acc <= 1
	}

	if len1 == len2 {
		if canInline64 {
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				if v2[i].B0_63|v2[i].B64_127 == 0 {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				signy := v2[i].B64_127 >> 63
				absY64 := (v2[i].B0_63 ^ (-signy)) + signy
				ax := types.Decimal128{B0_63: v1[i].B0_63, B64_127: v1[i].B64_127}
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[i].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				signExt := ^uint64(0) * (r.B64_127 >> 63)
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127, B128_191: signExt, B192_255: signExt}
			}
			return nil
		}
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
			r, ok := modFn(d256toD128(v1[i]), y, pow10a, twoStep, pow10b)
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
		if canInline64 {
			x := types.Decimal128{B0_63: v1[0].B0_63, B64_127: v1[0].B64_127}
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				if v2[i].B0_63|v2[i].B64_127 == 0 {
					if shouldError {
						return moerr.NewDivByZeroNoCtx()
					}
					rsnull.Add(uint64(i))
					continue
				}
				signy := v2[i].B64_127 >> 63
				absY64 := (v2[i].B0_63 ^ (-signy)) + signy
				ax := x
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[0].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				signExt := ^uint64(0) * (r.B64_127 >> 63)
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127, B128_191: signExt, B192_255: signExt}
			}
			return nil
		}
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
			r, ok := modFn(x, y, pow10a, twoStep, pow10b)
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
		if d256IsZero(v2[0]) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			for i := 0; i < len1; i++ {
				rsnull.Add(uint64(i))
			}
			return nil
		}
		if canInline64 {
			signy := v2[0].B64_127 >> 63
			absY64 := (v2[0].B0_63 ^ (-signy)) + signy
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				ax := types.Decimal128{B0_63: v1[i].B0_63, B64_127: v1[i].B64_127}
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					var err error
					rs[i], _, err = v1[i].Mod(v2[0], scale1, scale2)
					if err != nil {
						return err
					}
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				signExt := ^uint64(0) * (r.B64_127 >> 63)
				rs[i] = types.Decimal256{B0_63: r.B0_63, B64_127: r.B64_127, B128_191: signExt, B192_255: signExt}
			}
			return nil
		}
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
			r, ok := modFn(d256toD128(v1[i]), y, pow10a, twoStep, pow10b)
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

// d256IntDivKernel returns a closure for Decimal256 → int64 integer division.
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

	// Hoist pow10 factors for scale-down.
	var sdPow10a uint64
	var sdTwoStep bool
	var sdPow10b uint64
	if scale > 0 {
		sdPow10a, sdTwoStep, sdPow10b = scalePow10Factors(scale)
	}

	// Pre-compute inline fast path parameters.
	var scaleFactor uint64
	canInline := scaleAdj >= 0 && scaleAdj <= 19
	if canInline {
		scaleFactor = types.Pow10[scaleAdj]
	}

	intDivElem := func(divResult types.Decimal128) (int64, error) {
		if scale > 0 {
			d128ScaleDownPow10(&divResult, sdPow10a, sdTwoStep, sdPow10b)
		}
		return decimal128ToInt64(divResult)
	}

	// Branchless prescan: check if all D256 divisors (narrowed to D128) fit in 64 bits.
	d256DivFit64 := func(vs []types.Decimal256, n int) bool {
		var acc uint64
		for i := 0; i < n; i++ {
			acc |= vs[i].B64_127 + 1
		}
		return acc <= 1
	}

	divOneFallback := func(x, y types.Decimal128, dst *int64, idx uint64) error {
		var divResult types.Decimal128
		if err := d128DivOne(x, y, &divResult, scaleAdj, rsnull, idx, shouldError, scale1, scale2); err != nil {
			return err
		}
		var err error
		*dst, err = intDivElem(divResult)
		return err
	}

	if len1 == len2 {
		if canInline && d256DivFit64(v2, len2) {
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
				signy := d128Abs(&y)
				var divResult types.Decimal128
				if !d128DivInline(d256toD128(v1[i]), y.B0_63, signy, scaleFactor, &divResult) {
					if err := divOneFallback(d256toD128(v1[i]), d256toD128(v2[i]), &rs[i], uint64(i)); err != nil {
						return err
					}
					continue
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
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
			if err := divOneFallback(d256toD128(v1[i]), y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		x := d256toD128(v1[0])
		if canInline && d256DivFit64(v2, len2) {
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
				signy := d128Abs(&y)
				var divResult types.Decimal128
				if !d128DivInline(x, y.B0_63, signy, scaleFactor, &divResult) {
					if err := divOneFallback(x, d256toD128(v2[i]), &rs[i], uint64(i)); err != nil {
						return err
					}
					continue
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
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
			if err := divOneFallback(x, y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	} else {
		y := d256toD128(v2[0])
		if d128IsZero(y) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		// vec/const: precompute divisor for inline path.
		if canInline {
			absY := y
			signyU := d128Abs(&absY)
			if absY.B64_127 == 0 {
				absY64 := absY.B0_63
				for i := 0; i < len1; i++ {
					if hasNull && bmp.Contains(uint64(i)) {
						continue
					}
					var divResult types.Decimal128
					if !d128DivInline(d256toD128(v1[i]), absY64, signyU, scaleFactor, &divResult) {
						if err := divOneFallback(d256toD128(v1[i]), y, &rs[i], uint64(i)); err != nil {
							return err
						}
						continue
					}
					var err error
					rs[i], err = intDivElem(divResult)
					if err != nil {
						return err
					}
				}
				return nil
			}
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := divOneFallback(d256toD128(v1[i]), y, &rs[i], uint64(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- Decimal64 add/sub ----

// d64Add is the batch kernel for Decimal64 addition.
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

// d64MulScaled handles the case where scaleAdj != 0 (need to scale down after multiply).

func d64MulScaled(v1, v2 []types.Decimal64, rs []types.Decimal128, scaleAdj int32, rsnull *nulls.Nulls) error {
	n := len(rs)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	negAdj := -scaleAdj
	pow10a, twoStep, pow10b := scalePow10Factors(negAdj)

	// Prescan: if all values fit in int32, products fit in int64 → single
	// 64-bit multiply + single 64÷64 division, replacing bits.Mul64 →
	// d128Abs → 2×bits.Div64 → d128Negate (saves one DIV per element).
	// For D64 mul, negAdj ≤ 18 so twoStep is always false.
	if d64AllFitInt32(v1) && d64AllFitInt32(v2) {
		d := pow10a
		halfD := (d + 1) >> 1
		if len(v1) == 1 {
			x := int64(v1[0])
			for i := 0; i < n; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := x * int64(v2[i])
				sign := uint64(p >> 63)
				abs := uint64((p ^ int64(sign)) - int64(sign))
				q, rem := bits.Div64(0, abs, d)
				_, borrow := bits.Sub64(rem, halfD, 0)
				q += 1 - borrow
				signedQ := int64((q ^ sign) - sign)
				rs[i] = types.Decimal128{B0_63: uint64(signedQ), B64_127: uint64(signedQ >> 63)}
			}
		} else if len(v2) == 1 {
			y := int64(v2[0])
			for i := 0; i < n; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := int64(v1[i]) * y
				sign := uint64(p >> 63)
				abs := uint64((p ^ int64(sign)) - int64(sign))
				q, rem := bits.Div64(0, abs, d)
				_, borrow := bits.Sub64(rem, halfD, 0)
				q += 1 - borrow
				signedQ := int64((q ^ sign) - sign)
				rs[i] = types.Decimal128{B0_63: uint64(signedQ), B64_127: uint64(signedQ >> 63)}
			}
		} else {
			for i := 0; i < n; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				p := int64(v1[i]) * int64(v2[i])
				sign := uint64(p >> 63)
				abs := uint64((p ^ int64(sign)) - int64(sign))
				q, rem := bits.Div64(0, abs, d)
				_, borrow := bits.Sub64(rem, halfD, 0)
				q += 1 - borrow
				signedQ := int64((q ^ sign) - sign)
				rs[i] = types.Decimal128{B0_63: uint64(signedQ), B64_127: uint64(signedQ >> 63)}
			}
		}
		return nil
	}

	if len(v1) == 1 {
		x := v1[0]
		for i := 0; i < n; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r := d64MulInline(x, v2[i])
			d128ScaleDownPow10(&r, pow10a, twoStep, pow10b)
			rs[i] = r
		}
	} else if len(v2) == 1 {
		y := v2[0]
		for i := 0; i < n; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r := d64MulInline(v1[i], y)
			d128ScaleDownPow10(&r, pow10a, twoStep, pow10b)
			rs[i] = r
		}
	} else {
		for i := 0; i < n; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r := d64MulInline(v1[i], v2[i])
			d128ScaleDownPow10(&r, pow10a, twoStep, pow10b)
			rs[i] = r
		}
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
		return types.Decimal128{B0_63: uint64(v), B64_127: uint64(int64(v) >> 63)}
	}

	if len1 == len2 {
		// vec / vec: D64 always fits in 64 bits — skip dispatch when canInline.
		if canInline {
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					if v2[i] == 0 {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signyBit := uint64(v2[i]) >> 63
					absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
					x := d64toD128(v1[i])
					if !d128DivInline(x, absY64, signyBit, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, d64toD128(v2[i]), &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					if v2[i] == 0 {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signyBit := uint64(v2[i]) >> 63
					absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
					x := d64toD128(v1[i])
					if !d128DivInline(x, absY64, signyBit, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, d64toD128(v2[i]), &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
		} else {
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
		}
	} else if len1 == 1 {
		// const / vec: D64 always fits in 64 bits — skip dispatch when canInline.
		x := d64toD128(v1[0])
		if canInline {
			if rsnull.IsEmpty() {
				for i := 0; i < len2; i++ {
					if v2[i] == 0 {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signyBit := uint64(v2[i]) >> 63
					absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
					if !d128DivInline(x, absY64, signyBit, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, d64toD128(v2[i]), &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			} else {
				for i := 0; i < len2; i++ {
					if bmp.Contains(uint64(i)) {
						continue
					}
					if v2[i] == 0 {
						if shouldError {
							return moerr.NewDivByZeroNoCtx()
						}
						rsnull.Add(uint64(i))
						continue
					}
					signyBit := uint64(v2[i]) >> 63
					absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
					if !d128DivInline(x, absY64, signyBit, scaleFactor, &rs[i]) {
						if err := d128DivOne(x, d64toD128(v2[i]), &rs[i], scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
							return err
						}
					}
				}
			}
		} else {
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
		}
	} else {
		y := d64toD128(v2[0])
		// Check constant divisor for zero once.
		if d128IsZero(y) {
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

	// Diff-scale fast path: hoist pow10 factors, widen D64 to D128.
	diff := scale2 - scale1
	scaleX := diff > 0
	dMask := diff >> 31
	scaleDiff := (diff ^ dMask) - dMask
	pow10a, twoStep, pow10b := scalePow10Factors(scaleDiff)

	modFn := d128ModDiffScaleYPow10
	if scaleX {
		modFn = d128ModDiffScaleXPow10
	}

	// When scaleX, y is not scaled — D64 always fits in 64 bits, so inline directly.
	if scaleX {
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
				signyBit := uint64(v2[i]) >> 63
				absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
				sx := int64(v1[i])
				ax := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					r64, _, err := v1[i].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					rs[i] = r64
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				rs[i] = types.Decimal64(r.B0_63)
			}
		} else if len1 == 1 {
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
				signyBit := uint64(v2[i]) >> 63
				absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
				sx := int64(v1[0])
				ax := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					r64, _, err := v1[0].Mod(v2[i], scale1, scale2)
					if err != nil {
						return err
					}
					rs[i] = r64
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
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
			signyBit := uint64(v2[0]) >> 63
			absY64 := (uint64(v2[0]) ^ (-signyBit)) + signyBit
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sx := int64(v1[i])
				ax := types.Decimal128{B0_63: uint64(sx), B64_127: uint64(sx >> 63)}
				signx := d128Abs(&ax)
				if !d128Mul1Limb(&ax, pow10a) || (twoStep && !d128Mul1Limb(&ax, pow10b)) {
					r64, _, err := v1[i].Mod(v2[0], scale1, scale2)
					if err != nil {
						return err
					}
					rs[i] = r64
					continue
				}
				_, rhi := bits.Div64(0, ax.B64_127, absY64)
				_, rlo := bits.Div64(rhi, ax.B0_63, absY64)
				r := types.Decimal128{B0_63: rlo}
				d128Negate(&r, signx)
				rs[i] = types.Decimal64(r.B0_63)
			}
		}
		return nil
	}

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
			r, ok := modFn(x128, y128, pow10a, twoStep, pow10b)
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
			r, ok := modFn(x128, y128, pow10a, twoStep, pow10b)
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
			r, ok := modFn(x128, y128, pow10a, twoStep, pow10b)
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

// d64IntDivKernel returns a closure for Decimal64 → int64 integer division.
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
		return types.Decimal128{B0_63: uint64(v), B64_127: uint64(int64(v) >> 63)}
	}

	// Hoist pow10 factors for scale-down outside the loop.
	var sdPow10a uint64
	var sdTwoStep bool
	var sdPow10b uint64
	if scale > 0 {
		sdPow10a, sdTwoStep, sdPow10b = scalePow10Factors(scale)
	}

	intDivElem := func(divResult types.Decimal128) (int64, error) {
		if scale > 0 {
			d128ScaleDownPow10(&divResult, sdPow10a, sdTwoStep, sdPow10b)
		}
		return decimal128ToInt64(divResult)
	}

	len1, len2 := len(v1), len(v2)

	if len1 == len2 {
		// vec / vec: D64 always fits in 64 bits — skip dispatch when canInline.
		if canInline {
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
				signyBit := uint64(v2[i]) >> 63
				absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
				x := d64toD128(v1[i])
				var divResult types.Decimal128
				if !d128DivInline(x, absY64, signyBit, scaleFactor, &divResult) {
					if err := d128DivOne(x, d64toD128(v2[i]), &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i < len1; i++ {
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
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		// const / vec: D64 always fits in 64 bits — skip dispatch when canInline.
		x := d64toD128(v1[0])
		if canInline {
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
				signyBit := uint64(v2[i]) >> 63
				absY64 := (uint64(v2[i]) ^ (-signyBit)) + signyBit
				var divResult types.Decimal128
				if !d128DivInline(x, absY64, signyBit, scaleFactor, &divResult) {
					if err := d128DivOne(x, d64toD128(v2[i]), &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
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
			var divResult types.Decimal128
			if err := d128DivOneDispatch(x, d64toD128(v2[i]), &divResult, scaleAdj, scaleFactor, canInline, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
		}
	} else {
		// vec / const: precompute divisor properties.
		if v2[0] == 0 {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			nulls.AddRange(rsnull, 0, uint64(len1))
			return nil
		}
		y := d64toD128(v2[0])
		signyU := y.B64_127 >> 63
		negMaskY := -signyU
		absY := types.Decimal128{B0_63: y.B0_63 ^ negMaskY, B64_127: y.B64_127 ^ negMaskY}
		var cY uint64
		absY.B0_63, cY = bits.Add64(absY.B0_63, signyU, 0)
		absY.B64_127, _ = bits.Add64(absY.B64_127, 0, cY)
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				x := d64toD128(v1[i])
				var divResult types.Decimal128
				if !d128DivInline(x, absY64, signyU, scaleFactor, &divResult) {
					if err := d128DivOne(x, y, &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
						return err
					}
				}
				var err error
				rs[i], err = intDivElem(divResult)
				if err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			x := d64toD128(v1[i])
			var divResult types.Decimal128
			if err := d128DivOne(x, y, &divResult, scaleAdj, rsnull, uint64(i), shouldError, scale1, scale2); err != nil {
				return err
			}
			var err error
			rs[i], err = intDivElem(divResult)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- Scale helpers ----

func d64ScaleIntoRs(vec, rs []types.Decimal64, n int, scaleDiff int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	f := types.Pow10[scaleDiff]
	maxSafe := uint64(math.MaxInt64) / f

	// Prescan: if every |vec[i]| ≤ maxSafe, overflow is impossible and we can
	// use a plain multiply without bits.Mul64.  The prescan checks all slots
	// (including null positions whose values are harmless); if a garbage null
	// value exceeds maxSafe we simply fall back to the checked path.
	allSafe := true
	for i := 0; i < n; i++ {
		signBit := uint64(vec[i]) >> 63
		abs := (uint64(vec[i]) ^ -signBit) + signBit
		if abs > maxSafe {
			allSafe = false
			break
		}
	}

	if allSafe {
		if rsnull.IsEmpty() {
			for i := 0; i < n; i++ {
				signBit := uint64(vec[i]) >> 63
				mask := -signBit
				abs := (uint64(vec[i]) ^ mask) + signBit
				rs[i] = types.Decimal64((abs*f ^ mask) + signBit)
			}
		} else {
			for i := 0; i < n; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				signBit := uint64(vec[i]) >> 63
				mask := -signBit
				abs := (uint64(vec[i]) ^ mask) + signBit
				rs[i] = types.Decimal64((abs*f ^ mask) + signBit)
			}
		}
		return nil
	}

	// Fallback: use bits.Mul64 for overflow detection (replaces Mul64's
	// division-based check).
	if rsnull.IsEmpty() {
		for i := 0; i < n; i++ {
			signBit := uint64(vec[i]) >> 63
			mask := -signBit
			abs := (uint64(vec[i]) ^ mask) + signBit
			hi, lo := bits.Mul64(abs, f)
			if hi|(lo>>63) != 0 {
				return moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", vec[i].Format(scaleDiff))
			}
			rs[i] = types.Decimal64((lo ^ mask) + signBit)
		}
	} else {
		for i := 0; i < n; i++ {
			if bmp.Contains(uint64(i)) {
				continue
			}
			signBit := uint64(vec[i]) >> 63
			mask := -signBit
			abs := (uint64(vec[i]) ^ mask) + signBit
			hi, lo := bits.Mul64(abs, f)
			if hi|(lo>>63) != 0 {
				return moerr.NewInvalidInputNoCtxf("Decimal64 scale overflow: %s", vec[i].Format(scaleDiff))
			}
			rs[i] = types.Decimal64((lo ^ mask) + signBit)
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
	if hi|(lo>>63) != 0 {
		return x, false
	}
	return types.Decimal64((lo ^ mask) + signBit), true
}

// d128Mul1Limb multiplies unsigned D128 x by f in-place. Returns false on overflow.
// The gate keeps the common B64_127==0 path fast (skips cross-multiply), while the
// else-if catches the case where hi overflows into bit 63 (sign bit of D128).
func d128Mul1Limb(x *types.Decimal128, f uint64) bool {
	hi, lo := bits.Mul64(x.B0_63, f)
	if x.B64_127 != 0 {
		crossHi, crossLo := bits.Mul64(x.B64_127, f)
		var c uint64
		hi, c = bits.Add64(hi, crossLo, 0)
		if crossHi|c|(hi>>63) != 0 {
			return false
		}
	} else if hi>>63 != 0 {
		return false
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
	// Branchless round half-up: round = 1 iff rem >= ceil(d/2).
	_, borrow := bits.Sub64(rem, (d+1)>>1, 0)
	round := 1 - borrow
	lo, c := bits.Add64(x.B0_63, round, 0)
	x.B0_63 = lo
	x.B64_127 += c
}
