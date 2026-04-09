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
//   - Multiply: d64Mul, d128Mul, d256Mul
//   - Add/Sub:  d64Add/d64Sub, d128Add/d128Sub, d256Add/d256Sub
//   - Division: d64Div, d128Div, d256Div

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ---- Decimal64 add/sub ----

// d64Add is the batch kernel for Decimal64 addition.
func d64Add(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d64AddSameScale(v1, v2, rs, rsnull)
	}
	return d64AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

func d64AddSameScale(v1, v2, rs []types.Decimal64, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	len1, len2 := len(v1), len(v2)
	if len1 == len2 {
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				var err error
				rs[i], err = v1[i].Add64(v2[i])
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = v1[i].Add64(v2[i])
				if err != nil {
					return err
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		if rsnull.IsEmpty() {
			for i := 0; i < len2; i++ {
				var err error
				rs[i], err = a.Add64(v2[i])
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = a.Add64(v2[i])
				if err != nil {
					return err
				}
			}
		}
	} else {
		b := v2[0]
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				var err error
				rs[i], err = v1[i].Add64(b)
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = v1[i].Add64(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// d64AddDiffScale handles Decimal64 addition when scales differ.
// Pre-scales constants or vectors once, then delegates to same-scale loop.

func d64AddDiffScale(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1, len2 := len(v1), len(v2)

	if len1 == 1 {
		if scale1 < scale2 {
			a, err := v1[0].Scale(scale2 - scale1)
			if err != nil {
				return err
			}
			return d64AddSameScale([]types.Decimal64{a}, v2, rs, rsnull)
		}
		if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return err
		}
		return d64AddSameScale(rs[:len2], []types.Decimal64{v1[0]}, rs, rsnull)
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b, err := v2[0].Scale(scale1 - scale2)
			if err != nil {
				return err
			}
			return d64AddSameScale(v1, []types.Decimal64{b}, rs, rsnull)
		}
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d64AddSameScale(rs[:len1], []types.Decimal64{v2[0]}, rs, rsnull)
	}

	// vec-vec: scale the lower-scale one into rs, then same-scale add.
	if scale1 < scale2 {
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d64AddSameScale(rs[:len1], v2, rs, rsnull)
	}
	if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return err
	}
	return d64AddSameScale(v1, rs[:len2], rs, rsnull)
}

// d64Sub is the batch kernel for Decimal64 subtraction.

func d64Sub(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d64SubSameScale(v1, v2, rs, rsnull)
	}
	return d64SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

func d64SubSameScale(v1, v2, rs []types.Decimal64, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	len1, len2 := len(v1), len(v2)
	if len1 == len2 {
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				var err error
				rs[i], err = v1[i].Sub64(v2[i])
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = v1[i].Sub64(v2[i])
				if err != nil {
					return err
				}
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		if rsnull.IsEmpty() {
			for i := 0; i < len2; i++ {
				var err error
				rs[i], err = a.Sub64(v2[i])
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = a.Sub64(v2[i])
				if err != nil {
					return err
				}
			}
		}
	} else {
		b := v2[0]
		if rsnull.IsEmpty() {
			for i := 0; i < len1; i++ {
				var err error
				rs[i], err = v1[i].Sub64(b)
				if err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				var err error
				rs[i], err = v1[i].Sub64(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// d64SubDiffScale handles Decimal64 subtraction (v1 - v2) when scales differ.
// Pre-scales constants or vectors once, then delegates to same-scale loop.

func d64SubDiffScale(v1, v2, rs []types.Decimal64, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1, len2 := len(v1), len(v2)

	if len1 == 1 {
		// const - vector
		if scale1 < scale2 {
			a, err := v1[0].Scale(scale2 - scale1)
			if err != nil {
				return err
			}
			return d64SubSameScale([]types.Decimal64{a}, v2, rs, rsnull)
		}
		if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return err
		}
		return d64SubSameScale([]types.Decimal64{v1[0]}, rs[:len2], rs, rsnull)
	}

	if len2 == 1 {
		// vector - const
		if scale2 < scale1 {
			b, err := v2[0].Scale(scale1 - scale2)
			if err != nil {
				return err
			}
			return d64SubSameScale(v1, []types.Decimal64{b}, rs, rsnull)
		}
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d64SubSameScale(rs[:len1], []types.Decimal64{v2[0]}, rs, rsnull)
	}

	// vec-vec: scale the lower-scale operand into rs.
	if scale1 < scale2 {
		if err := d64ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d64SubSameScale(rs[:len1], v2, rs, rsnull)
	}
	if err := d64ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return err
	}
	return d64SubSameScale(v1, rs[:len2], rs, rsnull)
}

// d64ScaleIntoRs scales vec[i] by 10^scaleDiff into rs[i], respecting nulls.

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

// ---- Decimal64 multiply ----

// d64Mul is the batch kernel for Decimal64 × Decimal64 → Decimal128.
// len(v1)==1 or len(v2)==1 indicates a broadcast constant.
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
			if err := r.ScaleInplace(scaleAdj); err != nil {
				return err
			}
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
		if err := r.ScaleInplace(scaleAdj); err != nil {
			return err
		}
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
		signy := y.Sign()
		absY := y
		if signy {
			absY = absY.Minus()
		}
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					x := d64toD128(v1[i])
					if !d128DivInline(x, absY64, signy, scaleFactor, &rs[i]) {
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
					if !d128DivInline(x, absY64, signy, scaleFactor, &rs[i]) {
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
			if v2[i] == 0 {
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
		if v2[0] == 0 {
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

// ---- Decimal128 add/sub ----

// d128Add is the batch kernel for Decimal128 addition.
// Same-scale: direct bits.Add64 without overflow checking.
// Different-scale: scales operand(s) first, then uses bits.Add64 loop.
func d128Add(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d128AddSameScale(v1, v2, rs, rsnull)
	}
	return d128AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

// d128Sub is the batch kernel for Decimal128 subtraction.
// Same-scale: direct bits.Sub64 without overflow checking.
// Different-scale: scales operand(s) first, then uses bits.Sub64 loop.

func d128AddSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	len1 := len(v1)
	len2 := len(v2)
	noNull := rsnull.IsEmpty()

	var carry uint64

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, v2[i].B64_127, carry)
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, v2[i].B64_127, carry)
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i].B0_63, carry = bits.Add64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(a.B64_127, v2[i].B64_127, carry)
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, carry = bits.Add64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(a.B64_127, v2[i].B64_127, carry)
			}
		}
	} else {
		b := v2[0]
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, b.B64_127, carry)
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, carry = bits.Add64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Add64(v1[i].B64_127, b.B64_127, carry)
			}
		}
	}
	return nil
}

func d128AddDiffScale(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)

	// Addition is commutative: normalize to (const, vec) or (vec, vec).
	if len1 == 1 {
		if scale1 < scale2 {
			// Scale const once, then same-scale add.
			a := v1[0]
			if err := a.ScaleInplace(scale2 - scale1); err != nil {
				return err
			}
			return d128AddSameScale([]types.Decimal128{a}, v2, rs, rsnull)
		}
		// scale1 > scale2: scale vector v2 into rs, then add const.
		if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return err
		}
		return d128AddSameScale(rs[:len2], []types.Decimal128{v1[0]}, rs, rsnull)
	}

	if len2 == 1 {
		if scale2 < scale1 {
			b := v2[0]
			if err := b.ScaleInplace(scale1 - scale2); err != nil {
				return err
			}
			return d128AddSameScale(v1, []types.Decimal128{b}, rs, rsnull)
		}
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d128AddSameScale(rs[:len1], []types.Decimal128{v2[0]}, rs, rsnull)
	}

	// vector-vector: scale the lower-scale one into rs, then add.
	if scale1 < scale2 {
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d128AddSameScale(rs[:len1], v2, rs, rsnull)
	}
	if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return err
	}
	return d128AddSameScale(v1, rs[:len2], rs, rsnull)
}

// d128SubDiffScale handles Decimal128 subtraction (v1 - v2) when scales differ.

func d128Sub(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d128SubSameScale(v1, v2, rs, rsnull)
	}
	return d128SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

// d128AddDiffScale handles Decimal128 addition when scales differ.
// For const operands: scale the constant once, then same-scale bits.Add64.
// For vector-vector: scale lower-scale vector into rs, then bits.Add64 with other.

func d128SubSameScale(v1, v2, rs []types.Decimal128, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	len1 := len(v1)
	len2 := len(v2)
	noNull := rsnull.IsEmpty()

	var borrow uint64

	if len1 == len2 {
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, v2[i].B64_127, borrow)
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, v2[i].B64_127, borrow)
			}
		}
	} else if len1 == 1 {
		a := v1[0]
		if noNull {
			for i := 0; i < len2; i++ {
				rs[i].B0_63, borrow = bits.Sub64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(a.B64_127, v2[i].B64_127, borrow)
			}
		} else {
			for i := 0; i < len2; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, borrow = bits.Sub64(a.B0_63, v2[i].B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(a.B64_127, v2[i].B64_127, borrow)
			}
		}
	} else {
		b := v2[0]
		if noNull {
			for i := 0; i < len1; i++ {
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, b.B64_127, borrow)
			}
		} else {
			for i := 0; i < len1; i++ {
				if bmp.Contains(uint64(i)) {
					continue
				}
				rs[i].B0_63, borrow = bits.Sub64(v1[i].B0_63, b.B0_63, 0)
				rs[i].B64_127, _ = bits.Sub64(v1[i].B64_127, b.B64_127, borrow)
			}
		}
	}
	return nil
}

func d128SubDiffScale(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)

	if len1 == 1 {
		// const - vector
		if scale1 < scale2 {
			a := v1[0]
			if err := a.ScaleInplace(scale2 - scale1); err != nil {
				return err
			}
			return d128SubSameScale([]types.Decimal128{a}, v2, rs, rsnull)
		}
		// scale1 > scale2: scale v2 into rs, then const - rs[i]
		if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
			return err
		}
		return d128SubSameScale([]types.Decimal128{v1[0]}, rs[:len2], rs, rsnull)
	}

	if len2 == 1 {
		// vector - const
		if scale2 < scale1 {
			b := v2[0]
			if err := b.ScaleInplace(scale1 - scale2); err != nil {
				return err
			}
			return d128SubSameScale(v1, []types.Decimal128{b}, rs, rsnull)
		}
		// scale2 > scale1: scale v1 into rs, then rs[i] - const
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d128SubSameScale(rs[:len1], []types.Decimal128{v2[0]}, rs, rsnull)
	}

	// vector-vector: scale the lower-scale operand into rs.
	if scale1 < scale2 {
		// scale v1 into rs, then rs[i] - v2[i]
		if err := d128ScaleIntoRs(v1, rs, len1, scale2-scale1, rsnull); err != nil {
			return err
		}
		return d128SubSameScale(rs[:len1], v2, rs, rsnull)
	}
	// scale v2 into rs, then v1[i] - rs[i]
	if err := d128ScaleIntoRs(v2, rs, len2, scale1-scale2, rsnull); err != nil {
		return err
	}
	return d128SubSameScale(v1, rs[:len2], rs, rsnull)
}

// d128ScaleIntoRs scales vec[i] * 10^n into rs[i], respecting nulls.

func d128ScaleIntoRs(vec, rs []types.Decimal128, n int, scaleDiff int32, rsnull *nulls.Nulls) error {
	bmp := rsnull.GetBitmap()
	if rsnull.IsEmpty() {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			if err := rs[i].ScaleInplace(scaleDiff); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < n; i++ {
			rs[i] = vec[i]
			if bmp.Contains(uint64(i)) {
				continue
			}
			if err := rs[i].ScaleInplace(scaleDiff); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- Decimal128 multiply ----

// d128Mul is the batch kernel for Decimal128 multiplication. It inlines a fast
// multiply path for D64-origin values and falls back to MulInplace for
// genuinely large Decimal128 values.
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
	// Fast path: both fit in int64 (B64_127 is 0 or ^0).
	if (xhi+1) <= 1 && (yhi+1) <= 1 {
		// Extract sign: bit 63 of B64_127.
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
			if err := dst.ScaleInplace(scaleAdj); err != nil {
				return err
			}
		}
		// Branchless conditional negate (two's complement: XOR + add carry).
		negMask := -neg // 0 or 0xFF..FF
		dst.B0_63 ^= negMask
		dst.B64_127 ^= negMask
		var c uint64
		dst.B0_63, c = bits.Add64(dst.B0_63, 0, neg)
		dst.B64_127, _ = bits.Add64(dst.B64_127, 0, c)
		return nil
	}

	// Slow path: genuinely large Decimal128 values.
	*dst = *x
	return dst.MulInplace(y, scaleAdj, scale1, scale2)
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
		signy := b.Sign()
		absY := b
		if signy {
			absY = absY.Minus()
		}
		// Use fully-inlined path if divisor fits in 64 bits and scale is small.
		if canInline && absY.B64_127 == 0 {
			absY64 := absY.B0_63
			if rsnull.IsEmpty() {
				for i := 0; i < len1; i++ {
					if !d128DivInline(v1[i], absY64, signy, scaleFactor, &rs[i]) {
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
					if !d128DivInline(v1[i], absY64, signy, scaleFactor, &rs[i]) {
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
		signy := y.B64_127>>63 != 0
		absY := y
		if signy {
			absY = absY.Minus()
		}
		if absY.B64_127 == 0 {
			if d128DivInline(x, absY.B0_63, signy, scaleFactor, dst) {
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

	signx := x.Sign()
	signy := y.Sign()
	if signx {
		x = x.Minus()
	}
	if signy {
		y = y.Minus()
	}

	z, err := x.Scale(scaleAdj)
	if err != nil {
		// Overflow in scale: fall back to D256 division.
		x2 := types.Decimal256{B0_63: x.B0_63, B64_127: x.B64_127}
		y2 := types.Decimal256{B0_63: y.B0_63, B64_127: y.B64_127}
		x2, err = x2.Scale(scaleAdj)
		if err != nil {
			return moerr.NewInvalidInputNoCtxf("Decimal128 Div overflow: %s(Scale:%d)/%s(Scale:%d)",
				x.Format(0), scale1, y.Format(0), scale2)
		}
		x2, err = x2.Div256(y2)
		if err != nil || x2.B192_255 != 0 || x2.B128_191 != 0 || x2.B64_127>>63 != 0 {
			return moerr.NewInvalidInputNoCtxf("Decimal128 Div overflow: %s(Scale:%d)/%s(Scale:%d)",
				x.Format(0), scale1, y.Format(0), scale2)
		}
		z = types.Decimal128{B0_63: x2.B0_63, B64_127: x2.B64_127}
		if signx != signy {
			z = z.Minus()
		}
		*dst = z
		return nil
	}

	z, err = z.Div128(y)
	if err != nil {
		return err
	}
	if signx != signy {
		z = z.Minus()
	}
	*dst = z
	return nil
}

// d128DivInline is the fast inline path for D128 division.
// Pre-conditions: absY > 0, absY fits in 64 bits (absY64 > 0),
// scaleAdj ≤ 19. x is a signed D128. signy is the sign of the divisor.
// Returns false if the fast path overflows and the caller should fall back.
func d128DivInline(x types.Decimal128, absY64 uint64, signy bool,
	scaleFactor uint64, dst *types.Decimal128) bool {

	// Extract sign and absolute value of x.
	signx := x.B64_127>>63 != 0
	if signx {
		if x.B0_63 == 0 {
			x.B64_127 = ^x.B64_127 + 1
		} else {
			x.B64_127 = ^x.B64_127
			x.B0_63 = ^x.B0_63 + 1
		}
	}

	// Inline Scale: multiply |x| by scaleFactor (a uint64 power of 10).
	// This is Mul128 with y = {scaleFactor, 0}, but without error allocation.
	var zHi, zLo uint64
	zHi, zLo = bits.Mul64(x.B0_63, scaleFactor)
	if x.B64_127 != 0 {
		crossHi, crossLo := bits.Mul64(x.B64_127, scaleFactor)
		if crossHi != 0 {
			return false // overflow
		}
		zHi += crossLo
		if zHi>>63 != 0 {
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

	// Restore sign.
	z := types.Decimal128{B0_63: zLo, B64_127: zHi}
	if signx != signy {
		z = z.Minus()
	}
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
	isZero := func(d types.Decimal128) bool {
		return d.B0_63 == 0 && d.B64_127 == 0
	}
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
			if isZero(v2[i]) {
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
			if isZero(v2[i]) {
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
		if isZero(v2[0]) {
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

// ---- Decimal256 add/sub ----

// d256Add is the batch kernel for Decimal256 addition.
func d256Add(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d256AddSameScale(v1, v2, rs, rsnull)
	}
	return d256AddDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

// d256Sub is the batch kernel for Decimal256 subtraction.
func d256Sub(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	if scale1 == scale2 {
		return d256SubSameScale(v1, v2, rs, rsnull)
	}
	return d256SubDiffScale(v1, v2, rs, scale1, scale2, rsnull)
}

// d256AddSameScale adds two Decimal256 vectors with the same scale, inlining
// the 4-limb carry chain from Add256.
func d256AddSameScale(v1, v2, rs []types.Decimal256, rsnull *nulls.Nulls) error {
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
			r, err := v1[i].Add256(v2[i])
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else if len1 == 1 {
		a := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, err := a.Add256(v2[i])
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else {
		b := v2[0]
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, err := v1[i].Add256(b)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	}
	return nil
}

// d256AddDiffScale handles Decimal256 addition when scales differ.
func d256AddDiffScale(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	// Pre-scale the vector with lower scale.
	if scale1 > scale2 {
		scaleDiff := scale1 - scale2
		if len2 == 1 {
			sv, err := v2[0].Scale(scaleDiff)
			if err != nil {
				return err
			}
			v2 = []types.Decimal256{sv}
		} else {
			scaled := make([]types.Decimal256, len2)
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sv, err := v2[i].Scale(scaleDiff)
				if err != nil {
					return err
				}
				scaled[i] = sv
			}
			v2 = scaled
		}
	} else {
		scaleDiff := scale2 - scale1
		if len1 == 1 {
			sv, err := v1[0].Scale(scaleDiff)
			if err != nil {
				return err
			}
			v1 = []types.Decimal256{sv}
		} else {
			scaled := make([]types.Decimal256, len1)
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sv, err := v1[i].Scale(scaleDiff)
				if err != nil {
					return err
				}
				scaled[i] = sv
			}
			v1 = scaled
		}
	}

	return d256AddSameScale(v1, v2, rs, rsnull)
}

// d256SubSameScale subtracts two Decimal256 vectors with the same scale.
func d256SubSameScale(v1, v2, rs []types.Decimal256, rsnull *nulls.Nulls) error {
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
			r, err := v1[i].Sub256(v2[i])
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else if len1 == 1 {
		a := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, err := a.Sub256(v2[i])
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else {
		b := v2[0]
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			r, err := v1[i].Sub256(b)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	}
	return nil
}

// d256SubDiffScale handles Decimal256 subtraction when scales differ.
func d256SubDiffScale(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}

	if scale1 > scale2 {
		scaleDiff := scale1 - scale2
		if len2 == 1 {
			sv, err := v2[0].Scale(scaleDiff)
			if err != nil {
				return err
			}
			v2 = []types.Decimal256{sv}
		} else {
			scaled := make([]types.Decimal256, len2)
			for i := 0; i < len2; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sv, err := v2[i].Scale(scaleDiff)
				if err != nil {
					return err
				}
				scaled[i] = sv
			}
			v2 = scaled
		}
	} else {
		scaleDiff := scale2 - scale1
		if len1 == 1 {
			sv, err := v1[0].Scale(scaleDiff)
			if err != nil {
				return err
			}
			v1 = []types.Decimal256{sv}
		} else {
			scaled := make([]types.Decimal256, len1)
			for i := 0; i < len1; i++ {
				if hasNull && bmp.Contains(uint64(i)) {
					continue
				}
				sv, err := v1[i].Scale(scaleDiff)
				if err != nil {
					return err
				}
				scaled[i] = sv
			}
			v1 = scaled
		}
	}

	return d256SubSameScale(v1, v2, rs, rsnull)
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
			if err := d256MulInline(&v1[i], &v2[i], &rs[i], scaleAdj); err != nil {
				return err
			}
		}
	} else if len1 == 1 {
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d256MulInline(&v1[0], &v2[i], &rs[i], scaleAdj); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if err := d256MulInline(&v1[i], &v2[0], &rs[i], scaleAdj); err != nil {
				return err
			}
		}
	}
	return nil
}

// d256MulInline performs a single D256 multiply with inlined schoolbook 4×4 cross-product.
// Uses a single overflow flag instead of per-branch error returns.
func d256MulInline(x, y *types.Decimal256, dst *types.Decimal256, scaleAdj int32) error {
	signx := x.B192_255>>63 != 0
	signy := y.B192_255>>63 != 0
	var x0, x1, x2, x3, y0, y1, y2, y3 uint64
	if signx {
		ax := x.Minus()
		x0, x1, x2, x3 = ax.B0_63, ax.B64_127, ax.B128_191, ax.B192_255
	} else {
		x0, x1, x2, x3 = x.B0_63, x.B64_127, x.B128_191, x.B192_255
	}
	if signy {
		ay := y.Minus()
		y0, y1, y2, y3 = ay.B0_63, ay.B64_127, ay.B128_191, ay.B192_255
	} else {
		y0, y1, y2, y3 = y.B0_63, y.B64_127, y.B128_191, y.B192_255
	}

	// Early overflow: if both operands have high limbs set, product can't fit.
	if (x3 != 0 && (y3|y2|y1) != 0) || (x2 != 0 && (y3|y2) != 0) || (x1 != 0 && y3 != 0) {
		return moerr.NewInvalidInputNoCtxf("Decimal256 Mul overflow: %s*%s", x.Format(0), y.Format(0))
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
		return moerr.NewInvalidInputNoCtxf("Decimal256 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}

	z := types.Decimal256{B0_63: z0, B64_127: z1, B128_191: z2, B192_255: z3}
	if scaleAdj != 0 {
		var err error
		z, err = z.Scale(scaleAdj)
		if err != nil {
			return err
		}
	}
	if signx != signy {
		z = z.Minus()
	}
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

func d256Div(v1, v2, rs []types.Decimal256, scale1, scale2 int32, rsnull *nulls.Nulls, shouldError bool) error {
	len1, len2 := len(v1), len(v2)
	hasNull := !rsnull.IsEmpty()
	var bmp *bitmap.Bitmap
	if hasNull {
		bmp = rsnull.GetBitmap()
	}
	isZero := func(d types.Decimal256) bool {
		return d.B0_63 == 0 && d.B64_127 == 0 && d.B128_191 == 0 && d.B192_255 == 0
	}

	if len1 == len2 {
		for i := 0; i < len1; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if isZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, _, err := v1[i].Div(v2[i], scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else if len1 == 1 {
		a := v1[0]
		for i := 0; i < len2; i++ {
			if hasNull && bmp.Contains(uint64(i)) {
				continue
			}
			if isZero(v2[i]) {
				if shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsnull.Add(uint64(i))
				continue
			}
			r, _, err := a.Div(v2[i], scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
		}
	} else {
		b := v2[0]
		if isZero(b) {
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
			r, _, err := v1[i].Div(b, scale1, scale2)
			if err != nil {
				return err
			}
			rs[i] = r
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
	isZero := func(d types.Decimal256) bool {
		return d.B0_63 == 0 && d.B64_127 == 0 && d.B128_191 == 0 && d.B192_255 == 0
	}
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
			if isZero(v2[i]) {
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
			if isZero(v2[i]) {
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
		if isZero(v2[0]) {
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
