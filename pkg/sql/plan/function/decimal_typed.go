// Copyright 2021 Matrix Origin
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

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// decimalMul128By64 computes an exact signed 128x64 product. Its magnitude is
// at most 2^190, so three magnitude limbs and the sign limb fit Decimal256.
// No scale change or SQL precision decision is made here; those belong to the
// prepared expression's original operation boundary.
func decimalMul128By64(x types.Decimal128, y types.Decimal64) types.Decimal256 {
	// A narrow intermediate needs only one exact multiply, without a batch
	// range scan or allocation. The sign-extension check includes negatives.
	if x.B64_127 == uint64(int64(x.B0_63)>>63) {
		return types.Decimal256FromDecimal128(d64MulInline(types.Decimal64(x.B0_63), y))
	}
	sx := x.B64_127 >> 63
	sy := uint64(int64(y) >> 63)
	if sx != 0 {
		x = x.Minus()
	}
	uy := (uint64(y) ^ sy) - sy
	high0, low := bits.Mul64(x.B0_63, uy)
	high1, low1 := bits.Mul64(x.B64_127, uy)
	mid, carry := bits.Add64(high0, low1, 0)
	top, _ := bits.Add64(high1, 0, carry)
	result := types.Decimal256{B0_63: low, B64_127: mid, B128_191: top}
	if sx != (sy & 1) {
		result = result.Minus()
	}
	return result
}

// decimalAdd128Exact retains the carry beyond the signed 128-bit inputs.
// Its result needs at most 129 signed bits; no decimal rounding is involved.
func decimalAdd128Exact(x, y types.Decimal128) types.Decimal256 {
	xe, ye := uint64(int64(x.B64_127)>>63), uint64(int64(y.B64_127)>>63)
	lo, carry := bits.Add64(x.B0_63, y.B0_63, 0)
	hi, carry := bits.Add64(x.B64_127, y.B64_127, carry)
	top, carry := bits.Add64(xe, ye, carry)
	sign, _ := bits.Add64(xe, ye, carry)
	return types.Decimal256{B0_63: lo, B64_127: hi, B128_191: top, B192_255: sign}
}

// decimalSub128Exact preserves the extra sign bit for MinInt128-MaxInt128.
func decimalSub128Exact(x, y types.Decimal128) types.Decimal256 {
	xe, ye := uint64(int64(x.B64_127)>>63), uint64(int64(y.B64_127)>>63)
	lo, borrow := bits.Sub64(x.B0_63, y.B0_63, 0)
	hi, borrow := bits.Sub64(x.B64_127, y.B64_127, borrow)
	top, borrow := bits.Sub64(xe, ye, borrow)
	sign, _ := bits.Sub64(xe, ye, borrow)
	return types.Decimal256{B0_63: lo, B64_127: hi, B128_191: top, B192_255: sign}
}

// TryDecimal64Binary fills a proved-safe, unscaled binary operation directly
// from its original physical operands. Preparation owns the domain proof;
// unsupported runtime shapes leave output untouched for generic evaluation.
func TryDecimal64Binary(op int32, x, y, out *vector.Vector) bool {
	n := out.Length()
	if x.IsConst() || y.IsConst() || x.HasNull() || y.HasNull() || x.HasGrouping() || y.HasGrouping() || x.Length() != n || y.Length() != n || x.GetType().Oid != types.T_decimal64 || y.GetType().Oid != types.T_decimal64 {
		return false
	}
	if op != PLUS && op != MINUS && op != MULTI {
		return false
	}
	return decimalChunk64(op, x, y, out, 0, 0, 0, n)
}

// DecimalArithmeticChunk evaluates one admitted, exact operation. Input offsets
// address original columns or bounded scratch; constants broadcast physical row
// zero. The owner proves scales and per-operation bounds before calling it.
func DecimalArithmeticChunk(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) error {
	if op == MULTI && (out.GetType().Oid == types.T_decimal256 || out.GetType().Oid == types.T_decimal128) {
		if x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal128 {
			x, y = y, x
			xi, yi = yi, xi
		}
		if x.GetType().Oid == types.T_decimal128 && y.GetType().Oid == types.T_decimal64 {
			a := vector.MustFixedColNoTypeCheck[types.Decimal128](x)
			b := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
			xm, ym := -1, -1
			if x.IsConst() {
				xm = 0
			}
			if y.IsConst() {
				ym = 0
			}
			if out.GetType().Oid == types.T_decimal128 {
				dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
				for i := 0; i < n; i++ {
					v := decimalMul128By64(a[(xi+i)&xm], b[(yi+i)&ym])
					dst[oi+i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
				}
			} else {
				dst := vector.MustFixedColNoTypeCheck[types.Decimal256](out)
				for i := 0; i < n; i++ {
					dst[oi+i] = decimalMul128By64(a[(xi+i)&xm], b[(yi+i)&ym])
				}
			}
			return nil
		}
	}
	if x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal64 && (op == MULTI || (x.GetType().Scale == out.GetType().Scale && y.GetType().Scale == out.GetType().Scale)) {
		if decimalChunk64(op, x, y, out, xi, yi, oi, n) {
			return nil
		}
	}
	sx, sy := int32(0), int32(0)
	if op != MULTI {
		sx = out.GetType().Scale - x.GetType().Scale
		sy = out.GetType().Scale - y.GetType().Scale
	}
	switch x.GetType().Oid {
	case types.T_decimal64:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal64](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	case types.T_decimal128:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal128](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	default:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal256](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	}
}

type decimalCoefficient interface {
	types.Decimal64 | types.Decimal128 | types.Decimal256
}

func decimalWide[T decimalCoefficient](v T) types.Decimal256 {
	switch x := any(v).(type) {
	case types.Decimal64:
		return types.Decimal256FromInt64(int64(x))
	case types.Decimal128:
		return types.Decimal256FromDecimal128(x)
	default:
		return any(v).(types.Decimal256)
	}
}
func decimalChunkRight[A decimalCoefficient](op int32, x []A, y, out *vector.Vector, xi, yi, oi, n int, xc bool, sx, sy int32) error {
	switch y.GetType().Oid {
	case types.T_decimal64:
		return decimalChunkOutput(op, x, vector.MustFixedColNoTypeCheck[types.Decimal64](y), out, xi, yi, oi, n, xc, y.IsConst(), sx, sy)
	case types.T_decimal128:
		return decimalChunkOutput(op, x, vector.MustFixedColNoTypeCheck[types.Decimal128](y), out, xi, yi, oi, n, xc, y.IsConst(), sx, sy)
	default:
		return decimalChunkOutput(op, x, vector.MustFixedColNoTypeCheck[types.Decimal256](y), out, xi, yi, oi, n, xc, y.IsConst(), sx, sy)
	}
}
func decimalChunkOutput[A, B decimalCoefficient](op int32, x []A, y []B, out *vector.Vector, xi, yi, oi, n int, xc, yc bool, sx, sy int32) error {
	switch out.GetType().Oid {
	case types.T_decimal64:
		return decimalChunkLoop(op, x, y, vector.MustFixedColNoTypeCheck[types.Decimal64](out), xi, yi, oi, n, xc, yc, sx, sy)
	case types.T_decimal128:
		return decimalChunkLoop(op, x, y, vector.MustFixedColNoTypeCheck[types.Decimal128](out), xi, yi, oi, n, xc, yc, sx, sy)
	default:
		return decimalChunkLoop(op, x, y, vector.MustFixedColNoTypeCheck[types.Decimal256](out), xi, yi, oi, n, xc, yc, sx, sy)
	}
}
func decimalNarrow[R decimalCoefficient](v types.Decimal256) R {
	var zero R
	switch any(zero).(type) {
	case types.Decimal64:
		return any(types.Decimal64(v.B0_63)).(R)
	case types.Decimal128:
		return any(types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}).(R)
	default:
		return any(v).(R)
	}
}
func decimalChunkLoop[A, B, R decimalCoefficient](op int32, x []A, y []B, out []R, xi, yi, oi, n int, xc, yc bool, sx, sy int32) error {
	xm, ym := -1, -1
	if xc {
		xm = 0
	}
	if yc {
		ym = 0
	}
	switch op {
	case PLUS:
		for i := 0; i < n; i++ {
			a, b := decimalWide(x[(xi+i)&xm]), decimalWide(y[(yi+i)&ym])
			var err error
			if sx > 0 {
				a, err = a.Scale(sx)
				if err != nil {
					return err
				}
			}
			if sy > 0 {
				b, err = b.Scale(sy)
				if err != nil {
					return err
				}
			}
			v, err := a.Add256(b)
			if err != nil {
				return err
			}
			out[oi+i] = decimalNarrow[R](v)
		}
	case MINUS:
		for i := 0; i < n; i++ {
			a, b := decimalWide(x[(xi+i)&xm]), decimalWide(y[(yi+i)&ym])
			var err error
			if sx > 0 {
				a, err = a.Scale(sx)
				if err != nil {
					return err
				}
			}
			if sy > 0 {
				b, err = b.Scale(sy)
				if err != nil {
					return err
				}
			}
			v, err := a.Sub256(b)
			if err != nil {
				return err
			}
			out[oi+i] = decimalNarrow[R](v)
		}
	case MULTI:
		for i := 0; i < n; i++ {
			a, b := decimalWide(x[(xi+i)&xm]), decimalWide(y[(yi+i)&ym])
			var v types.Decimal256
			if a.B128_191 == uint64(int64(a.B64_127)>>63) && a.B192_255 == a.B128_191 && b.B64_127 == uint64(int64(b.B0_63)>>63) && b.B128_191 == b.B64_127 && b.B192_255 == b.B64_127 {
				v = decimalMul128By64(types.Decimal128{B0_63: a.B0_63, B64_127: a.B64_127}, types.Decimal64(b.B0_63))
			} else {
				var err error
				v, err = a.Mul256(b)
				if err != nil {
					return err
				}
			}
			out[oi+i] = decimalNarrow[R](v)
		}
	}
	return nil
}

func decimalChunk64(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	xm, ym := -1, -1
	if x.IsConst() {
		xm = 0
	}
	if y.IsConst() {
		ym = 0
	}
	a := vector.MustFixedColNoTypeCheck[types.Decimal64](x)
	b := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
	switch out.GetType().Oid {
	case types.T_decimal64:
		dst := vector.MustFixedColNoTypeCheck[types.Decimal64](out)
		switch op {
		case PLUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = a[ai] + b[bi]
			}
		case MINUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = a[ai] - b[bi]
			}
		default:
			return false
		}
	case types.T_decimal128:
		dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
		switch op {
		case MULTI:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = d64MulInline(a[ai], b[bi])
			}
		case PLUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				lo, c := bits.Add64(uint64(a[ai]), uint64(b[bi]), 0)
				hi, _ := bits.Add64(uint64(int64(a[ai])>>63), uint64(int64(b[bi])>>63), c)
				dst[i] = types.Decimal128{B0_63: lo, B64_127: hi}
			}
		case MINUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				lo, c := bits.Sub64(uint64(a[ai]), uint64(b[bi]), 0)
				hi, _ := bits.Sub64(uint64(int64(a[ai])>>63), uint64(int64(b[bi])>>63), c)
				dst[i] = types.Decimal128{B0_63: lo, B64_127: hi}
			}
		}
	case types.T_decimal256:
		dst := vector.MustFixedColNoTypeCheck[types.Decimal256](out)
		switch op {
		case MULTI:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = types.Decimal256FromDecimal128(d64MulInline(a[ai], b[bi]))
			}
		case PLUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = decimalAdd128Exact(types.Decimal128{B0_63: uint64(a[ai]), B64_127: uint64(int64(a[ai]) >> 63)}, types.Decimal128{B0_63: uint64(b[bi]), B64_127: uint64(int64(b[bi]) >> 63)})
			}
		case MINUS:
			for row := 0; row < n; row++ {
				i := oi + row
				ai := (xi + row) & xm
				bi := (yi + row) & ym
				dst[i] = decimalSub128Exact(types.Decimal128{B0_63: uint64(a[ai]), B64_127: uint64(int64(a[ai]) >> 63)}, types.Decimal128{B0_63: uint64(b[bi]), B64_127: uint64(int64(b[bi]) >> 63)})
			}
		}
	default:
		return false
	}
	return true
}
