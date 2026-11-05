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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// decimalMulSub64 evaluates x*(c-y) without intermediate widening vectors.
// The owner has checked scales, NULLs, representations and destination capacity.
// A full int64 difference fits 65 signed bits; its product with int64 fits
// Decimal128. The wide route uses two exact products without overflow.
func decimalMulSub64(x, y []types.Decimal64, c types.Decimal64, dst []types.Decimal128) bool {
	if len(x) != len(y) || len(dst) != len(x) {
		return false
	}
	for i, a := range x {
		ci, yi := int64(c), int64(y[i])
		difference := ci - yi
		if ((ci ^ yi) & (ci ^ difference)) >= 0 {
			dst[i] = d64MulInline(a, types.Decimal64(difference))
			continue
		}
		// The difference can need 65 signed bits. Each product fits 128,
		// and |x*(c-y)| <= 2^63*(2^64-1) < 2^127, so their subtraction
		// cannot overflow. Reuse the same signed 64x64 multiplication.
		ac, ay := d64MulInline(a, c), d64MulInline(a, y[i])
		var borrow uint64
		dst[i].B0_63, borrow = bits.Sub64(ac.B0_63, ay.B0_63, 0)
		dst[i].B64_127, _ = bits.Sub64(ac.B64_127, ay.B64_127, borrow)
	}
	return true
}

// TryDecimalMulSub64 closes the same total coefficient arithmetic over
// row-aligned, constant and nullable inputs. The caller owns output allocation.
func TryDecimalMulSub64(x, y *vector.Vector, c types.Decimal64, out *vector.Vector) bool {
	n := out.Length()
	if n == 0 {
		return true
	}
	if x.IsConstNull() || y.IsConstNull() {
		out.SetAllNulls(n)
		return true
	}
	xv, yv := vector.MustFixedColNoTypeCheck[types.Decimal64](x), vector.MustFixedColNoTypeCheck[types.Decimal64](y)
	xc, yc := x.IsConst(), y.IsConst()
	if xc && yc {
		dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
		if !decimalMulSub64(xv[:1], yv[:1], c, dst[:1]) {
			return false
		}
		for i := 1; i < n; i++ {
			dst[i] = dst[0]
		}
		return true
	}
	if !xc && !yc && !x.HasNull() && !y.HasNull() {
		dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
		if n >= decimalSIMDFusedMinRows && len(xv) == n && len(yv) == n && int64(xv[0]) == int64(int32(xv[0])) {
			ci, yi := int64(c), int64(yv[0])
			delta := ci - yi
			if ((ci^yi)&(ci^delta)) >= 0 && delta == int64(int32(delta)) {
				first := decimalMulSubSIMDPrefix(xv, yv, c, dst)
				return decimalMulSub64(xv[first:], yv[first:], c, dst[first:])
			}
		}
		return decimalMulSub64(xv, yv, c, dst)
	}
	dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
	if n == 1 {
		if x.IsNull(0) || y.IsNull(0) {
			out.GetNulls().Add(0)
			return true
		}
		out.GetNulls().Del(0)
		ci, yi := int64(c), int64(yv[0])
		delta := ci - yi
		if ((ci ^ yi) & (ci ^ delta)) >= 0 {
			dst[0] = d64MulInline(xv[0], types.Decimal64(delta))
			return true
		}
		return decimalMulSub64(xv[:1], yv[:1], c, dst[:1])
	}
	var xn, yn *nulls.Nulls
	if !xc {
		xn = x.GetNulls()
	}
	if !yc {
		yn = y.GetNulls()
	}
	if xn.HasBorrowedValidity() || yn.HasBorrowedValidity() {
		// GetBitmap materializes borrowed validity. Keep input leases immutable.
		for i := 0; i < n; i++ {
			if xn.Contains(uint64(i)) || yn.Contains(uint64(i)) {
				out.GetNulls().Add(uint64(i))
			}
		}
	} else {
		nulls.Or(xn, yn, out.GetNulls())
	}
	masked := !out.GetNulls().IsEmpty()
	bitmap := out.GetNulls().GetBitmap()
	for i := 0; i < n; i++ {
		if masked && bitmap.Contains(uint64(i)) {
			continue
		}
		xi, yi := i, i
		if xc {
			xi = 0
		}
		if yc {
			yi = 0
		}
		a, b := xv[xi], yv[yi]
		ci, bi := int64(c), int64(b)
		difference := ci - bi
		if ((ci ^ bi) & (ci ^ difference)) >= 0 {
			dst[i] = d64MulInline(a, types.Decimal64(difference))
			continue
		}
		ac, ab := d64MulInline(a, c), d64MulInline(a, b)
		var borrow uint64
		dst[i].B0_63, borrow = bits.Sub64(ac.B0_63, ab.B0_63, 0)
		dst[i].B64_127, _ = bits.Sub64(ac.B64_127, ab.B64_127, borrow)
	}
	return true
}
