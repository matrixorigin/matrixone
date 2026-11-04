//go:build amd64 && go1.27 && goexperiment.simd

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
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func decimalSIMDFits32(lo, hi archsimd.Uint64x4) bool {
	narrow := lo.AsInt64x4().ShiftAllLeft(32).ShiftAllRight(32).AsUint64x4()
	sign := lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
	return lo.Equal(narrow).And(hi.Equal(sign)).ToBits() == 15
}
func tryDecimalSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	if n < 256 || !archsimd.X86.AVX2() || x.IsConstNull() || y.IsConstNull() || x.HasNull() || y.HasNull() || x.HasGrouping() || y.HasGrouping() || (x.IsConst() && y.IsConst()) {
		return false
	}
	if op == MULTI && x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal128 {
		x, y = y, x
		xi, yi = yi, xi
	}
	xt, yt, rt := x.GetType().Oid, y.GetType().Oid, out.GetType().Oid
	if xt != types.T_decimal64 && xt != types.T_decimal128 {
		return false
	}
	if yt != types.T_decimal64 {
		return false
	}
	if op == MULTI {
		if (rt != types.T_decimal128 && rt != types.T_decimal256) || out.GetType().Scale != x.GetType().Scale+y.GetType().Scale {
			return false
		}
	} else {
		if (op != PLUS && op != MINUS) || xt != types.T_decimal64 || (rt != types.T_decimal64 && rt != types.T_decimal128) || x.GetType().Scale != out.GetType().Scale || y.GetType().Scale != out.GetType().Scale {
			return false
		}
	}
	xd, yd, od := types.DecodeSlice[uint64](x.GetData()), types.DecodeSlice[uint64](y.GetData()), types.DecodeSlice[uint64](out.GetData())
	xc, yc := x.IsConst(), y.IsConst()
	ones := archsimd.BroadcastUint64x4(^uint64(0))
	i := 0
	for ; i+4 <= n; i += 4 {
		var a, ah, b, bh archsimd.Uint64x4
		if xt == types.T_decimal64 {
			if xc {
				a = archsimd.BroadcastUint64x4(xd[0])
			} else {
				a = archsimd.LoadUint64x4(xd[xi+i : xi+i+4])
			}
			ah = a.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		} else if xc {
			a = archsimd.BroadcastUint64x4(xd[0])
			ah = archsimd.BroadcastUint64x4(xd[1])
		} else {
			at := (xi + i) * 2
			l, h := archsimd.LoadUint64x4(xd[at:at+4]), archsimd.LoadUint64x4(xd[at+4:at+8])
			even, odd := l.ConcatPermute128Scalars(0, 2, h), l.ConcatPermute128Scalars(1, 3, h)
			a, ah = even.InterleaveLoGrouped(odd), even.InterleaveHiGrouped(odd)
		}
		if yc {
			b = archsimd.BroadcastUint64x4(yd[0])
		} else {
			b = archsimd.LoadUint64x4(yd[yi+i : yi+i+4])
		}
		bh = b.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		var lo, hi archsimd.Uint64x4
		switch op {
		case MULTI:
			if !decimalSIMDFits32(a, ah) || !decimalSIMDFits32(b, bh) {
				if i == 0 {
					return false
				}
				// Scalar span preserves committed prefix and processes each remaining row once.
				decimalSIMDScalarSpan(op, x, y, out, xi+i, yi+i, oi+i, n-i)
				return true
			}
			lo = a.AsInt32x8().MulWidenEven(b.AsInt32x8()).AsUint64x4()
			hi = lo.AsInt64x4().ShiftAllRight(63).AsUint64x4()
		case PLUS:
			lo = a.Add(b)
			carry := a.And(b).Or(a.Or(b).And(lo.Xor(ones))).ShiftAllRight(63)
			hi = ah.Add(bh).Add(carry)
		case MINUS:
			lo = a.Sub(b)
			borrow := a.Xor(ones).And(b).Or(a.Xor(b).Xor(ones).And(lo)).ShiftAllRight(63)
			hi = ah.Sub(bh).Sub(borrow)
		}
		at := oi + i
		switch rt {
		case types.T_decimal64:
			lo.Store(od[at : at+4])
		case types.T_decimal128:
			l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
			l.ConcatPermute128Scalars(0, 2, h).Store(od[at*2 : at*2+4])
			l.ConcatPermute128Scalars(1, 3, h).Store(od[at*2+4 : at*2+8])
		case types.T_decimal256:
			l, h := lo.InterleaveLoGrouped(hi), lo.InterleaveHiGrouped(hi)
			m, n := hi.InterleaveLoGrouped(hi), hi.InterleaveHiGrouped(hi)
			l.ConcatPermute128Scalars(0, 2, m).Store(od[at*4 : at*4+4])
			h.ConcatPermute128Scalars(0, 2, n).Store(od[at*4+4 : at*4+8])
			l.ConcatPermute128Scalars(1, 3, m).Store(od[at*4+8 : at*4+12])
			h.ConcatPermute128Scalars(1, 3, n).Store(od[at*4+12 : at*4+16])
		}
	}
	if i < n {
		decimalSIMDScalarSpan(op, x, y, out, xi+i, yi+i, oi+i, n-i)
	}
	return true
}

// Only the admitted exact families reach this remainder, so it needs neither
// error-producing general arithmetic nor another speculative SIMD check.
func decimalSIMDScalarSpan(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) {
	if x.GetType().Oid == types.T_decimal64 {
		decimalChunk64(op, x, y, out, xi, yi, oi, n)
		return
	}
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
}

// Avoid entering the large vector frame for an obviously wide first operand.
// This is a refusal hint only; the full block guard remains authoritative.
func decimalSIMDQuickCheck(op int32, x, y *vector.Vector, xi, yi int) bool {
	if op != MULTI {
		return true
	}
	return decimalSIMDFirstNarrow(x, xi) && decimalSIMDFirstNarrow(y, yi)
}
func decimalSIMDFirstNarrow(v *vector.Vector, at int) bool {
	if v.IsConstNull() {
		return false
	}
	if v.IsConst() {
		at = 0
	}
	switch v.GetType().Oid {
	case types.T_decimal64:
		x := int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, at))
		return x == int64(int32(x))
	case types.T_decimal128:
		x := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, at)
		return x.B64_127 == uint64(int64(x.B0_63)>>63) && int64(x.B0_63) == int64(int32(x.B0_63))
	default:
		return false
	}
}
