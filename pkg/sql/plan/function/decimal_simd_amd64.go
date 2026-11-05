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

func tryDecimalSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	if n < 256 || !archsimd.X86.AVX2() || x.IsConstNull() || y.IsConstNull() || x.HasNull() || y.HasNull() || x.HasGrouping() || y.HasGrouping() || (x.IsConst() && y.IsConst()) {
		return false
	}
	if op == MULTI && x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal128 {
		x, y = y, x
		xi, yi = yi, xi
	}
	xt, yt, rt := x.GetType().Oid, y.GetType().Oid, out.GetType().Oid
	if op == PLUS || op == MINUS {
		if x.GetType().Scale != out.GetType().Scale || y.GetType().Scale != out.GetType().Scale {
			return false
		}
		if xt == types.T_decimal128 && yt == types.T_decimal128 && (rt == types.T_decimal128 || rt == types.T_decimal256) {
			return decimal128PreparedSIMD(op, x, y, out, xi, yi, oi, n)
		}
		if xt != types.T_decimal64 || yt != types.T_decimal64 {
			return false
		}
		if rt == types.T_decimal64 {
			decimal64ResultSIMD(op, x, y, out, xi, yi, oi, n)
			return true
		}
		if rt == types.T_decimal128 {
			decimal64WideSIMD(op, x, y, out, xi, yi, oi, n)
			return true
		}
		return false
	}
	if op != MULTI || yt != types.T_decimal64 || (xt != types.T_decimal64 && xt != types.T_decimal128) || (rt != types.T_decimal128 && rt != types.T_decimal256) || out.GetType().Scale != x.GetType().Scale+y.GetType().Scale {
		return false
	}
	xc, yc := x.IsConst(), y.IsConst()
	if (xc && !decimalSIMDFirstNarrow(x, xi)) || (yc && !decimalSIMDFirstNarrow(y, yi)) {
		return false
	}
	if xt == types.T_decimal64 {
		if rt == types.T_decimal128 {
			if xc {
				return decimalMul64To128Left(x, y, out, xi, yi, oi, n)
			}
			if yc {
				return decimalMul64To128Right(x, y, out, xi, yi, oi, n)
			}
			return decimalMul64To128Flat(x, y, out, xi, yi, oi, n)
		} else {
			if xc {
				return decimalMul64To256Left(x, y, out, xi, yi, oi, n)
			}
			if yc {
				return decimalMul64To256Right(x, y, out, xi, yi, oi, n)
			}
			return decimalMul64To256Flat(x, y, out, xi, yi, oi, n)
		}
	} else {
		if rt == types.T_decimal128 {
			if xc {
				return decimalMul128To128Left(x, y, out, xi, yi, oi, n)
			}
			if yc {
				return decimalMul128To128Right(x, y, out, xi, yi, oi, n)
			}
			return decimalMul128To128Flat(x, y, out, xi, yi, oi, n)
		} else {
			if xc {
				return decimalMul128To256Left(x, y, out, xi, yi, oi, n)
			}
			if yc {
				return decimalMul128To256Right(x, y, out, xi, yi, oi, n)
			}
			return decimalMul128To256Flat(x, y, out, xi, yi, oi, n)
		}
	}
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

func decimalSIMDFitsLane32(v archsimd.Uint64x4) bool {
	return v.Equal(v.AsInt64x4().ShiftAllLeft(32).ShiftAllRight(32).AsUint64x4()).ToBits() == 15
}
