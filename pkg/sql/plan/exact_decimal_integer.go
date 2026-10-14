// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// exactDecimalIntegerFits proves the executed constant value without rounding
// or floating point. Unknown expressions retain the promoted comparison domain.
func exactDecimalIntegerFits(expr *plan.Expr, target types.T) bool {
	if !target.IsInteger() {
		return false
	}
	coefficient, scale, ok := exactDecimalCoefficient(expr)
	if !ok {
		return false
	}
	if scale > 0 {
		zeros, proven := decimal256TrailingZerosStatus(coefficient, scale)
		if !zeros || !proven {
			return false
		}
		var err error
		coefficient, err = coefficient.ScaleTruncate(-scale)
		if err != nil {
			return false
		}
	}
	var lower, upper types.Decimal256
	switch target {
	case types.T_int8:
		lower, upper = types.Decimal256FromInt64(math.MinInt8), types.Decimal256FromInt64(math.MaxInt8)
	case types.T_int16:
		lower, upper = types.Decimal256FromInt64(math.MinInt16), types.Decimal256FromInt64(math.MaxInt16)
	case types.T_int32:
		lower, upper = types.Decimal256FromInt64(math.MinInt32), types.Decimal256FromInt64(math.MaxInt32)
	case types.T_int64:
		lower, upper = types.Decimal256FromInt64(math.MinInt64), types.Decimal256FromInt64(math.MaxInt64)
	case types.T_uint8:
		upper = types.Decimal256{B0_63: math.MaxUint8}
	case types.T_uint16:
		upper = types.Decimal256{B0_63: math.MaxUint16}
	case types.T_uint32:
		upper = types.Decimal256{B0_63: math.MaxUint32}
	case types.T_uint64:
		upper = types.Decimal256{B0_63: math.MaxUint64}
	default:
		return false
	}
	return coefficient.Compare(lower) >= 0 && coefficient.Compare(upper) <= 0
}

func exactDecimalCoefficient(expr *plan.Expr) (types.Decimal256, int32, bool) {
	if expr == nil || !types.T(expr.Typ.Id).IsDecimal() || expr.Typ.Scale < 0 || expr.Typ.Scale > types.T(expr.Typ.Id).ToType().Width || expr.Typ.Width < 0 || expr.Typ.Width > types.T(expr.Typ.Id).ToType().Width || isExplicitPreparedCast(expr) {
		return types.Decimal256{}, 0, false
	}
	scale := expr.Typ.Scale
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		switch value := lit.Value.(type) {
		case *plan.Literal_Decimal64Val:
			if value.Decimal64Val != nil && expr.Typ.Id == int32(types.T_decimal64) {
				return types.Decimal256FromInt64(value.Decimal64Val.A), scale, true
			}
		case *plan.Literal_Decimal128Val:
			if value.Decimal128Val != nil && expr.Typ.Id == int32(types.T_decimal128) {
				return types.Decimal256FromDecimal128(types.Decimal128{B0_63: uint64(value.Decimal128Val.A), B64_127: uint64(value.Decimal128Val.B)}), scale, true
			}
		}
		return types.Decimal256{}, 0, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return types.Decimal256{}, 0, false
	}
	if (fn.Func.ObjName == "unary_minus" || fn.Func.ObjName == "unary_plus") && len(fn.Args) == 1 && expr.Typ.Id == fn.Args[0].Typ.Id && scale == fn.Args[0].Typ.Scale {
		value, _, ok := exactDecimalCoefficient(fn.Args[0])
		if fn.Func.ObjName == "unary_minus" {
			value = value.Minus()
		}
		return value, scale, ok
	}
	// Natural decimal tokens are carried by one implicit text-to-decimal cast.
	// Do not inspect a source behind an explicit or rescaling cast chain.
	if fn.Func.ObjName != "cast" || len(fn.Args) != 2 || expr.Typ.Width <= 0 || expr.Typ.Width > types.T(expr.Typ.Id).ToType().Width || scale > expr.Typ.Width {
		return types.Decimal256{}, 0, false
	}
	source := fn.Args[0]
	if !types.T(source.Typ.Id).IsMySQLString() || source.GetLit() == nil || source.GetLit().Isnull {
		return types.Decimal256{}, 0, false
	}
	text, ok := source.GetLit().Value.(*plan.Literal_Sval)
	if !ok || !isPlainDecimalLiteral(text.Sval) {
		return types.Decimal256{}, 0, false
	}
	value, sourceScale, err := types.Parse256(text.Sval)
	if err != nil || sourceScale != scale {
		return types.Decimal256{}, 0, false
	}
	// Validate the target carrier and declared precision as execution does.
	_, err = types.ParseDecimal256(text.Sval, expr.Typ.Width, scale)
	return value, scale, err == nil
}
