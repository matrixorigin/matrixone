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
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// normalizeDecimalIntervalValue converts a constant decimal interval to the
// microsecond representation used by date and window functions. Decimal256
// literals are represented as string-to-decimal casts in plan expressions, so
// this helper owns both direct decimal literals and that cast representation.
// The returned negative flag describes the exact value before rounding; window
// frame validation uses it to reject a negative sub-microsecond bound.
func normalizeDecimalIntervalValue(
	expr *Expr, intervalType types.IntervalType,
) (value int64, negative, handled bool, err error) {
	if expr == nil || !types.T(expr.Typ.Id).IsDecimal() {
		return 0, false, false, nil
	}
	multiplier, ok := intervalMicrosecondMultiplier(intervalType)
	if !ok {
		return 0, false, false, nil
	}

	text, ok, err := decimalIntervalText(expr)
	if err != nil || !ok {
		return 0, false, false, err
	}
	rational, ok := new(big.Rat).SetString(text)
	if !ok {
		return 0, false, false, moerr.NewInvalidInputNoCtxf(
			"invalid decimal interval value %q", text)
	}
	negative = rational.Sign() < 0

	rational.Mul(rational, new(big.Rat).SetInt64(multiplier))
	truncated, remainder := new(big.Int).QuoRem(
		new(big.Int).Abs(rational.Num()), rational.Denom(), new(big.Int))
	// math.Round rounds half away from zero. Apply the same rule to the exact
	// rational value so Decimal256 never loses the deciding fractional digits
	// through float64 conversion.
	doubledRemainder := new(big.Int).Lsh(new(big.Int).Set(remainder), 1)
	if doubledRemainder.Cmp(rational.Denom()) >= 0 {
		truncated.Add(truncated, big.NewInt(1))
	}
	if rational.Sign() < 0 {
		truncated.Neg(truncated)
	}
	if !truncated.IsInt64() {
		return 0, false, false, moerr.NewOutOfRangeNoCtxf(
			"int64", "decimal interval value %q", text)
	}
	return truncated.Int64(), negative, true, nil
}

func intervalMicrosecondMultiplier(intervalType types.IntervalType) (int64, bool) {
	switch intervalType {
	case types.Second:
		return int64(types.MicroSecsPerSec), true
	case types.Minute:
		return int64(types.MicroSecsPerSec * types.SecsPerMinute), true
	case types.Hour:
		return int64(types.MicroSecsPerSec * types.SecsPerHour), true
	case types.Day:
		return int64(types.MicroSecsPerSec * types.SecsPerDay), true
	default:
		return 0, false
	}
}

// decimalIntervalText returns the exact value after an optional constant
// decimal cast. Parsing through the target type is important for explicit
// casts whose target scale differs from the source literal's scale.
func decimalIntervalText(expr *Expr) (string, bool, error) {
	source := expr
	casted := false
	if source.GetLit() == nil {
		fn := source.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
			return "", false, nil
		}
		source = fn.Args[0]
		casted = true
	}
	lit := source.GetLit()
	if lit == nil || lit.Isnull {
		return "", false, nil
	}
	text, ok := decimalIntervalSourceText(source)
	if !ok {
		return "", false, nil
	}
	if !casted {
		return text, true, nil
	}

	scale := expr.Typ.Scale
	if scale < 0 {
		scale = 0
	}
	width := expr.Typ.Width
	switch types.T(expr.Typ.Id) {
	case types.T_decimal64:
		if width <= 0 {
			width = types.T_decimal64.ToType().Width
		}
		value, err := types.ParseDecimal64(text, width, scale)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	case types.T_decimal128:
		if width <= 0 {
			width = types.T_decimal128.ToType().Width
		}
		value, err := types.ParseDecimal128(text, width, scale)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	case types.T_decimal256:
		if width <= 0 {
			width = types.T_decimal256.ToType().Width
		}
		value, err := types.ParseDecimal256(text, width, scale)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	default:
		return "", false, nil
	}
}

func decimalIntervalSourceText(expr *Expr) (string, bool) {
	lit := expr.GetLit()
	if lit == nil {
		return "", false
	}
	scale := expr.Typ.Scale
	if scale < 0 {
		scale = 0
	}
	switch value := lit.Value.(type) {
	case *planpb.Literal_Decimal64Val:
		return types.Decimal64(value.Decimal64Val.A).Format(scale), true
	case *planpb.Literal_Decimal128Val:
		decimal := types.Decimal128{
			B0_63:   uint64(value.Decimal128Val.A),
			B64_127: uint64(value.Decimal128Val.B),
		}
		return decimal.Format(scale), true
	case *planpb.Literal_Sval:
		return value.Sval, true
	default:
		return "", false
	}
}

func makeDecimalIntervalValueExpr(source *Expr, value int64) *Expr {
	expr := makePlan2Int64ConstExprWithType(value)
	if decimalIntervalRequiresProtocol(source) {
		expr.GetLit().DecimalLiteralRequiresV82 = true
	}
	return expr
}

func decimalIntervalRequiresProtocol(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.DecimalLiteralRequiresV82
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return false
	}
	return decimalIntervalRequiresProtocol(fn.Args[0])
}
