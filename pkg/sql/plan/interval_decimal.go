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
	"math/big"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// normalizeDecimalIntervalValue converts a constant DECIMAL interval to the
// microsecond representation used by date and window functions. Decimal256
// literals are represented as string-to-decimal casts in plan expressions, so
// this helper owns both direct decimal literals and that cast representation.
// Previously bound plans already contain their rounded int64 interval value;
// exact binding here affects only newly bound expressions.
// The returned negative flag describes the exact value before rounding; window
// frame validation uses it to reject a negative sub-microsecond bound.
func normalizeDecimalIntervalValue(
	expr *Expr, intervalType types.IntervalType,
) (value int64, negative, handled bool, err error) {
	if expr == nil {
		return 0, false, false, nil
	}
	switch types.T(expr.Typ.Id) {
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	default:
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

// decimalIntervalText returns the exact value after a constant decimal cast
// chain. Parsing through every target type is important for explicit casts
// whose target scale differs from the source literal's scale. Numeric source
// literals use the same textual representation as the decimal cast executor.
func decimalIntervalText(expr *Expr) (string, bool, error) {
	if expr == nil {
		return "", false, nil
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return "", false, nil
		}
		text, ok := decimalIntervalSourceText(expr)
		return text, ok, nil
	}

	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		return "", false, nil
	}
	name := fn.Func.GetObjName()
	if name == "unary_plus" || name == "unary_minus" {
		if len(fn.Args) != 1 {
			return "", false, nil
		}
		text, ok, err := decimalIntervalText(fn.Args[0])
		if err != nil || !ok {
			return "", false, err
		}
		if name == "unary_minus" {
			text = negateDecimalIntervalText(text)
		}
		return text, true, nil
	}
	if name != "cast" {
		return "", false, nil
	}
	explicit := decimalIntervalCastIsExplicit(fn)
	if floatText, handled, err := decimalIntervalFloatCastText(fn.Args[0], expr.Typ, explicit); handled || err != nil {
		return floatText, handled, err
	}
	text, ok, err := decimalIntervalText(fn.Args[0])
	if err != nil || !ok {
		return "", false, err
	}
	return decimalIntervalCastText(
		text,
		expr.Typ,
		explicit,
		decimalIntervalSourceIsBinary(fn.Args[0]),
	)
}

func negateDecimalIntervalText(text string) string {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "-") {
		return text[1:]
	}
	if strings.HasPrefix(text, "+") {
		return "-" + text[1:]
	}
	return "-" + text
}

// decimalIntervalFloatCastText keeps the source float's conversion semantics
// at the first decimal CAST. Converting the float to text first would route
// Decimal64/128 through decimal parsing and can disagree at binary-float
// rounding boundaries such as 1.005 -> DECIMAL(*, 2).
func decimalIntervalFloatCastText(
	source *Expr, typ planpb.Type, explicit bool,
) (string, bool, error) {
	value, ok := decimalIntervalFloatSourceValue(source)
	if !ok {
		return "", false, nil
	}
	scale := typ.Scale
	if scale < 0 {
		scale = 0
	}
	width := typ.Width
	if width <= 0 {
		switch types.T(typ.Id) {
		case types.T_decimal64:
			width = types.T_decimal64.ToType().Width
		case types.T_decimal128:
			width = types.T_decimal128.ToType().Width
		case types.T_decimal256:
			width = types.T_decimal256.ToType().Width
		default:
			return "", false, nil
		}
	}

	format := strconv.FormatFloat(value, 'g', -1, 64)
	switch types.T(typ.Id) {
	case types.T_decimal64:
		result, err := types.Decimal64FromFloat64(value, width, scale)
		if err != nil && explicit && !math.IsNaN(value) && !math.IsInf(value, 0) {
			result, err = planfunction.ParseExplicitDecimal64CastString(format, width, scale)
		}
		if err != nil {
			return "", false, err
		}
		return result.Format(scale), true, nil
	case types.T_decimal128:
		result, err := types.Decimal128FromFloat64(value, width, scale)
		if err != nil && explicit && !math.IsNaN(value) && !math.IsInf(value, 0) {
			result, err = planfunction.ParseExplicitDecimal128CastString(format, width, scale)
		}
		if err != nil {
			return "", false, err
		}
		return result.Format(scale), true, nil
	case types.T_decimal256:
		result, err := types.Decimal256FromFloat64(value, width, scale)
		if err != nil && explicit && !math.IsNaN(value) && !math.IsInf(value, 0) {
			result, err = planfunction.ParseExplicitDecimal256CastString(format, width, scale)
		}
		if err != nil {
			return "", false, err
		}
		return result.Format(scale), true, nil
	default:
		return "", false, nil
	}
}

func decimalIntervalFloatSourceValue(expr *Expr) (float64, bool) {
	if expr == nil || (types.T(expr.Typ.Id) != types.T_float32 && types.T(expr.Typ.Id) != types.T_float64) {
		return 0, false
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		switch value := lit.Value.(type) {
		case *planpb.Literal_Dval:
			return value.Dval, true
		case *planpb.Literal_Fval:
			return float64(value.Fval), true
		}
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return 0, false
	}
	value, ok := decimalIntervalFloatSourceValue(fn.Args[0])
	if !ok {
		return 0, false
	}
	switch fn.Func.GetObjName() {
	case "unary_plus":
		return value, true
	case "unary_minus":
		return -value, true
	default:
		return 0, false
	}
}

func decimalIntervalCastText(
	text string,
	typ planpb.Type,
	explicit, binary bool,
) (string, bool, error) {
	scale := typ.Scale
	if scale < 0 {
		scale = 0
	}
	width := typ.Width
	switch types.T(typ.Id) {
	case types.T_decimal64:
		if width <= 0 {
			width = types.T_decimal64.ToType().Width
		}
		value, err := parseDecimal64IntervalCast(text, width, scale, explicit, binary)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	case types.T_decimal128:
		if width <= 0 {
			width = types.T_decimal128.ToType().Width
		}
		value, err := parseDecimal128IntervalCast(text, width, scale, explicit, binary)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	case types.T_decimal256:
		if width <= 0 {
			width = types.T_decimal256.ToType().Width
		}
		value, err := parseDecimal256IntervalCast(text, width, scale, explicit, binary)
		if err != nil {
			return "", false, err
		}
		return value.Format(scale), true, nil
	default:
		return "", false, nil
	}
}

func parseDecimal64IntervalCast(
	text string, width, scale int32, explicit, binary bool,
) (types.Decimal64, error) {
	if binary {
		return types.ParseDecimal64FromByte(text, width, scale)
	}
	if explicit {
		return planfunction.ParseExplicitDecimal64CastString(text, width, scale)
	}
	return planfunction.ParseDecimal64CastString(text, width, scale)
}

func parseDecimal128IntervalCast(
	text string, width, scale int32, explicit, binary bool,
) (types.Decimal128, error) {
	if binary {
		return types.ParseDecimal128FromByte(text, width, scale)
	}
	if explicit {
		return planfunction.ParseExplicitDecimal128CastString(text, width, scale)
	}
	return planfunction.ParseDecimal128CastString(text, width, scale)
}

func parseDecimal256IntervalCast(
	text string, width, scale int32, explicit, binary bool,
) (types.Decimal256, error) {
	if binary {
		return types.ParseDecimal256FromByte(text, width, scale)
	}
	if explicit {
		return planfunction.ParseExplicitDecimal256CastString(text, width, scale)
	}
	return planfunction.ParseDecimal256CastString(text, width, scale)
}

func decimalIntervalCastIsExplicit(fn *planpb.Function) bool {
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.GetSyntaxExplicitCast() {
		return true
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
	return overload != 0
}

func decimalIntervalSourceIsBinary(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.GetIsBin()
	}
	return false
}

func decimalIntervalSourceText(expr *Expr) (string, bool) {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
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
	case *planpb.Literal_Dval:
		return strconv.FormatFloat(value.Dval, 'g', -1, 64), true
	case *planpb.Literal_Fval:
		return strconv.FormatFloat(float64(value.Fval), 'g', -1, 64), true
	case *planpb.Literal_I8Val:
		return strconv.FormatInt(int64(value.I8Val), 10), true
	case *planpb.Literal_I16Val:
		return strconv.FormatInt(int64(value.I16Val), 10), true
	case *planpb.Literal_I32Val:
		return strconv.FormatInt(int64(value.I32Val), 10), true
	case *planpb.Literal_I64Val:
		return strconv.FormatInt(value.I64Val, 10), true
	case *planpb.Literal_U8Val:
		return strconv.FormatUint(uint64(value.U8Val), 10), true
	case *planpb.Literal_U16Val:
		return strconv.FormatUint(uint64(value.U16Val), 10), true
	case *planpb.Literal_U32Val:
		return strconv.FormatUint(uint64(value.U32Val), 10), true
	case *planpb.Literal_U64Val:
		return strconv.FormatUint(value.U64Val, 10), true
	case *planpb.Literal_Bval:
		if value.Bval {
			return "1", true
		}
		return "0", true
	default:
		return "", false
	}
}

func makeDecimalIntervalValueExpr(source *Expr, value int64) *Expr {
	expr := makePlan2Int64ConstExprWithType(value)
	// Decimal256 interval normalization is a new exact plan contract even
	// when its source is a float or a cast chain without literal provenance.
	// Fence the rewritten value itself so an older CN cannot replay the old
	// integer-second fallback.
	if types.T(source.Typ.Id) == types.T_decimal256 || decimalIntervalRequiresProtocol(source) {
		expr.GetLit().DecimalLiteralRequiresV82 = true
	}
	return expr
}

func decimalIntervalRequiresProtocol(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if types.T(expr.Typ.Id) == types.T_decimal256 {
		return true
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
