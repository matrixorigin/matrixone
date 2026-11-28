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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	DefaultDivPrecisionIncrement int32 = 4
	maxDivPrecisionIncrement     int32 = 30
	maxDecimalDivisionPrecision  int32 = 65
	maxDecimalDivisionScale      int32 = 30
)

type divPrecisionIncrementKey struct{}

// WithDivPrecisionIncrement attaches the statement's div_precision_increment
// value to function binding. The value is resolved once by QueryBuilder; the
// bound result type then carries the scale needed by every execution CN.
func WithDivPrecisionIncrement(ctx context.Context, increment int32) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	increment = clampDivPrecisionIncrement(increment)
	if current, ok := ctx.Value(divPrecisionIncrementKey{}).(int32); ok && current == increment {
		return ctx
	}
	return context.WithValue(ctx, divPrecisionIncrementKey{}, increment)
}

func getDivPrecisionIncrement(ctx context.Context) int32 {
	if ctx != nil {
		if increment, ok := ctx.Value(divPrecisionIncrementKey{}).(int32); ok {
			return clampDivPrecisionIncrement(increment)
		}
	}
	return DefaultDivPrecisionIncrement
}

func clampDivPrecisionIncrement(increment int32) int32 {
	if increment < 0 {
		return 0
	}
	if increment > maxDivPrecisionIncrement {
		return maxDivPrecisionIncrement
	}
	return increment
}

// IsExactDivisionOperand reports whether oid belongs to the exact numeric or
// temporal domain whose `/` division produces DECIMAL under MySQL rules.
func IsExactDivisionOperand(oid types.T) bool {
	switch {
	case oid.IsDecimal(), oid.IsInteger(), oid == types.T_bit, oid == types.T_bool:
		return true
	case oid == types.T_date, oid == types.T_year, oid == types.T_time,
		oid == types.T_datetime, oid == types.T_timestamp:
		return true
	default:
		return false
	}
}

func exactDivisionOperandDimensions(source types.Type) (precision, scale int32, ok bool) {
	switch {
	case source.Oid.IsDecimal():
		precision = source.Width
		if precision <= 0 {
			precision = types.DecimalMaxPrecision(source.Oid)
		}
		return precision, max32(source.Scale, 0), true
	case source.Oid.IsInteger(), source.Oid == types.T_bit:
		return integerDecimalDigits(source.Oid), 0, true
	case source.Oid == types.T_bool:
		// TRUE/FALSE divide as 1/0: exactly one integral digit, scale zero.
		// Skipping this would strand wide quotients in Decimal128 and ignore
		// a nondefault div_precision_increment.
		return 1, 0, true
	case source.Oid == types.T_date:
		return 8, 0, true
	case source.Oid == types.T_year:
		return 4, 0, true
	case source.Oid == types.T_time, source.Oid == types.T_datetime, source.Oid == types.T_timestamp:
		scale = max32(source.Scale, 0)
		return 14 + scale, scale, true
	default:
		return 0, 0, false
	}
}

func exactDivisionOperandType(source, effective types.Type) (precision, scale int32, ok bool) {
	if source.Oid == types.T_any && effective.Oid.IsDecimal() {
		precision = effective.Width
		if precision <= 0 {
			precision = types.DecimalMaxPrecision(effective.Oid)
		}
		return precision, max32(effective.Scale, 0), true
	}
	if source.Oid.IsDateRelate() && !effective.Oid.IsDecimal() {
		return 0, 0, false
	}
	return exactDivisionOperandDimensions(source)
}

func exactDivisionTargetType(source types.Type, targetOid types.T) types.Type {
	if source.Oid == targetOid {
		return source
	}
	if precision, scale, ok := exactDivisionOperandDimensions(source); ok {
		return types.New(targetOid, precision, scale)
	}
	target := targetOid.ToType()
	target.Scale = 0
	return target
}

func commonExactDivisionDecimalOid(s1, s2 types.Type, p1, p2 int32) types.T {
	if s1.Oid == types.T_decimal256 || s2.Oid == types.T_decimal256 {
		return types.T_decimal256
	}
	if s1.Oid == types.T_decimal128 || s2.Oid == types.T_decimal128 || p1 > 18 || p2 > 18 {
		return types.T_decimal128
	}
	return types.T_decimal64
}

// divTypeCastRule resolves implicit operand casts for `/` independently of
// INTEGER_DIV: exact/exact (and exact/NULL) promote to a common DECIMAL type,
// while approximate/string/JSON/vector inputs keep fixedTypeCastRule2 behavior.
func divTypeCastRule(s1, s2 types.Type) (bool, types.Type, types.Type) {
	var promoted1, promoted2 bool
	s1, promoted1 = promoteBlockScaledVector(s1)
	s2, promoted2 = promoteBlockScaledVector(s2)
	if promoted1 || promoted2 {
		if cast, left, right := divTypeCastRule(s1, s2); cast {
			return true, left, right
		}
		return true, s1, s2
	}

	if s1.Oid.IsArrayRelate() || s2.Oid.IsArrayRelate() {
		return fixedTypeCastRule2(s1, s2)
	}

	s1Exact := IsExactDivisionOperand(s1.Oid)
	s2Exact := IsExactDivisionOperand(s2.Oid)
	if s1Exact && s2Exact {
		if s1.Oid == s2.Oid && s1.Oid.IsDecimal() {
			return false, s1, s2
		}
		p1, _, _ := exactDivisionOperandDimensions(s1)
		p2, _, _ := exactDivisionOperandDimensions(s2)
		targetOid := commonExactDivisionDecimalOid(s1, s2, p1, p2)
		return true, exactDivisionTargetType(s1, targetOid), exactDivisionTargetType(s2, targetOid)
	}
	if s1.Oid == types.T_any && s2Exact {
		p2, _, _ := exactDivisionOperandDimensions(s2)
		targetOid := commonExactDivisionDecimalOid(s2, s2, p2, p2)
		target := exactDivisionTargetType(s2, targetOid)
		return true, target, target
	}
	if s1Exact && s2.Oid == types.T_any {
		p1, _, _ := exactDivisionOperandDimensions(s1)
		targetOid := commonExactDivisionDecimalOid(s1, s1, p1, p1)
		target := exactDivisionTargetType(s1, targetOid)
		return true, target, target
	}
	if s1.Oid == types.T_bit || s2.Oid == types.T_bit {
		// T_bit is omitted from fixedBinaryCastRule2; pair it with approximate
		// or string/JSON operands using DOUBLE.
		other := s2
		if s2.Oid == types.T_bit {
			other = s1
		}
		if other.Oid.IsFloat() || isLowPrecFloat(other.Oid) || other.Oid.IsMySQLString() || other.Oid == types.T_json {
			return true, types.T_float64.ToType(), types.T_float64.ToType()
		}
	}
	return fixedTypeCastRule2(s1, s2)
}

// decimalDivisionReturnType follows MySQL's exact-value division rule:
// precision = left precision + right scale + div_precision_increment, and
// scale = left scale + div_precision_increment, capped by DECIMAL(65, 30).
func decimalDivisionReturnType(original, effective []types.Type, increment int32) (types.Type, bool) {
	if len(original) != 2 || len(effective) != 2 ||
		!effective[0].Oid.IsDecimal() || !effective[1].Oid.IsDecimal() {
		return types.Type{}, false
	}

	leftPrecision, leftScale, leftExact := exactDivisionOperandType(original[0], effective[0])
	_, rightScale, rightExact := exactDivisionOperandType(original[1], effective[1])
	if !leftExact || !rightExact {
		return types.Type{}, false
	}

	increment = clampDivPrecisionIncrement(increment)
	precision := leftPrecision + rightScale + increment
	if precision > maxDecimalDivisionPrecision {
		precision = maxDecimalDivisionPrecision
	}
	scale := leftScale + increment
	if scale > maxDecimalDivisionScale {
		scale = maxDecimalDivisionScale
	}
	if precision < scale {
		precision = scale
	}

	oid := types.T_decimal128
	if effective[0].Oid == types.T_decimal256 || effective[1].Oid == types.T_decimal256 || precision > 38 {
		oid = types.T_decimal256
	}
	return types.New(oid, precision, scale), true
}

func decimal256DivisionOperandType(source, effective types.Type) types.Type {
	precision, scale, ok := exactDivisionOperandType(source, effective)
	if !ok {
		precision, scale = effective.Width, effective.Scale
	}
	if precision <= 0 {
		precision = types.DecimalMaxPrecision(types.T_decimal256)
	}
	return types.New(types.T_decimal256, precision, scale)
}

func reconcileDivOperandTargets(result types.Type, original, effective []types.Type) (types.Type, types.Type, bool) {
	// The full packed TIME value can exceed Decimal64 when it has five or
	// six fractional digits. The other operand may also exceed that range.
	// Keep the narrower result when possible, but cast both operands to a
	// representation that can hold their declared precision.
	if result.Oid == types.T_decimal128 &&
		effective[0].Oid == types.T_decimal64 && effective[1].Oid == types.T_decimal64 {
		leftPrecision, leftScale, _ := exactDivisionOperandType(original[0], effective[0])
		rightPrecision, rightScale, _ := exactDivisionOperandType(original[1], effective[1])
		if leftPrecision > 18 || rightPrecision > 18 {
			return types.New(types.T_decimal128, leftPrecision, leftScale),
				types.New(types.T_decimal128, rightPrecision, rightScale),
				true
		}
	}

	if result.Oid == types.T_decimal256 &&
		(effective[0].Oid != types.T_decimal256 || effective[1].Oid != types.T_decimal256) {
		return decimal256DivisionOperandType(original[0], effective[0]),
			decimal256DivisionOperandType(original[1], effective[1]),
			true
	}
	return effective[0], effective[1], false
}

func (fr *FuncGetResult) applyDivPrecisionIncrement(ctx context.Context, original []types.Type) {
	if fr.fid != DIV || fr.overloadId != 0 || len(original) != 2 {
		return
	}

	effective := original
	if fr.needCast {
		effective = fr.targetTypes
	}
	result, ok := decimalDivisionReturnType(original, effective, getDivPrecisionIncrement(ctx))
	if !ok {
		return
	}
	fr.retType = result

	t1, t2, widened := reconcileDivOperandTargets(result, original, effective)
	if widened {
		fr.needCast = true
		fr.targetTypes = []types.Type{t1, t2}
	}
}
