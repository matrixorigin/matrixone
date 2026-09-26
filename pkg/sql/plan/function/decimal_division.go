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

func exactDivisionOperandType(source, effective types.Type) (precision, scale int32, ok bool) {
	switch {
	case source.Oid.IsDecimal():
		precision = source.Width
		if precision <= 0 {
			precision = decimalStorageWidth(source.Oid)
		}
		return precision, max32(source.Scale, 0), true
	case source.Oid.IsInteger():
		return integerDecimalDigits(source.Oid), 0, true
	case source.Oid == types.T_bit:
		return integerDecimalDigits(source.Oid), 0, true
	case source.Oid.IsDateRelate() && effective.Oid.IsDecimal():
		// Temporal decimals contain packed calendar fields, not an arbitrary
		// value spanning the storage width of the implicit decimal cast.
		switch source.Oid {
		case types.T_date:
			return 8, 0, true
		case types.T_year:
			return 4, 0, true
		case types.T_time, types.T_datetime, types.T_timestamp:
			scale = max32(source.Scale, 0)
			return 14 + scale, scale, true
		}
		return 0, 0, false
	case source.Oid == types.T_any && effective.Oid.IsDecimal():
		precision = effective.Width
		if precision <= 0 {
			precision = decimalStorageWidth(effective.Oid)
		}
		return precision, max32(effective.Scale, 0), true
	default:
		return 0, 0, false
	}
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
		precision = decimalStorageWidth(types.T_decimal256)
	}
	return types.New(types.T_decimal256, precision, scale)
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

	// The full packed TIME value can exceed Decimal64 when it has five or
	// six fractional digits. The other operand may also exceed that range.
	// Keep the narrower result when possible, but cast both operands to a
	// representation that can hold their declared precision.
	if result.Oid == types.T_decimal128 &&
		effective[0].Oid == types.T_decimal64 && effective[1].Oid == types.T_decimal64 {
		leftPrecision, leftScale, _ := exactDivisionOperandType(original[0], effective[0])
		rightPrecision, rightScale, _ := exactDivisionOperandType(original[1], effective[1])
		if leftPrecision > 18 || rightPrecision > 18 {
			fr.needCast = true
			fr.targetTypes = []types.Type{
				types.New(types.T_decimal128, leftPrecision, leftScale),
				types.New(types.T_decimal128, rightPrecision, rightScale),
			}
		}
	}

	if result.Oid == types.T_decimal256 &&
		(effective[0].Oid != types.T_decimal256 || effective[1].Oid != types.T_decimal256) {
		fr.needCast = true
		fr.targetTypes = []types.Type{
			decimal256DivisionOperandType(original[0], effective[0]),
			decimal256DivisionOperandType(original[1], effective[1]),
		}
	}
}
