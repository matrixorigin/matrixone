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

type legacyIntegerDecimalContextKey struct{}

// WithLegacyIntegerDecimalCoercions reconstructs complete pre-width-check
// bindings only for DUMP/LOAD origin verification. It does not change runtime
// casts, declared assignments or explicit CAST contracts.
func WithLegacyIntegerDecimalCoercions(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyIntegerDecimalContextKey{}, true)
}

func legacyIntegerDecimalCoercions(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(legacyIntegerDecimalContextKey{}).(bool)
	return value
}

func legacyIntegerDecimalCheck(id int32, inputs []types.Type, current checkResult) checkResult {
	hasInteger := false
	for _, input := range inputs {
		hasInteger = hasInteger || input.IsIntOrUint() || input.Oid == types.T_bit
	}
	hasDecimalArithmetic := (id == PLUS || id == MINUS || id == MULTI) && len(inputs) == 2 &&
		(inputs[0].Oid.IsDecimal() || inputs[1].Oid.IsDecimal())
	if !hasInteger && !hasDecimalArithmetic {
		return current
	}
	if (id == BETWEEN && len(inputs) == 3) || (id == IN_RANGE && len(inputs) == 4) {
		// Mixed-width bounds can make the current BETWEEN checker fail before
		// the binder falls back to binary comparisons. Reconstruct the original
		// range signature independently; that fallback is not the stored tree.
		if id == IN_RANGE && inputs[3].Oid != types.T_uint8 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		has0, left, lower := fixedTypeCastRule1(inputs[0], inputs[1])
		has1, right, upper := fixedTypeCastRule1(inputs[0], inputs[2])
		if !left.Oid.IsDecimal() || !right.Oid.IsDecimal() {
			return current
		}
		targets := []types.Type{left, lower, upper}
		if id == IN_RANGE {
			aligned, ok := alignDecimalComparisonTypesMode(targets, inputs[:3], true)
			if !ok {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			has0 = has0 || aligned
			targets = append(targets, inputs[3])
		} else if left.Oid != right.Oid || left.Scale != lower.Scale || right.Scale != upper.Scale {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if !otherCompareOperatorSupports(targets[0], targets[1]) || !otherCompareOperatorSupports(targets[0], targets[2]) {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		// Both historical range functions have overload 0, including when the
		// current result is a failure and supplies no usable overload identity.
		if has0 || has1 {
			return newCheckResultWithCast(0, targets)
		}
		return newCheckResultWithSuccess(0)
	}
	if current.status != succeedWithCast {
		return current
	}
	if len(inputs) != 2 {
		return current
	}
	switch id {
	case EQUAL, NULL_SAFE_EQUAL, NOT_EQUAL, GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL,
		PLUS, MINUS, MULTI, MOD, INTEGER_DIV:
	default:
		return current
	}
	_, left, right := fixedTypeCastRule1(inputs[0], inputs[1])
	if id == INTEGER_DIV {
		_, left, right = fixedTypeCastRule2(inputs[0], inputs[1])
	}
	if !left.Oid.IsDecimal() || !right.Oid.IsDecimal() {
		return current
	}
	targets := []types.Type{left, right}
	var arithmetic string
	switch id {
	case PLUS:
		arithmetic = "+"
	case MINUS:
		arithmetic = "-"
	case MULTI:
		arithmetic = "*"
	}
	if id == EQUAL || id == NULL_SAFE_EQUAL {
		if _, ok := alignDecimalComparisonTypesMode(targets, inputs, true); !ok {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	if id == MULTI {
		wideUnsigned := inputs[0].Oid == types.T_uint64 || inputs[0].Oid == types.T_bit ||
			inputs[1].Oid == types.T_uint64 || inputs[1].Oid == types.T_bit
		if wideUnsigned && left.Oid == types.T_decimal64 && right.Oid == types.T_decimal64 {
			targets = []types.Type{types.New(types.T_decimal128, 38, left.Scale), types.New(types.T_decimal128, 38, right.Scale)}
		}
	}
	if arithmetic != "" {
		if widened, ok := legacyWidenedDecimalArithmeticInputs(arithmetic, inputs, targets); ok {
			return newCheckResultWithCast(current.idx, widened)
		}
	}
	if id == MULTI {
		wideUnsigned := inputs[0].Oid == types.T_uint64 || inputs[0].Oid == types.T_bit ||
			inputs[1].Oid == types.T_uint64 || inputs[1].Oid == types.T_bit
		if left.Oid == types.T_decimal128 && right.Oid == types.T_decimal128 && !wideUnsigned &&
			(inputs[0].Oid == types.T_decimal64 || inputs[1].Oid == types.T_decimal64) &&
			inputs[0].Oid != types.T_decimal128 && inputs[1].Oid != types.T_decimal128 {
			for i := range targets {
				targets[i] = types.T_decimal64.ToType()
				SetTargetScaleFromSource(&inputs[i], &targets[i])
			}
		}
	}
	return newCheckResultWithCast(current.idx, targets)
}

// These formulas reconstruct the pre-intermediate-precision binding only for
// persisted-origin verification. Normal arithmetic uses the current domains.
func legacyDecimalArithmeticDomain(input types.Type) (width, scale int32, ok bool) {
	if input.Oid.IsDecimal() {
		width = input.Width
		if width <= 0 {
			width = input.Oid.ToType().Width
		}
		scale = max(input.Scale, int32(0))
		return width, scale, true
	}
	if input.IsIntOrUint() {
		return integerIntegralWidth(input.Oid), 0, true
	}
	// BIT participates in exact numeric coercion as an unsigned integer even
	// though it is intentionally outside IsIntOrUint. BIT(64) spans the full
	// uint64 domain, so reserve all 20 decimal digits when deriving a widened
	// arithmetic result.
	if input.Oid == types.T_bit {
		return 20, 0, true
	}
	return 0, 0, false
}

// legacyWidenedDecimalArithmeticInputs derives the Decimal256 cast domains from the
// original numeric operands. fixedTypeCastRule1 intentionally uses a full
// Decimal128 envelope for mixed decimal/integer operands, so deciding from its
// output alone loses the integer's actual 3..20 digit domain and can either
// miss a required widening or make small literal arithmetic unnecessarily wide.
func legacyWidenedDecimalArithmeticInputs(operator string, inputs, coerced []types.Type) ([]types.Type, bool) {
	if len(inputs) != 2 || len(coerced) != 2 ||
		coerced[0].Oid != types.T_decimal128 || coerced[1].Oid != types.T_decimal128 {
		return nil, false
	}

	leftWidth, leftScale, leftOK := legacyDecimalArithmeticDomain(inputs[0])
	rightWidth, rightScale, rightOK := legacyDecimalArithmeticDomain(inputs[1])
	if !leftOK || !rightOK || (!inputs[0].Oid.IsDecimal() && !inputs[1].Oid.IsDecimal()) {
		return nil, false
	}

	var precision int32
	switch operator {
	case "+", "-":
		scale := max(leftScale, rightScale)
		integerDigits := max(leftWidth-leftScale, rightWidth-rightScale)
		precision = integerDigits + scale + 1
	case "*":
		precision = leftWidth + rightWidth
	default:
		return nil, false
	}
	if precision <= types.T_decimal128.ToType().Width {
		return nil, false
	}
	return []types.Type{
		types.New(types.T_decimal256, leftWidth, leftScale),
		types.New(types.T_decimal256, rightWidth, rightScale),
	}, true
}

func legacyDecimalAddSubReturnType(parameters []types.Type) types.Type {
	scale := max(parameters[0].Scale, parameters[1].Scale)
	if parameters[0].Oid == types.T_decimal256 || parameters[1].Oid == types.T_decimal256 {
		integerDigits := max(
			parameters[0].Width-parameters[0].Scale,
			parameters[1].Width-parameters[1].Scale,
		)
		return types.New(types.T_decimal256, min(integerDigits+scale+1, int32(65)), scale)
	}
	if parameters[0].Oid == types.T_decimal128 || parameters[1].Oid == types.T_decimal128 {
		return types.New(types.T_decimal128, 38, scale)
	}
	return types.New(types.T_decimal64, 18, scale)
}

func legacyDecimalMultiplyReturnType(parameters []types.Type) types.Type {
	scale := int32(12)
	scale1, scale2 := parameters[0].Scale, parameters[1].Scale
	if scale1 > scale {
		scale = scale1
	}
	if scale2 > scale {
		scale = scale2
	}
	if scale1+scale2 < scale {
		scale = scale1 + scale2
	}
	if parameters[0].Oid == types.T_decimal256 || parameters[1].Oid == types.T_decimal256 {
		return types.New(types.T_decimal256, min(parameters[0].Width+parameters[1].Width, int32(65)), scale)
	}
	return types.New(types.T_decimal128, 38, scale)
}

func (fr *FuncGetResult) applyLegacyDecimalArithmeticReturnType(inputs []types.Type) {
	if fr.overloadId != 0 {
		return
	}
	parameters := inputs
	if fr.needCast {
		parameters = fr.targetTypes
	}
	if len(parameters) != 2 || !parameters[0].Oid.IsDecimal() {
		return
	}
	switch fr.fid {
	case PLUS, MINUS:
		fr.retType = legacyDecimalAddSubReturnType(parameters)
	case MULTI:
		fr.retType = legacyDecimalMultiplyReturnType(parameters)
	}
}
