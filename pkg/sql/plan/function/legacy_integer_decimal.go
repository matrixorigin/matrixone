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
	if !hasInteger {
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
		if widened, ok := widenedDecimalArithmeticInputs(arithmetic, inputs, targets); ok {
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
