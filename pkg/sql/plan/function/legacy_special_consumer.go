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

type legacySpecialConsumerContextKey struct{}

// WithLegacySpecialConsumers is only for validating pre-migration catalog
// expressions against their SQL origin in table DUMP/LOAD. It must never be
// used for new DDL or ordinary query binding.
func WithLegacySpecialConsumers(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacySpecialConsumerContextKey{}, true)
}

// LegacySpecialConsumers reports whether dump origin validation is using
// pre-migration execution signatures. It must not affect normal SQL binding.
func LegacySpecialConsumers(ctx context.Context) bool {
	value, _ := ctx.Value(legacySpecialConsumerContextKey{}).(bool)
	return value
}

func legacySpecialConsumerCheck(id int32, overloads []overload, inputs []types.Type) (checkResult, bool) {
	switch id {
	case FORMAT:
		return legacyFormatCheck(overloads[:FormatIntegerPrecisionOverload], inputs), true
	case MAKEDATE:
		return fixedTypeMatch(overloads[:MakeDateIntegerOverload], inputs), true
	case MAKETIME:
		return legacyMakeTimeCheck(overloads[:MakeTimeIntegerFloatOverload], inputs), true
	default:
		return checkResult{}, false
	}
}

// Keep the pre-migration FORMAT type checker byte-for-byte in behavior: dump
// verification compares the entire bound tree, including implicit CAST types.
func legacyFormatCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) < 2 || len(inputs) > 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if inputs[0].IsNumeric() {
		index := len(inputs) - 2
		targets := append([]types.Type(nil), inputs...)
		needsCast := false
		for i := 1; i < len(targets); i++ {
			if targets[i].Oid.IsMySQLString() {
				continue
			}
			targets[i] = formattedScalarStringType(targets[i])
			SetTargetScaleFromSource(&inputs[i], &targets[i])
			needsCast = true
		}
		if needsCast {
			return newCheckResultWithCast(index, targets)
		}
		return newCheckResultWithSuccess(index)
	}
	if inputs[0].Oid.IsDateRelate() {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	return fixedTypeMatch(overloads, inputs)
}

// Historical mixed-domain MAKETIME overload selection is used only to prove
// the SQL origin of an already persisted execution tree.
func legacyMakeTimeCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	exactSecond := isMakeTimeTextType(inputs[2].Oid) || inputs[2].Oid.IsDecimal()
	exactHour := inputs[0].Oid.IsDecimal()
	exactMinute := inputs[1].Oid.IsDecimal()
	if !isMakeTimeTextType(inputs[0].Oid) && !isMakeTimeTextType(inputs[1].Oid) && !exactHour && !exactMinute && !exactSecond {
		return fixedTypeMatch(overloads, inputs)
	}
	targetOids := []types.T{types.T_float64, types.T_float64, types.T_float64}
	if isMakeTimeTextType(inputs[0].Oid) {
		targetOids[0] = types.T_varchar
	} else if exactHour {
		if inputs[0].Oid == types.T_decimal256 {
			targetOids[0] = types.T_decimal256
		} else {
			targetOids[0] = types.T_decimal128
		}
	}
	if isMakeTimeTextType(inputs[1].Oid) {
		targetOids[1] = types.T_varchar
	} else if exactMinute {
		if inputs[1].Oid == types.T_decimal256 {
			targetOids[1] = types.T_decimal256
		} else {
			targetOids[1] = types.T_decimal128
		}
	}
	if exactSecond {
		targetOids[2] = types.T_varchar
	}
	status, _ := tryToMatch(inputs, targetOids)
	if status == matchFailed {
		return fixedTypeMatch(overloads, inputs)
	}
	for i, ov := range overloads {
		if len(ov.args) != len(targetOids) || ov.args[0] != targetOids[0] || ov.args[1] != targetOids[1] || ov.args[2] != targetOids[2] {
			continue
		}
		if status == matchDirectly && !exactSecond {
			return newCheckResultWithSuccess(i)
		}
		targets := make([]types.Type, len(inputs))
		for j := range targets {
			if inputs[j].Oid == targetOids[j] {
				targets[j] = inputs[j]
			} else {
				targets[j] = targetOids[j].ToType()
				SetTargetScaleFromSource(&inputs[j], &targets[j])
			}
		}
		if exactSecond {
			if inputs[2].Oid.IsDecimal() {
				targets[2].Scale = inputs[2].Scale
			} else {
				targets[2].Scale = -1
			}
		}
		return newCheckResultWithCast(i, targets)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}
