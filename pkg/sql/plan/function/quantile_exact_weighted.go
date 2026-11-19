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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

// quantileExactWeightedCheck validates the three plan arguments of
// quantileExactWeighted: [value, weight, level].
//
// The value argument follows the exact ordered-percentile admission. The
// weight argument stays in its concrete numeric type through registration so
// the ordinary numeric registration cast never touches a typed weight: only
// an untyped (T_any) weight expression — a bare numeric literal — normalizes
// to DOUBLE, the established percentile-parameter convention. The weight set is
// explicitly decision D4's (signed/unsigned integers, BIT, FLOAT32/64,
// DECIMAL64/128): DECIMAL256 is a numeric type but is rejected here so
// registration, saved-state sizing, and the executor rules describe one
// admission list. The level argument is validated in [0,1] by the compile
// time percentile configuration codec, the same contract as
// percentile_cont / percentile_disc.
func quantileExactWeightedCheck(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 3 {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}

	finalTypes := append([]types.Type(nil), inputs...)
	needCast := false

	// Arg[0] (value): the exact ordered-percentile numeric domain.
	if finalTypes[0].Oid == types.T_any {
		finalTypes[0] = types.T_float64.ToType()
		needCast = true
	} else if !aggexec.QuantileExactWeightedSupportedValueType(finalTypes[0]) {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}

	// Arg[1] (weight): decision D4's typed set, retained without a cast.
	if finalTypes[1].Oid == types.T_any {
		finalTypes[1] = types.T_float64.ToType()
		needCast = true
	} else if !aggexec.QuantileExactWeightedSupportedWeightType(finalTypes[1]) {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}

	// Arg[2] (level): numeric configuration; its range is checked by the
	// validated percentile config codec at physical construction.
	if finalTypes[2].Oid == types.T_any {
		finalTypes[2] = types.T_float64.ToType()
		needCast = true
	} else if !finalTypes[2].IsNumeric() ||
		finalTypes[2].Oid == types.T_decimal256 {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}

	if needCast {
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithSuccess(0)
}
