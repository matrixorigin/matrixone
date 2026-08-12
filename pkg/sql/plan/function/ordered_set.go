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
)

// orderedSetPercentileCheck validates the two plan arguments used by the
// ordered-set implementation: the value in the WITHIN GROUP ORDER BY clause
// followed by the percentile p. The parser keeps p as the only function
// argument, and the binder appends the order expression before overload
// resolution so the executor receives one data vector after compile-time
// configuration extraction.
func orderedSetPercentileCheck(_ []overload, inputs []types.Type) checkResult {
	return orderedSetPercentileCheckWithMode(inputs, false)
}

func orderedSetPercentileContCheck(_ []overload, inputs []types.Type) checkResult {
	return orderedSetPercentileCheckWithMode(inputs, true)
}

func orderedSetPercentileCheckWithMode(inputs []types.Type, continuous bool) checkResult {
	if len(inputs) != 2 {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}

	finalTypes := append([]types.Type(nil), inputs...)
	needCast := false
	if finalTypes[0].Oid == types.T_any {
		finalTypes[0] = types.T_float64.ToType()
		needCast = true
	}
	if finalTypes[1].Oid == types.T_any {
		finalTypes[1] = types.T_float64.ToType()
		needCast = true
	}
	if !finalTypes[0].IsNumeric() || !finalTypes[1].IsNumeric() {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}
	// The executor currently has exact implementations for the same numeric
	// family as MEDIAN (decimal256 is intentionally excluded).
	if finalTypes[0].Oid == types.T_decimal256 || finalTypes[1].Oid == types.T_decimal256 {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}
	if continuous && finalTypes[0].IsDecimal() && finalTypes[0].Width >= 38 {
		return newCheckResultWithFailure(failedAggParametersWrong)
	}
	if needCast {
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithSuccess(0)
}
