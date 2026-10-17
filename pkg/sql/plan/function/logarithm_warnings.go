// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// appendLogarithmArgWarnings is the diagnostic counterpart of the LN/LOG/LOG2/
// LOG10 value kernels: the templates own the NULL results, this pass walks the
// rows after evaluation and reports one warning 3020 per evaluated row whose
// arguments are invalid. It is called only after a successful kernel run and
// mirrors appendDivisionByZeroWarnings: batches without NULL results or with
// every row masked do no per-row work (#30214).
func appendLogarithmArgWarnings(parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int, selectList *FunctionSelectList) {
	if length <= 0 || proc == nil || (!result.IsConstNull() && !result.GetNulls().Any()) {
		return
	}
	if selectList != nil && selectList.IgnoreAllRow() {
		return
	}
	binaryLog := len(parameters) == 2
	if !binaryLog && len(parameters) != 1 {
		return
	}
	left := parameters[0]
	var right *vector.Vector
	if binaryLog {
		right = parameters[1]
		if left.IsConstNull() || right.IsConstNull() {
			return
		}
	} else if left.IsConstNull() {
		return
	}
	lefts := vector.MustFixedColNoTypeCheck[float64](left)
	var rights []float64
	if binaryLog {
		rights = vector.MustFixedColNoTypeCheck[float64](right)
	}
	var warnings process.WarningAccumulator
	defer warnings.Reset()
	masked := selectList != nil && !selectList.ShouldEvalAllRow()
	for i := 0; i < length; i++ {
		row := uint64(i)
		if (masked && selectList.Contains(row)) || !result.IsNull(row) || left.IsNull(row) ||
			(binaryLog && right.IsNull(row)) {
			// NULL operands propagate NULL silently, like the division
			// diagnostics; only rows with two evaluated non-NULL arguments
			// that produce an invalid logarithm report warning 3020.
			continue
		}
		leftIndex := i
		if left.IsConst() {
			leftIndex = 0
		}
		invalid := lefts[leftIndex] <= 0
		if binaryLog {
			rightIndex := i
			if right.IsConst() {
				rightIndex = 0
			}
			invalid = invalid || lefts[leftIndex] == 1 || rights[rightIndex] <= 0
		}
		if !invalid {
			continue
		}
		if warnings.Total == 0 {
			warnings.SetWarningRetentionForProcess(proc)
		}
		warnings.Add(moerr.ER_INVALID_ARGUMENT_FOR_LOGARITHM, "Invalid argument for logarithm")
	}
	warnings.Flush(proc)
}
