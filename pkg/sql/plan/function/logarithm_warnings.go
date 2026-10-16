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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ScalarStrToDateNeedsRowEvaluation decides at doFold time whether one
// scalar STR_TO_DATE evaluation of the given constant arguments would emit a
// STR_TO_DATE diagnostic. Those constants must stay non-folded so the real
// batch length carries the per-row warning count (#30213, folded scalar
// warning-count collapse). target is the declared return type: the overload's
// third argument is a type marker rather than data.
func ScalarStrToDateNeedsRowEvaluation(parameters []*vector.Vector, target types.T, proc *process.Process) bool {
	if target != types.T_date && target != types.T_datetime && target != types.T_time {
		return false
	}
	if len(parameters) < 2 || parameters[0].IsConstNull() || parameters[1].IsConstNull() {
		// NULL operands produce values, not diagnostics.
		return false
	}
	if !parameters[0].IsConst() || !parameters[1].IsConst() {
		// Literal vectors are also accepted by the runtime folder; they are
		// not proven constants, so preserve their per-row evaluation.
		return true
	}
	return StrToDateConstantsWouldWarn(proc.Ctx, target,
		functionUtil.QuickBytesToStr(parameters[0].GetBytesAt(0)),
		functionUtil.QuickBytesToStr(parameters[1].GetBytesAt(0)))
}

// ScalarFormatNeedsRowEvaluation mirrors the STR_TO_DATE guard for FORMAT's
// locale warning (#30217, same folded-scalar collapse class): a NULL or
// unknown constant locale warns once per evaluated row, which a cached
// constant cannot represent. The 2-arg overload carries no locale and never
// warns.
func ScalarFormatNeedsRowEvaluation(parameters []*vector.Vector) bool {
	if len(parameters) < 3 {
		return false
	}
	locale := parameters[2]
	if locale.IsConstNull() {
		return true
	}
	if !locale.IsConst() {
		return true
	}
	return !format.IsValidLocale(functionUtil.QuickBytesToStr(locale.GetBytesAt(0)))
}

// ScalarDiagnosticNeedsRowEvaluation is the one runtime-fold guard every
// diagnostic-bearing scalar function shares. A cached scalar result cannot
// represent per-evaluated-row diagnostics, so constants whose single
// evaluation would warn must stay non-folded; the DIV/INTEGER_DIV/MOD case
// preserves the #30211 behavior verbatim. resultType is the declared
// function return type, which identifies the STR_TO_DATE overload target.
func ScalarDiagnosticNeedsRowEvaluation(fid int32, parameters []*vector.Vector, proc *process.Process, resultType types.Type) bool {
	switch fid {
	case DIV, INTEGER_DIV, MOD:
		return ScalarDivisionNeedsRowEvaluation(parameters, proc)
	case LN, LOG, LOG2, LOG10:
		return ScalarLogarithmNeedsRowEvaluation(parameters)
	case STR_TO_DATE:
		return ScalarStrToDateNeedsRowEvaluation(parameters, resultType.Oid, proc)
	case FORMAT:
		return ScalarFormatNeedsRowEvaluation(parameters)
	default:
		return false
	}
}

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

// ScalarLogarithmNeedsRowEvaluation is used only for scalar LN/LOG/LOG2/LOG10.
// A cached constant result cannot carry one warning per evaluated row, so the
// runtime folder must preserve constants whose single evaluation would warn.
// Safe positive constants and NULL operands keep the folding fast path
// (#30214).
func ScalarLogarithmNeedsRowEvaluation(parameters []*vector.Vector) bool {
	switch len(parameters) {
	case 1, 2:
	default:
		return false
	}
	left := parameters[0]
	if left.IsConstNull() {
		return false
	}
	if !left.IsConst() {
		// Literal vectors are also accepted by the runtime folder. They are
		// not a proven safe literal, so preserve their evaluation too.
		return true
	}
	if vector.GetFixedAtNoTypeCheck[float64](left, 0) <= 0 {
		return true
	}
	if len(parameters) == 2 {
		right := parameters[1]
		if right.IsConstNull() {
			return false
		}
		if !right.IsConst() {
			return true
		}
		if vector.GetFixedAtNoTypeCheck[float64](right, 0) <= 0 {
			return true
		}
		if vector.GetFixedAtNoTypeCheck[float64](left, 0) == 1 {
			return true
		}
	}
	return false
}
