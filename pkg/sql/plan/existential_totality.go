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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Existential MARK joins may hash their equality operands over more input rows
// than a nested loop would visit. Only expose expressions whose evaluation is
// total and free of observable effects. This proof is deliberately narrower
// than arbitrary deterministic expressions: an unchosen CASE arm must also
// be safe, and a cast may neither raise an error nor emit a warning.
func existentialPredicateIsTotal(expr *plan.Expr, storedTextIsBounded func(*plan.Expr) bool) bool {
	if expr == nil || types.T(expr.Typ.Id) != types.T_bool {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return existentialValueIsTotal(expr, storedTextIsBounded)
	}
	if fn.Func == nil {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.AND, function.OR, function.XOR:
		return len(fn.Args) == 2 && existentialPredicateIsTotal(fn.Args[0], storedTextIsBounded) && existentialPredicateIsTotal(fn.Args[1], storedTextIsBounded)
	case function.NOT:
		return len(fn.Args) == 1 && existentialPredicateIsTotal(fn.Args[0], storedTextIsBounded)
	case function.EQUAL, function.NOT_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL,
		function.LESS_THAN, function.LESS_EQUAL, function.IS, function.ISNOT:
		return len(fn.Args) == 2 && existentialValueIsTotal(fn.Args[0], storedTextIsBounded) &&
			existentialValueIsTotal(fn.Args[1], storedTextIsBounded) &&
			comparisonEvaluationIsTotal(id, fn.Args[0].Typ, fn.Args[1].Typ)
	case function.ISNULL, function.ISNOTNULL, function.ISTRUE, function.ISNOTTRUE,
		function.ISFALSE, function.ISNOTFALSE:
		return len(fn.Args) == 1 && existentialValueIsTotal(fn.Args[0], storedTextIsBounded)
	default:
		return false
	}
}

func existentialValueIsTotal(expr *plan.Expr, storedTextIsBounded func(*plan.Expr) bool) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return isTruncationSafePredicateValue(expr)
	}
	if fn.Func == nil {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
		return false
	}
	id, variant := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.CAST:
		// Assignment and set-operation casts have additional session-dependent
		// behavior; certify only normal, explicit and comparison casts here.
		if variant < 0 || variant > 2 || len(fn.Args) != 2 || fn.Args[1].GetT() == nil ||
			!isSameColumnType(fn.Args[1].Typ, expr.Typ) || !existentialValueIsTotal(fn.Args[0], storedTextIsBounded) {
			return false
		}
		if singleRowCastIsTotal(fn.Args[0].Typ, expr.Typ) || existentialStringCastIsTotal(fn.Args[0].Typ, expr.Typ) {
			return true
		}
		// Stored ordinary TEXT has a 65535-byte assignment limit. Width zero
		// on a computed TEXT expression does not establish that bound, so ask
		// the input-graph proof to trace this value to an actual column.
		return storedTextIsBounded != nil && storedTextIsBounded(fn.Args[0]) &&
			(types.T(expr.Typ.Id) == types.T_char || types.T(expr.Typ.Id) == types.T_varchar) &&
			expr.Typ.Width >= types.MaxStringSize
	case function.CASE:
		// The vector CASE implementation selects string values without a
		// conversion. Require each condition and every possible result arm to
		// be total, even when that arm is usually unreachable.
		if variant != 0 || len(fn.Args) < 2 ||
			(types.T(expr.Typ.Id) != types.T_char && types.T(expr.Typ.Id) != types.T_varchar) {
			return false
		}
		for i := 0; i+1 < len(fn.Args); i += 2 {
			if !existentialPredicateIsTotal(fn.Args[i], storedTextIsBounded) || fn.Args[i+1].Typ.Id != expr.Typ.Id ||
				!existentialValueIsTotal(fn.Args[i+1], storedTextIsBounded) {
				return false
			}
		}
		if len(fn.Args)%2 == 1 {
			last := fn.Args[len(fn.Args)-1]
			return last.Typ.Id == expr.Typ.Id && existentialValueIsTotal(last, storedTextIsBounded)
		}
		return true
	default:
		return false
	}
}

// Normal, explicit and comparison string casts operate on runes; their
// non-assignment paths do not validate a target collation or emit truncation
// warnings. A known source width fitting the target therefore cannot truncate.
// Integer formatting is ASCII and has a fixed maximum including the sign.
func existentialStringCastIsTotal(source, target plan.Type) bool {
	targetID := types.T(target.Id)
	if (targetID != types.T_char && targetID != types.T_varchar) || target.Width <= 0 {
		return false
	}
	sourceID := types.T(source.Id)
	if sourceID == types.T_char || sourceID == types.T_varchar {
		return source.Width > 0 && source.Width <= target.Width
	}
	digits := integerDecimalDigits(sourceID)
	if digits == 0 {
		return false
	}
	_, signed := integerTypeDomain(sourceID)
	if signed {
		digits++
	}
	return digits <= target.Width
}
