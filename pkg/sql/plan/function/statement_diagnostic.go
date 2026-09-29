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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// MayDiagnoseStatementParameter covers temporal evaluation and implicit numeric
// conversion whose EXECUTE-time value cannot be probed while planning. Keep
// row-scoped conversions out of the statement diagnostic owner.
func MayDiagnoseStatementParameter(expr *plan.Expr) bool {
	return MayDiagnoseStatementConstant(expr) && ContainsParameter(expr)
}

// MayDiagnoseStatementConstant also recognizes the literal expression left
// after a prepared parameter is materialized for this execution.
func MayDiagnoseStatementConstant(expr *plan.Expr) bool {
	if expr == nil || !IsStatementConstantInput(expr) ||
		ContainsRowScopedConversion(expr) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := DecodeOverloadID(fn.Func.Obj)
	switch id {
	case CAST:
		target := types.T(expr.Typ.Id)
		switch target {
		case types.T_time, types.T_date, types.T_datetime, types.T_timestamp:
			return true
		}
		return len(fn.Args) > 0 && fn.Args[0] != nil &&
			types.T(fn.Args[0].Typ.Id).IsMySQLString() && target.ToType().IsNumeric()
	case DATE:
		// DATE(text) reports an out-of-range SQL error for malformed input.
		// Typed temporal inputs use non-parsing overloads and need no owner.
		return len(fn.Args) > 0 && fn.Args[0] != nil &&
			types.T(fn.Args[0].Typ.Id).IsMySQLString()
	case TIME, MAKETIME, SEC_TO_TIME, TIMESTAMP,
		DATE_ADD, DATE_SUB, TIMESTAMPADD, ADDTIME, SUBTIME, TIMEDIFF,
		PERIOD_ADD, PERIOD_DIFF:
		return true
	default:
		return false
	}
}

// ContainsParameter reports a marker anywhere in an expression.
func ContainsParameter(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if ContainsParameter(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if ContainsParameter(item) {
				return true
			}
		}
	}
	return false
}

// IsStatementConstantInput excludes columns, session variables and volatile or
// real-time functions while admitting EXECUTE-time parameters.
func IsStatementConstantInput(expr *plan.Expr) bool {
	if expr != nil && expr.GetP() != nil {
		return true
	}
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func == nil {
			return false
		}
		f, ok := GetFunctionByIdWithoutError(fn.Func.GetObj())
		if !ok || f.CannotFold() || f.IsRealTimeRelated() || f.IsAgg() || f.IsWin() {
			return false
		}
		for _, arg := range fn.Args {
			if !IsStatementConstantInput(arg) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if !IsStatementConstantInput(arg) {
				return false
			}
		}
		return true
	}
	switch expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return true
	default:
		return false
	}
}

// ContainsRowScopedConversion preserves explicit numeric and assignment
// conversion diagnostics once per evaluated row, including nested conversions.
func ContainsRowScopedConversion(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil {
			id, _ := DecodeOverloadID(fn.Func.Obj)
			switch id {
			case CAST_STRICT, CAST_ASSIGN, CAST_IGNORE:
				return true
			case CAST:
				if fn.GetSyntaxExplicitCast() && len(fn.Args) > 0 && fn.Args[0] != nil &&
					types.T(fn.Args[0].Typ.Id).IsMySQLString() && types.T(expr.Typ.Id).ToType().IsNumeric() {
					return true
				}
			}
		}
		for _, arg := range fn.Args {
			if ContainsRowScopedConversion(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if ContainsRowScopedConversion(item) {
				return true
			}
		}
	}
	return false
}
