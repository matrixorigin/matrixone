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
	if expr.GetP() != nil {
		// A semantic type does not certify the TEXT transport representation.
		// The parameter executor uses CAST for every non-string concrete type;
		// retain its diagnostic owner until the current binding is proved safe.
		typ := types.T(expr.Typ.Id)
		return typ != types.T_any && !typ.IsMySQLString()
	}
	if IsStatementConstantTemporalCall(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := DecodeOverloadID(fn.Func.Obj)
	switch id {
	case CAST:
		target := types.T(expr.Typ.Id)
		return len(fn.Args) > 0 && fn.Args[0] != nil &&
			types.T(fn.Args[0].Typ.Id).IsMySQLString() &&
			(target.ToType().IsNumeric() ||
				(fn.Args[0].GetP() != nil && target != types.T_any && !target.IsMySQLString()))
	case INTERNAL_JSON_COMPARISON_PARAM, INTERNAL_JSON_ORDERING_PARAM:
		return true
	default:
		return false
	}
}

// IsStatementConstantTemporalCall identifies a temporal diagnostic owner before
// considering whether a child conversion needs its own row-scoped owner.
func IsStatementConstantTemporalCall(expr *plan.Expr) bool {
	if expr == nil || !IsStatementConstantInput(expr) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := DecodeOverloadID(fn.Func.Obj)
	switch id {
	case CAST:
		switch types.T(expr.Typ.Id) {
		case types.T_time, types.T_date, types.T_datetime, types.T_timestamp:
			return true
		}
		return false
	case DATE, DATE_FORMAT:
		return IsTemporalTextDiagnosticCall(expr)
	case TIME, MAKETIME, SEC_TO_TIME, TIMESTAMP,
		DATE_ADD, DATE_SUB, TIMESTAMPADD, ADDTIME, SUBTIME, TIMEDIFF,
		PERIOD_ADD, PERIOD_DIFF:
		return true
	default:
		return false
	}
}

func isTemporalTextSource(t types.T) bool {
	return t == types.T_char || t == types.T_varchar || t == types.T_text
}

// IsTemporalTextDiagnosticCall identifies the parsing overloads whose warnings
// must be owned by the actual evaluation context.
func IsTemporalTextDiagnosticCall(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 || fn.Args[0] == nil {
		return false
	}
	source := types.T(fn.Args[0].Typ.Id)
	if !isTemporalTextSource(source) && !(source == types.T_any && fn.Args[0].GetP() != nil) {
		return false
	}
	id, overload := DecodeOverloadID(fn.Func.Obj)
	switch id {
	case CAST:
		if overload != 0 && overload != 1 {
			return false
		}
		switch types.T(expr.Typ.Id) {
		case types.T_date, types.T_datetime, types.T_time:
			return true
		}
	case DATE:
		return overload == 3 || overload == 4
	case DATE_FORMAT:
		return overload == 2
	}
	return false
}

// ContainsTemporalTextDiagnostic detects this effect in an expression tree.
func ContainsTemporalTextDiagnostic(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if IsTemporalTextDiagnosticCall(expr) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if ContainsTemporalTextDiagnostic(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if ContainsTemporalTextDiagnostic(arg) {
				return true
			}
		}
	}
	return false
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

// IsExplicitDecimalCast identifies conversions whose range diagnostics belong
// to each evaluated row, even when their source is a constant or a prepared
// integer transported as text. Syntax provenance and the explicit overload
// both survive independently in serialized plans.
func IsExplicitDecimalCast(expr *plan.Expr) bool {
	if expr == nil || !types.T(expr.Typ.Id).IsDecimal() {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 || fn.Args[0] == nil {
		return false
	}
	id, overload := DecodeOverloadID(fn.Func.Obj)
	source := types.T(fn.Args[0].Typ.Id)
	return id == CAST && (overload == 1 || fn.GetSyntaxExplicitCast()) &&
		((types.Type{Oid: source}).IsNumeric() || source.IsMySQLString())
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
				if IsExplicitDecimalCast(expr) || fn.GetSyntaxExplicitCast() && len(fn.Args) > 0 && fn.Args[0] != nil &&
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
