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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ContainsConstantFilterDiagnostic probes a logical constant under a filter
// with an isolated warning sink. A diagnostic expression must retain one
// execution owner: copying it into a join or set-operation branch changes the
// number of warnings and can evaluate an inactive expression.
func ContainsConstantFilterDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	if proc == nil || proc.Base == nil || expr == nil {
		return false
	}
	return containsConstantFilterDiagnostic(proc, expr)
}

// ContainsStatementInvariantFilterDiagnostic excludes explicit numeric CASTs,
// whose existing conversion policy reports once per evaluated row.
func ContainsStatementInvariantFilterDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	if proc == nil || proc.Base == nil || expr == nil {
		return false
	}
	return containsStatementInvariantFilterDiagnostic(proc, expr)
}

func containsStatementInvariantFilterDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if mayDiagnoseFromStatementParameter(expr) {
			return true
		}
		if isExecutionConstant(expr) && !containsExplicitNumericCast(expr) {
			_, free, warned, err := rule.EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
			if free != nil {
				free()
			}
			return warned || err != nil
		}
		for _, arg := range fn.Args {
			if containsStatementInvariantFilterDiagnostic(proc, arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsStatementInvariantFilterDiagnostic(proc, item) {
				return true
			}
		}
	}
	return false
}

func mayDiagnoseFromStatementParameter(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || containsExplicitNumericCast(expr) ||
		!containsParameter(expr) || !isStatementInvariantInput(expr) {
		return false
	}
	switch fn.Func.GetObjName() {
	case "cast":
		return len(fn.Args) > 0 && fn.Args[0] != nil &&
			types.T(fn.Args[0].Typ.Id).IsMySQLString() &&
			types.T(expr.Typ.Id).ToType().IsNumeric()
	case "time", "addtime", "subtime", "timediff", "date_add", "date_sub":
		return true
	default:
		return false
	}
}

func containsParameter(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsParameter(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsParameter(item) {
				return true
			}
		}
	}
	return false
}

func isStatementInvariantInput(expr *plan.Expr) bool {
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
		f, ok := function.GetFunctionByIdWithoutError(fn.Func.GetObj())
		if !ok || f.CannotFold() || f.IsRealTimeRelated() {
			return false
		}
		for _, arg := range fn.Args {
			if !isStatementInvariantInput(arg) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if !isStatementInvariantInput(arg) {
				return false
			}
		}
		return true
	}
	return isExecutionConstant(expr)
}

func containsExplicitNumericCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.GetSyntaxExplicitCast() && fn.Func != nil && fn.Func.GetObjName() == "cast" &&
			len(fn.Args) > 0 && fn.Args[0] != nil &&
			types.T(fn.Args[0].Typ.Id).IsMySQLString() &&
			types.T(expr.Typ.Id).ToType().IsNumeric() {
			return true
		}
		for _, arg := range fn.Args {
			if containsExplicitNumericCast(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsExplicitNumericCast(item) {
				return true
			}
		}
	}
	return false
}

func containsConstantFilterDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if isExecutionConstant(expr) {
			_, free, warned, err := rule.EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
			if free != nil {
				free()
			}
			return warned || err != nil
		}
		for _, arg := range fn.Args {
			if containsConstantFilterDiagnostic(proc, arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsConstantFilterDiagnostic(proc, item) {
				return true
			}
		}
	}
	return false
}

// The optimizer deliberately leaves CASE unfolded so it can short-circuit at
// execution. We may still probe an entirely literal CASE for its active-arm
// diagnostics without publishing the probe's warning or changing the plan.
func isExecutionConstant(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return true
	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		f, ok := function.GetFunctionByIdWithoutError(e.F.Func.GetObj())
		if !ok || f.CannotFold() || f.IsRealTimeRelated() {
			return false
		}
		for _, arg := range e.F.Args {
			if !isExecutionConstant(arg) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if !isExecutionConstant(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
