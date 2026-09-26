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
		if function.MayDiagnoseStatementParameter(expr) {
			return true
		}
		if isExecutionConstant(expr) && !function.ContainsRowScopedConversion(expr) {
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
	return function.IsStatementConstantInput(expr) && !function.ContainsParameter(expr)
}
