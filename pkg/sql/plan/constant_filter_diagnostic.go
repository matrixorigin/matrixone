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
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type preparedJoinDiagnosticFreeKey struct{}

// WithPreparedJoinDiagnosticFree marks one execution-local replan after its
// normalized prepared ON operands were evaluated without diagnostics.
func WithPreparedJoinDiagnosticFree(ctx context.Context) context.Context {
	return context.WithValue(ctx, preparedJoinDiagnosticFreeKey{}, true)
}

func preparedJoinDiagnosticFree(ctx context.Context) bool {
	return ctx != nil && ctx.Value(preparedJoinDiagnosticFreeKey{}) == true
}

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
	return containsStatementInvariantFilterDiagnostic(proc, expr, false)
}

// ContainsGuardedJoinDiagnostic identifies a diagnostic below row-dependent
// flow control. A hash join cannot activate that operand before key matching
// without guessing which rows select the arm.
func ContainsGuardedJoinDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && !function.IsStatementConstantInput(expr) {
			id, _ := function.DecodeOverloadID(fn.Func.Obj)
			if (id == function.CASE || id == function.IFF || id == function.COALESCE) &&
				ContainsStatementInvariantFilterDiagnostic(proc, expr) {
				return true
			}
		}
		for _, arg := range fn.Args {
			if ContainsGuardedJoinDiagnostic(proc, arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if ContainsGuardedJoinDiagnostic(proc, item) {
				return true
			}
		}
	}
	return false
}

func containsStatementInvariantFilterDiagnostic(proc *process.Process, expr *plan.Expr, provenFree bool) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if function.MayDiagnoseStatementParameter(expr) {
			if !provenFree {
				return true
			}
		}
		if isExecutionConstant(expr) && !function.ContainsRowScopedConversion(expr) {
			_, free, warned, err := rule.EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
			if free != nil {
				free()
			}
			return warned || err != nil
		}
		for _, arg := range fn.Args {
			if containsStatementInvariantFilterDiagnostic(proc, arg, provenFree) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsStatementInvariantFilterDiagnostic(proc, item, provenFree) {
				return true
			}
		}
	}
	return false
}

// PreparedPlanHasJoinParameterDiagnostic is computed once per prepared plan
// generation so ordinary executions do not scan their query trees.
func PreparedPlanHasJoinParameterDiagnostic(p *plan.Plan) bool {
	if p == nil || p.GetQuery() == nil {
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType != plan.Node_JOIN {
			continue
		}
		for _, expr := range node.OnList {
			if containsStatementParameterDiagnostic(expr) {
				return true
			}
		}
	}
	return false
}

func containsStatementParameterDiagnostic(expr *plan.Expr) bool {
	return containsStatementDiagnostic(expr, false)
}

func containsStatementDiagnostic(expr *plan.Expr, materialized bool) bool {
	if expr == nil {
		return false
	}
	if (materialized && function.MayDiagnoseStatementConstant(expr)) ||
		(!materialized && function.MayDiagnoseStatementParameter(expr)) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsStatementDiagnostic(arg, materialized) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if containsStatementDiagnostic(item, materialized) {
				return true
			}
		}
	}
	return false
}

// ProbePreparedJoinParameterDiagnostics checks the actual normalized parameter
// values without touching the statement warning sink or cached plan. A SQL
// diagnostic keeps the conservative plan; resource and internal failures fail
// the execution rather than being mistaken for SQL diagnostics.
func ProbePreparedJoinParameterDiagnostics(proc *process.Process, p *plan.Plan) (bool, error) {
	if proc == nil || proc.Base == nil || p == nil || p.GetQuery() == nil {
		return false, nil
	}
	// The scoped optimizer flag only relaxes JOIN ON and filter pushdown.
	// Projection, grouping, and ordering keep their execution owners; a
	// diagnostic there must not disable an unrelated selective JOIN plan.
	// Visit all nodes, including auxiliary/subquery nodes outside Steps, since
	// those predicates can still be changed by the scoped replan.
	for _, node := range p.GetQuery().Nodes {
		for _, exprs := range [...][]*plan.Expr{node.OnList, node.FilterList, node.BlockFilterList} {
			for _, expr := range exprs {
				safe, err := probeJoinParameterExpression(proc, expr)
				if !safe || err != nil {
					return safe, err
				}
			}
		}
		if scan := node.VectorIndexScan; scan != nil {
			for _, expr := range scan.PreFilters {
				safe, err := probeJoinParameterExpression(proc, expr)
				if !safe || err != nil {
					return safe, err
				}
			}
		}
	}
	return true, nil
}

func probeJoinParameterExpression(proc *process.Process, expr *plan.Expr) (bool, error) {
	if expr == nil || !containsStatementDiagnostic(expr, true) {
		return true, nil
	}
	// Runtime specialization can replace a ParamRef with a literal. Probe the
	// resulting maximal constant expression as well as the original parameter
	// expression; checking only ContainsParameter would silently miss it.
	if function.IsStatementConstantInput(expr) && !function.ContainsRowScopedConversion(expr) {
		_, free, warned, err := rule.EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
		if free != nil {
			free()
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false, err
			}
			for _, code := range [...]uint16{
				moerr.ErrDivByZero, moerr.ErrOutOfRange, moerr.ErrDataTruncated,
				moerr.ErrInvalidArg, moerr.ErrTruncatedWrongValueForField,
				moerr.ErrTruncatedWrongValue, moerr.ErrInvalidInput,
				moerr.ErrWrongDatetimeSpec, moerr.ErrWrongArguments,
			} {
				if moerr.IsMoErrCode(err, code) {
					return false, nil
				}
			}
			return false, err
		}
		return !warned, nil
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			safe, err := probeJoinParameterExpression(proc, arg)
			if !safe || err != nil {
				return safe, err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			safe, err := probeJoinParameterExpression(proc, item)
			if !safe || err != nil {
				return safe, err
			}
		}
	}
	return true, nil
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
