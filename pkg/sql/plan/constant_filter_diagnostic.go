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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	return ContainsStatementInvariantFilterDiagnosticWithProof(proc, expr, false)
}

// ContainsStatementInvariantFilterDiagnosticWithProof applies the current
// binding's diagnostic proof while retaining literal and row-scoped warnings.
func ContainsStatementInvariantFilterDiagnosticWithProof(proc *process.Process, expr *plan.Expr, provenFree bool) bool {
	if proc == nil || proc.Base == nil || expr == nil {
		return false
	}
	return containsStatementInvariantFilterDiagnostic(proc, expr, provenFree)
}

// ContainsGuardedJoinDiagnostic identifies a diagnostic below row-dependent
// flow control. A hash join cannot activate that operand before key matching
// without guessing which rows select the arm.
func ContainsGuardedJoinDiagnostic(proc *process.Process, expr *plan.Expr) bool {
	return ContainsGuardedJoinDiagnosticWithProof(proc, expr, false)
}

// ContainsGuardedJoinDiagnosticWithProof accepts only a proof for the current
// execution's bound plan. It still checks literal and row-scoped diagnostics.
func ContainsGuardedJoinDiagnosticWithProof(proc *process.Process, expr *plan.Expr, provenFree bool) bool {
	if proc == nil || proc.Base == nil || expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && !function.IsStatementConstantInput(expr) {
			id, _ := function.DecodeOverloadID(fn.Func.Obj)
			if (id == function.CASE || id == function.IFF || id == function.COALESCE) &&
				containsStatementInvariantFilterDiagnostic(proc, expr, provenFree) {
				return true
			}
		}
		for _, arg := range fn.Args {
			if ContainsGuardedJoinDiagnosticWithProof(proc, arg, provenFree) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if ContainsGuardedJoinDiagnosticWithProof(proc, item, provenFree) {
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
	if function.MayDiagnoseStatementParameter(expr) && !provenFree {
		return true
	}
	if fn := expr.GetF(); fn != nil {
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

// PreparedPlanDiagnosticCandidates collects maximal statement-constant
// diagnostic expressions in predicate owners. Their references belong to p
// and must be discarded with that plan generation. Collecting the minimal
// probes once avoids searching every predicate tree on each EXECUTE.
func PreparedPlanDiagnosticCandidates(p *plan.Plan) []*plan.Expr {
	return preparedPlanDiagnosticCandidates(p, false)
}

// PreparedPlanRuntimeDiagnosticCandidates also includes diagnostic literals
// produced by value specialization. A cached runtime plan must recheck them
// for its current binding even when no ParamRef remains in that expression.
func PreparedPlanRuntimeDiagnosticCandidates(p *plan.Plan) []*plan.Expr {
	return preparedPlanDiagnosticCandidates(p, true)
}

func preparedPlanDiagnosticCandidates(p *plan.Plan, includeMaterialized bool) []*plan.Expr {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	var candidates []*plan.Expr
	seen := make(map[*plan.Expr]struct{})
	var addProbe func(*plan.Expr) bool
	addProbe = func(expr *plan.Expr) bool {
		if expr == nil || !containsStatementDiagnostic(expr, true) {
			return false
		}
		if function.IsStatementConstantInput(expr) && !function.ContainsRowScopedConversion(expr) {
			if _, found := seen[expr]; !found {
				seen[expr] = struct{}{}
				candidates = append(candidates, expr)
			}
			return true
		}
		found := false
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				found = addProbe(arg) || found
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				found = addProbe(item) || found
			}
		}
		return found
	}
	add := func(exprs []*plan.Expr) {
		for _, expr := range exprs {
			if !containsStatementParameterDiagnostic(expr) &&
				(!includeMaterialized || !containsStatementDiagnostic(expr, true)) {
				continue
			}
			foundProbe := addProbe(expr)
			// A new diagnostic with row-dependent inputs may have no constant
			// child. Keep its original conservative probe until it has a
			// dedicated static descriptor.
			if !foundProbe {
				if _, found := seen[expr]; !found {
					seen[expr] = struct{}{}
					candidates = append(candidates, expr)
				}
			}
		}
	}
	for _, node := range p.GetQuery().Nodes {
		if node == nil {
			continue
		}
		add(node.OnList)
		add(node.FilterList)
		add(node.BlockFilterList)
		if node.VectorIndexScan != nil {
			add(node.VectorIndexScan.PreFilters)
		}
	}
	return candidates
}

// PreparedPlanHasJoinParameterDiagnostic is retained for existing callers.
func PreparedPlanHasJoinParameterDiagnostic(p *plan.Plan) bool {
	return len(PreparedPlanDiagnosticCandidates(p)) != 0
}

// CompletePreparedDiagnosticBlockFilters restores zone-map pruning for
// parameterized scan predicates admitted by this binding's diagnostic proof.
// PREPARE may omit a block copy while the parameter is still unknown. Only
// predicates already admitted to the reader are considered here.
const PreparedBlockFilterDisabledScanOption = "matrixone:prepared_block_filter_disabled"

func CompletePreparedDiagnosticBlockFilters(
	ctx context.Context, node *plan.Node, storageFilters, blockFilters []*plan.Expr,
) []*plan.Expr {
	// ExtraOptions records an explicit blockFilter=2 decision in a prepared
	// plan. Other scan options are not ours to reinterpret as a stats omission.
	if node == nil || node.NodeType != plan.Node_TABLE_SCAN || node.ExtraOptions != "" {
		return blockFilters
	}
	selected := blockFilters
	for _, expr := range storageFilters {
		if !containsStatementParameterDiagnostic(expr) || !ExprIsZonemappable(ctx, expr) {
			continue
		}
		duplicate := false
		for _, existing := range selected {
			if blockFilterSemanticallyEquivalent(existing, expr) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			if len(selected) == len(blockFilters) {
				selected = append([]*plan.Expr(nil), blockFilters...)
			}
			selected = append(selected, DeepCopyExpr(expr))
		}
	}
	return selected
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
	if p == nil || p.GetQuery() == nil {
		return false, nil
	}
	return ProbePreparedDiagnosticCandidates(proc, PreparedPlanRuntimeDiagnosticCandidates(p))
}

// ProbePreparedDiagnosticCandidates checks the current binding without
// publishing warnings. The caller must probe both conservative and optimized
// template candidates before using a relaxed plan.
func ProbePreparedDiagnosticCandidates(proc *process.Process, candidates []*plan.Expr) (bool, error) {
	return ProbePreparedDiagnosticCandidatesWithProof(proc, candidates, nil)
}

// ProbePreparedDiagnosticCandidatesWithProof allows the caller that decoded
// this binding's protocol parameters to prove a narrow conversion directly.
// The callback must return false whenever its current-value proof is absent.
func ProbePreparedDiagnosticCandidatesWithProof(
	proc *process.Process, candidates []*plan.Expr, fastSafe func(*plan.Expr) bool,
) (bool, error) {
	if proc == nil || proc.Base == nil {
		return false, nil
	}
	for _, expr := range candidates {
		if fastSafe != nil && fastSafe(expr) {
			continue
		}
		safe, err := ProbeStatementParameterDiagnosticFree(proc, expr)
		if !safe || err != nil {
			return safe, err
		}
	}
	return true, nil
}

// PreparedDirectImplicitIntegerCastParam identifies only the bare conversion
// whose integer packet can be range-proved by the frontend. Composite
// expressions and explicit row-scoped casts still use diagnostic evaluation.
func PreparedDirectImplicitIntegerCastParam(expr *plan.Expr) (int32, types.T, bool) {
	if expr == nil {
		return 0, types.T_any, false
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 && types.T(expr.Typ.Id).IsInteger() {
		return param.Pos, types.T(expr.Typ.Id), true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.GetSyntaxExplicitCast() || len(fn.Args) != 2 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil {
		return 0, types.T_any, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	source, target := types.T(fn.Args[0].Typ.Id), types.T(expr.Typ.Id)
	// A target-domain proof must also certify the inner semantic parameter
	// conversion. Only a strictly narrower signed domain implies that proof.
	typedNarrowing := source.IsSignedInt() && target.IsSignedInt() && target.TypeLen() < source.TypeLen()
	if id != function.CAST || (!source.IsMySQLString() && !typedNarrowing) {
		return 0, types.T_any, false
	}
	param := fn.Args[0].GetP()
	if param == nil || param.Pos < 0 || fn.Args[1].Typ.Id != expr.Typ.Id {
		return 0, types.T_any, false
	}
	return param.Pos, types.T(expr.Typ.Id), true
}

// ProbeStatementParameterDiagnosticFree checks the current parameter binding
// without publishing warnings. A predicate can be copied to a storage reader
// only when its statement-constant conversions are diagnostic-free.
func ProbeStatementParameterDiagnosticFree(proc *process.Process, expr *plan.Expr) (bool, error) {
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
			if isStatementConversionError(err) {
				return false, nil
			}
			return false, err
		}
		return !warned, nil
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			safe, err := ProbeStatementParameterDiagnosticFree(proc, arg)
			if !safe || err != nil {
				return safe, err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			safe, err := ProbeStatementParameterDiagnosticFree(proc, item)
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

// isStatementConversionError identifies SQL diagnostics that must retain their
// runtime owner. Cancellation, memory and internal failures are not diagnostics.
func isStatementConversionError(err error) bool {
	if errors.Is(err, strconv.ErrSyntax) || errors.Is(err, strconv.ErrRange) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	for _, code := range [...]uint16{
		moerr.ErrDivByZero, moerr.ErrOutOfRange, moerr.ErrDataTruncated,
		moerr.ErrInvalidArg, moerr.ErrTruncatedWrongValueForField,
		moerr.ErrTruncatedWrongValue, moerr.ErrInvalidInput,
		moerr.ErrWrongDatetimeSpec, moerr.ErrWrongArguments,
	} {
		if moerr.IsMoErrCode(err, code) {
			return true
		}
	}
	return false
}
