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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// captureTemporalOriginalOwner records admission before optimizer rewrites
// can remove expressions or replace a scan. Every subsequent candidate must
// also pass the current-plan checks; this snapshot grants no placement or
// cost permission by itself.
func (builder *QueryBuilder) captureTemporalOriginalOwner(rootID int32) {
	if builder == nil || builder.qry == nil || !builder.hasTemporalPlanningIntent || !builder.temporalPlanningIntent.ParameterFree {
		return
	}
	ctx := context.Background()
	if builder.compCtx != nil {
		ctx = builder.GetContext()
	}
	budget := 4096
	owner := *builder.qry
	owner.Steps = append(append([]int32(nil), owner.Steps...), rootID)
	for id, node := range owner.Nodes {
		if node != nil && node.NodeType == plan.Node_TABLE_SCAN && temporalCandidateSimplePathSafe(&owner, int32(id)) {
			if builder.temporalOriginalSafeScans == nil {
				builder.temporalOriginalSafeScans = make(map[int32]bool)
			}
			builder.temporalOriginalSafeScans[int32(id)] = temporalOriginalOwnerFits(ctx, &owner, int32(id), &budget) && temporalOriginalSingleMixedLeaf(&owner, int32(id))
		}
	}
}

type temporalCandidateAtom struct {
	column *plan.Expr
	bound  *plan.Expr
	op     types.TemporalOp
	scale  int32
}

// temporalCandidateAtomForExpr identifies a single oriented comparison. It is
// only a shape and eager-bound proof, not the required query-level admission.
func temporalCandidateAtomForExpr(expr *plan.Expr) (temporalCandidateAtom, bool) {
	if expr == nil || !temporalCandidatePredicateSafe(expr) {
		return temporalCandidateAtom{}, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return temporalCandidateAtom{}, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	var op types.TemporalOp
	switch id {
	case function.EQUAL:
		op = types.TemporalEqual
	case function.LESS_THAN:
		op = types.TemporalLess
	case function.LESS_EQUAL:
		op = types.TemporalLessEqual
	case function.GREAT_THAN:
		op = types.TemporalGreater
	case function.GREAT_EQUAL:
		op = types.TemporalGreaterEqual
	default:
		return temporalCandidateAtom{}, false
	}
	column, bound := fn.Args[0], fn.Args[1]
	if column.GetCol() == nil {
		column, bound = bound, column
		switch op {
		case types.TemporalLess:
			op = types.TemporalGreater
		case types.TemporalLessEqual:
			op = types.TemporalGreaterEqual
		case types.TemporalGreater:
			op = types.TemporalLess
		case types.TemporalGreaterEqual:
			op = types.TemporalLessEqual
		}
	}
	if column.GetCol() == nil || !isDatetimeTimestampTypePair(types.T(column.Typ.Id), types.T(bound.Typ.Id)) || !temporalCandidateBoundSafe(bound) {
		return temporalCandidateAtom{}, false
	}
	scale := column.Typ.Scale
	if types.T(bound.Typ.Id) == types.T_timestamp {
		scale = bound.Typ.Scale
		if clock := bound.GetF(); clock != nil {
			wanted := int32(0)
			if len(clock.Args) == 1 {
				wanted, _ = temporalFunctionFSPFromPlanExpr(clock.Args[0])
			}
			if scale != wanted {
				return temporalCandidateAtom{}, false
			}
		}
	}
	if scale < 0 || scale > 6 {
		return temporalCandidateAtom{}, false
	}
	return temporalCandidateAtom{column: column, bound: bound, op: op, scale: scale}, true
}

// temporalCandidateBoundSafe is a positive proof for optional eager evaluation.
// Prepared parameter provenance must be checked separately before using it.
func temporalCandidateBoundSafe(expr *plan.Expr) bool {
	if expr == nil || temporalCandidateHasUnprovedSource(expr) {
		return false
	}
	if literal := expr.GetLit(); literal != nil {
		// A materialized parameter may have literal syntax. The query-level
		// admission gate must reject unknown or unproved provenance separately.
		return literal.Src == nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || types.T(expr.Typ.Id) != types.T_timestamp {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.CURRENT_TIMESTAMP || overload != 0 {
		return false
	}
	if _, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj); !ok {
		return false
	}
	switch len(fn.Args) {
	case 0:
		return true
	case 1:
		_, ok := temporalFunctionFSPFromPlanExpr(fn.Args[0])
		return ok
	default:
		return false
	}
}

// temporalCandidatePredicateSafe proves each original predicate, not merely
// the bound used by an optional candidate. It does not authorize a candidate by
// itself: the scan, ancestor and prepared-provenance gates are also required.
func temporalCandidatePredicateSafe(expr *plan.Expr) bool {
	if expr == nil || types.T(expr.Typ.Id) != types.T_bool || temporalCandidateHasUnprovedSource(expr) {
		return false
	}
	if isTruncationSafePredicateExpr(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if _, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj); !ok {
		return false
	}
	if id == function.AND || id == function.OR {
		return len(fn.Args) == 2 && temporalCandidatePredicateSafe(fn.Args[0]) && temporalCandidatePredicateSafe(fn.Args[1])
	}
	if !isTruncationSafeEquality(id) && !isTruncationSafeOrdering(id) || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	left, right := fn.Args[0], fn.Args[1]
	if left.GetCol() == nil {
		left, right = right, left
	}
	return left.GetCol() != nil && isDatetimeTimestampTypePair(types.T(left.Typ.Id), types.T(right.Typ.Id)) &&
		comparisonEvaluationIsTotal(id, left.Typ, right.Typ) && temporalCandidateBoundSafe(right)
}

// A literal's retained source may carry parameter or diagnostic provenance.
// Until those sources are proved per execution, do not narrow around them.
func temporalCandidateHasUnprovedSource(expr *plan.Expr) bool {
	if expr == nil || expr.GetP() != nil {
		return true
	}
	if lit := expr.GetLit(); lit != nil && lit.Src != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if temporalCandidateHasUnprovedSource(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if temporalCandidateHasUnprovedSource(item) {
				return true
			}
		}
	}
	return false
}

// temporalCandidateScanExpressionsSafe is only the scan-owner part of the
// proof. The caller must also prove all ancestor paths and query-wide prepared
// provenance before creating a hint. Pagination is declined until its separate
// order proof is connected.
func temporalCandidateScanExpressionsSafe(node *plan.Node) bool {
	if node == nil || node.NodeType != plan.Node_TABLE_SCAN || node.Limit != nil || node.Offset != nil {
		return false
	}
	for _, predicate := range node.FilterList {
		if !temporalCandidatePredicateSafe(predicate) {
			return false
		}
	}
	for _, expr := range node.ProjectList {
		if expr == nil || temporalCandidateHasUnprovedSource(expr) || !isTruncationSafeRowExpr(expr) {
			return false
		}
	}
	return true
}

// temporalCandidateSimplePathSafe provides an intentionally narrow first
// ancestor proof. SORT, pagination, multiple steps and non-unary operators are
// declined until their separate ordering and lifecycle proofs are connected.
// A caller must still prove that the original query had no unproved prepared
// parameters, since rebinding can erase parameter provenance.
func temporalCandidateSimplePathSafe(query *plan.Query, scanID int32) bool {
	if query == nil || query.StmtType != plan.Query_SELECT || len(query.Steps) != 1 || len(query.Params) != 0 {
		return false
	}
	visited := make(map[int32]struct{})
	for id := query.Steps[0]; ; {
		if id < 0 || int(id) >= len(query.Nodes) {
			return false
		}
		if _, exists := visited[id]; exists {
			return false
		}
		visited[id] = struct{}{}
		node := query.Nodes[id]
		if node == nil || node.Limit != nil || node.Offset != nil || node.RankOption != nil ||
			len(node.OnList) != 0 || len(node.GroupBy) != 0 || len(node.AggList) != 0 ||
			len(node.WinSpecList) != 0 || len(node.OrderBy) != 0 || len(node.TblFuncExprList) != 0 || len(node.SourceStep) != 0 {
			return false
		}
		if id == scanID {
			return len(node.Children) == 0 && temporalCandidateScanExpressionsSafe(node)
		}
		if node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER || len(node.Children) != 1 {
			return false
		}
		for _, expr := range node.ProjectList {
			if expr == nil || temporalCandidateHasUnprovedSource(expr) || !isTruncationSafeRowExpr(expr) {
				return false
			}
		}
		for _, expr := range node.FilterList {
			if !temporalCandidatePredicateSafe(expr) {
				return false
			}
		}
		id = node.Children[0]
	}
}

// Bound the original owner before the older recursive diagnostics proof. The
// independent capture allowance belongs to bindSelect, before Optimize starts.
func temporalOriginalOwnerFits(ctx context.Context, q *plan.Query, scan int32, budget *int) bool {
	if q == nil || len(q.Steps) != 1 || len(q.Nodes) > 64 {
		return false
	}
	nodes := 0
	for id, steps := q.Steps[0], 0; steps < 64; steps++ {
		if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil || !spendTemporalCostWork(ctx, budget, 1) {
			return false
		}
		n := q.Nodes[id]
		for _, list := range [][]*plan.Expr{n.ProjectList, n.FilterList, n.BlockFilterList} {
			if len(list) > 64 {
				return false
			}
			for _, e := range list {
				if !temporalCostExpressionFits(ctx, e, 0, &nodes, budget) {
					return false
				}
			}
		}
		if id == scan {
			return true
		}
		if len(n.Children) != 1 || (n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER) {
			return false
		}
		id = n.Children[0]
	}
	return false
}

// Called only after the bounded full original-owner diagnostics proof. A bool
// value grants cost evidence; map key presence retains the older admission.
func temporalOriginalSingleMixedLeaf(q *plan.Query, scan int32) bool {
	var predicate *plan.Expr
	for id, steps := q.Steps[0], 0; steps < 64; steps++ {
		n := q.Nodes[id]
		for _, e := range n.FilterList {
			if predicate != nil {
				return false
			}
			predicate = e
		}
		if id == scan {
			atom, ok := temporalCandidateAtomForExpr(predicate)
			if !ok {
				return false
			}
			ref := atom.column.GetCol()
			table := n.TableDef
			if len(n.BindingTags) != 1 || ref.RelPos != n.BindingTags[0] || table == nil || ref.ColPos < 0 || int(ref.ColPos) >= len(table.Cols) || table.Cols[ref.ColPos] == nil {
				return false
			}
			col := table.Cols[ref.ColPos]
			return col.Typ.Id == atom.column.Typ.Id && col.Typ.Scale == atom.column.Typ.Scale
		}
		id = n.Children[0]
	}
	return false
}
