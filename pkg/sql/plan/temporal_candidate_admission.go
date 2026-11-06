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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

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
	column := indexComparisonColumn(fn)
	if column == nil {
		return temporalCandidateAtom{}, false
	}
	bound := fn.Args[1]
	if column == fn.Args[1] {
		bound = fn.Args[0]
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
	if !isDatetimeTimestampTypePair(types.T(column.Typ.Id), types.T(bound.Typ.Id)) || !temporalCandidateBoundSafe(bound) {
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
	if unstable, known := function.GetFunctionValueUnstableByID(fn.Func.Obj); !known || unstable || !function.IsRuntimeConstant(expr) {
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

// temporalCandidateSimplePathSafe checks the current rewritten path. The
// original-owner proof and final catalog/publication proof are separate.
func temporalCandidateSimplePathSafe(query *plan.Query, scanID int32) bool {
	path, ok := traceTemporalCandidatePath(query)
	return ok && path.ids[path.len-1] == scanID && temporalPathExpressionsSafe(query, path)
}
