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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// configureAsofJoin validates the logical predecessor-join contract and records
// the right timestamp column needed by the physical operator.  The temporal
// predicate remains in OnList, so strictness and tolerance use the ordinary
// expression engine and retain TIMESTAMP/session-time-zone semantics.
func (builder *QueryBuilder) configureAsofJoin(
	node *planpb.Node,
	tbl *tree.JoinTableExpr,
	ctx *BindContext,
	leftChildID, rightChildID int32,
) error {
	leftTags := builder.collectBindingTags(builder.qry.Nodes[leftChildID])
	rightTags := builder.collectBindingTags(builder.qry.Nodes[rightChildID])

	equalityKeys := 0
	var leftTime, rightTime *planpb.Expr
	for _, condition := range node.OnList {
		fn := condition.GetF()
		if fn == nil || len(fn.Args) != 2 {
			return moerr.NewSyntaxError(builder.GetContext(),
				"ASOF JOIN ON supports equality keys and one temporal inequality")
		}
		leftSide := getJoinSide(fn.Args[0], leftTags, rightTags, 0)
		rightSide := getJoinSide(fn.Args[1], leftTags, rightTags, 0)
		crossesInputs := leftSide == JoinSideLeft && rightSide == JoinSideRight ||
			leftSide == JoinSideRight && rightSide == JoinSideLeft
		if !crossesInputs {
			return moerr.NewSyntaxError(builder.GetContext(),
				"ASOF JOIN predicates must compare the left and right inputs")
		}

		if IsEqualFunc(fn.Func.GetObj()) {
			equalityKeys++
			continue
		}

		op := fn.Func.GetObjName()
		var candidateLeft, candidateRight *planpb.Expr
		switch {
		case leftSide == JoinSideLeft && rightSide == JoinSideRight && (op == ">" || op == ">="):
			candidateLeft, candidateRight = fn.Args[0], fn.Args[1]
		case leftSide == JoinSideRight && rightSide == JoinSideLeft && (op == "<" || op == "<="):
			candidateLeft, candidateRight = fn.Args[1], fn.Args[0]
		default:
			return moerr.NewSyntaxError(builder.GetContext(),
				"ASOF JOIN temporal predicate must look backward (left_time >= right_time or left_time > right_time)")
		}
		if leftTime != nil {
			return moerr.NewSyntaxError(builder.GetContext(),
				"ASOF JOIN requires exactly one temporal inequality")
		}
		leftCol, leftOK := candidateLeft.Expr.(*planpb.Expr_Col)
		rightCol, rightOK := candidateRight.Expr.(*planpb.Expr_Col)
		if !leftOK || !rightOK {
			return moerr.NewSyntaxError(builder.GetContext(),
				"ASOF JOIN temporal operands must be columns")
		}
		if !isAsofTemporalType(candidateLeft.Typ.Id) || candidateLeft.Typ.Id != candidateRight.Typ.Id {
			return moerr.NewInvalidInput(builder.GetContext(),
				"ASOF JOIN temporal columns must have the same DATE, DATETIME, TIMESTAMP, or TIME type")
		}
		_ = leftCol
		leftTime, rightTime = candidateLeft, candidateRight
		node.AsofRightCol = rightCol.Col.ColPos
	}

	if equalityKeys == 0 {
		return moerr.NewSyntaxError(builder.GetContext(), "ASOF JOIN requires at least one equality key")
	}
	if leftTime == nil {
		return moerr.NewSyntaxError(builder.GetContext(), "ASOF JOIN requires exactly one temporal inequality")
	}

	if tbl.Tolerance != nil {
		tolerance, err := ctx.binder.BindExpr(tbl.Tolerance, 0, true)
		if err != nil {
			return err
		}
		if !rule.IsConstant(tolerance, true) {
			return moerr.NewInvalidInput(builder.GetContext(), "ASOF JOIN TOLERANCE must be a constant interval")
		}
		threshold, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "date_sub", []*planpb.Expr{DeepCopyExpr(leftTime), tolerance})
		if err != nil {
			return err
		}
		withinTolerance, err := BindFuncExprImplByPlanExpr(builder.GetContext(), ">=", []*planpb.Expr{DeepCopyExpr(rightTime), threshold})
		if err != nil {
			return err
		}
		node.OnList = append(node.OnList, withinTolerance)
	}
	return nil
}

func isAsofTemporalType(typeID int32) bool {
	switch types.T(typeID) {
	case types.T_date, types.T_datetime, types.T_timestamp, types.T_time:
		return true
	default:
		return false
	}
}

// refreshAsofRightColAfterRemap keeps the physical timestamp position in sync
// with projection pruning. The original child column number is not stable once
// remapAllColRefs has compacted the build-side projection.
func refreshAsofRightColAfterRemap(node *planpb.Node) error {
	for _, condition := range node.OnList {
		fn := condition.GetF()
		if fn == nil || len(fn.Args) != 2 {
			continue
		}
		left := fn.Args[0].GetCol()
		right := fn.Args[1].GetCol()
		if left == nil || right == nil {
			continue
		}
		switch fn.Func.GetObjName() {
		case ">", ">=":
			if left.RelPos == 0 && right.RelPos == 1 {
				node.AsofRightCol = right.ColPos
				return nil
			}
		case "<", "<=":
			if left.RelPos == 1 && right.RelPos == 0 {
				node.AsofRightCol = left.ColPos
				return nil
			}
		}
	}
	return moerr.NewInternalErrorNoCtx("ASOF temporal predicate was lost during column remapping")
}
