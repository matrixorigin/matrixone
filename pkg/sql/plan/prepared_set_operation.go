// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func preparedSetOperation(node *plan.Node) bool {
	switch node.NodeType {
	case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL, plan.Node_MINUS:
		return true
	}
	return false
}

// Synthetic aggregate columns address GroupBy/AggList, not the child batch.
func preparedAggregateOutput(node *plan.Node, colPos int32) *plan.Expr {
	if node.NodeType != plan.Node_AGG || colPos < 0 || int(colPos) >= len(node.ProjectList) {
		return nil
	}
	col := node.ProjectList[colPos].GetCol()
	if col == nil {
		return nil
	}
	switch col.RelPos {
	case -1:
		if col.ColPos >= 0 && int(col.ColPos) < len(node.GroupBy) {
			return node.GroupBy[col.ColPos]
		}
	case -2:
		pos := col.ColPos - int32(len(node.GroupBy))
		if pos >= 0 && int(pos) < len(node.AggList) {
			return node.AggList[pos]
		}
	}
	return nil
}

func markPreparedOutputSource(expr *plan.Expr, nodeID, colPos int32, positions map[int32]struct{}) {
	m := ensurePreparedNumericMetadata(expr)
	m.Fallback = true
	m.FallbackSource = true
	m.FallbackSourceNodeId = nodeID
	m.FallbackSourceColPos = colPos
	m.ParamPos = minimumPreparedPosition(positions)
}

// Reconcile the entire input ABI before publishing a set-operation output type.
// This runs bottom-up on the execute-time copy, after parameter materialization.
func (rule *preparedNumericSourceRefreshRule) rebindSetOperation(node *plan.Node) error {
	query := rule.reset.preparedPlan.GetQuery()
	for col, output := range node.ProjectList {
		if !output.GetPreparedNumeric().GetFallbackSource() {
			continue
		}
		inputs := make([]*plan.Expr, len(node.Children))
		argTypes := make([]types.Type, len(inputs))
		hasDecimal := false
		for i, childID := range node.Children {
			if childID < 0 || int(childID) >= len(query.Nodes) || query.Nodes[childID] == nil || col >= len(query.Nodes[childID].ProjectList) {
				return moerr.NewInternalErrorNoCtx("invalid prepared set-operation input")
			}
			inputs[i] = query.Nodes[childID].ProjectList[col]
			argTypes[i] = makeTypeByPlan2Expr(inputs[i])
			hasDecimal = hasDecimal || argTypes[i].Oid.IsDecimal()
		}
		if hasDecimal {
			for i, input := range inputs {
				if exact, ok := setOperationIntegerLiteralDecimalType(input); ok {
					argTypes[i] = exact
				}
			}
		}
		resolved, err := function.GetFunctionByName(rule.reset.ctx, "coalesce", argTypes)
		if err != nil {
			return err
		}
		target := resolved.GetReturnType()
		casts, cast := resolved.ShouldDoImplicitTypeCast()
		if cast && len(casts) > 0 {
			target = casts[0]
		}
		targetType := makePlan2Type(&target)
		// Preserve row nullability independently of the common physical type.
		targetType.NotNullable = inputs[0].Typ.NotNullable
		for _, input := range inputs[1:] {
			targetType = setOperationOutputType(node.NodeType, targetType, input.Typ)
		}
		for i, input := range inputs {
			bound, err := appendSetOperationCastBeforeExpr(rule.reset.ctx, input, targetType)
			if err != nil {
				return err
			}
			query.Nodes[node.Children[i]].ProjectList[col] = bound
		}
		output.Typ = targetType
		rule.reset.specialized = true
	}
	return nil
}
