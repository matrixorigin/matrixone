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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func bitCountFloatFieldOverload(oid types.T) int32 {
	switch oid { // typeswitch:partial only native MySQL FLOAT and DOUBLE fields have private overloads.
	case types.T_float32:
		return function.BitCountFieldFloat32Overload
	case types.T_float64:
		return function.BitCountFieldFloat64Overload
	default:
		return -1
	}
}

func bindBitCountFloatField(ctx context.Context, args []*Expr) (*Expr, error) {
	t := makeTypeByPlan2Expr(args[0])
	f, err := function.GetFunctionByNameWithOverload(ctx, "bit_count", []types.Type{t}, bitCountFloatFieldOverload(t.Oid))
	if err != nil {
		return nil, err
	}
	id := f.GetEncodedOverloadID()
	ret := f.GetReturnType()
	typ := makePlan2Type(&ret)
	typ.NotNullable = function.DeduceNotNullableForBinding(ctx, id, args)
	return &Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: getFunctionObjRef(id, "bit_count"),
		Args: args,
	}}}, nil
}

// A mergeable projection forwards its operand's conversion. A materialized
// query block exposes a field even when the original expression was a CAST.
func (b *baseBinder) bitCountReadsFloatField(expr *Expr) bool {
	// A same-block group slot represents the original GROUP BY expression.
	// Inspect its conversion without replacing the slot used for evaluation.
	if col := expr.GetCol(); col != nil && b.ctx != nil && b.ctx.groupTag > 0 &&
		col.RelPos == b.ctx.groupTag && col.ColPos >= 0 && int(col.ColPos) < len(b.ctx.groups) {
		expr = b.ctx.groups[col.ColPos]
	}
	if b.builder == nil || b.builder.qry == nil {
		return expr.GetCol() != nil || expr.GetCorr() != nil
	}
	// Projections are bound before consumers. The decreasing node ID keeps
	// recursive or self-referencing projections from reopening the lookup.
	nodes := b.builder.qry.Nodes
	limit := len(nodes)
	for {
		var tag, pos int32
		if col := expr.GetCol(); col != nil {
			tag, pos = col.RelPos, col.ColPos
		} else if col := expr.GetCorr(); col != nil {
			tag, pos = col.RelPos, col.ColPos
		} else {
			return false
		}
		id, ok := b.builder.tag2NodeID[tag]
		if !ok || id < 0 || int(id) >= limit {
			// Aggregate and window outputs can be bound before their node.
			return true
		}
		limit = int(id)
		node := nodes[id]
		if node.NodeType != plan.Node_PROJECT || pos < 0 || int(pos) >= len(node.ProjectList) {
			return true
		}
		scope := b.builder.ctxByNode[id]
		if scope != nil && (scope.isDistinct || scope.hasHaving || len(scope.aggregates) != 0 ||
			len(scope.groups) != 0 || len(scope.windows) != 0) {
			return true
		}
		for current := node; ; {
			if current.Limit != nil || current.Offset != nil || current.NodeType == plan.Node_VALUE_SCAN ||
				current.NodeType == plan.Node_MATERIAL || isPreparedSetOperationNode(current.NodeType) {
				return true
			}
			if len(current.Children) != 1 || b.builder.ctxByNode[current.Children[0]] != scope {
				break
			}
			current = nodes[current.Children[0]]
		}
		expr = node.ProjectList[pos]
	}
}
