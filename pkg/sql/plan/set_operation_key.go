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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// setArmHasSingletonBoundary proves a bound before correlated predicates are
// pulled through the arm. Do not infer it from estimated cardinality or from
// a base-table key: the bound must survive every outer correlation key.
func (builder *QueryBuilder) setArmHasSingletonBoundary(id int32) bool {
	node := builder.qry.Nodes[id]
	if limit, ok := getLiteralUint64(node.Limit); ok && limit <= 1 {
		return true
	}
	if node.NodeType == planpb.Node_AGG {
		return len(node.GroupBy) == 0 && len(node.GroupingFlag) == 0 && len(node.AggList) > 0
	}
	switch node.NodeType {
	case planpb.Node_PROJECT, planpb.Node_SORT, planpb.Node_FILTER:
		return len(node.Children) == 1 && builder.setArmHasSingletonBoundary(node.Children[0])
	}
	return false
}

// setArmHasReuseBoundary keeps general arm reuse within the existing single-arm
// aggregate/pagination decorrelation owners. Plain multirow arms retain the set
// route, whose range-correlated scalar fallback deduplicates after outer matching.
func (builder *QueryBuilder) setArmHasReuseBoundary(id int32) bool {
	node := builder.qry.Nodes[id]
	if _, literal := getLiteralUint64(node.Limit); literal {
		return true
	}
	if node.NodeType == planpb.Node_AGG {
		return true
	}
	switch node.NodeType {
	case planpb.Node_PROJECT, planpb.Node_SORT, planpb.Node_FILTER:
		return len(node.Children) == 1 && builder.setArmHasReuseBoundary(node.Children[0])
	}
	return false
}

// buildSetOperationPhysicalKeys refines membership without changing the visible
// row or the shared GROUP BY/DISTINCT equality domain. Both initial planning and
// prepared-plan specialization must derive keys from the resolved output types.
func buildSetOperationPhysicalKeys(ctx context.Context, exprs []*planpb.Expr, padSpace []bool) ([]*planpb.Expr, error) {
	if len(exprs) != len(padSpace) {
		return nil, moerr.NewInternalError(ctx, "physical equality key shape does not match projected row")
	}
	required := false
	for i, expr := range exprs {
		if expr == nil {
			return nil, moerr.NewInternalErrorf(ctx, "set-operation output has a nil projection at column %d", i)
		}
		oid := types.T(expr.Typ.Id)
		required = required || padSpace[i] || oid == types.T_float32 || oid == types.T_float64
	}
	if !required {
		return nil, nil
	}
	keys := make([]*planpb.Expr, len(exprs))
	for i, expr := range exprs {
		key := DeepCopyExpr(expr)
		var err error
		switch types.T(expr.Typ.Id) {
		case types.T_float32, types.T_float64:
			key, err = BindFuncExprImplByPlanExpr(ctx, "set_operation_float_key", []*planpb.Expr{key})
		default:
			if padSpace[i] {
				key, err = appendSetOperationCastBeforeExpr(ctx, key, key.Typ)
			}
		}
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}
