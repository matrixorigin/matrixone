// Copyright 2021 - 2022 Matrix Origin
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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildSubQuery(subquery *tree.Subquery, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext) (*Expr, error) {
	subqueryParentIds := append([]int32{node.NodeId}, binderCtx.subqueryParentIds...)
	newCtx := &BinderContext{
		columnAlias:          make(map[string]*Expr),
		subqueryIsCorrelated: false,
		subqueryParentIds:    subqueryParentIds,
		cteTables:            binderCtx.cteTables,
	}

	var nodeId int32
	var err error
	switch sub := subquery.Select.(type) {
	case *tree.ParenSelect:
		nodeId, err = buildSelect(sub.Select, ctx, query, newCtx)
		if err != nil {
			return nil, err
		}
	case *tree.SelectClause:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport select statement: %T", subquery))
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", subquery))
	}

	expr := &plan.SubQuery{
		NodeId:       nodeId,
		IsScalar:     false,
		IsCorrelated: newCtx.subqueryIsCorrelated,
	}

	returnExpr := &Expr{
		Expr: &plan.Expr_Sub{
			Sub: expr,
		},
		Typ: &plan.Type{
			Id: plan.Type_TUPLE,
		},
	}
	if subquery.Exists {
		returnExpr, _, err = getFunctionExprByNameAndPlanExprs("exists", []*Expr{returnExpr})
		if err != nil {
			return nil, err
		}
	}
	return returnExpr, nil
}
