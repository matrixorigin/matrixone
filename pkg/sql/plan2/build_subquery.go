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

func buildSubQuery(subquery *tree.Subquery, ctx CompilerContext, query *Query, selectCtx *SelectContext) (*plan.Expr, error) {
	nowLength := len(query.Nodes)
	nodeId := query.Nodes[nowLength-1].NodeId
	subQueryParentId := append([]int32{nodeId}, selectCtx.subQueryParentId...)
	newCtx := &SelectContext{
		columnAlias:          make(map[string]*plan.Expr),
		subQueryIsCorrelated: false,
		subQueryParentId:     subQueryParentId,
		cteTables:            selectCtx.cteTables,
	}

	expr := &plan.SubQuery{
		NodeId:       nodeId,
		IsCorrelated: false,
		IsScalar:     false,
	}

	switch sub := subquery.Select.(type) {
	case *tree.ParenSelect:
		err := buildSelect(sub.Select, ctx, query, newCtx)
		if err != nil {
			return nil, err
		}
	case *tree.SelectClause:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("support select statement: %T", subquery))
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", subquery))
	}
	expr.IsCorrelated = newCtx.subQueryIsCorrelated

	//move subquery node to top
	for i := len(query.Nodes) - 1; i >= 0; i-- {
		if query.Nodes[i].NodeId == nodeId {
			query.Nodes = append(query.Nodes[i+1:], query.Nodes[:i+1]...)
			break
		}
	}

	returnExpr := &plan.Expr{
		Expr: &plan.Expr_Sub{
			Sub: expr,
		},
		Typ: &plan.Type{
			Id: plan.Type_TUPLE,
		},
	}
	if subquery.Exists {
		returnExpr = &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: getFunctionObjRef("EXISTS"),
					Args: []*plan.Expr{returnExpr},
				},
			},
			Typ: &plan.Type{
				Id: plan.Type_BOOL,
			},
		}
	}
	return returnExpr, nil
}
