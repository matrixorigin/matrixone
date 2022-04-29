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

func getSubqueryParentScanNodeId(query *Query, node *plan.Node) (int32, bool) {
	if node.TableDef != nil {
		return node.NodeId, true
	}
	if node.NodeType == plan.Node_JOIN {
		return node.NodeId, true
	}
	for _, id := range node.Children {
		nodeId, ok := getSubqueryParentScanNodeId(query, query.Nodes[id])
		if ok {
			return nodeId, true
		}
	}
	return 0, false
}

func buildSubQuery(subquery *tree.Subquery, ctx CompilerContext, query *Query, SelectCtx *SelectContext) (*plan.Expr, error) {
	nowLength := len(query.Nodes)
	nodeId, _ := getSubqueryParentScanNodeId(query, query.Nodes[nowLength-1])

	newCtx := &SelectContext{
		tableAlias:           make(map[string]string),
		columnAlias:          make(map[string]*plan.Expr),
		subQueryIsCorrelated: false,
		subQueryParentId:     int32(nowLength) - 1,
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
		// buildSelect(selectClause.Select, ctx, query)
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("support select statement: %T", subquery))
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", subquery))
	}

	expr.IsCorrelated = newCtx.subQueryIsCorrelated

	//把子查询的节点放到前面去
	query.Steps = []int32{int32(nowLength - 1)}
	query.Nodes = append(query.Nodes[nowLength:], query.Nodes[:nowLength]...)

	return &plan.Expr{
		Expr: &plan.Expr_Sub{
			Sub: expr,
		},
		Typ: &plan.Type{
			Id:       plan.Type_ARRAY,
			Nullable: false,
		},
	}, nil
}
