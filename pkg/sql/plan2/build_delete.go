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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			// todo confirm how to set row_id
			Exprs: tree.SelectExprs{
				tree.SelectExpr{
					Expr: tree.UnqualifiedStar{},
				},
			},
			From:  &tree.From{Tables: tree.TableExprs{stmt.Table}},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
	}
	query, binderCtx := newQueryAndSelectCtx(plan.Query_DELETE)
	nodeId, err := buildSelect(selectStmt, ctx, query, binderCtx)
	if err != nil {
		return nil, err
	}
	query.Steps = append(query.Steps, nodeId)

	return appendDeleteNode(query)
}

func appendDeleteNode(query *Query) (*Plan, error) {
	// get tableDef
	objRef, tableDef := getLastTableDef(query)
	if tableDef == nil {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cannot find delete table")
	}

	// append delete node
	node := &Node{
		NodeType: plan.Node_DELETE,
		ObjRef:   objRef,
		TableDef: tableDef,
	}
	appendQueryNode(query, node)

	// reset root node
	preNode := query.Nodes[len(query.Nodes)-1]
	query.Steps[len(query.Steps)-1] = preNode.NodeId

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}
