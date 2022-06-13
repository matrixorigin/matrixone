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

func buildUpdate(stmt *tree.Update, ctx CompilerContext) (*Plan, error) {
	query, _ := newQueryAndSelectCtx(plan.Query_UPDATE)

	// build select
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
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
	binderCtx := &BinderContext{
		columnAlias: make(map[string]*Expr),
	}
	nodeId, err := buildSelect(selectStmt, ctx, query, binderCtx)
	if err != nil {
		return nil, err
	}

	query.Steps = append(query.Steps, nodeId)

	// get table def
	objRef, tableDef := getLastTableDef(query)
	if tableDef == nil {
		return nil, errors.New(errno.CaseNotFound, "can not find table in sql")
	}

	getColumnName := func(name string) *Expr {
		for idx, col := range tableDef.Cols {
			if col.Name == name {
				return &Expr{
					TableName: tableDef.Name,
					ColName:   col.Name,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							RelPos: 0,
							ColPos: int32(idx),
						},
					},
					Typ: col.Typ,
				}
			}
		}
		return nil
	}

	columnLength := len(stmt.Exprs)
	if columnLength == 0 {
		return nil, errors.New(errno.CaseNotFound, "no column will be update")
	}

	node := &Node{
		NodeType: plan.Node_UPDATE,
		ObjRef:   objRef,
		TableDef: tableDef,
		Children: []int32{nodeId},
	}

	columns := make([]*Expr, 0, columnLength)
	values := make([]*Expr, 0, columnLength)
	for _, expr := range stmt.Exprs {
		if len(expr.Names) != 1 {
			return nil, errors.New(errno.CaseNotFound, "the set list of update must be one")
		}
		if expr.Names[0].NumParts != 1 {
			return nil, errors.New(errno.CaseNotFound, "the set list of update must be one")
		}

		column := getColumnName(expr.Names[0].Parts[0])
		if column == nil {
			return nil, errors.New(errno.CaseNotFound, fmt.Sprintf("set column name [%v] is not found", expr.Names[0].Parts[0]))
		}

		value, _, err := buildExpr(expr.Expr, ctx, query, node, binderCtx, false)
		if err != nil {
			return nil, err
		}

		// cast value type
		if column.Typ.Id != value.Typ.Id {
			tmp, err := appendCastExpr(value, column.Typ)
			if err != nil {
				return nil, err
			}
			value = tmp
		}

		columns = append(columns, column)
		values = append(values, value)
	}

	node.UpdateList = &plan.UpdateList{
		Columns: columns,
		Values:  values,
	}
	appendQueryNode(query, node)

	preNode := query.Nodes[len(query.Nodes)-1]
	query.Steps[len(query.Steps)-1] = preNode.NodeId

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}
