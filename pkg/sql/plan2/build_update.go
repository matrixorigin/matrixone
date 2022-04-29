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

func getLastTableDef(query *Query, node *plan.Node) *plan.TableDef {
	if node.TableDef != nil {
		return node.TableDef
	}
	for _, id := range node.Children {
		val := getLastTableDef(query, query.Nodes[id])
		if val != nil {
			return val
		}
	}
	return nil
}

func buildUpdate(stmt *tree.Update, ctx CompilerContext, query *Query) error {
	//build select
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
	SelectCtx := &SelectContext{
		tableAlias:  make(map[string]string),
		columnAlias: make(map[string]*plan.Expr),
	}
	err := buildSelect(selectStmt, ctx, query, SelectCtx)
	if err != nil {
		return err
	}

	//get table def
	tableDef := getLastTableDef(query, query.Nodes[len(query.Nodes)-1])
	if tableDef == nil {
		return errors.New(errno.CaseNotFound, "can not find table in sql")
	}

	getColumnName := func(name string) *plan.Expr {
		for idx, col := range tableDef.Cols {
			if col.Name == name {
				return &plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   col.Name,
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

	var columns []*plan.Expr
	var values []*plan.Expr
	for _, expr := range stmt.Exprs {
		if len(expr.Names) != 1 {
			return errors.New(errno.CaseNotFound, "the set list of update must be one")
		}
		if expr.Names[0].NumParts != 1 {
			return errors.New(errno.CaseNotFound, "the set list of update must be one")
		}
		column := getColumnName(expr.Names[0].Parts[0])
		if column == nil {
			return errors.New(errno.CaseNotFound, fmt.Sprintf("set column name [%v] is not found", expr.Names[0].Parts[0]))
		}
		columns = append(columns, column)

		value, err := buildExpr(expr.Expr, ctx, query, nil)
		if err != nil {
			return err
		}

		//check value type
		matchType(column.Typ, value.Typ)

		values = append(values, value)
	}

	if len(columns) == 0 {
		return errors.New(errno.CaseNotFound, "no column will be update")
	}

	node := &plan.Node{
		NodeType: plan.Node_UPDATE,
		UpdateList: &plan.UpdateList{
			Columns: columns,
			Values:  values,
		},
		Children: []int32{int32(len(query.Nodes) - 1)},
	}
	appendQueryNode(query, node, true)

	return nil
}

func matchType(left *plan.Type, right *plan.Type) error {
	if left.Id == right.Id {
		return nil
	}
	//todo check update type
	// return errors.New(errno.CaseNotFound, "type not match")
	return nil
}
