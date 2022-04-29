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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

//splitExprToAND split a expression to a list of AND conditions.
func splitExprToAND(expr tree.Expr) []*tree.Expr {
	var exprs []*tree.Expr

	switch typ := expr.(type) {
	case *tree.AndExpr:
		exprs = append(exprs, splitExprToAND(typ.Left)...)
		exprs = append(exprs, splitExprToAND(typ.Right)...)
	case *tree.ParenExpr:
		exprs = append(exprs, splitExprToAND(typ.Expr)...)
	default:
		exprs = append(exprs, &expr)
	}

	return exprs
}

func getColumnIndex(tableDef *plan.TableDef, name string) int32 {
	for idx, col := range tableDef.Cols {
		if col.Name == name {
			return int32(idx)
		}
	}
	return -1
}

func getColumnsWithSameName(left *plan.TableDef, right *plan.TableDef) []*plan.Expr {
	var exprs []*plan.Expr

	funName := getFunctionObjRef("=")
	for leftIdx, leftCol := range left.Cols {
		for rightIdx, rightCol := range right.Cols {
			if leftCol.Name == rightCol.Name {
				exprs = append(exprs, &plan.Expr{
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: funName,
							Args: []*plan.Expr{
								{
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											Name:   leftCol.Name,
											RelPos: 0,
											ColPos: int32(leftIdx),
										},
									},
								},
								{
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											Name:   rightCol.Name,
											RelPos: 1,
											ColPos: int32(rightIdx),
										},
									},
								},
							},
						},
					},
				})
			}
		}
	}
	return exprs
}

func appendQueryNode(query *Query, node *plan.Node, isRoot bool) {
	nodeLength := len(query.Nodes)
	node.NodeId = int32(nodeLength)
	query.Nodes = append(query.Nodes, node)
	if isRoot {
		query.Steps = []int32{node.NodeId}
	}
}

func fillTableScanProjectList(node *plan.Node) {
	var exprs []*plan.Expr
	for idx, col := range node.TableDef.Cols {
		exprs = append(exprs,
			&plan.Expr{
				Alias: node.TableDef.Name + "." + col.Name,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						Name:   col.Name,
						ColPos: int32(idx),
					},
				},
				Typ: col.Typ,
			})
	}
	node.ProjectList = exprs
}

func fillJoinProjectList(node *plan.Node, leftNode *plan.Node, rightNode *plan.Node) {
	var exprs []*plan.Expr
	for idx, expr := range leftNode.ProjectList {
		exprs = append(exprs, &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					Name:   expr.Alias,
					RelPos: 0,
					ColPos: int32(idx),
				},
			},
			Alias: expr.Alias,
			Typ:   expr.Typ,
		})
	}
	for idx, expr := range rightNode.ProjectList {
		exprs = append(exprs, &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					Name:   expr.Alias,
					RelPos: 1,
					ColPos: int32(idx),
				},
			},
			Alias: expr.Alias,
			Typ:   expr.Typ,
		})
	}

	node.ProjectList = exprs
}

func getExprFromUnresolvedName(query *Query, name string, table string, SelectCtx *SelectContext) (*plan.Expr, error) {
	preNode := query.Nodes[len(query.Nodes)-1]
	aliasName := table + "." + name

	colRef := &plan.ColRef{
		Name: "",
	}
	colExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: colRef,
		},
	}

	matchName := func(alias string) bool {
		if table == "" {
			arr := strings.SplitN(alias, ".", 2)
			return len(arr) > 1 && arr[1] == name
		}
		return alias == aliasName
	}

	for idx, col := range preNode.ProjectList {
		if matchName(col.Alias) {
			if colRef.Name != "" {
				return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' in the field list is ambiguous", name))
			}
			colRef.Name = col.Alias
			colRef.RelPos = 0
			colRef.ColPos = int32(idx)
			colExpr.Alias = col.Alias
			colExpr.Typ = col.Typ
		}
	}
	if colRef.Name != "" {
		return colExpr, nil
	}

	//get from parent if i'am subquery
	{
		colRef := &plan.CorrColRef{
			Name: "",
		}
		colExpr := &plan.Expr{
			Expr: &plan.Expr_Corr{
				Corr: colRef,
			},
		}
		for idx, col := range query.Nodes[SelectCtx.subQueryParentId].ProjectList {
			if matchName(col.Alias) {
				if colRef.Name != "" {
					return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' in the field list is ambiguous", name))
				}
				SelectCtx.subQueryIsCorrelated = true

				colExpr = &plan.Expr{
					Expr: &plan.Expr_Corr{
						Corr: colRef,
					},
				}

				colRef.Name = col.Alias
				colRef.RelPos = 0
				colRef.ColPos = int32(idx)
				colRef.NodeId = SelectCtx.subQueryParentId
				colExpr.Alias = col.Alias
				colExpr.Typ = col.Typ
			}
		}
		if colRef.Name != "" {
			return colExpr, nil
		}
	}

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' not found", name))
}

//if table == ""  => select * from tbl
//if table is not empty => select a.* from a,b on a.id = b.id
func unfoldStar(query *Query, list *plan.ExprList, table string) error {
	preNode := query.Nodes[len(query.Nodes)-1]

	matchName := func(alias string) bool {
		if table != "" {
			arr := strings.SplitN(alias, ".", 2)
			return arr[0] == table
		}
		return true
	}

	for _, expr := range preNode.ProjectList {
		if matchName(expr.Alias) {
			list.List = append(list.List, expr)
		}
	}
	return nil
}

func setDerivedTableAlias(query *Query, ctx CompilerContext, SelectCtx *SelectContext, alias string) {
	preNode := query.Nodes[len(query.Nodes)-1]
	newName := func(name string) string {
		arr := strings.SplitN(name, ".", 2)
		if len(arr) > 1 {
			return alias + "." + strings.ToUpper(arr[1])
		}
		return alias + "." + strings.ToUpper(arr[0])
	}
	for _, expr := range preNode.ProjectList {
		expr.Alias = newName(expr.Alias)
	}
	SelectCtx.tableAlias[alias] = alias

	//create new project node by default。
	//if next node is join, you need remove this node
	node := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: preNode.ProjectList,
		Children:    []int32{preNode.NodeId},
	}
	appendQueryNode(query, node, true)
}
