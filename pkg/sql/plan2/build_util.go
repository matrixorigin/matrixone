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

func getFunctionExprByNameAndExprs(name string, exprs []tree.Expr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) (*plan.Expr, error) {
	funObjRef := getFunctionObjRef(name)
	var args []*plan.Expr
	for _, astExpr := range exprs {
		expr, err := buildExpr(astExpr, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		args = append(args, expr)
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: funObjRef,
				Args: args,
			},
		},
	}, nil
}

func getFunctionObjRef(name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		ObjName: name,
	}
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

func fillProjectList(node *plan.Node) {
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
		})
	}

	node.ProjectList = exprs
}

func getExprFromUnresolvedName(query *Query, name string, table string) (*plan.Expr, error) {
	preNode := query.Nodes[len(query.Nodes)-1]
	colRef := &plan.ColRef{
		Name: "",
	}
	colExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: colRef,
		},
	}
	aliasName := table + "." + name

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
			// expr := col.Expr.(*plan.Expr_Col)
			// colRef.Name = expr.Col.Name
			// colRef.RelPos = expr.Col.RelPos
			// colRef.ColPos = expr.Col.ColPos
			// colExpr.Alias = col.Alias

			colRef.Name = col.Alias
			colRef.RelPos = 0
			colRef.ColPos = int32(idx)
			colExpr.Alias = col.Alias
		}
	}

	if colRef.Name == "" {
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", aliasName))
	} else {
		return colExpr, nil
	}
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

func setDerivedTableAlias(query *Query, ctx CompilerContext, aliasCtx *AliasContext, alias string) {
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
	aliasCtx.tableAlias[alias] = alias

	// new project node// 如果后面没有join的话，那么就有一个新的node
	node := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: preNode.ProjectList,
		Children:    []int32{preNode.NodeId},
	}
	appendQueryNode(query, node, true)
}
