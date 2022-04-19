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

func getLastSimpleNode(query *Query) *plan.Node {
	nodeLength := len(query.Nodes)
	if nodeLength < 0 {
		return nil
	} else if nodeLength == 1 {
		return query.Nodes[nodeLength-1]
	} else {
		lastNode := query.Nodes[nodeLength-1]

		for {
			if lastNode.TableDef != nil {
				return lastNode
			}
			childrenLength := len(lastNode.Children)
			lastNode = query.Nodes[lastNode.Children[childrenLength-1]]
		}
	}
}

func getDefaultTableDef(query *Query) *plan.TableDef {
	node := getLastSimpleNode(query)
	if node != nil {
		return node.TableDef
	}
	return nil
}

//splitExprToAND split a expression to a list of AND conditions.
func splitExprToAND(expr tree.Expr) []*tree.Expr {
	var exprs []*tree.Expr

	switch typ := expr.(type) {
	case nil:
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
	funObjRef, err := getFunctionObjRef(name)
	if err != nil {
		return nil, err
	}
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

func getFunctionObjRef(name string) (*plan.ObjectRef, error) {
	//todo need to check if function name exist
	return &plan.ObjectRef{
		ObjName: name,
	}, nil
}

func getColExprByFieldName(name string, tableDef *plan.TableDef) (*plan.Expr, error) {
	if tableDef == nil {
		return nil, errors.New(errno.InvalidTableDefinition, fmt.Sprintf("table is not exist"))
	}

	for idx, col := range tableDef.Cols {
		if col.Name == name {
			return &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						Name:   name,
						ColPos: int32(idx),
					},
				},
			}, nil
		}
	}
	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
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

	for leftIdx, leftCol := range left.Cols {
		for rightIdx, rightCol := range right.Cols {
			if leftCol.Name == rightCol.Name {
				exprs = append(exprs, &plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   leftCol.Name,
							RelPos: int32(rightIdx), //todo confirm what RelPos means
							ColPos: int32(leftIdx),
						},
					},
				})
			}
		}
	}

	return exprs
}
