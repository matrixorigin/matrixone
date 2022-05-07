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
	"log"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildFrom(stmt tree.TableExprs, ctx CompilerContext, query *Query, selectCtx *SelectContext) error {
	if len(stmt) == 1 {
		_, err := buildTable(stmt[0], ctx, query, selectCtx)
		return err
	}

	//build first and second table as join
	joinTbl := &tree.JoinTableExpr{
		JoinType: tree.JOIN_TYPE_INNER,
		Left:     stmt[0],
		Right:    stmt[1],
	}
	err := buildJoinTable(joinTbl, ctx, query, selectCtx)
	if err != nil {
		return err
	}

	//build the rest table with preNode as join step by step
	i := 2
	for i < len(stmt) {
		leftNode := query.Nodes[len(query.Nodes)-1]

		isDerivedTable, err := buildTable(stmt[i], ctx, query, selectCtx)
		if err != nil {
			return err
		}
		if isDerivedTable {
			query.Nodes = query.Nodes[:len(query.Nodes)-1]
		}
		rightNode := query.Nodes[len(query.Nodes)-1]
		rightNode.JoinType = plan.Node_INNER

		node := &plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{leftNode.NodeId, rightNode.NodeId},
		}
		fillJoinProjectList(node, leftNode, rightNode)
		appendQueryNode(query, node, true)
		i++
	}
	return nil
}

func buildTable(stmt tree.TableExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) (bool, error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		tmpCtx := &SelectContext{
			tableAlias:  make(map[string]string),
			columnAlias: make(map[string]*plan.Expr),
		}
		err := buildSelect(tbl, ctx, query, tmpCtx)
		if err != nil {
			return true, err
		}
		return true, nil
	case *tree.TableName:
		name := string(tbl.ObjectName)
		if len(tbl.SchemaName) > 0 {
			name = strings.Join([]string{string(tbl.SchemaName), name}, ".")
		}
		obj, tableDef := ctx.Resolve(name)
		if tableDef == nil {
			return false, errors.New(errno.InvalidSchemaName, fmt.Sprintf("table '%v' is not exist", name))
		}
		node := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			ObjRef:   obj,
			TableDef: tableDef,
		}
		fillTableScanProjectList(node)
		selectCtx.tableAlias[tableDef.Name] = tableDef.Name
		appendQueryNode(query, node, false)
		return false, nil
	case *tree.JoinTableExpr:
		//todo confirm how to deal with alias
		err := buildJoinTable(tbl, ctx, query, selectCtx)
		if err != nil {
			return true, err
		}
		return false, nil
	case *tree.ParenTableExpr:
		return buildTable(tbl.Expr, ctx, query, selectCtx)
	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		alias := string(tbl.As.Alias)
		if alias == "" {
			return buildTable(tbl.Expr, ctx, query, selectCtx)
		}

		isDerivedTable, err := buildTable(tbl.Expr, ctx, query, selectCtx)
		if err != nil {
			return isDerivedTable, err
		}
		if isDerivedTable {
			setDerivedTableAlias(query, ctx, selectCtx, alias)
		} else {
			delete(selectCtx.tableAlias, query.Nodes[len(query.Nodes)-1].TableDef.Name)
			selectCtx.tableAlias[alias] = query.Nodes[len(query.Nodes)-1].TableDef.Name
		}
		//todo add tableAlias(colAlias1, colAlias2) support (ast not support now)
		return isDerivedTable, nil
	case *tree.StatementSource:
		log.Printf("StatementSource")
		return false, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
	// Values table not support
	return false, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
}

func buildJoinTable(tbl *tree.JoinTableExpr, ctx CompilerContext, query *Query, selectCtx *SelectContext) error {
	var leftJoinType plan.Node_JoinFlag
	var rightJoinType plan.Node_JoinFlag

	//todo need confirm
	switch tbl.JoinType {
	case tree.JOIN_TYPE_FULL:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_LEFT:
		leftJoinType = plan.Node_OUTER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_CROSS:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_RIGHT:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_OUTER
	case tree.JOIN_TYPE_NATURAL:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_INNER:
		// if tbl.Cond == nil {
		// 	leftJoinType = plan.Node_INNER
		// 	rightJoinType = plan.Node_INNER
		// } else {
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_INNER
		// }
	}

	isDerivedTable, err := buildTable(tbl.Left, ctx, query, selectCtx)
	if err != nil {
		return err
	}
	if isDerivedTable {
		query.Nodes = query.Nodes[:len(query.Nodes)-1]
	}
	lefNode := query.Nodes[len(query.Nodes)-1]
	lefNode.JoinType = rightJoinType

	isDerivedTable, err = buildTable(tbl.Right, ctx, query, selectCtx)
	if err != nil {
		return err
	}
	if isDerivedTable {
		query.Nodes = query.Nodes[:len(query.Nodes)-1]
	}
	rightNode := query.Nodes[len(query.Nodes)-1]
	rightNode.JoinType = leftJoinType

	node := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{lefNode.NodeId, rightNode.NodeId},
	}
	fillJoinProjectList(node, lefNode, rightNode)
	appendQueryNode(query, node, true)

	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		exprs, err := splitAndBuildExpr(cond.Expr, ctx, query, selectCtx)
		if err != nil {
			return err
		}
		node.OnList = exprs
	case *tree.UsingJoinCond:
		for _, identifier := range cond.Cols {
			name := string(identifier)
			leftColIndex := getColumnIndex(lefNode.TableDef, name)
			if leftColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
			}
			rightColIndex := getColumnIndex(rightNode.TableDef, name)
			if rightColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
			}
			funName := getFunctionObjRef("=")
			node.OnList = append(node.OnList, &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: funName,
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										Name:   name,
										RelPos: 0,
										ColPos: leftColIndex,
									},
								},
							},
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										Name:   name,
										RelPos: 1,
										ColPos: rightColIndex,
									},
								},
							},
						},
					},
				},
			})
		}
	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL { //natural join.  the cond will be nil
			columns := getColumnsWithSameName(lefNode.TableDef, rightNode.TableDef)
			node.OnList = columns
		}
	}
	return nil
}
