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

func buildFrom(stmt tree.TableExprs, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, err error) {
	nodeId, err = buildTable(stmt[0], ctx, query, binderCtx)
	if err != nil {
		return
	}

	var rightChildId int32
	// build the rest table with preNode as join step by step
	for i := 1; i < len(stmt); i++ {
		rightChildId, err = buildTable(stmt[i], ctx, query, binderCtx)
		if err != nil {
			return
		}
		leftChild := query.Nodes[nodeId]
		leftChild.JoinType = plan.Node_INNER
		rightChild := query.Nodes[rightChildId]
		rightChild.JoinType = plan.Node_INNER

		node := &Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, rightChildId},
		}

		fillJoinProjectList(node, leftChild, rightChild)
		nodeId = appendQueryNode(query, node)
	}

	return
}

func buildTable(stmt tree.TableExpr, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		nowLength := len(query.Nodes)
		var subqueryParentIds []int32
		if nowLength > 0 {
			nodeId := query.Nodes[nowLength-1].NodeId
			subqueryParentIds = append([]int32{nodeId}, binderCtx.subqueryParentIds...)
		}
		newCtx := &BinderContext{
			columnAlias:          make(map[string]*Expr),
			subqueryIsCorrelated: false,
			subqueryParentIds:    subqueryParentIds,
			cteTables:            binderCtx.cteTables,
		}
		return buildSelect(tbl, ctx, query, newCtx)

	case *tree.TableName:
		tblName := string(tbl.ObjectName)
		dbName := string(tbl.SchemaName)
		if strings.ToLower(tblName) == "dual" { //special table name
			node := &Node{
				NodeType: plan.Node_VALUE_SCAN,
			}
			nodeId = appendQueryNode(query, node)
		} else {
			obj, tableDef, isCte := getResolveTable(dbName, tblName, ctx, binderCtx)
			if tableDef == nil {
				return 0, errors.New(errno.InvalidTableDefinition, fmt.Sprintf("table '%v' does not exist", tblName))
			}
			node := &Node{
				ObjRef:   obj,
				TableDef: tableDef,
			}
			if isCte {
				node.NodeType = plan.Node_MATERIAL_SCAN
			} else {
				node.NodeType = plan.Node_TABLE_SCAN
			}
			nodeId = appendQueryNode(query, node)
		}
		return

	case *tree.JoinTableExpr:
		return buildJoinTable(tbl, ctx, query, binderCtx)

	case *tree.ParenTableExpr:
		return buildTable(tbl.Expr, ctx, query, binderCtx)

	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("subquery in FROM must have an alias: %T", stmt))
			}
		}

		nodeId, err = buildTable(tbl.Expr, ctx, query, binderCtx)
		if err != nil {
			return
		}
		nodeId, err = fillTableProjectList(query, nodeId, tbl.As)

		return

	case *tree.StatementSource:
		// log.Printf("StatementSource")
		return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
	// Values table not support
	return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
}

func buildJoinTable(tbl *tree.JoinTableExpr, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, err error) {
	var leftJoinType plan.Node_JoinFlag
	var rightJoinType plan.Node_JoinFlag

	// todo need confirm
	switch tbl.JoinType {
	case tree.JOIN_TYPE_CROSS, tree.JOIN_TYPE_INNER, tree.JOIN_TYPE_NATURAL:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_LEFT, tree.JOIN_TYPE_NATURAL_LEFT:
		leftJoinType = plan.Node_OUTER
		rightJoinType = plan.Node_INNER
	case tree.JOIN_TYPE_RIGHT, tree.JOIN_TYPE_NATURAL_RIGHT:
		leftJoinType = plan.Node_INNER
		rightJoinType = plan.Node_OUTER
	case tree.JOIN_TYPE_FULL:
		leftJoinType = plan.Node_OUTER
		rightJoinType = plan.Node_OUTER
	}

	leftChildId, err := buildTable(tbl.Left, ctx, query, binderCtx)
	if err != nil {
		return
	}

	rightChildId, err := buildTable(tbl.Right, ctx, query, binderCtx)
	if err != nil {
		return
	}

	node := &Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{leftChildId, rightChildId},
	}

	leftChild := query.Nodes[leftChildId]
	rightChild := query.Nodes[rightChildId]
	leftChild.JoinType = leftJoinType
	rightChild.JoinType = rightJoinType
	fillJoinProjectList(node, leftChild, rightChild)
	nodeId = appendQueryNode(query, node)

	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		exprs, err := splitAndBuildExpr(cond.Expr, ctx, query, node, binderCtx, false)
		if err != nil {
			return 0, err
		}
		node.OnList = exprs

	case *tree.UsingJoinCond:
		for _, colName := range cond.Cols {
			name := string(colName)
			leftColIndex, leftColType := getColumnIndexAndType(leftChild.ProjectList, name)
			if leftColIndex < 0 {
				return 0, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", name))
			}
			rightColIndex, rightColType := getColumnIndexAndType(rightChild.ProjectList, name)
			if rightColIndex < 0 {
				return 0, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", name))
			}
			leftColExpr := &Expr{
				ColName: name,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: leftColIndex,
					},
				},
				Typ: leftColType,
			}
			rigthColExpr := &Expr{
				ColName: name,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 1,
						ColPos: rightColIndex,
					},
				},
				Typ: rightColType,
			}

			// append equal function expr to onlist
			var equalFunctionExpr *Expr
			equalFunctionExpr, _, err = getFunctionExprByNameAndPlanExprs("=", []*Expr{leftColExpr, rigthColExpr})
			if err != nil {
				return
			}
			node.OnList = append(node.OnList, equalFunctionExpr)
		}

	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			// natural join.  the cond will be nil
			var columns []*Expr
			columns, err = getColumnsWithSameName(leftChild.ProjectList, rightChild.ProjectList)
			if err != nil {
				return
			}
			node.OnList = columns
		}
	}

	return
}
