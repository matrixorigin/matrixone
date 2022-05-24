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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildSelect(stmt *tree.Select, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, err error) {
	// with
	err = buildCTE(stmt.With, ctx, query, binderCtx)
	if err != nil {
		return
	}

	var projList []*Expr

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		nodeId, projList, err = buildSelectClause(selectClause, ctx, query, binderCtx)
		if err != nil {
			return
		}
	case *tree.ParenSelect:
		return buildSelect(selectClause.Select, ctx, query, binderCtx)
	default:
		return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	// Build ORDER BY clause
	if stmt.OrderBy != nil {
		node := query.Nodes[nodeId]
		sortNode := &Node{
			NodeType:    plan.Node_SORT,
			Children:    []int32{nodeId},
			ProjectList: node.ProjectList,
		}

		orderBys := make([]*plan.OrderBySpec, 0, len(stmt.OrderBy))
		for _, order := range stmt.OrderBy {
			orderBy := &plan.OrderBySpec{}
			expr, _, err := buildExpr(order.Expr, ctx, query, sortNode, binderCtx, false)
			if err != nil {
				return 0, err
			}
			orderBy.Expr = expr

			switch order.Direction {
			case tree.DefaultDirection:
				orderBy.Flag = plan.OrderBySpec_INTERNAL
			case tree.Ascending:
				orderBy.Flag = plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag = plan.OrderBySpec_DESC
			}
			orderBys = append(orderBys, orderBy)
		}
		sortNode.OrderBy = orderBys

		sortNode.ProjectList = make([]*Expr, len(projList))
		for i, expr := range node.ProjectList {
			if expr == nil {
				panic(i)
			}
			node.ProjectList[i] = &Expr{
				Typ:       expr.Typ,
				TableName: expr.TableName,
				ColName:   expr.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}
		}

		nodeId = appendQueryNode(query, sortNode)
	}

	// Build LIMIT clause
	if stmt.Limit != nil {
		node := query.Nodes[nodeId]

		// offset
		if stmt.Limit.Offset != nil {
			expr, _, err := buildExpr(stmt.Limit.Offset, ctx, query, node, binderCtx, false)
			if err != nil {
				return 0, err
			}
			node.Offset = expr
		}

		// limit
		if stmt.Limit.Count != nil {
			expr, _, err := buildExpr(stmt.Limit.Count, ctx, query, node, binderCtx, false)
			if err != nil {
				return 0, err
			}
			node.Limit = expr
		}
	}

	query.Nodes[nodeId].ProjectList = projList

	return
}

func buildCTE(withExpr *tree.With, ctx CompilerContext, query *Query, binderCtx *BinderContext) error {
	if withExpr == nil {
		return nil
	}

	var err error
	for _, cte := range withExpr.CTEs {
		switch stmt := cte.Stmt.(type) {
		case *tree.Select:
			_, err = buildSelect(stmt, ctx, query, binderCtx)
		case *tree.ParenSelect:
			_, err = buildSelect(stmt.Select, ctx, query, binderCtx)
		default:
			err = errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
		}
		if err != nil {
			return err
		}

		// add a projection node
		alias := string(cte.Name.Alias)
		preNode := query.Nodes[len(query.Nodes)-1]

		node := &Node{
			NodeType: plan.Node_MATERIAL,
			Children: []int32{preNode.NodeId},
		}

		columnLength := len(preNode.ProjectList)
		tableDef := &TableDef{
			Name: alias,
			Cols: make([]*ColDef, columnLength),
		}
		exprs := make([]*Expr, columnLength)
		if cte.Name.Cols != nil {
			if len(preNode.ProjectList) != len(cte.Name.Cols) {
				return errors.New(errno.InvalidColumnReference, "CTE table column length not match")
			}
			for idx, col := range cte.Name.Cols {
				exprs[idx] = &Expr{
					Expr:      nil,
					TableName: alias,
					ColName:   string(col),
					Typ:       preNode.ProjectList[idx].Typ,
				}
				tableDef.Cols[idx] = &ColDef{
					Typ:  preNode.ProjectList[idx].Typ,
					Name: string(col),
				}
			}
			node.ProjectList = exprs
		} else {
			for idx, col := range preNode.ProjectList {
				exprs[idx] = &Expr{
					Expr:      nil,
					TableName: alias,
					ColName:   col.ColName,
					Typ:       col.Typ,
				}
				tableDef.Cols[idx] = &ColDef{
					Typ:  col.Typ,
					Name: col.ColName,
				}
			}
			node.ProjectList = exprs
		}

		// set cte table to binderCtx
		binderCtx.cteTables[strings.ToLower(alias)] = tableDef
		// append node
		appendQueryNode(query, node)

		// set cte table node_id to step
		cteNodeId := query.Nodes[len(query.Nodes)-1].NodeId
		query.Steps = append(query.Steps, cteNodeId)
	}

	return nil
}

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, projList []*Expr, err error) {
	// from
	nodeId, err = buildFrom(stmt.From.Tables, ctx, query, binderCtx)
	if err != nil {
		return
	}

	node := query.Nodes[nodeId]

	// filter
	if stmt.Where != nil {
		node.WhereList, err = splitAndBuildExpr(stmt.Where.Expr, ctx, query, node, binderCtx, false)
		if err != nil {
			return
		}
	}

	// FIXME: Agg (group by && having)
	if stmt.GroupBy != nil || stmt.Having != nil {
		aggNode := &Node{
			NodeType: plan.Node_AGG,
			Children: []int32{nodeId},
		}

		// group_by
		if stmt.GroupBy != nil {
			exprs := make([]*Expr, 0, len(stmt.GroupBy))
			for _, groupByExpr := range stmt.GroupBy {
				expr, _, err := buildExpr(groupByExpr, ctx, query, aggNode, binderCtx, false)
				if err != nil {
					return 0, nil, err
				}
				exprs = append(exprs, expr)
			}
			aggNode.GroupBy = exprs
		}

		// having
		if stmt.Having != nil {
			exprs, err := splitAndBuildExpr(stmt.Having.Expr, ctx, query, aggNode, binderCtx, true)
			if err != nil {
				return 0, nil, err
			}
			aggNode.WhereList = append(aggNode.WhereList, exprs...)
		}

		aggNode.ProjectList = make([]*Expr, len(node.ProjectList))
		for i, expr := range node.ProjectList {
			if expr == nil {
				panic(i)
			}
			aggNode.ProjectList[i] = &Expr{
				Typ:       expr.Typ,
				TableName: expr.TableName,
				ColName:   expr.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}
		}

		nodeId = appendQueryNode(query, aggNode)
		node = query.Nodes[nodeId]
	}

	// FIXME: projection
	needAgg := node.NodeType == plan.Node_AGG
	for _, selectExpr := range stmt.Exprs {
		expr, _, err := buildExpr(selectExpr.Expr, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return 0, nil, err
		}
		switch listExpr := expr.Expr.(type) {
		case *plan.Expr_List:
			// select a.* from tbl a or select * from tbl,   buildExpr() will return plan.ExprList
			projList = append(projList, listExpr.List.List...)
		default:
			alias := string(selectExpr.As)
			if alias == "" {
				expr.ColName = tree.String(&selectExpr, dialect.MYSQL)
			} else {
				expr.TableName = ""
				expr.ColName = alias
				binderCtx.columnAlias[alias] = expr
			}
			projList = append(projList, expr)
		}
	}

	// FIXME: distinct
	if stmt.Distinct {
		if stmt.GroupBy != nil {
			return 0, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport select statement(distinct with group by): %T", stmt))
		}
		distinctNode := &Node{
			NodeType: plan.Node_AGG,
			Children: []int32{nodeId},
		}
		distinctNode.GroupBy = projList
		distinctNode.ProjectList = projList
		nodeId = appendQueryNode(query, distinctNode)
	}

	return
}
