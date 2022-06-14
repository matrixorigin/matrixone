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

	var selectExprs tree.SelectExprs

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		nodeId, selectExprs, err = buildSelectClause(selectClause, ctx, query, binderCtx)
		if err != nil {
			return
		}
	case *tree.ParenSelect:
		return buildSelect(selectClause.Select, ctx, query, binderCtx)
	default:
		return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	node := query.Nodes[nodeId]

	var selectList []*Expr

	// build SELECT clause
	needAgg := node.NodeType == plan.Node_AGG
	for _, selectExpr := range selectExprs {
		expr, isAgg, err := buildExpr(selectExpr.Expr, ctx, query, node, binderCtx, needAgg)
		if err != nil {
			return 0, err
		}
		if isAgg {
			if !needAgg {
				// Found aggregate function in SELECT without GROUP BY or HAVING. Need to append an AGG node
				aggNode := &Node{
					NodeType: plan.Node_AGG,
					Children: []int32{nodeId},
				}

				for _, selectExpr := range selectExprs {
					expr, isAgg, err := buildExpr(selectExpr.Expr, ctx, query, aggNode, binderCtx, true)
					if err != nil {
						return 0, err
					}
					if !isAgg {
						return 0, errors.New(errno.InvalidColumnReference, fmt.Sprintf("'%s' must appear in the GROUP BY clause or be used in an aggregate function", tree.String(selectExpr.Expr, dialect.MYSQL)))
					}
					switch listExpr := expr.Expr.(type) {
					case *plan.Expr_List:
						// select a.* from tbl a or select * from tbl,   buildExpr() will return plan.ExprList
						selectList = append(selectList, listExpr.List.List...)
					default:
						alias := string(selectExpr.As)
						if alias == "" {
							expr.ColName = tree.String(&selectExpr, dialect.MYSQL)
						} else {
							binderCtx.columnAlias[alias] = &plan.Expr{
								Typ:     expr.Typ,
								ColName: alias,
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 0,
										ColPos: int32(len(aggNode.ProjectList)),
									},
								},
							}
							expr.TableName = ""
							expr.ColName = alias
							aggNode.ProjectList = append(aggNode.ProjectList, DeepCopyExpr(expr))
						}
						selectList = append(selectList, expr)
					}
				}

				nodeId = appendQueryNode(query, aggNode)
				node = query.Nodes[nodeId]

				break
			}

			node.ProjectList = append(node.ProjectList, DeepCopyExpr(expr))
		}
		switch listExpr := expr.Expr.(type) {
		case *plan.Expr_List:
			// select a.* from tbl a or select * from tbl,   buildExpr() will return plan.ExprList
			selectList = append(selectList, listExpr.List.List...)
		default:
			alias := string(selectExpr.As)
			if alias == "" {
				expr.ColName = tree.String(&selectExpr, dialect.MYSQL)
			} else {
				binderCtx.columnAlias[alias] = &plan.Expr{
					Typ:     expr.Typ,
					ColName: alias,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(len(node.ProjectList)),
						},
					},
				}
				expr.TableName = ""
				expr.ColName = alias
				node.ProjectList = append(node.ProjectList, DeepCopyExpr(expr))
			}
			selectList = append(selectList, expr)
		}
	}

	// Build ORDER BY clause
	if stmt.OrderBy != nil {
		preNode := query.Nodes[nodeId]
		sortNode := &Node{
			NodeType:    plan.Node_SORT,
			Children:    []int32{nodeId},
			ProjectList: preNode.ProjectList,
		}

		orderBys := make([]*plan.OrderBySpec, 0, len(stmt.OrderBy))
		var expr *Expr
		var isAgg bool
		for _, order := range stmt.OrderBy {
			if preNode.NodeType != plan.Node_AGG {
				expr, _, err = buildExpr(order.Expr, ctx, query, sortNode, binderCtx, false)
				if err != nil {
					return
				}
			} else {
				expr, isAgg, err = buildExpr(order.Expr, ctx, query, sortNode, binderCtx, false)
				if err != nil || isAgg {
					expr, isAgg, err = buildExpr(order.Expr, ctx, query, preNode, binderCtx, true)
					if err != nil {
						return
					}
					if !isAgg {
						err = errors.New(errno.GroupingError, fmt.Sprintf("'%s' must appear in the GROUP BY clause or be used in an aggregate function", tree.String(order.Expr, dialect.MYSQL)))
						return
					}
					preNode.ProjectList = append(preNode.ProjectList, expr)
					expr = &plan.Expr{
						Typ:       expr.Typ,
						TableName: expr.TableName,
						ColName:   expr.ColName,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 0,
								ColPos: int32(len(preNode.ProjectList) - 1),
							},
						},
					}
				}
			}

			orderBy := &plan.OrderBySpec{}
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

		// Re-bind projection list for SELECT clause
		sortNode.ProjectList = make([]*Expr, 0, len(selectList))
		for _, selectExpr := range selectExprs {
			alias := string(selectExpr.As)
			if len(alias) != 0 {
				sortNode.ProjectList = append(sortNode.ProjectList, DeepCopyExpr(binderCtx.columnAlias[alias]))
				continue
			}
			expr, _, err := buildExpr(selectExpr.Expr, ctx, query, sortNode, binderCtx, false)
			if err != nil {
				return 0, err
			}
			switch listExpr := expr.Expr.(type) {
			case *plan.Expr_List:
				// select a.* from tbl a or select * from tbl,   buildExpr() will return plan.ExprList
				sortNode.ProjectList = append(sortNode.ProjectList, listExpr.List.List...)
			default:
				sortNode.ProjectList = append(sortNode.ProjectList, expr)
			}
		}

		nodeId = appendQueryNode(query, sortNode)
		node = query.Nodes[nodeId]
	} else {
		node.ProjectList = selectList
	}

	// Build LIMIT clause
	if stmt.Limit != nil {

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

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query, binderCtx *BinderContext) (nodeId int32, selectExprs tree.SelectExprs, err error) {

	// build FROM clause
	nodeId, err = buildFrom(stmt.From.Tables, ctx, query, binderCtx)
	if err != nil {
		return
	}

	node := query.Nodes[nodeId]

	// build WHERE clause
	if stmt.Where != nil {
		node.WhereList, err = splitAndBuildExpr(stmt.Where.Expr, ctx, query, node, binderCtx, false)
		if err != nil {
			return
		}
	}

	// build GROUP BY and HAVING clause and DISTINCT
	if stmt.GroupBy != nil || stmt.Having != nil || stmt.Distinct {
		aggNode := &Node{
			NodeType: plan.Node_AGG,
			Children: []int32{nodeId},
		}

		if stmt.GroupBy != nil {
			if stmt.Distinct {
				err = errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport select statement(DISTINCT with GROUP BY): %T", stmt))
				return
			}

			aggNode.GroupBy = make([]*Expr, len(stmt.GroupBy))
			for i, groupByExpr := range stmt.GroupBy {
				aggNode.GroupBy[i], _, err = buildExpr(groupByExpr, ctx, query, aggNode, binderCtx, false)
				if err != nil {
					return
				}
			}
		} else if stmt.Distinct {
			aggNode.GroupBy = make([]*Expr, len(stmt.Exprs))
			for i, selectExpr := range stmt.Exprs {
				aggNode.GroupBy[i], _, err = buildExpr(selectExpr.Expr, ctx, query, aggNode, binderCtx, false)
				if err != nil {
					return
				}
			}
		}

		if stmt.Having != nil {
			if stmt.Distinct {
				err = errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport select statement(DISTINCT with HAVING): %T", stmt))
				return
			}

			aggNode.WhereList, err = splitAndBuildExpr(stmt.Having.Expr, ctx, query, aggNode, binderCtx, true)
			if err != nil {
				return
			}
		}

		aggNode.ProjectList = make([]*Expr, len(aggNode.GroupBy)+len(aggNode.AggList))
		var idx int32

		for _, expr := range aggNode.GroupBy {
			aggNode.ProjectList[idx] = &Expr{
				Typ:       expr.Typ,
				TableName: expr.TableName,
				ColName:   expr.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: idx,
					},
				},
			}
			idx++
		}

		for _, expr := range aggNode.AggList {
			aggNode.ProjectList[idx] = &Expr{
				Typ:       expr.Typ,
				TableName: expr.TableName,
				ColName:   expr.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: idx,
					},
				},
			}
			idx++
		}

		nodeId = appendQueryNode(query, aggNode)
	}

	selectExprs = stmt.Exprs

	return
}
