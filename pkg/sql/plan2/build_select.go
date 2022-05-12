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

func buildSelect(stmt *tree.Select, ctx CompilerContext, query *Query, selectCtx *SelectContext) error {
	//with
	err := buildCTE(stmt.With, ctx, query, selectCtx)
	if err != nil {
		return err
	}

	//clause
	var projections []*plan.Expr

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		projections, err = buildSelectClause(selectClause, ctx, query, selectCtx)
		if err != nil {
			return err
		}
	case *tree.ParenSelect:
		return buildSelect(selectClause.Select, ctx, query, selectCtx)
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	rootId := int32(len(query.Nodes) - 1)

	// Build ORDER BY clause
	if stmt.OrderBy != nil {
		node := &plan.Node{
			NodeType: plan.Node_SORT,
			NodeId:   rootId + 1,
			Children: []int32{rootId},
		}

		orderBys := make([]*plan.OrderBySpec, 0, len(stmt.OrderBy))
		for _, order := range stmt.OrderBy {
			orderBy := &plan.OrderBySpec{}
			expr, err := buildExpr(order.Expr, ctx, query, selectCtx)
			if err != nil {
				return err
			}
			orderBy.OrderBy = expr

			switch order.Direction {
			case tree.DefaultDirection:
				orderBy.OrderByFlags = plan.OrderBySpec_INTERNAL
			case tree.Ascending:
				orderBy.OrderByFlags = plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.OrderByFlags = plan.OrderBySpec_DESC
			}
			orderBys = append(orderBys, orderBy)
		}
		node.OrderBy = orderBys

		query.Nodes = append(query.Nodes, node)
		rootId++
	}

	// Build LIMIT clause
	if stmt.Limit != nil {
		//offset
		if stmt.Limit.Offset != nil {
			expr, err := buildExpr(stmt.Limit.Offset, ctx, query, selectCtx)
			if err != nil {
				return err
			}
			query.Nodes[rootId].Offset = expr
		}

		//limit
		if stmt.Limit.Count != nil {
			expr, err := buildExpr(stmt.Limit.Count, ctx, query, selectCtx)
			if err != nil {
				return err
			}
			query.Nodes[rootId].Limit = expr
		}
	}

	//fetch

	//lock
	preNode := query.Nodes[len(query.Nodes)-1]
	preNode.ProjectList = projections
	query.Steps = append(query.Steps, preNode.NodeId)
	return nil
}

func buildCTE(withExpr *tree.With, ctx CompilerContext, query *Query, selectCtx *SelectContext) error {
	if withExpr == nil {
		return nil
	}

	for _, cte := range withExpr.CTEs {
		err := buildStatement(cte.Stmt, ctx, query)
		if err != nil {
			return err
		}
		//add a projection node
		alias := string(cte.Name.Alias)
		node := &plan.Node{
			NodeType: plan.Node_MATERIAL,
		}
		preNode := query.Nodes[len(query.Nodes)-1]

		columnLength := len(preNode.ProjectList)
		tableDef := &plan.TableDef{
			Name: alias,
			Cols: make([]*plan.ColDef, columnLength),
		}
		exprs := make([]*plan.Expr, columnLength)
		if cte.Name.Cols != nil {
			if len(preNode.ProjectList) != len(cte.Name.Cols) {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("CTE table column length not match"))
			}
			for idx, col := range cte.Name.Cols {
				exprs[idx] = &plan.Expr{
					Expr:  nil,
					Alias: alias + "." + string(col),
					Typ:   preNode.ProjectList[idx].Typ,
				}
				tableDef.Cols[idx] = &plan.ColDef{
					Typ:  preNode.ProjectList[idx].Typ,
					Name: string(col),
				}
			}
			node.ProjectList = exprs
		} else {
			for idx, col := range preNode.ProjectList {
				exprs[idx] = &plan.Expr{
					Expr:  nil,
					Alias: alias + "." + col.Alias,
					Typ:   col.Typ,
				}
				tableDef.Cols[idx] = &plan.ColDef{
					Typ:  col.Typ,
					Name: col.Alias,
				}
			}
			node.ProjectList = exprs
		}

		//set cte table to selectCtx
		selectCtx.cteTables[strings.ToUpper(alias)] = tableDef
		//append node
		appendQueryNode(query, node, false)

		//set cte table node_id to step
		cteNodeId := query.Nodes[len(query.Nodes)-1].NodeId
		query.Steps = append(query.Steps, cteNodeId)
	}

	return nil
}

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query, selectCtx *SelectContext) ([]*plan.Expr, error) {
	//from
	err := buildFrom(stmt.From.Tables, ctx, query, selectCtx)
	if err != nil {
		return nil, err
	}

	node := query.Nodes[len(query.Nodes)-1]

	//filter
	if stmt.Where != nil {
		exprs, err := splitAndBuildExpr(stmt.Where.Expr, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		node.WhereList = exprs
	}

	//projection
	//todo get windowspec
	var projectionList []*plan.Expr
	for _, selectExpr := range stmt.Exprs {
		expr, err := buildExpr(selectExpr.Expr, ctx, query, selectCtx)
		if err != nil {
			return nil, err
		}
		switch listExpr := expr.Expr.(type) {
		case *plan.Expr_List:
			//select a.* from tbl a or select * from tbl,   buildExpr() will return ExprList
			for _, col := range listExpr.List.List {
				projectionList = append(projectionList, col)
			}
		default:
			alias := string(selectExpr.As)
			if alias == "" {
				expr.Alias = tree.String(&selectExpr, dialect.MYSQL)
			} else {
				expr.Alias = alias
				selectCtx.columnAlias[alias] = expr
			}
			projectionList = append(projectionList, expr)
		}
	}

	//Agg (group by && having)
	if stmt.GroupBy != nil || stmt.Having != nil {
		aggNode := &plan.Node{
			NodeType: plan.Node_AGG,
		}

		//group_by
		if stmt.GroupBy != nil {
			exprs := make([]*plan.Expr, 0, len(stmt.GroupBy))
			for _, groupByExpr := range stmt.GroupBy {
				expr, err := buildExpr(groupByExpr, ctx, query, selectCtx)
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, expr)
			}
			aggNode.GroupBy = exprs
		}

		//having
		if stmt.Having != nil {
			exprs, err := splitAndBuildExpr(stmt.Having.Expr, ctx, query, selectCtx)
			if err != nil {
				return nil, err
			}
			aggNode.WhereList = append(aggNode.WhereList, exprs...)
		}
		appendQueryNode(query, aggNode, false)
	}

	//distinct
	if stmt.Distinct {
		if stmt.GroupBy != nil {
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport select statement(distinct with group by): %T", stmt))
		}
		distinctNode := &plan.Node{
			NodeType: plan.Node_AGG,
		}
		distinctNode.GroupBy = projectionList
		appendQueryNode(query, distinctNode, false)
	}

	return projectionList, nil
}
