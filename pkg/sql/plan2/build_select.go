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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildSelect(stmt *tree.Select, ctx CompilerContext, query *Query) error {
	aliasCtx := &AliasContext{
		tableAlias:  make(map[string]string),
		columnAlias: make(map[string]*plan.Expr),
	}

	//with

	//clause
	var projections []*plan.Expr
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		tmp, err := buildSelectClause(selectClause, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		projections = tmp
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	preNode := query.Nodes[len(query.Nodes)-1]

	if stmt.OrderBy == nil && stmt.Limit == nil {
		if projections != nil {
			preNode.ProjectList = projections
		}
		return nil
	}

	node := &plan.Node{
		NodeType:    plan.Node_SORT,
		ProjectList: projections,
		Children:    []int32{preNode.NodeId},
	}

	//orderby
	if stmt.OrderBy != nil {
		var orderBys []*plan.OrderBySpec
		for _, order := range stmt.OrderBy {
			orderBy := &plan.OrderBySpec{}
			expr, err := buildExpr(order.Expr, ctx, query, aliasCtx)
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
	}

	//limit
	if stmt.Limit != nil {
		//offset
		expr, err := buildExpr(stmt.Limit.Offset, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		node.Offset = expr

		//limit
		expr, err = buildExpr(stmt.Limit.Count, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		node.Limit = expr
	}
	appendQueryNode(query, node, true)

	//fetch

	//lock

	return nil
}

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query, aliasCtx *AliasContext) ([]*plan.Expr, error) {
	//from
	err := buildFrom(stmt.From.Tables, ctx, query, aliasCtx)
	if err != nil {
		return nil, err
	}

	node := query.Nodes[len(query.Nodes)-1]

	//filter
	if stmt.Where != nil {
		exprs, err := splitAndBuildExpr(stmt.Where.Expr, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		node.WhereList = exprs
	}

	//projection
	//todo get windowspec
	var projections []*plan.Expr
	for _, selectExpr := range stmt.Exprs {
		expr, err := buildExpr(selectExpr.Expr, ctx, query, aliasCtx)
		if err != nil {
			return nil, err
		}
		switch listExpr := expr.Expr.(type) {
		case *plan.Expr_List:
			//select a.* from tbl a or select * from tbl,   buildExpr() will return ExprList
			for _, col := range listExpr.List.List {
				projections = append(projections, col)
			}
		default:
			alias := string(selectExpr.As)
			if alias == "" {
				expr.Alias = tree.String(&selectExpr, dialect.MYSQL)
			} else {
				expr.Alias = alias
				aliasCtx.columnAlias[alias] = expr
			}
			projections = append(projections, expr)
		}
	}

	if stmt.GroupBy == nil && stmt.Having == nil && !stmt.Distinct {
		return projections, nil
	}

	if stmt.GroupBy != nil || stmt.Having != nil {
		preNode := query.Nodes[len(query.Nodes)-1]
		aggNode := &plan.Node{
			NodeType:    plan.Node_AGG,
			ProjectList: projections,
		}
		//group_by
		if stmt.GroupBy != nil {
			var exprs []*plan.Expr
			for _, groupByExpr := range stmt.GroupBy {
				expr, err := buildExpr(groupByExpr, ctx, query, aliasCtx)
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, expr)
			}
			aggNode.GroupBy = exprs
		}
		//having
		if stmt.Having != nil {
			if stmt.GroupBy == nil {
				//todo select a from tbl having max(a) > 10   will rewrite to  select a from tbl group by null having max(a) > 10
			}
			exprs, err := splitAndBuildExpr(stmt.Having.Expr, ctx, query, aliasCtx)
			if err != nil {
				return nil, err
			}
			//todo confirm
			aggNode.WhereList = append(aggNode.WhereList, exprs...)
		}
		aggNode.Children = []int32{preNode.NodeId}
		appendQueryNode(query, aggNode, true)
		if !stmt.Distinct {
			projections = nil
		}
	}

	//distinct
	if stmt.Distinct {
		preNode := query.Nodes[len(query.Nodes)-1]
		distinctNode := &plan.Node{
			NodeType: plan.Node_AGG,
		}
		distinctNode.GroupBy = projections
		distinctNode.Children = []int32{preNode.NodeId}
		appendQueryNode(query, distinctNode, true)
		projections = nil
	}

	return projections, nil
}
