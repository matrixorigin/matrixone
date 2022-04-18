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

func buildSelect(stmt *tree.Select, ctx CompilerContext, query *Query) error {
	//with

	//clause
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		err := buildSelectClause(selectClause, ctx, query)
		if err != nil {
			return err
		}
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	//orderby.   push down sort operator is not implement
	if stmt.OrderBy != nil {
		orderBy := &plan.OrderBySpec{}
		for _, order := range stmt.OrderBy {
			expr, err := buildExpr(order.Expr, ctx, query)
			if err != nil {
				return err
			}
			orderBy.OrderBy = append(orderBy.OrderBy, expr)

			switch order.Direction {
			case tree.DefaultDirection:
				orderBy.OrderByFlags = append(orderBy.OrderByFlags, plan.OrderBySpec_INTERNAL)
			case tree.Ascending:
				orderBy.OrderByFlags = append(orderBy.OrderByFlags, plan.OrderBySpec_ASC)
			case tree.Descending:
				orderBy.OrderByFlags = append(orderBy.OrderByFlags, plan.OrderBySpec_DESC)
			}
			//todo confirm what OrderByCollations mean
		}
		query.Nodes[len(query.Nodes)-1].OrderBy = orderBy
	}

	//limit
	if stmt.Limit != nil {
		//offset
		expr, err := buildExpr(stmt.Limit.Offset, ctx, query)
		if err != nil {
			return err
		}
		query.Nodes[len(query.Nodes)-1].Offset = expr

		//limit
		expr, err = buildExpr(stmt.Limit.Count, ctx, query)
		if err != nil {
			return err
		}
		query.Nodes[len(query.Nodes)-1].Limit = expr
	}

	//fetch

	//lock

	return nil
}

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query) error {
	var winspec *plan.WindowSpec

	//from
	err := buildFrom(stmt.From.Tables, ctx, query)
	if err != nil {
		return err
	}

	//projection
	//todo get windowspec, aggregation
	var projections []*plan.Expr
	for _, selectExpr := range stmt.Exprs {
		expr, err := buildExpr(selectExpr.Expr, ctx, query)
		if err != nil {
			return err
		}
		projections = append(projections, expr)
	}
	query.Nodes[len(query.Nodes)-1].ProjectList = projections

	//distinct
	if stmt.Distinct {
		if stmt.GroupBy != nil {
			//todo confirm: throw error in this case?
		}

		query.Nodes[len(query.Nodes)-1].GroupBy = projections
	}

	//filter
	if stmt.Where != nil {
		exprs, err := splitAndBuildExpr(stmt.Where.Expr, ctx, query)
		if err != nil {
			return err
		}
		query.Nodes[len(query.Nodes)-1].WhereList = exprs
	}

	//group_by
	if stmt.GroupBy != nil {
		var exprs []*plan.Expr

		for _, groupByExpr := range stmt.GroupBy {
			expr, err := buildExpr(groupByExpr, ctx, query)
			if err != nil {
				return err
			}
			exprs = append(exprs, expr)
		}
		query.Nodes[len(query.Nodes)-1].GroupBy = exprs
	}

	//having
	if stmt.Having != nil {
		exprs, err := splitAndBuildExpr(stmt.Where.Expr, ctx, query)
		if err != nil {
			return err
		}
		//todo confirm
		query.Nodes[len(query.Nodes)-1].WhereList = append(query.Nodes[len(query.Nodes)-1].WhereList, exprs...)
	}

	//window
	if winspec != nil {
		query.Nodes[len(query.Nodes)-1].WinSpec = winspec
	}

	return nil
}
