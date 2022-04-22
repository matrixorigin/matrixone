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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildSelect(stmt *tree.Select, ctx CompilerContext, query *Query) error {
	aliasCtx := &AliasContext{
		tableAlias:  make(map[string]*plan.TableDef),
		columnAlias: make(map[string]*plan.Expr),
	}

	//with

	//clause
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		err := buildSelectClause(selectClause, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}

	node := getLastSimpleNode(query)

	//orderby.   push down sort operator is not implement
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

	//fetch

	//lock

	return nil
}

func buildSelectClause(stmt *tree.SelectClause, ctx CompilerContext, query *Query, aliasCtx *AliasContext) error {
	var winspec *plan.WindowSpec

	//from
	err := buildFrom(stmt.From.Tables, ctx, query, aliasCtx)
	if err != nil {
		return err
	}

	node := getLastSimpleNode(query)

	//projection
	//todo get windowspec, aggregation
	var projections []*plan.Expr
	for _, selectExpr := range stmt.Exprs {
		expr, err := buildExpr(selectExpr.Expr, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		if selectExpr.As != "" {
			aliasCtx.columnAlias[string(selectExpr.As)] = expr
		}
		projections = append(projections, expr)
	}
	node.ProjectList = projections

	//distinct
	if stmt.Distinct {
		if stmt.GroupBy != nil {
			panic(moerr.NewError(moerr.NYI, "Distinc NYI"))
		}

		node.GroupBy = projections
	}

	//filter
	if stmt.Where != nil {
		exprs, err := splitAndBuildExpr(stmt.Where.Expr, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		node.WhereList = exprs
	}

	//group_by
	if stmt.GroupBy != nil {
		var exprs []*plan.Expr

		for _, groupByExpr := range stmt.GroupBy {
			expr, err := buildExpr(groupByExpr, ctx, query, aliasCtx)
			if err != nil {
				return err
			}
			exprs = append(exprs, expr)
		}
		node.GroupBy = exprs
	}

	//having
	if stmt.Having != nil {
		exprs, err := splitAndBuildExpr(stmt.Having.Expr, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		//todo confirm
		node.WhereList = append(node.WhereList, exprs...)
	}

	//window
	if winspec != nil {
		node.WinSpec = winspec
	}

	return nil
}
