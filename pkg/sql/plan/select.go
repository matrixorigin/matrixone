// Copyright 2021 Matrix Origin
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

package plan

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildSelectStatement(stmt tree.SelectStatement, qry *Query) error {
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, qry)
	case *tree.SelectClause:
		return b.buildSelectClause(stmt, qry)
	}
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement '%T'", stmt))
}

func (b *build) buildSelect(stmt *tree.Select, qry *Query) error {
	fetch := stmt.Limit
	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				return errors.New(errno.SyntaxErrororAccessRuleViolation, "multiple ORDER BY clauses not allowed")
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if fetch != nil {
				return errors.New(errno.SyntaxErrororAccessRuleViolation, "multiple LIMIT clauses not allowed")
			}
			fetch = stmt.Limit
		}
	}
	return b.buildSelectWithoutParens(wrapped, orderBy, fetch, qry)
}

func (b *build) buildSelectWithoutParens(stmt tree.SelectStatement, orderBy tree.OrderBy, fetch *tree.Limit, qry *Query) error {

	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("%T in buildSelectStmtWithoutParens", stmt))
	case *tree.SelectClause:
		if err := b.buildSelectClause(stmt, qry); err != nil {
			return err
		}
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}
	if len(orderBy) > 0 {
		if err := b.buildOrderBy(orderBy, qry); err != nil {
			return err
		}
	}
	if fetch != nil {
		if err := b.buildFetch(fetch, qry); err != nil {
			return err
		}
	}
	qry.Reduce()
	return nil
}

func (b *build) buildSelectClause(stmt *tree.SelectClause, qry *Query) error {
	if stmt.From == nil {
		return errors.New(errno.SQLStatementNotYetComplete, "need from clause")
	}
	qry.Distinct = stmt.Distinct
	if err := b.buildFrom(stmt.From.Tables, qry); err != nil {
		return err
	}
	if stmt.Where != nil {
		if err := b.buildWhere(stmt.Where, qry); err != nil {
			return err
		}
	}
	if err := b.buildProjection(stmt.Exprs, qry); err != nil {
		return err
	}
	if len(stmt.GroupBy) > 0 {
		if err := b.buildGroupBy(stmt.GroupBy, qry); err != nil {
			return err
		}
	}
	if stmt.Having != nil {
		if err := b.buildHaving(stmt.Having, qry); err != nil {
			return err
		}
	}
	return nil
}
