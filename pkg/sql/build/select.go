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

package build

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildSelectStatement(stmt tree.SelectStatement) (op.OP, error) {
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select)
	case *tree.SelectClause:
		o, _, err := b.buildSelectClause(stmt, nil)
		return o, err
	default:
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement '%T'", stmt))
	}
}

func (b *build) buildSelect(stmt *tree.Select) (op.OP, error) {
	fetch := stmt.Limit
	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, "multiple ORDER BY clauses not allowed")
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if fetch != nil {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, "multiple LIMIT clauses not allowed")
			}
			fetch = stmt.Limit
		}
	}
	return b.buildSelectWithoutParens(wrapped, orderBy, fetch)
}

func (b *build) buildSelectWithoutParens(stmt tree.SelectStatement, orderBy tree.OrderBy, fetch *tree.Limit) (op.OP, error) {
	var o op.OP
	var err error
	var es []*projection.Extend

	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("%T in buildSelectStmtWithoutParens", stmt))
	case *tree.SelectClause:
		if o, es, err = b.buildSelectClause(stmt, orderBy); err != nil {
			return nil, err
		}
	default:
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}
	cs := o.Columns()
	if len(orderBy) > 0 {
		if fetch != nil && fetch.Offset == nil && fetch.Count != nil {
			e, err := b.buildExtend(o, fetch.Count)
			if err != nil {
				return nil, err
			}
			v, ok := e.(*extend.ValueExtend)
			if !ok {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			if o, err = b.buildTop(o, orderBy, v.V.Col.([]int64)[0]); err != nil {
				return nil, err
			}
			if len(es) > 0 {
				return projection.New(o, es)
			}
			return o, nil
		} else {
			if o, err = b.buildOrderBy(o, orderBy); err != nil {
				return nil, err
			}
		}
	}
	if fetch != nil {
		if fetch.Offset != nil {
			e, err := b.buildExtend(o, fetch.Offset)
			if err != nil {
				return nil, err
			}
			v, ok := e.(*extend.ValueExtend)
			if !ok {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			o = offset.New(o, v.V.Col.([]int64)[0])
		}
		if fetch.Count != nil {
			e, err := b.buildExtend(o, fetch.Count)
			if err != nil {
				return nil, err
			}
			v, ok := e.(*extend.ValueExtend)
			if !ok {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
			}
			o = limit.New(o, v.V.Col.([]int64)[0])
		}
	}
	if len(es) > 0 {
		if o, err = projection.New(o, es); err != nil {
			return nil, err
		}
	}
	o.SetColumns(cs)
	return o, nil
}

func (b *build) buildSelectClause(stmt *tree.SelectClause, orderBy tree.OrderBy) (op.OP, []*projection.Extend, error) {
	if err := b.checkProjection(stmt.Exprs); err != nil {
		return nil, nil, err
	}
	if stmt.From == nil {
		return nil, nil, sqlerror.New(errno.SQLStatementNotYetComplete, "need from clause")
	}
	if b.hasSummarize(stmt.Exprs) {
		return b.buildSelectClauseWithSummarize(stmt)
	}
	return b.buildSelectClauseWithoutSummarize(stmt, orderBy)
}

func (b *build) buildSelectClauseWithSummarize(stmt *tree.SelectClause) (op.OP, []*projection.Extend, error) {
	var o op.OP
	var err error

	if o, err = b.buildFrom(stmt.From.Tables); err != nil {
		return nil, nil, err
	}
	if len(stmt.GroupBy) != 0 {
		if len(stmt.Exprs) != 0 {
			if o, err = b.buildGroupBy(o, stmt.Exprs, stmt.GroupBy, stmt.Where); err != nil {
				return nil, nil, err
			}
		}
	} else {
		if o, err = b.buildSummarize(o, stmt.Exprs, stmt.Where); err != nil {
			return nil, nil, err
		}
	}
	if stmt.Distinct {
		if o, err = b.buildDedup(o); err != nil {
			return nil, nil, err
		}
		attrs := o.Columns()
		mp := o.Attribute()
		pes := make([]*projection.Extend, len(attrs))
		for i, attr := range attrs {
			pes[i] = &projection.Extend{
				Alias: attr,
				E: &extend.Attribute{
					Name: attr,
					Type: mp[attr].Oid,
				},
			}
		}
		if o, err = projection.New(o, pes); err != nil {
			return nil, nil, err
		}
		return o, pes, nil
	}
	return o, nil, nil
}

func (b *build) buildSelectClauseWithoutSummarize(stmt *tree.SelectClause, orderBy tree.OrderBy) (op.OP, []*projection.Extend, error) {
	var o op.OP
	var err error
	var es, pes []*projection.Extend

	if o, err = b.buildFrom(stmt.From.Tables); err != nil {
		return nil, nil, err
	}
	mp := make(map[string]uint8)
	if stmt.Where != nil {
		if err := b.extractExtend(o, stmt.Where.Expr, &es, mp); err != nil {
			return nil, nil, err
		}
	}
	if len(orderBy) > 0 {
		for _, ord := range orderBy {
			if err := b.extractExtend(o, ord.Expr, &es, mp); err != nil {
				return nil, nil, err
			}
		}
	}
	if len(stmt.GroupBy) != 0 {
		var gs []*extend.Attribute
		if o, pes, err = b.buildProjectionWithOrder(o, stmt.Exprs, es, mp); err != nil {
			return nil, nil, err
		}
		if stmt.Where != nil {
			if o, err = b.buildWhere(o, stmt.Where); err != nil {
				return nil, nil, err
			}
		}
		for _, g := range stmt.GroupBy {
			e, err := b.buildExtend(o, g)
			if err != nil {
				return nil, nil, err
			}
			gs = append(gs, &extend.Attribute{
				Name: e.String(),
				Type: e.ReturnType(),
			})
		}
		o = dedup.New(o, gs)
	} else {
		if o, pes, err = b.buildProjectionWithOrder(o, stmt.Exprs, es, mp); err != nil {
			return nil, nil, err
		}
		if stmt.Where != nil {
			if o, err = b.buildWhere(o, stmt.Where); err != nil {
				return nil, nil, err
			}
		}
	}
	if stmt.Having != nil {
		if o, err = b.buildWhere(o, stmt.Having); err != nil {
			return nil, nil, err
		}
	}
	if stmt.Distinct {
		if o, err = b.buildDedup(o); err != nil {
			return nil, nil, err
		}
	}
	return o, pes, nil
}
