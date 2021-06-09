package build

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildSelectStatement(stmt tree.SelectStatement) (op.OP, error) {
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select)
	case *tree.SelectClause:
		return b.buildSelectClause(stmt)
	default:
		return nil, fmt.Errorf("unknown select statement '%T'", stmt)
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
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			fetch = stmt.Limit
		}
	}
	return b.buildSelectWithoutParens(wrapped, orderBy, fetch)
}

func (b *build) buildSelectWithoutParens(stmt tree.SelectStatement, orderBy tree.OrderBy, fetch *tree.Limit) (op.OP, error) {
	var o op.OP
	var err error

	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return nil, fmt.Errorf("%T in buildSelectStmtWithoutParens", stmt)
	case *tree.SelectClause:
		o, err = b.buildSelectClause(stmt)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown select statement: %T", stmt)
	}
	if len(orderBy) > 0 {
		if fetch != nil && fetch.Offset == nil && fetch.Count != nil {
			e, err := b.buildExtend(o, fetch.Count)
			if err != nil {
				return nil, err
			}
			v, ok := e.(*extend.ValueExtend)
			if !ok {
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
			}
			return b.buildTop(o, orderBy, v.V.Col.([]int64)[0])
		} else {
			o, err = b.buildOrderBy(o, orderBy)
			if err != nil {
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
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
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
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
			}
			if v.V.Typ.Oid != types.T_int64 {
				return nil, fmt.Errorf("Undeclared variable '%s'", e)
			}
			o = limit.New(o, v.V.Col.([]int64)[0])
		}
	}
	return o, nil
}

func (b *build) buildSelectClause(stmt *tree.SelectClause) (op.OP, error) {
	var o op.OP
	var err error

	if stmt.From == nil {
		return nil, errors.New("need from clause")
	}
	if o, err = b.buildFrom(stmt.From.Tables); err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		if o, err = b.buildWhere(o, stmt.Where); err != nil {
			return nil, err
		}
	}
	if len(stmt.GroupBy) != 0 {
		var gs []*extend.Attribute
		if o, gs, err = b.buildGroupBy(o, stmt.GroupBy); err != nil {
			return nil, err
		}
		if len(stmt.Exprs) != 0 {
			if b.hasSummarize(stmt.Exprs) {
				if o, err = b.buildProjectionWithGroup(o, stmt.Exprs, gs); err != nil {
					return nil, err
				}
			} else {
				o = dedup.New(o, gs)
				if o, err = b.buildProjection(o, stmt.Exprs); err != nil {
					return nil, err
				}
			}
		}
		if stmt.Having != nil {
			if o, err = b.buildWhere(o, stmt.Having); err != nil {
				return nil, err
			}
		}
		if stmt.Distinct {
			if o, err = b.buildDedup(o); err != nil {
				return nil, err
			}
		}
		return o, nil
	}
	if len(stmt.Exprs) != 0 {
		if b.hasSummarize(stmt.Exprs) {
			if o, err = b.buildSummarize(o, stmt.Exprs); err != nil {
				return nil, err
			}
		} else {
			if o, err = b.buildProjection(o, stmt.Exprs); err != nil {
				return nil, err
			}
		}
	}
	if stmt.Having != nil {
		if o, err = b.buildWhere(o, stmt.Having); err != nil {
			return nil, err
		}
	}
	if stmt.Distinct {
		if o, err = b.buildDedup(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}
