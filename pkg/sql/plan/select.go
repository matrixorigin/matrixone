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

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildSelectStatement(stmt tree.SelectStatement, qry *Query) error {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return b.buildSelect(stmt, qry)
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, qry)
	case *tree.SelectClause:
		return b.buildSelectClause(stmt, nil, qry)
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
		if err := b.buildSelectClause(stmt, orderBy, qry); err != nil {
			return err
		}
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
	}
	if fetch != nil {
		if err := b.buildFetch(fetch, qry); err != nil {
			return err
		}
	}
	return nil
}

func (b *build) buildSelectClause(stmt *tree.SelectClause, orderBy tree.OrderBy, qry *Query) error {
	var err error
	var fs []*Field
	var fvars []string
	var e1, e2 extend.Extend // e1 = where filter condition, e2 = having filter condition
	var jconds []*JoinCondition

	if stmt.From == nil {
		return errors.New(errno.SQLStatementNotYetComplete, "need from clause")
	}
	if err = b.buildFrom(stmt.From.Tables, qry); err != nil {
		return err
	}
	qry.Scope = qry.Pop().Scopes[0]
	proj := new(Projection)
	groupProj, orderProj := new(Projection), new(Projection)
	if stmt.Where != nil {
		if e1, jconds, err = b.buildWhere(stmt.Where, qry); err != nil {
			return err
		}
	}
	if stmt.Having != nil {
		if e2, err = b.buildHaving(stmt.Having, qry); err != nil {
			return err
		}
	}
	result, err := b.buildProjection(stmt.Exprs, proj, qry)
	if err != nil {
		return err
	}
	if len(stmt.GroupBy) > 0 {
		if fvars, err = b.buildGroupBy(stmt.GroupBy, proj, groupProj, qry); err != nil {
			return err
		}
	}
	if len(orderBy) > 0 {
		if fs, err = b.buildOrderBy(orderBy, proj, orderProj, qry); err != nil {
			return err
		}
	}
	{ // construct result attributes
		for _, attr := range result.As {
			qry.Result = append(qry.Result, attr)
		}
	}
	proj = pruneProjection(proj)
	if len(orderBy) > 0 {
		orderProj = pruneProjection(orderProj)
	}
	if len(stmt.GroupBy) > 0 {
		groupProj = pruneProjection(groupProj)
	}
	qry.Aggs = pruneAggregation(qry.Aggs)

	if e1 != nil {
		e1 = pushDownRestrict(true, e1, qry)
	}
	if e2 != nil {
		e2 = pushDownRestrict(false, e2, qry)
	}
	if len(qry.Aggs) > 0 {
		if err = pushDownAggregation(qry.Aggs, qry); err != nil {
			return err
		}
	}
	if len(stmt.GroupBy) > 0 {
		if err = pushDownFreeVariables(groupProj.Es, qry); err != nil {
			return err
		}
	}
	{ // rebuild
		qry0 := &Query{
			Flg:        true,
			RenameRels: make(map[string]*Scope),
			Rels:       make(map[string]map[string]*Scope),
		}
		qry.Scope.registerRelations(qry0)
		if err = b.buildFrom(stmt.From.Tables, qry0); err != nil {
			return err
		}
		qry0.Scope = qry0.Pop().Scopes[0]
		qry.Scope = qry0.Scope
		if jp, ok := qry.Scope.Op.(*Join); ok && jp.Type == CROSS {
			if len(jconds) > 0 {
				ss := newScopeSet()
				ss.Conds = jconds
				ss.Scopes = qry.Scope.Children
				s, err := b.buildQualifiedJoin(INNER, ss)
				if err != nil {
					return err
				}
				qry.Scope = s
			}
		}
		qry.Scope.pushDownJoinAttribute(nil)
	}
	if e1 != nil {
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		op := &Restrict{E: e1}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for _, attr := range qry.Scope.Result.Attrs {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type,
				}
			}
		}
		s.Op = op
		qry.Scope = s
	}
	if len(fvars) > 0 || len(qry.Aggs) > 0 {
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for _, attr := range qry.Scope.Result.Attrs {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type,
				}
			}
		}
		s.Op = &Untransform{fvars}
		qry.Scope = s
	}
	{
		proj.As = append(proj.As, groupProj.As...)
		proj.Es = append(proj.Es, groupProj.Es...)
		proj.Rs = append(proj.Rs, groupProj.Rs...)
		proj.As = append(proj.As, orderProj.As...)
		proj.Es = append(proj.Es, orderProj.Es...)
		proj.Rs = append(proj.Rs, orderProj.Rs...)
		proj = pruneProjection(proj)
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for i, attr := range proj.As {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: proj.Es[i].ReturnType().ToType(),
				}
			}
		}
		s.Op = proj
		qry.Scope = s
	}
	if e2 != nil {
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		op := &Restrict{E: e2}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for _, attr := range qry.Scope.Result.Attrs {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type,
				}
			}
		}
		s.Op = op
		qry.Scope = s
	}
	if stmt.Distinct {
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for _, attr := range qry.Scope.Result.Attrs {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type,
				}
			}
		}
		s.Op = &Dedup{}
		qry.Scope = s
	}
	if len(fs) > 0 {
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for _, attr := range qry.Scope.Result.Attrs {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type,
				}
			}
		}
		s.Op = &Order{fs}
		qry.Scope = s
	}
	{
		s := &Scope{
			Name:     qry.Scope.Name,
			Children: []*Scope{qry.Scope},
		}
		{ // construct result
			s.Result.AttrsMap = make(map[string]*Attribute)
			for i, attr := range result.As {
				s.Result.Attrs = append(s.Result.Attrs, attr)
				s.Result.AttrsMap[attr] = &Attribute{
					Name: attr,
					Type: result.Es[i].ReturnType().ToType(),
				}
			}
		}
		s.Op = result
		qry.Scope = s
	}
	{ // remove redundant attributes
		attrsMap := make(map[string]uint64)
		for _, attr := range qry.Result {
			attrsMap[attr] = 1
		}
		qry.Scope.Prune(attrsMap, nil)
		if isCAQ(false, qry.Scope) {
			pushDownUntransform(qry.Scope, nil)
			qry.Scope.pruneProjection(qry.Scope.getAggregations())
		}
	}
	return nil
}
