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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (b *build) buildFrom(tbls tree.TableExprs, qry *Query) error {
	if len(tbls) == 1 {
		return b.buildTableReference(tbls[0], qry)
	}
	ss := newScopeSet()
	ss.JoinType = CROSS
	for _, tbl := range tbls {
		if err := b.buildTableReference(tbl, qry); err != nil {
			return err
		}
		ss.Scopes = append(ss.Scopes, qry.Pop().Scopes...)
	}
	s, err := b.buildJoinedScope(ss)
	if err != nil {
		return err
	}
	rs := newScopeSet()
	rs.JoinType = RELATION
	rs.Scopes = append(rs.Scopes, s)
	qry.Push(rs)
	return nil
}

func (b *build) buildTableReference(tbl tree.TableExpr, qry *Query) error {
	switch tbl := tbl.(type) {
	case *tree.Select:
		return b.buildSubQuery(tbl.Select, qry)
	case *tree.TableName:
		return b.buildTable(tbl, qry)
	case *tree.JoinTableExpr:
		return b.buildJoinedTable(tbl, qry)
	case *tree.ParenTableExpr:
		return b.buildTableReference(tbl.Expr, qry)
	case *tree.AliasedTableExpr:
		return b.buildAliasedTable(tbl.Expr, string(tbl.As.Alias), qry)
	}
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Unknown table expr: %T", tbl))
}

func (b *build) buildTableReferenceInJoinedTable(tbl tree.TableExpr, qry *Query) error {
	switch tbl := tbl.(type) {
	case *tree.Select:
		return b.buildSubQuery(tbl.Select, qry)
	case *tree.TableName:
		return b.buildTable(tbl, qry)
	case *tree.JoinTableExpr:
		return b.buildJoinedTree(tbl, qry)
	case *tree.ParenTableExpr:
		return b.buildTableReference(tbl.Expr, qry)
	case *tree.AliasedTableExpr:
		return b.buildAliasedTable(tbl.Expr, string(tbl.As.Alias), qry)
	}
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Unknown table expr: %T", tbl))
}

func (b *build) buildSubQuery(stmt tree.SelectStatement, qry *Query) error {
	subqry := &Query{}
	if err := b.buildSelectStatement(stmt, subqry); err != nil {
		return err
	}
	s := &Scope{
		Name:     subqry.Scope.Name,
		Children: []*Scope{subqry.Scope},
	}
	{ // construct result and derived relation
		rel := new(DerivedRelation)
		s.Result.AttrsMap = make(map[string]*Attribute)
		for _, attr := range subqry.Scope.Result.Attrs {
			s.Result.Attrs = append(s.Result.Attrs, attr)
			typ := subqry.Scope.Result.AttrsMap[attr].Type
			s.Result.AttrsMap[attr] = &Attribute{
				Type: typ,
				Name: attr,
			}
			rel.Proj.Rs = append(rel.Proj.Rs, 0)
			rel.Proj.As = append(rel.Proj.As, attr)
			rel.Proj.Es = append(rel.Proj.Es, &extend.Attribute{
				Name: attr,
				Type: typ.Oid,
			})
		}
		s.Op = rel
	}
	ss := newScopeSet()
	ss.JoinType = RELATION
	ss.Scopes = append(ss.Scopes, s)
	qry.Push(ss)
	return nil
}

func (b *build) buildTable(tbl *tree.TableName, qry *Query) error {
	s := new(Scope)
	rel := new(Relation)
	rel.Name = string(tbl.ObjectName)
	if len(tbl.SchemaName) == 0 {
		rel.Schema = b.db
	} else {
		rel.Schema = string(tbl.SchemaName)
	}
	s.Name = rel.Name
	if qry.Flg && qry.Rels[rel.Schema][rel.Name] != nil {
		s = qry.Rels[rel.Schema][rel.Name]
		ss := newScopeSet()
		ss.JoinType = RELATION
		ss.Scopes = append(ss.Scopes, s)
		qry.Push(ss)
		return nil
	}
	attrs, attrsMap, rows, err := b.getSchemaInfo(rel.Schema, rel.Name)
	if err != nil {
		return err
	}
	rel.Rows = rows
	rel.Attrs = attrsMap
	{ // construct projection
		for _, attr := range attrs {
			rel.Proj.Rs = append(rel.Proj.Rs, 0)
			rel.Proj.As = append(rel.Proj.As, attr)
			rel.Proj.Es = append(rel.Proj.Es, &extend.Attribute{
				Name: attr,
				Type: attrsMap[attr].Type.Oid,
			})
		}
	}
	s.Op = rel
	s.Result.Attrs = attrs
	s.Result.AttrsMap = attrsMap
	ss := newScopeSet()
	ss.JoinType = RELATION
	ss.Scopes = append(ss.Scopes, s)
	qry.Push(ss)
	return nil
}

func (b *build) buildAliasedTable(tbl tree.TableExpr, alias string, qry *Query) error {
	if qry.Flg {
		switch tbl.(type) {
		case *tree.TableName:
			if qry.RenameRels[alias] != nil {
				ss := newScopeSet()
				ss.JoinType = RELATION
				ss.Scopes = append(ss.Scopes, qry.RenameRels[alias])
				qry.Push(ss)
				return nil
			}
		case *tree.ParenTableExpr:
			ss := newScopeSet()
			ss.JoinType = RELATION
			ss.Scopes = append(ss.Scopes, qry.RenameRels[alias])
			qry.Push(ss)
			return nil
		}

	}
	if err := b.buildTableReference(tbl, qry); err != nil {
		return err
	}
	if len(alias) == 0 {
		return nil
	}
	ss := qry.Top()
	child := ss.Scopes[len(ss.Scopes)-1]
	if _, ok := child.Op.(*DerivedRelation); ok {
		child.Name = alias
		ss.Scopes[len(ss.Scopes)-1] = child
		return nil
	}
	s := &Scope{
		Name:     alias,
		Children: []*Scope{child},
	}
	op := &Rename{}
	{ // construct result
		s.Result.AttrsMap = make(map[string]*Attribute)
		for _, attr := range child.Result.Attrs {
			_, name := util.SplitTableAndColumn(attr)
			if _, ok := s.Result.AttrsMap[name]; ok {
				return errors.New(errno.DuplicateColumn, fmt.Sprintf("Duplicate column name '%s'", name))
			}
			{ // construct operator
				op.Rs = append(op.Rs, 0)
				op.As = append(op.As, name)
				op.Es = append(op.Es, &extend.Attribute{
					Name: attr,
					Type: child.Result.AttrsMap[attr].Type.Oid,
				})
			}
			s.Result.Attrs = append(s.Result.Attrs, name)
			s.Result.AttrsMap[name] = &Attribute{
				Name: name,
				Type: child.Result.AttrsMap[attr].Type,
			}
		}
	}
	s.Op = op
	ss.Scopes[len(ss.Scopes)-1] = s
	return nil
}

func (b *build) buildJoinedTable(stmt *tree.JoinTableExpr, qry *Query) error {
	if err := b.buildJoinedTree(stmt, qry); err != nil {
		return err
	}
	if qry.Top().JoinType != RELATION {
		s, err := b.buildJoinedScope(qry.Pop())
		if err != nil {
			return err
		}
		ss := newScopeSet()
		ss.JoinType = RELATION
		ss.Scopes = append(ss.Scopes, s)
		qry.Push(ss)
	}
	return nil
}

func (b *build) buildJoinedTree(stmt *tree.JoinTableExpr, qry *Query) error {
	var jt int // join type

	if err := b.buildTableReferenceInJoinedTable(stmt.Left, qry); err != nil {
		return err
	}
	if err := b.buildTableReferenceInJoinedTable(stmt.Right, qry); err != nil {
		return err
	}
	switch stmt.JoinType {
	case tree.JOIN_TYPE_FULL:
		jt = FULL
	case tree.JOIN_TYPE_LEFT:
		jt = LEFT
	case tree.JOIN_TYPE_CROSS:
		jt = CROSS
	case tree.JOIN_TYPE_RIGHT:
		jt = RIGHT
	case tree.JOIN_TYPE_NATURAL:
		jt = NATURAL
	case tree.JOIN_TYPE_INNER:
		if stmt.Cond == nil {
			jt = CROSS
		} else {
			jt = INNER
		}
	}
	if jt == FULL || jt == LEFT || jt == RIGHT || jt == INNER {
		switch stmt.Cond.(type) {
		case *tree.OnJoinCond:
		case *tree.UsingJoinCond:
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", tree.String(stmt.Cond, dialect.MYSQL)))
		default:
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", tree.String(stmt.Cond, dialect.MYSQL)))
		}
	}
	rss := qry.Pop()
	lss := qry.Pop()
	rjt, ljt := rss.JoinType, lss.JoinType
	if (jt == ljt && jt == rjt) || (ljt == RELATION && rjt == RELATION) ||
		(ljt == RELATION && rjt == jt) || (rjt == RELATION && ljt == jt) {
		ss := newScopeSet()
		ss.JoinType = jt
		ss.Conds = append(ss.Conds, lss.Conds...)
		ss.Conds = append(ss.Conds, rss.Conds...)
		ss.Scopes = append(ss.Scopes, lss.Scopes...)
		ss.Scopes = append(ss.Scopes, rss.Scopes...)
		switch cond := stmt.Cond.(type) {
		case *tree.OnJoinCond:
			if err := b.buildJoinCond(cond.Expr, ss); err != nil {
				return err
			}
		case *tree.UsingJoinCond:
		default:
		}
		qry.Push(ss)
		return nil
	}
	left, err := b.buildJoinedScope(lss)
	if err != nil {
		return err
	}
	right, err := b.buildJoinedScope(rss)
	if err != nil {
		return err
	}
	ss := newScopeSet()
	ss.JoinType = jt
	ss.Scopes = append(ss.Scopes, left)
	ss.Scopes = append(ss.Scopes, right)
	switch cond := stmt.Cond.(type) {
	case *tree.OnJoinCond:
		if err := b.buildJoinCond(cond.Expr, ss); err != nil {
			return err
		}
	case *tree.UsingJoinCond:
	default:
	}
	qry.Push(ss)
	return nil

}

func (b *build) buildJoinedScope(ss *ScopeSet) (*Scope, error) {
	switch ss.JoinType {
	case RELATION:
		return ss.Scopes[0], nil
	case CROSS:
		return b.buildCrossJoin(ss)
	case NATURAL:
		return b.buildNaturalJoin(ss.JoinType, ss)
	default:
		return b.buildQualifiedJoin(ss.JoinType, ss)
	}
}

func (b *build) getSchemaInfo(schema string, name string) ([]string, map[string]*Attribute, int64, error) {
	var attrs []string

	db, err := b.e.Database(schema)
	if err != nil {
		return nil, nil, -1, errors.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	r, err := db.Relation(name)
	if err != nil {
		return nil, nil, -1, errors.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	defer r.Close()
	defs := r.TableDefs()
	attrsMap := make(map[string]*Attribute)
	for _, def := range defs {
		if v, ok := def.(*engine.AttributeDef); ok {
			attrsMap[v.Attr.Name] = &Attribute{
				Name: v.Attr.Name,
				Type: v.Attr.Type,
			}
			attrs = append(attrs, v.Attr.Name)
		}
	}
	if b.isModify {
		priKeys, _ := r.GetPriKeyOrHideKey()
		for _, key := range priKeys {
			if _, ok := attrsMap[key.Name]; !ok {
				attrsMap[key.Name] = &Attribute{
					Name: key.Name,
					Type: key.Type,
				}
				attrs = append(attrs, key.Name)
			}
		}
	}
	return attrs, attrsMap, r.Rows(), nil
}
