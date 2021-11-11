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
	"matrixone/pkg/sql/parsers/tree"
	"matrixone/pkg/vm/engine"
)

func (b *build) buildFrom(tbls tree.TableExprs, qry *Query) error {
	if _, err := b.buildFromTable(tbls[0], "", qry); err != nil {
		return err
	}
	if tbls = tbls[1:]; len(tbls) == 0 {
		return nil
	}
	return b.buildFrom(tbls, qry)
}

func (b *build) buildFromTable(tbl tree.TableExpr, alias string, qry *Query) ([]string, error) {
	switch tbl := tbl.(type) {
	case *tree.Subquery:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("subquery not support now"))
	case *tree.TableName:
		name, err := b.buildTable(tbl, qry)
		if err != nil {
			return nil, err
		}
		if len(alias) > 0 {
			return []string{alias}, b.renameRelation(name, alias, qry)
		}
		return []string{name}, nil
	case *tree.JoinTableExpr:
		return b.buildJoin(tbl, qry)
	case *tree.ParenTableExpr:
		return b.buildFromTable(tbl.Expr, alias, qry)
	case *tree.AliasedTableExpr:
		return b.buildFromTable(tbl.Expr, string(tbl.As.Alias), qry)
	}
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Unknown table expr: %T", tbl))
}

func (b *build) buildTable(tbl *tree.TableName, qry *Query) (string, error) {
	var r Relation

	r.Name = string(tbl.ObjectName)
	if len(tbl.SchemaName) == 0 {
		r.Schema = b.db
		r.Alias = r.Name
	} else {
		r.Schema = string(tbl.SchemaName)
		r.Alias = r.Schema + "." + r.Name
	}
	if _, ok := qry.RelsMap[r.Alias]; ok {
		return "", errors.New(errno.DuplicateTable, fmt.Sprintf("Not unique table/alias: '%s'", r.Alias))
	}
	attrs, attrsMap, err := b.getSchemaInfo(r.Schema, r.Name)
	if err != nil {
		return "", err
	}
	r.Attrs = attrs
	r.AttrsMap = attrsMap
	qry.RelsMap[r.Alias] = &r
	qry.Rels = append(qry.Rels, r.Alias)
	return r.Alias, nil
}

func (b *build) buildJoin(stmt *tree.JoinTableExpr, qry *Query) ([]string, error) {
	lefts, err := b.buildFromTable(stmt.Left, "", qry)
	if err != nil {
		return nil, err
	}
	rights, err := b.buildFromTable(stmt.Right, "", qry)
	if err != nil {
		return nil, err
	}
	{
		switch stmt.JoinType {
		case tree.JOIN_TYPE_FULL:
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		case tree.JOIN_TYPE_LEFT:
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		case tree.JOIN_TYPE_RIGHT:
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		}
	}
	if stmt.Cond == nil {
		return append(lefts, rights...), nil
	}
	switch cond := stmt.Cond.(type) {
	case *tree.OnJoinCond:
		if err := b.buildJoinCond(cond.Expr, lefts, rights, qry); err != nil {
			return nil, err
		}
		return append(lefts, rights...), nil
	case *tree.UsingJoinCond:
		if err := b.buildUsingJoinCond(cond.Cols, lefts, rights, qry); err != nil {
			return nil, err
		}
		return append(lefts, rights...), nil
	}
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", stmt.Cond))
}

func (b *build) renameRelation(old, new string, qry *Query) error {
	v, ok := qry.RelsMap[old]
	if !ok {
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Table '%s' doesn't exist", old))
	}
	for i := 0; i < len(qry.Rels); i++ {
		if qry.Rels[i] == old {
			v.Alias = new
			qry.Rels[i] = new
			qry.RelsMap[new] = v
			delete(qry.RelsMap, old)
			break
		}
	}
	return nil
}

func (b *build) getSchemaInfo(schema string, name string) ([]string, map[string]*Attribute, error) {
	var attrs []string

	db, err := b.e.Database(schema)
	if err != nil {
		return nil, nil, errors.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	r, err := db.Relation(name)
	if err != nil {
		return nil, nil, errors.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	defer r.Close()
	defs := r.TableDefs()
	mp := make(map[string]*Attribute)
	for _, def := range defs {
		if v, ok := def.(*engine.AttributeDef); ok {
			mp[v.Attr.Name] = &Attribute{
				Name: v.Attr.Name,
				Type: v.Attr.Type,
			}
			attrs = append(attrs, v.Attr.Name)
		}
	}
	return attrs, mp, nil
}
