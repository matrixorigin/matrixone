// Copyright 2024 Matrix Origin
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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// applyRemapDb substitutes the database of qualified table references in the
// parsed statements (db.table -> remap[db].table). It runs after parsing and
// before privilege checks / planning, which otherwise resolve the original
// database and would reject a remapped-away database before the planner sees it.
// It covers SELECT and INSERT/UPDATE/DELETE (including their target tables,
// read sources, INSERT ... SELECT bodies and CTE bodies).
//
// Only QUALIFIED references are rewritten. An unqualified name may be a CTE or
// derived-table alias rather than a base table, so attaching a database to it
// could change its meaning; the current-database case is instead handled by
// remapping USE (the session lands on the target database and unqualified names
// resolve there naturally). Sub-selects in expressions (e.g. WHERE IN (...)) are
// not walked.
func applyRemapDb(stmts []tree.Statement, remap map[string]string) {
	if len(remap) == 0 {
		return
	}
	for _, stmt := range stmts {
		remapDbInStmt(stmt, remap)
	}
}

func remapDbInStmt(stmt tree.Statement, remap map[string]string) {
	switch s := stmt.(type) {
	case *tree.Select:
		remapDbInSelect(s, remap)
	case *tree.ParenSelect:
		remapDbInSelect(s.Select, remap)
	case *tree.Insert:
		remapDbInWith(s.With, remap)
		remapDbInTableExpr(s.Table, remap)
		if s.Rows != nil {
			remapDbInSelect(s.Rows, remap)
		}
	case *tree.Update:
		remapDbInWith(s.With, remap)
		remapDbInTableExprs(s.Tables, remap)
		if s.From != nil {
			remapDbInTableExprs(s.From.Tables, remap)
		}
	case *tree.Delete:
		remapDbInWith(s.With, remap)
		remapDbInTableExprs(s.Tables, remap)
		remapDbInTableExprs(s.TableRefs, remap)
	}
}

func remapDbInWith(w *tree.With, remap map[string]string) {
	if w == nil {
		return
	}
	for _, cte := range w.CTEs {
		if cte != nil {
			remapDbInStmt(cte.Stmt, remap)
		}
	}
}

func remapDbInSelect(sel *tree.Select, remap map[string]string) {
	if sel == nil {
		return
	}
	remapDbInWith(sel.With, remap)
	remapDbInSelectStatement(sel.Select, remap)
}

func remapDbInSelectStatement(s tree.SelectStatement, remap map[string]string) {
	switch c := s.(type) {
	case *tree.SelectClause:
		if c.From != nil {
			remapDbInTableExprs(c.From.Tables, remap)
		}
	case *tree.UnionClause:
		remapDbInSelectStatement(c.Left, remap)
		remapDbInSelectStatement(c.Right, remap)
	case *tree.ParenSelect:
		remapDbInSelect(c.Select, remap)
	case *tree.Select:
		remapDbInSelect(c, remap)
	}
}

func remapDbInTableExprs(tes tree.TableExprs, remap map[string]string) {
	for _, te := range tes {
		remapDbInTableExpr(te, remap)
	}
}

func remapDbInTableExpr(te tree.TableExpr, remap map[string]string) {
	switch t := te.(type) {
	case *tree.TableName:
		remapTableName(t, remap)
	case *tree.AliasedTableExpr:
		remapDbInTableExpr(t.Expr, remap)
	case *tree.JoinTableExpr:
		remapDbInTableExpr(t.Left, remap)
		remapDbInTableExpr(t.Right, remap)
	case *tree.ParenTableExpr:
		remapDbInTableExpr(t.Expr, remap)
	case *tree.Select:
		remapDbInSelect(t, remap)
	case *tree.ParenSelect:
		remapDbInSelect(t.Select, remap)
	}
}

// remapTableName substitutes the database of a qualified table reference. An
// unqualified reference (no explicit schema) is left untouched.
func remapTableName(tn *tree.TableName, remap map[string]string) {
	if tn == nil || !tn.ExplicitSchema {
		return
	}
	if target, ok := remap[string(tn.SchemaName)]; ok {
		tn.SchemaName = tree.Identifier(target)
	}
}
