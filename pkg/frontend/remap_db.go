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
// parsed statements (db.table -> remapdb[db].table), using the remapdb config
// carried on each statement's RewriteOption. It runs after parsing and before
// privilege checks / planning, which otherwise resolve the original database and
// would reject a remapped-away database before the planner sees it.
//
// Only QUALIFIED references are rewritten. An unqualified name may be a CTE or
// derived-table alias rather than a base table, so attaching a database to it
// could change its meaning; the current-database case is instead handled by
// remapping USE (the session lands on the target database and unqualified names
// resolve there naturally).
func applyRemapDb(stmts []tree.Statement) {
	for _, stmt := range stmts {
		sel, ok := asRemapSelect(stmt)
		if !ok || sel.RewriteOption == nil || len(sel.RewriteOption.RemapDb) == 0 {
			continue
		}
		remapDbInSelect(sel, sel.RewriteOption.RemapDb)
	}
}

func asRemapSelect(stmt tree.Statement) (*tree.Select, bool) {
	switch s := stmt.(type) {
	case *tree.Select:
		return s, true
	case *tree.ParenSelect:
		return s.Select, s.Select != nil
	}
	return nil, false
}

func remapDbInSelect(sel *tree.Select, remap map[string]string) {
	if sel == nil {
		return
	}
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if cte != nil {
				if s, ok := asRemapSelect(cte.Stmt); ok {
					remapDbInSelect(s, remap)
				}
			}
		}
	}
	remapDbInSelectStatement(sel.Select, remap)
}

func remapDbInSelectStatement(s tree.SelectStatement, remap map[string]string) {
	switch c := s.(type) {
	case *tree.SelectClause:
		if c.From != nil {
			for _, te := range c.From.Tables {
				remapDbInTableExpr(te, remap)
			}
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
