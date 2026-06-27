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
// resolve there naturally). Sub-selects nested in expressions (e.g. WHERE id IN
// (SELECT ... FROM dbx.t), EXISTS (...), join ON, projections, GROUP/HAVING) are
// also walked so their qualified references are remapped.
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
		for _, ue := range s.Exprs {
			if ue != nil {
				remapDbInExpr(ue.Expr, remap)
			}
		}
		remapDbInWhere(s.Where, remap)
	case *tree.Delete:
		remapDbInWith(s.With, remap)
		remapDbInTableExprs(s.Tables, remap)
		remapDbInTableExprs(s.TableRefs, remap)
		remapDbInWhere(s.Where, remap)

	// Table-level DDL: the target table/view/index is a table-level object, so a
	// qualified <src>.t is remapped. CREATE/ALTER ... AS SELECT bodies are walked
	// too. Database-level DDL (CREATE/DROP/ALTER DATABASE, USE) is intentionally
	// absent here and never remapped.
	case *tree.CreateTable:
		remapTableName(&s.Table, remap)
		remapTableName(&s.LikeTableName, remap)
		if s.AsSource != nil {
			remapDbInSelect(s.AsSource, remap)
		}
	case *tree.CreateView:
		remapTableName(s.Name, remap)
		if s.AsSource != nil {
			remapDbInSelect(s.AsSource, remap)
		}
	case *tree.CreateIndex:
		remapTableName(s.Table, remap)
	case *tree.AlterTable:
		remapTableName(s.Table, remap)
	case *tree.AlterView:
		remapTableName(s.Name, remap)
		if s.AsSource != nil {
			remapDbInSelect(s.AsSource, remap)
		}
	case *tree.DropTable:
		for _, n := range s.Names {
			remapTableName(n, remap)
		}
	case *tree.DropView:
		for _, n := range s.Names {
			remapTableName(n, remap)
		}
	case *tree.DropIndex:
		remapTableName(s.TableName, remap)
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
		for _, se := range c.Exprs {
			remapDbInExpr(se.Expr, remap)
		}
		remapDbInWhere(c.Where, remap)
		remapDbInWhere(c.Having, remap)
		if c.GroupBy != nil {
			for _, exprs := range c.GroupBy.GroupByExprsList {
				remapDbInExprs(exprs, remap)
			}
			remapDbInExprs(c.GroupBy.GroupingSet, remap)
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

func remapDbInWhere(w *tree.Where, remap map[string]string) {
	if w != nil {
		remapDbInExpr(w.Expr, remap)
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
		if on, ok := t.Cond.(*tree.OnJoinCond); ok {
			remapDbInExpr(on.Expr, remap)
		}
	case *tree.ParenTableExpr:
		remapDbInTableExpr(t.Expr, remap)
	case *tree.Select:
		remapDbInSelect(t, remap)
	case *tree.ParenSelect:
		remapDbInSelect(t.Select, remap)
	}
}

func remapDbInExprs(exprs tree.Exprs, remap map[string]string) {
	for _, e := range exprs {
		remapDbInExpr(e, remap)
	}
}

// remapDbInExpr walks an expression looking for nested sub-selects (a
// *tree.Subquery, e.g. WHERE id IN (SELECT ... FROM dbx.t) or EXISTS (...)) and
// remaps the qualified table references inside them. It recurses through the
// common boolean/comparison/function expression containers; expression kinds
// that cannot contain a sub-select are no-ops.
func remapDbInExpr(expr tree.Expr, remap map[string]string) {
	switch e := expr.(type) {
	case nil:
		return
	case *tree.Subquery:
		remapDbInSelectStatement(e.Select, remap)
	case *tree.ParenExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.NotExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.AndExpr:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.Right, remap)
	case *tree.OrExpr:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.Right, remap)
	case *tree.XorExpr:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.Right, remap)
	case *tree.ComparisonExpr:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.Right, remap)
		remapDbInExpr(e.Escape, remap)
	case *tree.RangeCond:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.From, remap)
		remapDbInExpr(e.To, remap)
	case *tree.UnaryExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.BinaryExpr:
		remapDbInExpr(e.Left, remap)
		remapDbInExpr(e.Right, remap)
	case *tree.CastExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.FuncExpr:
		remapDbInExprs(e.Exprs, remap)
	case *tree.Tuple:
		remapDbInExprs(e.Exprs, remap)
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
