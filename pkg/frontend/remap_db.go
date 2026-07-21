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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// applyRemapDb substitutes the database of qualified table and column
// references in parsed statements (db.table -> remap[db].table and
// db.table.column -> remap[db].table.column). It runs after parsing and before
// privilege checks / planning, which otherwise resolve the original database
// and would reject a remapped-away database before the planner sees it. It
// covers SELECT and INSERT/UPDATE/DELETE (including their target tables, read
// sources, expression containers, INSERT ... SELECT bodies and CTE bodies),
// table-level DDL, ANALYZE TABLE, and prepared statement bodies.
//
// Only QUALIFIED references are rewritten. An unqualified name may be a CTE or
// derived-table alias rather than a base table, so attaching a database to it
// could change its meaning. USE is intentionally not remapped; unqualified
// names are resolved through TxnCompilerContext.DefaultDatabase(), which applies
// the active database remap. Sub-selects nested in expressions (e.g. WHERE id
// IN (SELECT ... FROM dbx.t), EXISTS (...), join ON, projections,
// GROUP/HAVING) are also walked so their qualified references are remapped.
func applyRemapDb(stmts []tree.Statement, remap map[string]string) {
	if len(remap) == 0 {
		return
	}
	for _, stmt := range stmts {
		remapDbInStmt(stmt, remap)
	}
}

func applyRemapDbByStatement(ctx context.Context, stmts []tree.Statement, remaps []map[string]string) error {
	if len(stmts) != len(remaps) {
		return moerr.NewInternalError(ctx, "the count of remapdb policies is not equal to statements")
	}
	for i, stmt := range stmts {
		remapDbInStmt(stmt, remaps[i])
	}
	return nil
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
		remapDbInUpdateExprs(s.OnDuplicateUpdate, remap)
	case *tree.Update:
		remapDbInWith(s.With, remap)
		remapDbInTableExprs(s.Tables, remap)
		if s.From != nil {
			remapDbInTableExprs(s.From.Tables, remap)
		}
		remapDbInUpdateExprs(s.Exprs, remap)
		remapDbInWhere(s.Where, remap)
		remapDbInOrderBy(s.OrderBy, remap)
		remapDbInLimit(s.Limit, remap)
	case *tree.Delete:
		remapDbInWith(s.With, remap)
		remapDbInTableExprs(s.Tables, remap)
		remapDbInTableExprs(s.TableRefs, remap)
		remapDbInWhere(s.Where, remap)
		remapDbInOrderBy(s.OrderBy, remap)
		remapDbInLimit(s.Limit, remap)
	case *tree.ValuesStatement:
		for _, row := range s.Rows {
			remapDbInExprs(row, remap)
		}
		remapDbInOrderBy(s.OrderBy, remap)
		remapDbInLimit(s.Limit, remap)
	case *tree.AnalyzeStmt:
		for _, entry := range s.Entries {
			if entry != nil {
				remapTableName(entry.Table, remap)
			}
		}
	case *tree.PrepareStmt:
		remapDbInStmt(s.Stmt, remap)

	// Table-level DDL: the target table/view/index is a table-level object, so a
	// qualified <src>.t is remapped. CREATE/ALTER ... AS SELECT bodies are walked
	// too, as are TRUNCATE TABLE and RENAME TABLE. Database-level DDL
	// (CREATE/DROP/ALTER DATABASE, USE) is intentionally absent here and never
	// remapped.
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
	case *tree.TruncateTable:
		remapTableName(s.Name, remap)
	case *tree.RenameTable:
		// rename src.a to src.b, ... : both the source table and the rename
		// destination are qualified table-level references, so remap each.
		for _, at := range s.AlterTables {
			if at == nil {
				continue
			}
			remapTableName(at.Table, remap)
			for _, opt := range at.Options {
				if rn, ok := opt.(*tree.AlterOptionTableName); ok {
					remapObjectName(rn.Name, remap)
				}
			}
		}
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
	remapDbInTimeWindow(sel.TimeWindow, remap)
	remapDbInOrderBy(sel.OrderBy, remap)
	remapDbInLimit(sel.Limit, remap)
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
	case *tree.ValuesClause:
		for _, row := range c.Rows {
			remapDbInExprs(row, remap)
		}
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
	case *tree.ApplyTableExpr:
		remapDbInTableExpr(t.Left, remap)
		remapDbInTableExpr(t.Right, remap)
	case *tree.ParenTableExpr:
		remapDbInTableExpr(t.Expr, remap)
	case *tree.Select:
		remapDbInSelect(t, remap)
	case *tree.ParenSelect:
		remapDbInSelect(t.Select, remap)
	case *tree.Subquery:
		remapDbInSelectStatement(t.Select, remap)
	case *tree.StatementSource:
		if t.Statement != nil {
			remapDbInStmt(t.Statement, remap)
		}
	}
}

func remapDbInExprs(exprs tree.Exprs, remap map[string]string) {
	for _, e := range exprs {
		remapDbInExpr(e, remap)
	}
}

func remapDbInOrderBy(orderBy tree.OrderBy, remap map[string]string) {
	for _, order := range orderBy {
		if order != nil {
			remapDbInExpr(order.Expr, remap)
		}
	}
}

func remapDbInLimit(limit *tree.Limit, remap map[string]string) {
	if limit == nil {
		return
	}
	remapDbInExpr(limit.Offset, remap)
	remapDbInExpr(limit.Count, remap)
}

func remapDbInTimeWindow(timeWindow *tree.TimeWindow, remap map[string]string) {
	if timeWindow == nil {
		return
	}
	if timeWindow.Interval != nil {
		remapColumnName(timeWindow.Interval.Col, remap)
		remapDbInExpr(timeWindow.Interval.Val, remap)
	}
	if timeWindow.Sliding != nil {
		remapDbInExpr(timeWindow.Sliding.Val, remap)
	}
	if timeWindow.Fill != nil {
		remapDbInExpr(timeWindow.Fill.Val, remap)
	}
}

func remapDbInUpdateExprs(updateExprs tree.UpdateExprs, remap map[string]string) {
	for _, updateExpr := range updateExprs {
		if updateExpr == nil {
			continue
		}
		for _, name := range updateExpr.Names {
			remapColumnName(name, remap)
		}
		remapDbInExpr(updateExpr.Expr, remap)
	}
}

// remapDbInExpr walks expressions and their nested sub-selects (for example,
// WHERE id IN (SELECT ... FROM dbx.t)). Keep this aligned with
// pkg/sql/parsers/tree/expr.go when adding an expression wrapper with children.
func remapDbInExpr(expr tree.Expr, remap map[string]string) {
	switch e := expr.(type) {
	case nil:
		return
	case *tree.UnresolvedName:
		remapColumnName(e, remap)
	case *tree.Subquery:
		remapDbInSelectStatement(e.Select, remap)
	case *tree.ExprList:
		remapDbInExprs(e.Exprs, remap)
	case *tree.ParenExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.NotExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsNullExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsNotNullExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsUnknownExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsNotUnknownExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsTrueExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsNotTrueExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsFalseExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.IsNotFalseExpr:
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
	case *tree.BitCastExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.SerialExtractExpr:
		remapDbInExpr(e.SerialExpr, remap)
		remapDbInExpr(e.IndexExpr, remap)
	case *tree.IntervalExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.DefaultVal:
		remapDbInExpr(e.Expr, remap)
	case *tree.VarExpr:
		remapDbInExpr(e.Expr, remap)
	case *tree.FuncExpr:
		remapDbInExprs(e.Exprs, remap)
		remapDbInOrderBy(e.OrderBy, remap)
		remapDbInWindowSpec(e.WindowSpec, remap)
	case *tree.Tuple:
		remapDbInExprs(e.Exprs, remap)
	case *tree.CaseExpr:
		remapDbInExpr(e.Expr, remap)
		for _, when := range e.Whens {
			if when == nil {
				continue
			}
			remapDbInExpr(when.Cond, remap)
			remapDbInExpr(when.Val, remap)
		}
		remapDbInExpr(e.Else, remap)
	case tree.SampleExpr:
		columns, _ := e.GetColumns()
		remapDbInExprs(columns, remap)
	case *tree.SampleExpr:
		columns, _ := e.GetColumns()
		remapDbInExprs(columns, remap)
	case *tree.FullTextMatchExpr:
		for _, keyPart := range e.KeyParts {
			if keyPart == nil {
				continue
			}
			remapColumnName(keyPart.ColName, remap)
			remapDbInExpr(keyPart.Expr, remap)
		}
	}
}

func remapDbInWindowSpec(windowSpec *tree.WindowSpec, remap map[string]string) {
	if windowSpec == nil {
		return
	}
	remapDbInExprs(windowSpec.PartitionBy, remap)
	remapDbInOrderBy(windowSpec.OrderBy, remap)
	remapDbInFrameClause(windowSpec.Frame, remap)
}

func remapDbInFrameClause(frame *tree.FrameClause, remap map[string]string) {
	if frame == nil {
		return
	}
	remapDbInFrameBound(frame.Start, remap)
	remapDbInFrameBound(frame.End, remap)
}

func remapDbInFrameBound(bound *tree.FrameBound, remap map[string]string) {
	if bound != nil {
		remapDbInExpr(bound.Expr, remap)
	}
}

// remapTableName substitutes the database of a qualified table reference. An
// unqualified reference (no explicit schema) is left untouched.
func remapTableName(tn *tree.TableName, remap map[string]string) {
	if tn == nil {
		return
	}
	if tn.ExplicitSchema {
		if target, ok := remap[string(tn.SchemaName)]; ok {
			tn.SchemaName = tree.Identifier(target)
		}
	}
	if tn.AtTsExpr != nil {
		remapDbInExpr(tn.AtTsExpr.Expr, remap)
	}
}

// remapColumnName substitutes only the database component of a fully-qualified
// column name. Two-part names are table/alias.column and must remain unchanged:
// the first part can be a derived-table or CTE alias rather than a database.
func remapColumnName(name *tree.UnresolvedName, remap map[string]string) {
	if name == nil || name.NumParts < 3 {
		return
	}
	if target, ok := remap[name.DbName()]; ok {
		name.CStrParts[2] = tree.NewCStr(target, 1)
	}
}

// remapObjectName substitutes the database of a qualified object name (used for
// the destination of RENAME TABLE, carried as an UnresolvedObjectName). Parts[1]
// holds the schema and is only present when NumParts >= 2 (i.e. it is qualified).
func remapObjectName(on *tree.UnresolvedObjectName, remap map[string]string) {
	if on == nil || on.NumParts < 2 {
		return
	}
	if target, ok := remap[on.Parts[1]]; ok {
		on.Parts[1] = target
	}
}
