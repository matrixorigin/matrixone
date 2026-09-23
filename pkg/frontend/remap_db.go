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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type remapDbContext struct {
	databases           map[string]string
	lowerCaseTableNames int64
	remapUseDatabase    bool
	unsupported         *bool
	// rejectUseStateChanges makes dependency collection fail closed for routine
	// bodies that execute USE. The default database is then path-dependent for
	// control-flow branches, and assigning one database to every unqualified
	// reference would risk both false omission and an unsafe persisted body.
	rejectUseStateChanges bool
	// cteNames is populated only while dependency collection/remapping walks a
	// SELECT. It prevents an unqualified CTE reference from being collected as
	// a source table while preserving qualified references such as db.cte.
	cteNames             map[string]struct{}
	collectTableName     func(*tree.TableName)
	collectProcedureName func(*tree.ProcedureName)
	collectFunctionName  func(*tree.UnresolvedName)
}

func (remap remapDbContext) lookup(database string) (string, bool) {
	target, ok := remap.databases[tree.NewCStr(database, remap.lowerCaseTableNames).Compare()]
	return target, ok
}

func (remap remapDbContext) withVisibleCTEs(ctes ...*tree.CTE) remapDbContext {
	if len(ctes) == 0 {
		return remap
	}
	scoped := remap
	scoped.cteNames = make(map[string]struct{}, len(remap.cteNames)+len(ctes))
	for name := range remap.cteNames {
		scoped.cteNames[name] = struct{}{}
	}
	for _, cte := range ctes {
		if cte == nil || cte.Name == nil || cte.Name.Alias == "" {
			continue
		}
		scoped.cteNames[tree.NewCStr(
			string(cte.Name.Alias), remap.lowerCaseTableNames,
		).Compare()] = struct{}{}
	}
	return scoped
}

func (remap remapDbContext) isVisibleCTE(name string) bool {
	if len(remap.cteNames) == 0 {
		return false
	}
	_, ok := remap.cteNames[tree.NewCStr(name, remap.lowerCaseTableNames).Compare()]
	return ok
}

// applyRemapDb substitutes the database of qualified table and column
// references in parsed statements (db.table -> remap[db].table and
// db.table.column -> remap[db].table.column). It runs after parsing and before
// privilege checks / planning, which otherwise resolve the original database
// and would reject a remapped-away database before the planner sees it. It
// covers SELECT and INSERT/UPDATE/DELETE (including their target tables, read
// sources, expression containers, INSERT ... SELECT bodies and CTE bodies),
// SQL procedure compound/control-flow bodies, executable statement wrappers
// (EXPLAIN, CHECK, LOCK, MERGE, LOAD/DUMP and sequence operations), audited
// table-level DDL, ANALYZE TABLE, and prepared statement bodies.
//
// Only QUALIFIED references are rewritten. An unqualified name may be a CTE or
// derived-table alias rather than a base table, so attaching a database to it
// could change its meaning. One-shot remapping intentionally leaves USE
// unchanged; clone-routine remapping opts in because it persists the body.
// Sub-selects nested in
// expressions (e.g. WHERE id IN (SELECT ... FROM dbx.t), EXISTS (...), join ON, projections,
// GROUP/HAVING) are also walked so their qualified references are remapped.
func applyRemapDb(
	ctx context.Context,
	stmts []tree.Statement,
	remap map[string]string,
	lowerCaseTableNames int64,
) error {
	if len(remap) == 0 {
		return nil
	}
	normalized, err := parsers.NormalizeAndValidateRemapDb(ctx, remap, lowerCaseTableNames)
	if err != nil {
		return err
	}
	remapCtx := remapDbContext{databases: normalized, lowerCaseTableNames: lowerCaseTableNames}
	for _, stmt := range stmts {
		remapDbInStmt(stmt, remapCtx)
	}
	return nil
}

func applyRemapDbByStatement(
	ctx context.Context,
	stmts []tree.Statement,
	remaps []map[string]string,
	lowerCaseTableNames int64,
) error {
	if len(stmts) != len(remaps) {
		return moerr.NewInternalError(ctx, "the count of remapdb policies is not equal to statements")
	}
	for i, stmt := range stmts {
		normalized, err := parsers.NormalizeAndValidateRemapDb(ctx, remaps[i], lowerCaseTableNames)
		if err != nil {
			return err
		}
		remapDbInStmt(stmt, remapDbContext{
			databases: normalized, lowerCaseTableNames: lowerCaseTableNames,
		})
	}
	return nil
}

// remapCloneRoutineStatements applies the shared remapper under the stricter
// persistence rule for a cloned SQL routine: every executable statement must
// be structurally audited. Leaving an unhandled statement unchanged is safe
// for a one-shot query remap, but not for a procedure body that will execute
// after the source database may have been dropped.
func remapCloneRoutineStatements(
	ctx context.Context,
	stmts []tree.Statement,
	remap map[string]string,
	lowerCaseTableNames int64,
) error {
	normalized, err := parsers.NormalizeAndValidateRemapDb(ctx, remap, lowerCaseTableNames)
	if err != nil {
		return err
	}
	unsupported := false
	if !remapDbInStatements(stmts, remapDbContext{
		databases: normalized, lowerCaseTableNames: lowerCaseTableNames, remapUseDatabase: true, unsupported: &unsupported,
	}) || unsupported {
		return moerr.NewNotSupported(ctx,
			"cloned SQL routine contains a statement whose database references cannot be safely remapped",
		)
	}
	return nil
}

func (remap remapDbContext) markUnsupported() {
	if remap.unsupported != nil {
		*remap.unsupported = true
	}
}

// remapDbInStmt returns false when a statement cannot be structurally audited
// for database remapping. General query remapping preserves its existing
// best-effort behavior, while stored-procedure clone restoration rejects such
// statements rather than persisting a possible source-database reference.
func remapDbInStmt(stmt tree.Statement, remap remapDbContext) bool {
	if stmt == nil {
		return true
	}
	remappable := true
	switch s := stmt.(type) {
	case *tree.CompoundStmt:
		remappable = remapDbInStatements(s.Stmts, remap)
	case *tree.IfStmt:
		remapDbInExpr(s.Cond, remap)
		remappable = remapDbInStatements(s.Body, remap)
		for _, elif := range s.Elifs {
			if elif == nil {
				continue
			}
			remapDbInExpr(elif.Cond, remap)
			if !remapDbInStatements(elif.Body, remap) {
				remappable = false
			}
		}
		if !remapDbInStatements(s.Else, remap) {
			remappable = false
		}
	case *tree.CaseStmt:
		remapDbInExpr(s.Expr, remap)
		for _, when := range s.Whens {
			if when == nil {
				continue
			}
			remapDbInExpr(when.Cond, remap)
			if !remapDbInStatements(when.Body, remap) {
				remappable = false
			}
		}
		if !remapDbInStatements(s.Else, remap) {
			remappable = false
		}
	case *tree.WhileStmt:
		remapDbInExpr(s.Cond, remap)
		remappable = remapDbInStatements(s.Body, remap)
	case *tree.RepeatStmt:
		remappable = remapDbInStatements(s.Body, remap)
		remapDbInExpr(s.Cond, remap)
	case *tree.LoopStmt:
		remappable = remapDbInStatements(s.Body, remap)
	case *tree.IterateStmt, *tree.LeaveStmt:
		// These statements refer only to a local compound-statement label; they
		// cannot retain a database or table identity.
	case *tree.Declare:
		remapDbInExpr(s.DefaultVal, remap)
	case *tree.SetVar:
		for _, assignment := range s.Assignments {
			if assignment == nil {
				continue
			}
			remapDbInExpr(assignment.Value, remap)
			remapDbInExpr(assignment.Reserved, remap)
		}
	case *tree.CallStmt:
		remapProcedureName(s.Name, remap)
		remapDbInExprs(s.Args, remap)
	case *tree.Select:
		remapDbInSelect(s, remap)
	case *tree.ParenSelect:
		remapDbInSelect(s.Select, remap)
	case *tree.Insert:
		scoped := remapDbInWithScope(s.With, remap)
		remapDbInTableExpr(s.Table, scoped)
		remapInsertTarget(s.ColumnNames, &s.TargetDatabaseName, scoped)
		if s.Rows != nil {
			remapDbInSelect(s.Rows, scoped)
		}
		remapDbInUpdateExprs(s.OnDuplicateUpdate, scoped)
		remapDbInSelectExprs(s.Returning, scoped)
	case *tree.MultiInsert:
		scoped := remapDbInWithScope(s.With, remap)
		for _, target := range s.AllTargets() {
			remapDbInTableExpr(target.Table, scoped)
			remapInsertTarget(target.ColumnNames, nil, scoped)
			remapDbInExprs(target.Values, scoped)
		}
		for _, when := range s.Whens {
			remapDbInExpr(when.Cond, scoped)
		}
		if s.Source != nil {
			remapDbInSelect(s.Source, scoped)
		}
	case *tree.Replace:
		remapDbInTableExpr(s.Table, remap)
		remapInsertTarget(s.ColumnNames, &s.TargetDatabaseName, remap)
		if s.Rows != nil {
			remapDbInSelect(s.Rows, remap)
		}
		remapDbInSelectExprs(s.Returning, remap)
	case *tree.Update:
		scoped := remapDbInWithScope(s.With, remap)
		remapDbInTableExprs(s.Tables, scoped)
		if s.From != nil {
			remapDbInTableExprs(s.From.Tables, scoped)
		}
		remapDbInUpdateExprs(s.Exprs, scoped)
		remapDbInWhere(s.Where, scoped)
		remapDbInOrderBy(s.OrderBy, scoped)
		remapDbInLimit(s.Limit, scoped)
		remapDbInSelectExprs(s.Returning, scoped)
	case *tree.Delete:
		scoped := remapDbInWithScope(s.With, remap)
		remapDbInTableExprs(s.Tables, scoped)
		remapDbInTableExprs(s.TableRefs, scoped)
		remapDbInWhere(s.Where, scoped)
		remapDbInOrderBy(s.OrderBy, scoped)
		remapDbInLimit(s.Limit, scoped)
		remapDbInSelectExprs(s.Returning, scoped)
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
	case *tree.ExplainStmt:
		remappable = remapDbInStmt(s.Statement, remap)
	case *tree.ExplainAnalyze:
		remappable = remapDbInStmt(s.Statement, remap)
	case *tree.LockTableStmt:
		for i := range s.TableLocks {
			remapTableName(&s.TableLocks[i].Table, remap)
		}
	case *tree.CheckTableStmt:
		for _, table := range s.Tables {
			remapTableName(table, remap)
		}
	case *tree.PrepareStmt:
		remappable = remapDbInStmt(s.Stmt, remap)
	case *tree.Do:
		remapDbInExprs(s.Exprs, remap)
	case *tree.Merge:
		scoped := remapDbInWithScope(s.With, remap)
		remapDbInTableExpr(s.Target, scoped)
		remapDbInTableExpr(s.Source, scoped)
		remapDbInExpr(s.On, scoped)
		for _, clause := range s.Clauses {
			if clause == nil {
				continue
			}
			remapDbInExpr(clause.Condition, scoped)
			remapDbInUpdateExprs(clause.UpdateExprs, scoped)
			remapDbInExprs(clause.InsertValues, scoped)
		}
		remapDbInSelectExprs(s.Returning, scoped)
	case *tree.Load:
		remapTableName(s.Table, remap)
	case *tree.DumpTable:
		remapTableName(s.Table, remap)
	case *tree.LoadTable:
		remapTableName(s.Table, remap)
	case *tree.CreateSequence:
		remapTableName(s.Name, remap)
	case *tree.DropSequence:
		for _, name := range s.Names {
			remapTableName(name, remap)
		}
	case *tree.AlterSequence:
		remapTableName(s.Name, remap)
	case *tree.ShowCreateTable:
		remapObjectName(s.Name, remap)
		if s.AtTsExpr != nil {
			remapDbInExpr(s.AtTsExpr.Expr, remap)
		}
	case *tree.ShowCreateView:
		remapObjectName(s.Name, remap)
		if s.AtTsExpr != nil {
			remapDbInExpr(s.AtTsExpr.Expr, remap)
		}
	case *tree.ShowCreateDatabase:
		remapDatabaseName(&s.Name, remap)
		if s.AtTsExpr != nil {
			remapDbInExpr(s.AtTsExpr.Expr, remap)
		}
	case *tree.ShowDatabases:
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
		if s.AtTsExpr != nil {
			remapDbInExpr(s.AtTsExpr.Expr, remap)
		}
	case *tree.ShowColumns:
		remapObjectName(s.Table, remap)
		remapDatabaseName(&s.DBName, remap)
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowIndex:
		remapObjectName(s.TableName, remap)
		remapDatabaseName(&s.DbName, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowColumnNumber:
		remapObjectName(s.Table, remap)
		remapDatabaseName(&s.DbName, remap)
	case *tree.ShowTableValues:
		remapObjectName(s.Table, remap)
		remapDatabaseName(&s.DbName, remap)
	case *tree.ShowTableSize:
		remapObjectName(s.Table, remap)
		remapDatabaseName(&s.DbName, remap)
	case *tree.ShowTarget:
		remapDatabaseName(&s.DbName, remap)
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowTableStatus:
		remapDatabaseName(&s.DbName, remap)
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowSequences:
		remapDatabaseName(&s.DBName, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowTables:
		remapDatabaseName(&s.DBName, remap)
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
		if s.AtTsExpr != nil {
			remapDbInExpr(s.AtTsExpr.Expr, remap)
		}
	case *tree.ShowTableNumber:
		remapDatabaseName(&s.DbName, remap)
	case *tree.ShowCollation:
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowVariables:
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowStatus:
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowFunctionOrProcedureStatus:
		remapDbInOptionalComparisonExpr(s.Like, remap)
		remapDbInWhere(s.Where, remap)
	case *tree.ShowAccounts:
		remapDbInOptionalComparisonExpr(s.Like, remap)
	case *tree.ShowPublications:
		remapDbInOptionalComparisonExpr(s.Like, remap)
	case *tree.ShowSubscriptions:
		remapDbInOptionalComparisonExpr(s.Like, remap)
	case *tree.ShowRolesStmt:
		remapDbInOptionalComparisonExpr(s.Like, remap)
	case *tree.ShowGrants, *tree.ShowProcessList, *tree.ShowErrors,
		*tree.ShowWarnings, *tree.ShowNodeList, *tree.ShowLocks,
		*tree.ShowAccountUpgrade, *tree.ShowCreatePublications,
		*tree.ShowPublicationCoverage, *tree.ShowCcprSubscriptions,
		*tree.ShowBackendServers, *tree.ShowLogserviceReplicas,
		*tree.ShowLogserviceStores, *tree.ShowLogserviceSettings,
		*tree.ShowRules, *tree.Deallocate, *tree.Reset, *tree.Execute:
		// These forms carry no database or table identity. Their nested expression
		// fields, where present, are handled by their dedicated cases above.
	case *tree.Use:
		if remap.rejectUseStateChanges && !s.IsUseRole() {
			remap.markUnsupported()
		}
		if remap.remapUseDatabase {
			remapUseDatabase(s, remap)
		}

	// Table-level DDL: the target table/view/index is a table-level object, so a
	// qualified <src>.t is remapped. CREATE/ALTER ... AS SELECT bodies are walked
	// too, as are TRUNCATE TABLE and RENAME TABLE. Database-level statements that
	// are not structurally remapped remain unsupported in cloned SQL routines.
	case *tree.CreateTable:
		remapTableName(&s.Table, remap)
		remapTableName(&s.LikeTableName, remap)
		if !remapDbInTableDefs(s.Defs, remap) {
			remap.markUnsupported()
			remappable = false
		}
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
		remappable = remapDbInAlterTable(s, remap)
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
			if !remapDbInAlterTable(at, remap) {
				remappable = false
			}
		}
	default:
		remap.markUnsupported()
		return false
	}
	return remappable
}

// remapDbInAlterTable audits ALTER TABLE's nested option AST before a cloned
// routine can persist it. The target table alone is not enough: ADD CONSTRAINT
// carries a foreign-key reference, and column clauses can carry reference and
// expression children. Unrecognized options and partition forms are rejected
// by the clone-specific caller instead of retaining a source-database name.
func remapDbInAlterTable(stmt *tree.AlterTable, remap remapDbContext) bool {
	if stmt == nil || stmt.PartitionOption != nil {
		return false
	}
	remapTableName(stmt.Table, remap)
	for _, option := range stmt.Options {
		if !remapDbInAlterTableOption(option, remap) {
			return false
		}
	}
	return true
}

func remapDbInAlterTableOption(option tree.AlterTableOption, remap remapDbContext) bool {
	switch opt := option.(type) {
	case *tree.AlterOptionTableName:
		remapObjectName(opt.Name, remap)
	case *tree.AlterOptionAdd:
		return remapDbInTableDef(opt.Def, remap)
	case *tree.AlterTableModifyColumnClause:
		return remapDbInColumnTableDef(opt.NewColumn, remap)
	case *tree.AlterTableChangeColumnClause:
		remapColumnName(opt.OldColumnName, remap)
		return remapDbInColumnTableDef(opt.NewColumn, remap)
	case *tree.AlterTableAddColumnClause:
		for _, column := range opt.NewColumns {
			if !remapDbInColumnTableDef(column, remap) {
				return false
			}
		}
	case *tree.AlterTableAlterColumnClause:
		remapColumnName(opt.ColumnName, remap)
		if opt.DefaultExpr != nil {
			return remapDbInColumnAttribute(opt.DefaultExpr, remap)
		}
	case *tree.AlterTableRenameColumnClause:
		remapColumnName(opt.OldColumnName, remap)
		remapColumnName(opt.NewColumnName, remap)
	case *tree.AlterTableOrderByColumnClause:
		for _, order := range opt.AlterOrderByList {
			if order != nil {
				remapColumnName(order.Column, remap)
			}
		}
	case *tree.AlterAddCol:
		return remapDbInColumnTableDef(opt.Column, remap)
	default:
		return false
	}
	return true
}

func remapDbInStatements(stmts []tree.Statement, remap remapDbContext) bool {
	remappable := true
	for _, stmt := range stmts {
		if !remapDbInStmt(stmt, remap) {
			remappable = false
		}
	}
	return remappable
}

func remapDbInWithScope(w *tree.With, remap remapDbContext) remapDbContext {
	remapDbInWith(w, remap)
	if w == nil {
		return remap
	}
	return remap.withVisibleCTEs(w.CTEs...)
}

func remapDbInWith(w *tree.With, remap remapDbContext) {
	if w == nil {
		return
	}
	recursiveScope := remap.withVisibleCTEs(w.CTEs...)
	sequentialScope := remap
	for _, cte := range w.CTEs {
		if cte == nil {
			continue
		}
		cteScope := sequentialScope
		if w.IsRecursive {
			cteScope = recursiveScope
		}
		remapDbInStmt(cte.Stmt, cteScope)
		sequentialScope = sequentialScope.withVisibleCTEs(cte)
	}
}

func remapDbInSelectExprs(exprs tree.SelectExprs, remap remapDbContext) {
	for _, expr := range exprs {
		remapDbInExpr(expr.Expr, remap)
	}
}

func remapDbInSelect(sel *tree.Select, remap remapDbContext) {
	if sel == nil {
		return
	}
	remapDbInWith(sel.With, remap)
	scoped := remap
	if sel.With != nil {
		scoped = remap.withVisibleCTEs(sel.With.CTEs...)
	}
	remapDbInSelectStatement(sel.Select, scoped)
	remapDbInTimeWindow(sel.TimeWindow, scoped)
	remapDbInOrderBy(sel.OrderBy, scoped)
	remapDbInLimit(sel.Limit, scoped)
}

func remapDbInSelectStatement(s tree.SelectStatement, remap remapDbContext) {
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
		for _, definition := range c.Windows {
			if definition != nil {
				remapDbInWindowSpec(definition.Spec, remap)
			}
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
	case nil:
	default:
		remap.markUnsupported()
	}
}

func remapDbInWhere(w *tree.Where, remap remapDbContext) {
	if w != nil {
		remapDbInExpr(w.Expr, remap)
	}
}

func remapDbInTableExprs(tes tree.TableExprs, remap remapDbContext) {
	for _, te := range tes {
		remapDbInTableExpr(te, remap)
	}
}

func remapDbInTableExpr(te tree.TableExpr, remap remapDbContext) {
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
	case *tree.TableFunction:
		if t.Func != nil {
			remapDbInExpr(t.Func, remap)
		}
		if t.SelectStmt != nil {
			remapDbInSelect(t.SelectStmt, remap)
		}
	case nil:
	default:
		remap.markUnsupported()
	}
}

// remapDbInTableDefs closes CREATE TABLE over every child that can carry a
// qualified relation or a nested expression. Keep this switch aligned with
// tree.TableDefs: a new definition kind must be deliberately audited before a
// cloned routine is allowed to persist it.
func remapDbInTableDefs(defs tree.TableDefs, remap remapDbContext) bool {
	for _, def := range defs {
		if !remapDbInTableDef(def, remap) {
			return false
		}
	}
	return true
}

func remapDbInTableDef(def tree.TableDef, remap remapDbContext) bool {
	switch d := def.(type) {
	case *tree.ColumnTableDef:
		return remapDbInColumnTableDef(d, remap)
	case *tree.PrimaryKeyIndex:
		remapDbInKeyParts(d.KeyParts, remap)
	case *tree.Index:
		remapDbInKeyParts(d.KeyParts, remap)
	case *tree.UniqueIndex:
		remapDbInKeyParts(d.KeyParts, remap)
	case *tree.ForeignKey:
		remapDbInKeyParts(d.KeyParts, remap)
		remapDbInAttributeReference(d.Refer, remap)
	case *tree.FullTextIndex:
		remapDbInKeyParts(d.KeyParts, remap)
	case *tree.CheckIndex:
		remapDbInExpr(d.Expr, remap)
	case nil:
		// Parsed CREATE/ALTER TABLE definitions are never nil, but tolerate one
		// in callers constructing an AST directly.
	default:
		return false
	}
	return true
}

func remapDbInColumnTableDef(column *tree.ColumnTableDef, remap remapDbContext) bool {
	if column == nil {
		return false
	}
	remapColumnName(column.Name, remap)
	for _, attribute := range column.Attributes {
		if !remapDbInColumnAttribute(attribute, remap) {
			return false
		}
	}
	return true
}

func remapDbInColumnAttribute(attribute tree.ColumnAttribute, remap remapDbContext) bool {
	switch a := attribute.(type) {
	case nil,
		*tree.AttributeNull,
		*tree.AttributeAutoIncrement,
		*tree.AttributeUniqueKey,
		*tree.AttributeUnique,
		*tree.AttributeKey,
		*tree.AttributePrimaryKey,
		*tree.AttributeCollate,
		*tree.AttributeCharset,
		*tree.AttributeColumnFormat,
		*tree.AttributeStorage,
		*tree.AttributeLowCardinality,
		*tree.AttributeAutoRandom,
		*tree.AttributeSRID,
		*tree.AttributeVisable,
		*tree.AttributeMongoDBPath,
		*tree.AttributeMongoDBConvert:
		return true
	case *tree.AttributeDefault:
		remapDbInExpr(a.Expr, remap)
	case *tree.AttributeComment:
		remapDbInExpr(a.CMT, remap)
	case *tree.AttributeCheckConstraint:
		remapDbInExpr(a.Expr, remap)
	case *tree.AttributeGeneratedAlways:
		remapDbInExpr(a.Expr, remap)
	case *tree.AttributeReference:
		remapDbInAttributeReference(a, remap)
	case *tree.AttributeOnUpdate:
		remapDbInExpr(a.Expr, remap)
	case *tree.KeyPart:
		remapDbInKeyPart(a, remap)
	default:
		return false
	}
	return true
}

func remapDbInAttributeReference(reference *tree.AttributeReference, remap remapDbContext) {
	if reference == nil {
		return
	}
	remapTableName(reference.TableName, remap)
	remapDbInKeyParts(reference.KeyParts, remap)
}

func remapDbInKeyParts(parts []*tree.KeyPart, remap remapDbContext) {
	for _, part := range parts {
		remapDbInKeyPart(part, remap)
	}
}

func remapDbInKeyPart(part *tree.KeyPart, remap remapDbContext) {
	if part == nil {
		return
	}
	remapColumnName(part.ColName, remap)
	remapDbInExpr(part.Expr, remap)
}

func remapDbInExprs(exprs tree.Exprs, remap remapDbContext) {
	for _, e := range exprs {
		remapDbInExpr(e, remap)
	}
}

// SHOW nodes store LIKE as a pointer, which becomes a non-nil tree.Expr
// interface when it is nil. Do not send that typed nil to the generic walker.
func remapDbInOptionalComparisonExpr(expr *tree.ComparisonExpr, remap remapDbContext) {
	if expr != nil {
		remapDbInExpr(expr, remap)
	}
}

func remapDbInOrderBy(orderBy tree.OrderBy, remap remapDbContext) {
	for _, order := range orderBy {
		if order != nil {
			remapDbInExpr(order.Expr, remap)
		}
	}
}

func remapDbInLimit(limit *tree.Limit, remap remapDbContext) {
	if limit == nil {
		return
	}
	remapDbInExpr(limit.Offset, remap)
	remapDbInExpr(limit.Count, remap)
}

func remapDbInTimeWindow(timeWindow *tree.TimeWindow, remap remapDbContext) {
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

func remapDbInUpdateExprs(updateExprs tree.UpdateExprs, remap remapDbContext) {
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
func remapDbInExpr(expr tree.Expr, remap remapDbContext) {
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
		if name, ok := e.Func.FunctionReference.(*tree.UnresolvedName); ok {
			if remap.collectFunctionName != nil {
				remap.collectFunctionName(name)
			}
			remapFunctionName(name, remap)
		}
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
		remapDbInExpr(e.Pattern, remap)
	}
}

func remapDbInWindowSpec(windowSpec *tree.WindowSpec, remap remapDbContext) {
	if windowSpec == nil {
		return
	}
	remapDbInExprs(windowSpec.PartitionBy, remap)
	remapDbInOrderBy(windowSpec.OrderBy, remap)
	remapDbInFrameClause(windowSpec.Frame, remap)
}

func remapDbInFrameClause(frame *tree.FrameClause, remap remapDbContext) {
	if frame == nil {
		return
	}
	remapDbInFrameBound(frame.Start, remap)
	remapDbInFrameBound(frame.End, remap)
}

func remapDbInFrameBound(bound *tree.FrameBound, remap remapDbContext) {
	if bound != nil {
		remapDbInExpr(bound.Expr, remap)
	}
}

// remapTableName substitutes the database of a qualified table reference. An
// unqualified reference (no explicit schema) is left untouched.
func remapTableName(tn *tree.TableName, remap remapDbContext) {
	if tn == nil {
		return
	}
	if remap.collectTableName != nil && tn.ObjectName != "" &&
		(tn.ExplicitSchema || !remap.isVisibleCTE(string(tn.ObjectName))) {
		remap.collectTableName(tn)
	}
	if tn.ExplicitSchema {
		if target, ok := remap.lookup(string(tn.SchemaName)); ok {
			tn.SchemaName = tree.Identifier(target)
		}
	}
	if tn.AtTsExpr != nil {
		remapDbInExpr(tn.AtTsExpr.Expr, remap)
	}
}

func remapProcedureName(name *tree.ProcedureName, remap remapDbContext) {
	if name == nil {
		return
	}
	if remap.collectProcedureName != nil {
		remap.collectProcedureName(name)
	}
	if !name.Name.ExplicitSchema {
		return
	}
	if target, ok := remap.lookup(string(name.Name.SchemaName)); ok {
		name.Name.SchemaName = tree.Identifier(target)
	}
}

// remapColumnName substitutes only the database component of a fully-qualified
// column name. Two-part names are table/alias.column and must remain unchanged:
// the first part can be a derived-table or CTE alias rather than a database.
func remapColumnName(name *tree.UnresolvedName, remap remapDbContext) {
	if name == nil || name.NumParts < 3 {
		return
	}
	if target, ok := remap.lookup(name.DbName()); ok {
		name.CStrParts[2] = tree.NewCStr(target, remap.lowerCaseTableNames)
	}
}

// remapFunctionName substitutes a qualified function's database component.
// Function references are safe to treat separately from column names because
// the parser has already classified this UnresolvedName as a function name;
// a two-part name is therefore db.function rather than table.column.
func remapFunctionName(name *tree.UnresolvedName, remap remapDbContext) {
	if name == nil || name.NumParts < 2 {
		return
	}
	part := 1
	database := name.TblName()
	if name.NumParts >= 3 {
		part = 2
		database = name.DbName()
	}
	if target, ok := remap.lookup(database); ok {
		name.CStrParts[part] = tree.NewCStr(target, remap.lowerCaseTableNames)
	}
}

// remapInsertTarget keeps the user-visible logical target identity in sync
// with the execution table rewritten by remapDbInTableExpr. TargetTableName is
// unchanged because remapdb substitutes databases only.
func remapInsertTarget(columnNames []*tree.UnresolvedName, databaseName *tree.Identifier, remap remapDbContext) {
	if databaseName != nil {
		if target, ok := remap.lookup(string(*databaseName)); ok {
			*databaseName = tree.Identifier(target)
		}
	}
	for _, name := range columnNames {
		remapColumnName(name, remap)
	}
}

// remapObjectName substitutes the database of a qualified object name (used for
// the destination of RENAME TABLE, carried as an UnresolvedObjectName). Parts[1]
// holds the schema and is only present when NumParts >= 2 (i.e. it is qualified).
func remapObjectName(on *tree.UnresolvedObjectName, remap remapDbContext) {
	if on == nil || on.NumParts < 2 {
		return
	}
	if target, ok := remap.lookup(on.Parts[1]); ok {
		on.Parts[1] = target
	}
}

func remapDatabaseName(name *string, remap remapDbContext) {
	if name == nil {
		return
	}
	if target, ok := remap.lookup(*name); ok {
		*name = target
	}
}

func remapUseDatabase(stmt *tree.Use, remap remapDbContext) {
	if stmt == nil || stmt.IsUseRole() || stmt.Name == nil {
		return
	}
	if target, ok := remap.lookup(stmt.Name.Compare()); ok {
		stmt.Name = tree.NewCStr(target, remap.lowerCaseTableNames)
	}
}
