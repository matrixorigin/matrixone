// Copyright 2026 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// allowHiddenIndexTableAccessVariable is the escape-hatch system variable.
// When set to ON, the hidden index table guard is lifted for the session,
// which is only intended for debugging. It defaults to OFF.
const allowHiddenIndexTableAccessVariable = "allow_hidden_index_table_access"

// A procedure executes user-authored SQL using internal background sessions.
// Keep the original caller across nested CALLs, without applying its policy to
// the generated catalog/index-maintenance SQL those sessions also execute.
type storedSQLCallerKey struct{}

func storedSQLCaller(ctx context.Context, ses FeSession) *Session {
	if ctx != nil {
		if caller, ok := ctx.Value(storedSQLCallerKey{}).(*Session); ok {
			return caller
		}
	}
	return upstreamUserSession(ses)
}

// Prepared SQL is unwrapped/rebuilt after the Interpreter's initial check.
// Recheck its resolved AST at those boundaries, but leave non-procedure
// background/catalog work on its existing maintenance policy.
func verifyStoredSQLAgainstHiddenIndexTables(ctx context.Context, stmt tree.Statement) error {
	if ctx == nil {
		return nil
	}
	caller, ok := ctx.Value(storedSQLCallerKey{}).(*Session)
	if !ok {
		return nil
	}
	return verifyRealUserStatementAgainstHiddenIndexTables(ctx, caller, stmt)
}

// isHiddenIndexTableName reports whether the given table name belongs to a
// hidden physical index table ("__mo_index_secondary_*", "__mo_index_unique_*"
// and the other index-algorithm hidden tables). Index table names are minted
// with lower-case names, and MO identifiers are resolved case-insensitively,
// so the check lower-cases the candidate before matching the reserved prefix.
func isHiddenIndexTableName(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), catalog.IndexTableNamePrefix)
}

// directHiddenIndexTableAccessAllowed decides whether the session may reference
// hidden index tables directly in its statements.
//
// Allowed:
//   - internal sessions (the internal executor, background jobs, index
//     build/drop maintenance). They are not real client users and they are the
//     only legitimate writers of the hidden tables;
//   - the system account with the moadmin role (root), which owns the
//     internal objects;
//   - sessions that enabled the allow_hidden_index_table_access escape hatch.
//
// Every other (real, non-system) user is denied: hidden index tables must be
// reached only through the base table, mirroring MySQL's internal-object
// protection. This closes the privilege-layer blind spot where
// extractPrivilegeTipsFromPlan deliberately skips names carrying the index
// table prefix and, for a directly-named hidden table, generates no privilege
// tip at all.
func directHiddenIndexTableAccessAllowed(ses *Session) bool {
	if ses == nil || !ses.GetFromRealUser() {
		return true
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		return true
	}
	if tenant.IsSysTenant() && tenant.IsMoAdminRole() {
		return true
	}
	value, err := ses.GetSessionSysVar(allowHiddenIndexTableAccessVariable)
	if err == nil {
		if on, err2 := valueIsBoolTrue(value); err2 == nil && on {
			return true
		}
	}
	return false
}

// verifyRealUserStatementAgainstHiddenIndexTables rejects a statement written
// by a real client user when it references a hidden physical index table,
// unless the session is allowed direct access. It covers plain DML (write and
// read), SHOW CREATE/SHOW COLUMNS/SHOW INDEX, and the destructive DDL forms
// (DROP/ALTER/RENAME/TRUNCATE/CREATE under the reserved prefix).
func verifyRealUserStatementAgainstHiddenIndexTables(ctx context.Context, ses *Session, stmt tree.Statement) error {
	if stmt == nil || directHiddenIndexTableAccessAllowed(ses) {
		return nil
	}
	finder := &hiddenIndexTableRefFinder{}
	finder.scanStatement(stmt)
	return finder.accessError(ctx)
}

func (f *hiddenIndexTableRefFinder) accessError(ctx context.Context) error {
	if !f.found {
		return nil
	}
	qualified := f.table
	if f.schema != "" {
		qualified = f.schema + "." + f.table
	}
	return moerr.NewInternalErrorf(ctx,
		"do not have privilege to access the hidden internal table %q", qualified)
}

// hiddenIndexTableRefFinder walks a statement tree hunting a user-written
// reference to a hidden index table. Only names written by the user live in
// the AST; optimizer Introduced index-table scans never pass through this
// finder, so index maintenance and index-driven plans are not affected.
type hiddenIndexTableRefFinder struct {
	found  bool
	schema string
	table  string
}

func (f *hiddenIndexTableRefFinder) scanStatement(stmt tree.Statement) {
	if stmt == nil || f.found {
		return
	}
	switch st := stmt.(type) {
	case *tree.Select:
		f.scanSelect(st)
	case *tree.Insert:
		f.scanWith(st.With)
		f.scanTableExpr(st.Table)
		f.scanSelect(st.Rows)
		f.scanUpdateExprs(st.OnDuplicateUpdate)
		f.scanSelectExprs(st.Returning)
	case *tree.Update:
		f.scanWith(st.With)
		f.scanTableExprs(st.Tables)
		if st.From != nil {
			f.scanTableExprs(st.From.Tables)
		}
		f.scanUpdateExprs(st.Exprs)
		f.scanWhere(st.Where)
		f.scanOrderBy(st.OrderBy)
		f.scanLimit(st.Limit)
		f.scanSelectExprs(st.Returning)
	case *tree.Delete:
		f.scanWith(st.With)
		f.scanTableExprs(st.Tables)
		f.scanTableExprs(st.TableRefs)
		f.scanWhere(st.Where)
		f.scanOrderBy(st.OrderBy)
		f.scanLimit(st.Limit)
		f.scanSelectExprs(st.Returning)
	case *tree.Replace:
		f.scanTableExpr(st.Table)
		f.scanSelect(st.Rows)
		f.scanSelectExprs(st.Returning)
	case *tree.Merge:
		f.scanWith(st.With)
		f.scanTableExpr(st.Target)
		f.scanTableExpr(st.Source)
		f.scanExpr(st.On)
		f.scanMergeClauses(st.Clauses)
		f.scanSelectExprs(st.Returning)
	case *tree.Load:
		f.scanTableName(st.Table)
	case *tree.TruncateTable:
		f.scanTableName(st.Name)
	case *tree.DropTable:
		if st.Names != nil {
			for _, name := range st.Names {
				f.scanTableName(name)
			}
		}
	case *tree.AlterTable:
		f.scanTableName(st.Table)
		for _, option := range st.Options {
			if rename, ok := option.(*tree.AlterOptionTableName); ok {
				f.scanUnresolvedObjectName(rename.Name)
			}
		}
	case *tree.RenameTable:
		for _, alter := range st.AlterTables {
			f.scanStatement(alter)
		}
	case *tree.CreateTable:
		f.scanTableName(&st.Table)
		f.scanTableName(&st.LikeTableName)
		f.scanSelect(st.AsSource)
	case *tree.CreateView:
		f.scanTableName(st.Name)
		f.scanSelect(st.AsSource)
	case *tree.AlterView:
		f.scanTableName(st.Name)
		f.scanSelect(st.AsSource)
	case *tree.CreateIndex:
		f.scanTableName(st.Table)
	case *tree.DropIndex:
		f.scanTableName(st.TableName)
	case *tree.ShowCreateTable:
		f.scanUnresolvedObjectName(st.Name)
	case *tree.ShowCreateView:
		f.scanUnresolvedObjectName(st.Name)
	case *tree.ShowColumns:
		f.scanUnresolvedObjectName(st.Table)
	case *tree.ShowIndex:
		f.scanUnresolvedObjectName(st.TableName)
	case *tree.AnalyzeStmt:
		for _, entry := range st.Entries {
			if entry != nil {
				f.scanTableName(entry.Table)
			}
		}
	case *tree.ExplainStmt:
		f.scanStatement(st.Statement)
	case *tree.ExplainAnalyze:
		f.scanStatement(st.Statement)
	case *tree.ExplainPhyPlan:
		f.scanStatement(st.Statement)
	case *tree.ExplainFor:
		f.scanStatement(st.Statement)
	case *tree.Grant:
		if st.Typ == tree.GrantTypePrivilege {
			f.scanPrivilegeLevel(st.GrantPrivilege.Level)
		}
	case *tree.Revoke:
		if st.Typ == tree.RevokeTypePrivilege {
			f.scanPrivilegeLevel(st.RevokePrivilege.Level)
		}
	case *tree.CallStmt:
		f.scanExprs(st.Args)
	}
}

func (f *hiddenIndexTableRefFinder) scanWith(with *tree.With) {
	if with == nil || f.found {
		return
	}
	for _, cte := range with.CTEs {
		if cte == nil || f.found {
			continue
		}
		// A CTE name carrying the reserved prefix is treated the same as a
		// physical hidden table; shadowing the internal names is rejected.
		if cte.Name != nil && f.checkName("", string(cte.Name.Alias)) {
			return
		}
		f.scanStatement(cte.Stmt)
	}
}

func (f *hiddenIndexTableRefFinder) scanSelect(sel *tree.Select) {
	if sel == nil || f.found {
		return
	}
	f.scanWith(sel.With)
	f.scanSelectStatement(sel.Select)
	f.scanOrderBy(sel.OrderBy)
	f.scanLimit(sel.Limit)
}

func (f *hiddenIndexTableRefFinder) scanOrderBy(orderBy tree.OrderBy) {
	for _, order := range orderBy {
		if order != nil {
			f.scanExpr(order.Expr)
		}
	}
}

func (f *hiddenIndexTableRefFinder) scanLimit(limit *tree.Limit) {
	if limit != nil {
		f.scanExpr(limit.Offset)
		f.scanExpr(limit.Count)
	}
}

func (f *hiddenIndexTableRefFinder) scanSelectStatement(sel tree.SelectStatement) {
	if sel == nil || f.found {
		return
	}
	switch st := sel.(type) {
	case *tree.Select:
		f.scanSelect(st)
	case *tree.SelectClause:
		// UNION operands can be bare SelectClauses (no enclosing tree.Select)
		// per the grammar, so they must be scanned here as well.
		f.scanSelectClause(st)
	case *tree.ParenSelect:
		f.scanSelect(st.Select)
	case *tree.UnionClause:
		f.scanSelectStatement(st.Left)
		f.scanSelectStatement(st.Right)
	case *tree.ValuesClause:
		for _, row := range st.Rows {
			f.scanExprs(row)
		}
	}
}

func (f *hiddenIndexTableRefFinder) scanSelectClause(clause *tree.SelectClause) {
	if clause == nil || f.found {
		return
	}
	f.scanSelectExprs(clause.Exprs)
	if clause.From != nil {
		f.scanTableExprs(clause.From.Tables)
	}
	f.scanWhere(clause.Where)
	if clause.GroupBy != nil {
		for _, exprs := range clause.GroupBy.GroupByExprsList {
			f.scanExprs(exprs)
		}
		f.scanExprs(clause.GroupBy.GroupingSet)
	}
	f.scanWhere(clause.Having)
	for _, window := range clause.Windows {
		if window != nil {
			f.scanWindowSpec(window.Spec)
		}
	}
}

// A subquery in ordering/partitioning is still user-written SQL, even when
// the function has no ordinary arguments (for example ROW_NUMBER() OVER ...).
func (f *hiddenIndexTableRefFinder) scanWindowSpec(spec *tree.WindowSpec) {
	if spec == nil || f.found {
		return
	}
	f.scanExprs(spec.PartitionBy)
	f.scanOrderBy(spec.OrderBy)
	if spec.Frame != nil {
		if spec.Frame.Start != nil {
			f.scanExpr(spec.Frame.Start.Expr)
		}
		if spec.Frame.End != nil {
			f.scanExpr(spec.Frame.End.Expr)
		}
	}
}

func (f *hiddenIndexTableRefFinder) scanSelectExprs(exprs tree.SelectExprs) {
	if f.found {
		return
	}
	for _, item := range exprs {
		f.scanExpr(item.Expr)
	}
}

func (f *hiddenIndexTableRefFinder) scanWhere(where *tree.Where) {
	if where == nil || f.found {
		return
	}
	f.scanExpr(where.Expr)
}

func (f *hiddenIndexTableRefFinder) scanUpdateExprs(exprs tree.UpdateExprs) {
	if f.found {
		return
	}
	for _, item := range exprs {
		if item != nil {
			f.scanExpr(item.Expr)
		}
	}
}

func (f *hiddenIndexTableRefFinder) scanMergeClauses(clauses tree.MergeClauses) {
	if f.found {
		return
	}
	for _, clause := range clauses {
		if clause == nil {
			continue
		}
		f.scanExpr(clause.Condition)
		f.scanUpdateExprs(clause.UpdateExprs)
		f.scanExprs(clause.InsertValues)
	}
}

func (f *hiddenIndexTableRefFinder) scanTableExprs(exprs tree.TableExprs) {
	if f.found {
		return
	}
	for _, item := range exprs {
		f.scanTableExpr(item)
	}
}

func (f *hiddenIndexTableRefFinder) scanTableExpr(expr tree.TableExpr) {
	if expr == nil || f.found {
		return
	}
	switch t := expr.(type) {
	case *tree.TableName:
		f.scanTableName(t)
	case *tree.AliasedTableExpr:
		f.scanTableExpr(t.Expr)
	case *tree.ParenTableExpr:
		f.scanTableExpr(t.Expr)
	case *tree.JoinTableExpr:
		f.scanTableExpr(t.Left)
		f.scanTableExpr(t.Right)
		if on, ok := t.Cond.(*tree.OnJoinCond); ok {
			f.scanExpr(on.Expr)
		}
	case *tree.ApplyTableExpr:
		f.scanTableExpr(t.Left)
		f.scanTableExpr(t.Right)
	case *tree.Subquery:
		f.scanSelectStatement(t.Select)
	case *tree.Select:
		// a parenthesized derived table in FROM is (per parser) directly a
		// tree.Select wrapped in ParenTableExpr
		f.scanSelect(t)
	case *tree.ParenSelect:
		f.scanSelect(t.Select)
	case *tree.StatementSource:
		f.scanStatement(t.Statement)
	case *tree.TableFunction:
		if t.Func != nil {
			f.scanExpr(t.Func)
		}
		f.scanSelect(t.SelectStmt)
	}
}

func (f *hiddenIndexTableRefFinder) scanTableName(name *tree.TableName) {
	if name == nil || f.found {
		return
	}
	schema := ""
	if name.ExplicitSchema {
		schema = string(name.SchemaName)
	}
	f.checkName(schema, string(name.ObjectName))
}

func (f *hiddenIndexTableRefFinder) scanUnresolvedObjectName(name *tree.UnresolvedObjectName) {
	if name == nil || f.found {
		return
	}
	tableName := name.ToTableName()
	f.scanTableName(&tableName)
}

func (f *hiddenIndexTableRefFinder) scanPrivilegeLevel(level *tree.PrivilegeLevel) {
	if level == nil || f.found {
		return
	}
	if level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE || level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
		f.checkName(level.DbName, level.TabName)
	}
}

// checkName records the first hidden index table reference. The prefix match
// uses the catalog constants, so secondary, unique, fulltext and the vector
// index algorithm tables are all covered.
func (f *hiddenIndexTableRefFinder) checkName(schema, table string) bool {
	if f.found {
		return true
	}
	if table == "" || !isHiddenIndexTableName(table) {
		return false
	}
	f.found = true
	f.schema = schema
	f.table = strings.ToLower(table)
	return true
}

func (f *hiddenIndexTableRefFinder) scanExprs(exprs []tree.Expr) {
	if f.found {
		return
	}
	for _, expr := range exprs {
		f.scanExpr(expr)
	}
}

// scanExpr hunts scalar/row subqueries nested inside expressions.
func (f *hiddenIndexTableRefFinder) scanExpr(expr tree.Expr) {
	if expr == nil || f.found {
		return
	}
	switch e := expr.(type) {
	case *tree.Subquery:
		f.scanSelectStatement(e.Select)
	case *tree.ParenExpr:
		f.scanExpr(e.Expr)
	case *tree.AndExpr:
		f.scanExpr(e.Left)
		f.scanExpr(e.Right)
	case *tree.OrExpr:
		f.scanExpr(e.Left)
		f.scanExpr(e.Right)
	case *tree.XorExpr:
		f.scanExpr(e.Left)
		f.scanExpr(e.Right)
	case *tree.NotExpr:
		f.scanExpr(e.Expr)
	case *tree.ComparisonExpr:
		f.scanExpr(e.Left)
		f.scanExpr(e.Right)
		f.scanExpr(e.Escape)
	case *tree.BinaryExpr:
		f.scanExpr(e.Left)
		f.scanExpr(e.Right)
	case *tree.UnaryExpr:
		f.scanExpr(e.Expr)
	case *tree.IsNullExpr:
		f.scanExpr(e.Expr)
	case *tree.IsNotNullExpr:
		f.scanExpr(e.Expr)
	case *tree.IsUnknownExpr:
		f.scanExpr(e.Expr)
	case *tree.IsNotUnknownExpr:
		f.scanExpr(e.Expr)
	case *tree.IsTrueExpr:
		f.scanExpr(e.Expr)
	case *tree.IsNotTrueExpr:
		f.scanExpr(e.Expr)
	case *tree.IsFalseExpr:
		f.scanExpr(e.Expr)
	case *tree.IsNotFalseExpr:
		f.scanExpr(e.Expr)
	case *tree.RangeCond:
		f.scanExpr(e.Left)
		f.scanExpr(e.From)
		f.scanExpr(e.To)
	case *tree.CaseExpr:
		f.scanExpr(e.Expr)
		for _, when := range e.Whens {
			if when != nil {
				f.scanExpr(when.Cond)
				f.scanExpr(when.Val)
			}
		}
		f.scanExpr(e.Else)
	case *tree.CastExpr:
		f.scanExpr(e.Expr)
	case *tree.BitCastExpr:
		f.scanExpr(e.Expr)
	case *tree.SerialExtractExpr:
		f.scanExpr(e.SerialExpr)
		f.scanExpr(e.IndexExpr)
	case *tree.FuncExpr:
		f.scanExprs(e.Exprs)
		f.scanOrderBy(e.OrderBy)
		f.scanWindowSpec(e.WindowSpec)
	case *tree.Tuple:
		f.scanExprs(e.Exprs)
	case *tree.ExprList:
		f.scanExprs(e.Exprs)
	case *tree.FullTextMatchExpr:
		f.scanExpr(e.Pattern)
	}
}
