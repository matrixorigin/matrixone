// Copyright 2025 Matrix Origin
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

// enforceReadOnlyTxnAccessMode implements the MySQL READ ONLY transaction
// contract: a statement that writes persistent data or the persistent
// catalog is rejected with ERROR 1792 (25006) while the current transaction
// or, when no transaction is active yet, the session default is READ ONLY.
//
// It runs before TxnHandler.Create so that a DDL statement inside an
// explicit READ ONLY transaction is rejected before its implicit commit and
// the rejecting error never aborts the owning transaction: reads remain
// usable after the rejection, matching MySQL.
//
// Declared access mode resolution:
//   - an active transaction carries the mode resolved when it was created
//     (explicit START TRANSACTION READ ONLY/READ WRITE wins over the session
//     default at that point);
//   - without an active transaction, the first statement starts its
//     transaction with the session transaction_read_only default. This also
//     covers autocommit, where each statement is its own transaction.
//
// Internal executors are deliberately exempt: upgrade bootstrap, daemon jobs
// and mo_catalog maintenance run as internal statements or origin-less back
// sessions and must not observe client-facing access modes. Statements
// belonging to derived execution (SELECT-only by construction) and session
// migration replay are exempt as well.
//
// A back session with a user origin (procedure bodies, nested CALLs, starlark
// SQL callbacks) is NOT exempt: the restriction of the originating user
// session applies to each statement at execution time, with the session
// temporary bindings live at that moment (backSession.GetTempTable delegates
// upstream, so bindings mutated earlier inside the procedure are honoured).
// This closes the stale-check bypass where a statically screened body drops a
// temporary binding and then writes the persistent table behind it from the
// procedure's own committed transaction.
func enforceReadOnlyTxnAccessMode(
	ses FeSession,
	execCtx *ExecCtx,
	stmt tree.Statement,
	defaultDatabase string,
) error {
	if ses == nil || execCtx == nil || stmt == nil {
		return nil
	}
	if ses.GetIsInternal() || execCtx.inMigration {
		return nil
	}
	if execCtx.input != nil && execCtx.input.isInternal() {
		return nil
	}
	defaultDatabase = effectiveDefaultDatabaseWithRemap(ses, execCtx, defaultDatabase)
	deferOwnership := updateOwnershipDeferred(ses, stmt, defaultDatabase)
	if !statementRequiresPersistentWriteAccess(ses, stmt, defaultDatabase) && !deferOwnership {
		return nil
	}

	gateSes := ses
	if ses.IsBackgroundSession() {
		origin := upstreamUserSession(ses)
		if origin == nil {
			// Trusted maintenance without a user caller stays exempt.
			return nil
		}
		gateSes = origin
	}

	readOnly := false
	if gateSes != ses {
		// The caller's mode is read live: this statement executes inside the
		// caller's transaction lifetime (procedure body or callback), so the
		// mode of the caller's currently active transaction governs.
		var err error
		readOnly, err = callerTxnReadOnly(gateSes)
		if err != nil {
			return err
		}
	} else {
		txnHandler := gateSes.GetTxnHandler()
		if execCtx.txnOpt.activeTxnAtStart && txnHandler != nil {
			// The active transaction's mode was fixed when it started.
			// Changing the session default afterwards has no effect on it
			// (MySQL).
			readOnly = txnHandler.TxnReadOnly()
		} else {
			var err error
			readOnly, err = sessionTransactionReadOnly(execCtx)
			if err != nil {
				return err
			}
		}
	}
	if !readOnly {
		return nil
	}
	if deferOwnership {
		// The verdict needs a catalog transaction that does not exist yet:
		// executeStmtWithWorkspace re-runs this gate right after Create with
		// the transaction attached.
		execCtx.deferReadOnlyOwnership = true
		return nil
	}
	return moerr.NewCantExecuteInReadOnlyTransaction(execCtx.reqCtx)
}

// effectiveDefaultDatabaseWithRemap resolves the database unqualified names
// actually bind against for this statement, mirroring
// TxnCompilerContext.DefaultDatabase (compiler_context.go): the caller's
// default database is the prepared binding database for EXECUTE (captured
// when PREPAREd) or, when empty, the session's current database; an active
// remapdb policy then redirects it to the remapped destination. Admission
// must classify temporary-target and UPDATE column ownership against the
// same effective database — checking the un-remapped source would admit
// persistent writes to the destination (no ERROR 1792) or falsely reject
// temporary-only writes whose binding lives at the destination.
func effectiveDefaultDatabaseWithRemap(ses FeSession, execCtx *ExecCtx, defaultDatabase string) string {
	db := defaultDatabase
	if db == "" {
		db = ses.GetDatabaseName()
	}
	if db == "" || execCtx == nil || len(execCtx.remapDb) == 0 {
		return db
	}
	key := tree.NewCStr(db, parserLowerCaseTableNames(ses)).Compare()
	if dst, ok := execCtx.remapDb[key]; ok {
		return dst
	}
	return db
}

// catalogOwnershipResolvable reports whether the session currently has a
// catalog-capable transaction for resolving UPDATE assignment ownership. It
// is an indirection point so unit tests can drive both admission phases.
var catalogOwnershipResolvable = func(ses FeSession) bool {
	txnHandler := ses.GetTxnHandler()
	return txnHandler != nil && txnHandler.GetTxn() != nil
}

// updateOwnershipDeferred reports whether stmt is an UPDATE whose write
// verdict needs a catalog transaction that does not exist yet: an
// unqualified assignment column over multiple sources can only be owned by
// catalog resolution, which admission cannot perform before TxnHandler.Create
// (the autocommit path). The gate marks the statement for re-classification
// right after Create instead of conservatively rejecting a potentially
// temporary-only write. EXPLAIN ANALYZE/PHYPLAN wrappers defer with their
// wrapped statement.
func updateOwnershipDeferred(ses FeSession, stmt tree.Statement, defaultDatabase string) bool {
	switch st := stmt.(type) {
	case *tree.ExplainAnalyze:
		return updateOwnershipDeferred(ses, st.Statement, defaultDatabase)
	case *tree.ExplainPhyPlan:
		return updateOwnershipDeferred(ses, st.Statement, defaultDatabase)
	case *tree.Update:
		if catalogOwnershipResolvable(ses) {
			return false
		}
		ctes := make(map[string]struct{})
		if st.With != nil {
			for _, cte := range st.With.CTEs {
				ctes[string(cte.Name.Alias)] = struct{}{}
			}
		}
		sources := make(map[string][2]string)
		for _, expr := range st.Tables {
			collectUpdateSource(expr, ctes, sources)
		}
		if len(sources) < 2 {
			return false
		}
		for _, updateExpr := range st.Exprs {
			if updateExpr == nil || updateExpr.Tuple || len(updateExpr.Names) != 1 || updateExpr.Names[0] == nil {
				continue
			}
			if updateExpr.Names[0].NumParts <= 1 {
				return true
			}
		}
	}
	return false
}

// statementRequiresPersistentWriteAccess reports whether stmt mutates
// persistent data or the persistent catalog. Reads, SET/SHOW, transaction
// control and account-management (DCL) statements never require it.
//
// MySQL permits TEMPORARY table operations inside read-only transactions
// because they are session-local and never persist; the same exemption
// applies here through the session temporary alias map resolved against
// defaultDatabase. When a data-writing statement references a mix of
// temporary and persistent tables the statement still requires write
// access, again matching MySQL.
//
// The DDL fallback follows the parser-owned QueryType classification so a
// newly added DDL statement type cannot silently bypass the gate.
func statementRequiresPersistentWriteAccess(
	ses FeSession,
	stmt tree.Statement,
	defaultDatabase string,
) bool {
	switch st := stmt.(type) {
	case *tree.Insert:
		return !writeTargetIsSessionTemporary(ses, st.Table, defaultDatabase)
	case *tree.Replace:
		return !writeTargetIsSessionTemporary(ses, st.Table, defaultDatabase)
	case *tree.Update:
		// The planner derives the written tables of an UPDATE from the SET
		// assignment targets only (build_constraint_util.go getUpdateTableInfo
		// removes every unmodified source); a join partner or FROM source
		// that is merely read does not require write access. Mirror that:
		// classify the actual assignment targets so temporary-only writes
		// through a persistent join stay allowed, exactly like MySQL.
		return updateRequiresPersistentWriteAccess(ses, st, defaultDatabase)
	case *tree.Delete:
		if len(st.TableRefs) != 0 {
			// Multi-table DELETE lists bare write targets that resolve through
			// the FROM sources: `DELETE x FROM p AS x` writes p, not a table
			// named x. Resolve every target to its base table exactly like
			// buildDelete does before applying the temporary exemption, so a
			// temporary table sharing a name with the alias never shadows the
			// persistent victim.
			aliasMap := make(map[string][2]string)
			for _, ref := range st.TableRefs {
				deleteSourceAlias(ref, aliasMap)
			}
			for _, target := range st.Tables {
				resolved := resolveDeleteTarget(target, aliasMap)
				if resolved == nil {
					// Unresolvable target; fail closed.
					return true
				}
				if !isSessionTemporaryTable(ses, resolved, defaultDatabase) {
					return true
				}
			}
			return false
		}
		if len(st.Tables) != 0 {
			// Single-table DELETE writes the (possibly aliased) target itself.
			return !writeTargetIsSessionTemporary(ses, st.Tables[0], defaultDatabase)
		}
		// DELETE FROM t lists the written table only in the FROM clause.
		return !tableExprsAllSessionTemporary(ses, st.TableRefs, defaultDatabase)
	case *tree.Merge:
		return !writeTargetIsSessionTemporary(ses, st.Target, defaultDatabase)
	case *tree.MultiInsert:
		targetsFound := false
		for _, target := range st.AllTargets() {
			if target == nil {
				continue
			}
			targetsFound = true
			if !isSessionTemporaryTable(ses, target.Table, defaultDatabase) {
				return true
			}
		}
		// INSERT FIRST/ALL always carries at least one INTO clause; without
		// one, fail closed and require write access.
		return !targetsFound
	case *tree.Load:
		return st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase)
	case *tree.LoadTable:
		return st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase)
	case *tree.CreateTable:
		return !st.Temporary
	case *tree.DropTable:
		return len(capturePersistentDropTableTargets(ses, st, defaultDatabase)) > 0
	case *tree.TruncateTable:
		return st.Name == nil || !isSessionTemporaryTable(ses, st.Name, defaultDatabase)
	case *tree.AlterTable:
		return st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase)
	case *tree.ExplainAnalyze:
		// EXPLAIN ANALYZE executes the wrapped statement (result_row_stmt.go
		// runs its plan directly); gate exactly what it would run. Ordinary
		// EXPLAIN/EXPLAIN FOR only plans and stays exempt.
		return statementRequiresPersistentWriteAccess(ses, st.Statement, defaultDatabase)
	case *tree.ExplainPhyPlan:
		// EXPLAIN PHYPLAN shares the executing branch with EXPLAIN ANALYZE
		// (result_row_stmt.go *tree.ExplainAnalyze, *tree.ExplainPhyPlan runs
		// the plan for both through Exec.Run); classify its wrapped statement
		// exactly like EXPLAIN ANALYZE.
		return statementRequiresPersistentWriteAccess(ses, st.Statement, defaultDatabase)
	case *tree.DataBranchDiff:
		// Diffing is read-only unless the result is materialized into an
		// output table.
		return st.OutputOpt != nil && st.OutputOpt.As.ObjectName != ""
	}
	if st, ok := stmt.(tree.StatementType); ok && st.GetQueryType() == tree.QueryTypeDDL {
		return true
	}
	// The parser classifies these catalog writers as Other rather than DDL,
	// but they persist data or catalog state and must observe the read-only
	// gate as well. Data branch CREATE/DELETE mutate the branch catalog and
	// MERGE/PICK write the destination table. Operational toggles
	// (pause/resume/restart of tasks), account administration (DCL) and CALL
	// stay exempt.
	switch stmt.(type) {
	case *tree.CloneTable, *tree.CloneDatabase,
		*tree.CreateStage, *tree.AlterStage, *tree.DropStage, *tree.RemoveStageFiles,
		*tree.CreateSnapShot, *tree.DropSnapShot, *tree.RestoreSnapShot,
		*tree.CreateCDC, *tree.DropCDC,
		*tree.AlterPitr, *tree.DropPitr, *tree.RestorePitr,
		*tree.CreateProcedure, *tree.DropProcedure,
		*tree.DataBranchCreateTable, *tree.DataBranchCreateDatabase,
		*tree.DataBranchDeleteTable, *tree.DataBranchDeleteDatabase,
		*tree.DataBranchMerge, *tree.DataBranchPick:
		return true
	}
	return false
}

// updateRequiresPersistentWriteAccess reports whether an UPDATE writes a
// persistent table, mirroring getUpdateTableInfo (build_constraint_util.go)
// step for step: the written tables derive from the SET assignment targets,
// which resolve through the source alias map — an explicitly aliased source
// by its alias, an unaliased one by its object name, WITH-clause names never
// resolve. A qualified target the sources cannot name never writes (the
// planner reports it as an unknown table before execution). An unqualified
// column targets the unique source owning it; the gate resolves column
// ownership through the session compile context so temporary-owned columns
// stay writable, while genuinely ambiguous assignments and unresolvable
// persistent sources keep the conservative gate.
func updateRequiresPersistentWriteAccess(ses FeSession, st *tree.Update, defaultDatabase string) bool {
	sources := make(map[string][2]string)
	ctes := make(map[string]struct{})
	if st.With != nil {
		for _, cte := range st.With.CTEs {
			ctes[string(cte.Name.Alias)] = struct{}{}
		}
	}
	for _, expr := range st.Tables {
		collectUpdateSource(expr, ctes, sources)
	}

	for _, updateExpr := range st.Exprs {
		if updateExpr == nil || updateExpr.Tuple || len(updateExpr.Names) != 1 {
			// The planner rejects these forms (NYI); they never write.
			continue
		}
		parts := updateExpr.Names[0]
		if parts == nil {
			continue
		}
		if parts.NumParts > 1 {
			base, hit := sources[parts.TblName()]
			if !hit {
				// The planner reports the target as an unknown table; the
				// statement never writes.
				continue
			}
			if !isSessionTemporaryTable(ses, tree.NewTableName(
				tree.Identifier(base[1]),
				tree.ObjectNamePrefix{SchemaName: tree.Identifier(base[0])},
				nil,
			), defaultDatabase) {
				return true
			}
			continue
		}
		// Column-only assignment. A single source owns the column by
		// construction (the planner errors if it does not, and nothing
		// writes); no catalog access is needed for the verdict.
		if len(sources) == 1 {
			for _, base := range sources {
				if !isSessionTemporaryTable(ses, tree.NewTableName(
					tree.Identifier(base[1]),
					tree.ObjectNamePrefix{SchemaName: tree.Identifier(base[0])},
					nil,
				), defaultDatabase) {
					return true
				}
			}
			continue
		}
		if !catalogOwnershipResolvable(ses) {
			// Without a catalog transaction this outcome defers to the
			// post-Create gate (updateOwnershipDeferred). A bool-only
			// consumer still gets the conservative answer whenever a
			// persistent source may own the column.
			for _, base := range sources {
				if !isSessionTemporaryTable(ses, tree.NewTableName(
					tree.Identifier(base[1]),
					tree.ObjectNamePrefix{SchemaName: tree.Identifier(base[0])},
					nil,
				), defaultDatabase) {
					return true
				}
			}
			continue
		}
		// The planner picks the unique source owning
		// the column and errors on genuine ambiguity (dml_context.go). The
		// gate resolves the owner through the session compile context — the
		// same bindings and catalog view the statement will see at execution —
		// so a column owned only by a temporary table writes freely, matching
		// MySQL. Genuine ambiguity and unresolvable persistent sources stay
		// conservatively gated: the gate must never open for a write it
		// cannot classify.
		owners := make(map[string][2]string)
		unresolvedPersistent := false
		for alias, base := range sources {
			columns, err := resolveSourceColumnNames(ses, base[0], base[1], defaultDatabase)
			if err != nil {
				if !isSessionTemporaryTable(ses, tree.NewTableName(
					tree.Identifier(base[1]),
					tree.ObjectNamePrefix{SchemaName: tree.Identifier(base[0])},
					nil,
				), defaultDatabase) {
					unresolvedPersistent = true
				}
				continue
			}
			if _, hit := columns[parts.ColName()]; hit {
				owners[alias] = base
			}
		}
		switch len(owners) {
		case 0:
			// No source owns the column: the planner rejects the statement
			// (or an unresolvable source owns it) — gate only what cannot be
			// proven temporary.
			if unresolvedPersistent {
				return true
			}
		case 1:
			for _, base := range owners {
				if !isSessionTemporaryTable(ses, tree.NewTableName(
					tree.Identifier(base[1]),
					tree.ObjectNamePrefix{SchemaName: tree.Identifier(base[0])},
					nil,
				), defaultDatabase) {
					return true
				}
			}
		default:
			// Genuine ambiguity: the planner errors, but the write target is
			// unprovable statically, so keep the conservative gate.
			return true
		}
	}
	return false
}

// resolveSourceColumnNames resolves the column names of one UPDATE source
// through the session's transaction compile context — the same catalog and
// temporary-binding view the statement executes against — mirroring the
// planner's per-source column lookup. It is an indirection point so unit
// tests can stub catalog access; production behavior follows
// TxnCompilerContext.Resolve.
var resolveSourceColumnNames = func(ses FeSession, schemaName, tableName, defaultDatabase string) (map[string]struct{}, error) {
	dbName := schemaName
	if dbName == "" {
		dbName = defaultDatabase
		if dbName == "" {
			dbName = ses.GetDatabaseName()
		}
	}
	_, tableDef, err := ses.GetTxnCompileCtx().Resolve(dbName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(context.Background(), dbName, tableName)
	}
	columns := make(map[string]struct{}, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		columns[col.Name] = struct{}{}
	}
	return columns, nil
}

// collectUpdateSource mirrors how setTableExprToDmlTableInfo maps the UPDATE
// sources: an explicitly aliased source resolves by its alias, an unaliased
// one by its object name; joins recurse into both sides; WITH-clause names
// and derived sources never resolve to a table.
func collectUpdateSource(expr tree.TableExpr, ctes map[string]struct{}, sources map[string][2]string) {
	switch t := expr.(type) {
	case *tree.TableName:
		name := string(t.ObjectName)
		if _, isCTE := ctes[name]; isCTE {
			return
		}
		sources[name] = [2]string{string(t.SchemaName), name}
	case *tree.AliasedTableExpr:
		base := sourceTableName(t.Expr)
		if base == nil {
			return
		}
		name := string(base.ObjectName)
		if _, isCTE := ctes[name]; isCTE {
			return
		}
		alias := string(t.As.Alias)
		if alias == "" {
			alias = name
		}
		sources[alias] = [2]string{string(base.SchemaName), name}
	case *tree.JoinTableExpr:
		collectUpdateSource(t.Left, ctes, sources)
		collectUpdateSource(t.Right, ctes, sources)
	case *tree.ParenTableExpr:
		collectUpdateSource(t.Expr, ctes, sources)
	}
}

// deleteSourceAlias mirrors the alias map buildDelete constructs over the
// FROM sources of a multi-table DELETE (pkg/sql/plan/build_constraint_util.go
// getAliasToName): only explicitly aliased sources contribute, and joins
// recurse into both sides. Unaliased sources match a write target by name
// directly and need no entry.
func deleteSourceAlias(expr tree.TableExpr, aliasMap map[string][2]string) {
	switch t := expr.(type) {
	case *tree.TableName:
		// Only aliased sources map; the caller records the alias itself.
	case *tree.AliasedTableExpr:
		if name := sourceTableName(t.Expr); name != nil {
			if alias := string(t.As.Alias); alias != "" {
				aliasMap[alias] = [2]string{string(name.SchemaName), string(name.ObjectName)}
			}
		}
	case *tree.JoinTableExpr:
		deleteSourceAlias(t.Left, aliasMap)
		deleteSourceAlias(t.Right, aliasMap)
	}
}

// sourceTableName unwraps aliases and parentheses to the base table name of
// a FROM source, if the source is a real table.
func sourceTableName(expr tree.TableExpr) *tree.TableName {
	switch t := expr.(type) {
	case *tree.TableName:
		return t
	case *tree.AliasedTableExpr:
		return sourceTableName(t.Expr)
	case *tree.ParenTableExpr:
		return sourceTableName(t.Expr)
	}
	return nil
}

// resolveDeleteTarget maps one write target of a multi-table DELETE to the
// base table it names. The planner matches a target to an aliased FROM
// source by object name alone, overwriting even an explicit schema qualifier
// (build_constraint_util.go setTableExprToDmlTableInfo applies aliasMap
// unconditionally), so an alias hit wins here as well; without an alias the
// target names the table directly. Returns nil when the target cannot be
// resolved statically.
func resolveDeleteTarget(target tree.TableExpr, aliasMap map[string][2]string) *tree.TableName {
	name, ok := target.(*tree.TableName)
	if !ok || name == nil {
		return nil
	}
	if resolved, hit := aliasMap[string(name.ObjectName)]; hit {
		return tree.NewTableName(
			tree.Identifier(resolved[1]),
			tree.ObjectNamePrefix{SchemaName: tree.Identifier(resolved[0])},
			name.AtTsExpr,
		)
	}
	return name
}

// writeTargetIsSessionTemporary reports whether a single written table
// expression resolves to a session temporary table.
func writeTargetIsSessionTemporary(ses FeSession, target tree.TableExpr, defaultDatabase string) bool {
	names := collectTableNames(nil, target)
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if !isSessionTemporaryTable(ses, name, defaultDatabase) {
			return false
		}
	}
	return true
}

// tableExprsAllSessionTemporary reports whether every base table referenced
// by the expressions is a session temporary table. Derived subqueries and
// table functions carry no table names and are ignored for this purpose; a
// list without any base table is not temporary.
func tableExprsAllSessionTemporary(ses FeSession, exprs tree.TableExprs, defaultDatabase string) bool {
	var names []*tree.TableName
	for _, expr := range exprs {
		names = collectTableNames(names, expr)
	}
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if !isSessionTemporaryTable(ses, name, defaultDatabase) {
			return false
		}
	}
	return true
}

// collectTableNames gathers the base table names of a table expression,
// unwrapping aliases, parentheses and joins.
func collectTableNames(names []*tree.TableName, expr tree.TableExpr) []*tree.TableName {
	switch t := expr.(type) {
	case nil:
		return names
	case *tree.TableName:
		return append(names, t)
	case *tree.AliasedTableExpr:
		return collectTableNames(names, t.Expr)
	case *tree.ParenTableExpr:
		return collectTableNames(names, t.Expr)
	case *tree.JoinTableExpr:
		names = collectTableNames(names, t.Left)
		return collectTableNames(names, t.Right)
	}
	return names
}

// callerTxnReadOnly resolves the effective access mode of the user session a
// statement ultimately belongs to. Procedure bodies execute on background
// sessions that own and commit their own transactions, so gating every
// background session would leave CALL a write path; gating none of them
// would break maintenance jobs. Walking the upstream chain to the origin
// user session and applying its access mode matches MySQL, where procedure
// writes belong to the caller's transaction. Back executions without a user
// origin are trusted maintenance work and stay exempt.
func callerTxnReadOnly(ses FeSession) (bool, error) {
	origin := upstreamUserSession(ses)
	if origin == nil {
		return false, nil
	}
	txnHandler := origin.GetTxnHandler()
	if txnHandler != nil && txnHandler.InActiveTxn() {
		return txnHandler.TxnReadOnly(), nil
	}
	value, err := origin.GetSessionSysVar(transactionReadOnlySystemVariable)
	if err != nil {
		return false, err
	}
	return transactionReadOnlyValueEnabled(value), nil
}

// enforceReadOnlyOnProcedureBody enforces the only part of the procedure
// contract that cannot be checked at execution time: dynamic SQL. An
// EXECUTE resolves its statement at run time and the background executor
// runs it directly (back_status_stmt.go), outside the per-statement
// read-only gate, inside the procedure's own committed transaction that the
// caller cannot roll back. Under a READ ONLY caller a body carrying
// PREPARE/EXECUTE therefore fails closed up front, exactly like starlark
// procedure bodies. Statically resolvable writes are deliberately NOT
// pre-checked here: they are enforced per statement at execution by the
// caller-aware read-only gate, which classifies each statement against the
// session temporary bindings live at that moment (a body that drops a
// binding and then writes the persistent table behind it is rejected where
// it happens, without false-rejecting bodies that only ever touch temporary
// tables). DEALLOCATE PREPARE only frees a handle and can never write, so
// it stays allowed. It runs after the body was fetched and parsed but
// before the interpreter opens its own transaction, so a rejected CALL
// leaves no partial procedure changes behind. CDC guard / iceberg builtin
// CALLs (handled before doInterpretCall reaches this point) keep their
// earlier exemption.
func enforceReadOnlyOnProcedureBody(
	ctx context.Context,
	ses FeSession,
	body []tree.Statement,
	defaultDatabase string,
) error {
	readOnly, err := callerTxnReadOnly(ses)
	if err != nil {
		return err
	}
	if !readOnly {
		return nil
	}
	for _, stmt := range body {
		if procedureStatementRunsDynamicSQL(stmt) {
			return moerr.NewCantExecuteInReadOnlyTransaction(ctx)
		}
	}
	return nil
}

// procedureStatementRunsDynamicSQL walks one procedure body statement for
// dynamic SQL. Control-flow blocks recurse into their bodies; a nested CALL
// re-runs this check when its own body executes, so it is not followed here.
func procedureStatementRunsDynamicSQL(stmt tree.Statement) bool {
	switch st := stmt.(type) {
	case *tree.CompoundStmt:
		for _, inner := range st.Stmts {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.IfStmt:
		for _, inner := range st.Body {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		for _, elif := range st.Elifs {
			for _, inner := range elif.Body {
				if procedureStatementRunsDynamicSQL(inner) {
					return true
				}
			}
		}
		for _, inner := range st.Else {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.CaseStmt:
		for _, when := range st.Whens {
			for _, inner := range when.Body {
				if procedureStatementRunsDynamicSQL(inner) {
					return true
				}
			}
		}
		for _, inner := range st.Else {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.WhileStmt:
		for _, inner := range st.Body {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.LoopStmt:
		for _, inner := range st.Body {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.RepeatStmt:
		for _, inner := range st.Body {
			if procedureStatementRunsDynamicSQL(inner) {
				return true
			}
		}
		return false
	case *tree.PrepareStmt, *tree.PrepareString, *tree.PrepareVar, *tree.Execute:
		return true
	}
	return false
}
