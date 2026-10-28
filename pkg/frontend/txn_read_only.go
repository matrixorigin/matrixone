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
// Internal executors are deliberately exempt: background jobs, upgrade
// bootstrap and mo_catalog maintenance run through back sessions or
// internal statements and must not observe client-facing access modes.
// Statements belonging to derived execution (SELECT-only by construction)
// and session migration replay are exempt as well.
func enforceReadOnlyTxnAccessMode(
	ses FeSession,
	execCtx *ExecCtx,
	stmt tree.Statement,
	defaultDatabase string,
) error {
	if ses == nil || execCtx == nil || stmt == nil {
		return nil
	}
	if ses.IsBackgroundSession() || ses.GetIsInternal() || execCtx.inMigration {
		return nil
	}
	if execCtx.input != nil && execCtx.input.isInternal() {
		return nil
	}
	if !statementRequiresPersistentWriteAccess(ses, stmt, defaultDatabase) {
		return nil
	}

	readOnly := false
	txnHandler := ses.GetTxnHandler()
	if execCtx.txnOpt.activeTxnAtStart && txnHandler != nil {
		// The active transaction's mode was fixed when it started. Changing
		// the session default afterwards has no effect on it (MySQL).
		readOnly = txnHandler.TxnReadOnly()
	} else {
		var err error
		readOnly, err = sessionTransactionReadOnly(execCtx)
		if err != nil {
			return err
		}
	}
	if readOnly {
		return moerr.NewCantExecuteInReadOnlyTransaction(execCtx.reqCtx)
	}
	return nil
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
		return !tableExprsAllSessionTemporary(ses, st.Tables, defaultDatabase)
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
	}
	if st, ok := stmt.(tree.StatementType); ok && st.GetQueryType() == tree.QueryTypeDDL {
		return true
	}
	// The parser classifies these catalog writers as Other rather than DDL,
	// but they persist data or catalog state and must observe the read-only
	// gate as well. Operational toggles (pause/resume/restart of tasks),
	// account administration (DCL) and CALL stay exempt.
	switch stmt.(type) {
	case *tree.CloneTable, *tree.CloneDatabase,
		*tree.CreateStage, *tree.AlterStage, *tree.DropStage, *tree.RemoveStageFiles,
		*tree.CreateSnapShot, *tree.DropSnapShot, *tree.RestoreSnapShot,
		*tree.CreateCDC, *tree.DropCDC,
		*tree.AlterPitr, *tree.DropPitr, *tree.RestorePitr,
		*tree.CreateProcedure, *tree.DropProcedure:
		return true
	}
	return false
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
// base table it names. An alias hit wins over the session temporary map,
// matching engine resolution; without an alias the target names the table
// directly. Returns nil when the target cannot be resolved statically.
func resolveDeleteTarget(target tree.TableExpr, aliasMap map[string][2]string) *tree.TableName {
	name, ok := target.(*tree.TableName)
	if !ok || name == nil {
		return nil
	}
	if string(name.SchemaName) != "" {
		// Schema-qualified targets are spelled as the base table already.
		return name
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

// enforceReadOnlyOnProcedureBody rejects a procedure call whose body can
// write persistent state while the calling session is in READ ONLY mode. It
// runs after the body was fetched and parsed but before the interpreter
// opens its own transaction, so a rejected CALL leaves no partial procedure
// changes behind. Read-only procedures and CDC guard / iceberg builtin CALLs
// (handled before doInterpretCall reaches this point) stay executable.
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
		if procedureStatementRequiresWriteAccess(ses, stmt, defaultDatabase) {
			return moerr.NewCantExecuteInReadOnlyTransaction(ctx)
		}
	}
	return nil
}

// procedureStatementRequiresWriteAccess walks one procedure body statement.
// Control-flow blocks recurse into their bodies; a nested CALL is gated
// again when its own body executes, so it is not followed here.
func procedureStatementRequiresWriteAccess(ses FeSession, stmt tree.Statement, defaultDatabase string) bool {
	switch st := stmt.(type) {
	case *tree.CompoundStmt:
		for _, inner := range st.Stmts {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	case *tree.IfStmt:
		for _, inner := range st.Body {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		for _, elif := range st.Elifs {
			for _, inner := range elif.Body {
				if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
					return true
				}
			}
		}
		for _, inner := range st.Else {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	case *tree.CaseStmt:
		for _, when := range st.Whens {
			for _, inner := range when.Body {
				if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
					return true
				}
			}
		}
		for _, inner := range st.Else {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	case *tree.WhileStmt:
		for _, inner := range st.Body {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	case *tree.LoopStmt:
		for _, inner := range st.Body {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	case *tree.RepeatStmt:
		for _, inner := range st.Body {
			if procedureStatementRequiresWriteAccess(ses, inner, defaultDatabase) {
				return true
			}
		}
		return false
	}
	return statementRequiresPersistentWriteAccess(ses, stmt, defaultDatabase)
}
