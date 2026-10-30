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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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
	// Resolve the effective caller access mode for every admitted statement —
	// including read-classified ones — and stamp it on the statement's
	// shared process Base. Admission deliberately exempts carriers it cannot
	// fully see
	// into (a temporary-target INSERT keeps its target exemption even when a
	// sequence-mutating builtin hides in its SELECT arm, and stored views
	// expand only at binding), so the NEXTVAL/SETVAL mutation boundary
	// (seqfunc.go) reads this stamp and enforces ERROR 1792 itself. Unstamped
	// contexts (internal, background or remote execution) keep their previous
	// behavior.
	gateSes := ses
	originless := false
	if ses.IsBackgroundSession() {
		origin := upstreamUserSession(ses)
		if origin == nil {
			// Trusted maintenance without a user caller stays exempt; its
			// generated writes are not client-owned, so no stamp is needed.
			originless = true
		} else {
			gateSes = origin
		}
	}
	readOnly := false
	if !originless {
		var err error
		if gateSes != ses {
			// The caller's mode is read live: this statement executes inside
			// the caller's transaction lifetime (procedure body or callback),
			// so the mode of the caller's currently active transaction governs.
			readOnly, err = callerTxnReadOnly(gateSes)
		} else {
			txnHandler := gateSes.GetTxnHandler()
			if execCtx.txnOpt.activeTxnAtStart && txnHandler != nil {
				// The active transaction's mode was fixed when it started.
				// Changing the session default afterwards has no effect on it
				// (MySQL).
				readOnly = txnHandler.TxnReadOnly()
			} else {
				readOnly, err = sessionTransactionReadOnly(execCtx)
			}
		}
		if err != nil {
			return err
		}
		if execCtx.proc != nil {
			execCtx.proc.SetReadOnlyTxnAccessMode(readOnly)
		}
	}

	// The access mode is already stamped on the process. A READ WRITE or
	// origin-less statement cannot be rejected by this gate, so it does not
	// need the UPDATE ownership walk or its catalog lookups.
	if originless || !readOnly {
		return nil
	}
	defaultDatabase = effectiveDefaultDatabaseWithRemap(ses, execCtx, defaultDatabase)
	switch classifyStatementWrite(ses, stmt, defaultDatabase) {
	case writeAllowed:
		return nil
	case writeDeferred:
		// The post-Create gate resolves ownership once a catalog transaction exists.
		execCtx.deferReadOnlyOwnership = true
		return nil
	default:
		return moerr.NewCantExecuteInReadOnlyTransaction(execCtx.reqCtx)
	}
}

// effectiveDefaultDatabaseWithRemap resolves the database unqualified names
// actually bind against for this statement: the caller's
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
	if dst, ok := lookupRemappedDatabase(execCtx.remapDb, db, parserLowerCaseTableNames(ses)); ok {
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

type writeDisposition uint8

const (
	writeAllowed writeDisposition = iota
	writePersistent
	writeDeferred
)

func dispositionForPersistentWrite(persistent bool) writeDisposition {
	if persistent {
		return writePersistent
	}
	return writeAllowed
}

// classifyStatementWrite reports whether stmt mutates
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
func classifyStatementWrite(
	ses FeSession,
	stmt tree.Statement,
	defaultDatabase string,
) writeDisposition {
	switch st := stmt.(type) {
	case *tree.Insert:
		return dispositionForPersistentWrite(!writeTargetIsSessionTemporary(ses, st.Table, defaultDatabase))
	case *tree.Replace:
		return dispositionForPersistentWrite(!writeTargetIsSessionTemporary(ses, st.Table, defaultDatabase))
	case *tree.Update:
		// A source that is only read does not require write access. Classify
		// the SET owners, deferring when catalog facts are not available yet.
		return classifyUpdateWrite(ses, st, defaultDatabase)
	case *tree.Delete:
		if len(st.TableRefs) != 0 {
			// Multi-table DELETE lists bare write targets that resolve through
			// the FROM sources: `DELETE x FROM p AS x` writes p, not a table
			// named x. Resolve every target to its base table exactly like
			// buildDelete does before applying the temporary exemption, so a
			// temporary table sharing a name with the alias never shadows the
			// persistent victim.
			aliasMap := plan.CollectDeleteSourceAliases(st.TableRefs)
			for _, target := range st.Tables {
				name, ok := target.(*tree.TableName)
				if !ok || name == nil {
					// Unresolvable target; fail closed.
					return writePersistent
				}
				dbName, tableName, _ := plan.ResolveDMLTargetName(
					string(name.SchemaName), string(name.ObjectName), "", aliasMap)
				resolved := *name
				resolved.SchemaName, resolved.ObjectName = tree.Identifier(dbName), tree.Identifier(tableName)
				if !isSessionTemporaryTable(ses, &resolved, defaultDatabase) {
					return writePersistent
				}
			}
			return writeAllowed
		}
		if len(st.Tables) != 0 {
			// Single-table DELETE writes the (possibly aliased) target itself.
			return dispositionForPersistentWrite(!writeTargetIsSessionTemporary(ses, st.Tables[0], defaultDatabase))
		}
		// DELETE FROM t lists the written table only in the FROM clause.
		return dispositionForPersistentWrite(!tableExprsAllSessionTemporary(ses, st.TableRefs, defaultDatabase))
	case *tree.Merge:
		return dispositionForPersistentWrite(!writeTargetIsSessionTemporary(ses, st.Target, defaultDatabase))
	case *tree.MultiInsert:
		targetsFound := false
		for _, target := range st.AllTargets() {
			if target == nil {
				continue
			}
			targetsFound = true
			if !isSessionTemporaryTable(ses, target.Table, defaultDatabase) {
				return writePersistent
			}
		}
		// INSERT FIRST/ALL always carries at least one INTO clause; without
		// one, fail closed and require write access.
		return dispositionForPersistentWrite(!targetsFound)
	case *tree.Load:
		return dispositionForPersistentWrite(st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase))
	case *tree.LoadTable:
		return dispositionForPersistentWrite(st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase))
	case *tree.CreateTable:
		return dispositionForPersistentWrite(!st.Temporary)
	case *tree.DropTable:
		return dispositionForPersistentWrite(len(capturePersistentDropTableTargets(ses, st, defaultDatabase)) > 0)
	case *tree.TruncateTable:
		return dispositionForPersistentWrite(st.Name == nil || !isSessionTemporaryTable(ses, st.Name, defaultDatabase))
	case *tree.AlterTable:
		return dispositionForPersistentWrite(st.Table == nil || !isSessionTemporaryTable(ses, st.Table, defaultDatabase))
	case *tree.ExplainAnalyze:
		// EXPLAIN ANALYZE executes the wrapped statement (result_row_stmt.go
		// runs its plan directly); gate exactly what it would run. Ordinary
		// EXPLAIN/EXPLAIN FOR only plans and stays exempt.
		return classifyStatementWrite(ses, st.Statement, defaultDatabase)
	case *tree.ExplainPhyPlan:
		// EXPLAIN PHYPLAN shares the executing branch with EXPLAIN ANALYZE
		// (result_row_stmt.go *tree.ExplainAnalyze, *tree.ExplainPhyPlan runs
		// the plan for both through Exec.Run); classify its wrapped statement
		// exactly like EXPLAIN ANALYZE.
		return classifyStatementWrite(ses, st.Statement, defaultDatabase)
	case *tree.DataBranchDiff:
		// Diffing is read-only unless the result is materialized into an
		// output table.
		return dispositionForPersistentWrite(st.OutputOpt != nil && st.OutputOpt.As.ObjectName != "")
	}
	if st, ok := stmt.(tree.StatementType); ok && st.GetQueryType() == tree.QueryTypeDDL {
		return writePersistent
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
		return writePersistent
	}
	// A read-classified statement can still mutate persistent state through a
	// sequence-mutating builtin: NEXTVAL/SETVAL rewrite the catalog counter
	// inside the statement's own transaction (seqfunc.go) without ever
	// becoming a write-classified statement. Gate them like writes; their
	// session-local reads (CURRVAL/LASTVAL) stay exempt.
	return dispositionForPersistentWrite(statementMutatesSequence(stmt))
}

// sequenceMutationFunctions are the builtins that persistently mutate catalog
// sequence state. Function names resolve case-insensitively
// (function/function_id.go registers them lower-case).
var sequenceMutationFunctions = map[string]struct{}{
	"nextval": {},
	"setval":  {},
}

// statementMutatesSequence reports whether stmt evaluates a sequence-mutating
// builtin anywhere it can be reached at execution time. Only statement kinds
// that reach this point read-classified can smuggle the call — selecting the
// value, hiding it in a sub-select or expression container, or assigning it
// to a user variable through SET. Write-classified statements are gated long
// before this walk.
func statementMutatesSequence(stmt tree.Statement) bool {
	switch st := stmt.(type) {
	case *tree.Select:
		return selectMentionsSequenceMutation(st)
	case *tree.ParenSelect:
		return selectMentionsSequenceMutation(st.Select)
	case *tree.SetVar:
		for _, assignment := range st.Assignments {
			if assignment == nil {
				continue
			}
			if exprMentionsSequenceMutation(assignment.Value) ||
				exprMentionsSequenceMutation(assignment.Reserved) {
				return true
			}
		}
	}
	return false
}

func selectMentionsSequenceMutation(sel *tree.Select) bool {
	if sel == nil {
		return false
	}
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if cte == nil {
				continue
			}
			if body, ok := cte.Stmt.(tree.SelectStatement); ok {
				if selectStatementMentionsSequenceMutation(body) {
					return true
				}
			} else if statementMutatesSequence(cte.Stmt) {
				return true
			}
		}
	}
	if selectStatementMentionsSequenceMutation(sel.Select) {
		return true
	}
	if orderByMentionsSequenceMutation(sel.OrderBy) {
		return true
	}
	if sel.Limit != nil &&
		(exprMentionsSequenceMutation(sel.Limit.Offset) ||
			exprMentionsSequenceMutation(sel.Limit.Count)) {
		return true
	}
	if sel.TimeWindow != nil {
		tw := sel.TimeWindow
		if tw.Interval != nil && exprMentionsSequenceMutation(tw.Interval.Val) {
			return true
		}
		if tw.Sliding != nil && exprMentionsSequenceMutation(tw.Sliding.Val) {
			return true
		}
		if tw.Fill != nil && exprMentionsSequenceMutation(tw.Fill.Val) {
			return true
		}
	}
	return false
}

func selectStatementMentionsSequenceMutation(s tree.SelectStatement) bool {
	switch c := s.(type) {
	case nil:
		return false
	case *tree.SelectClause:
		if c.From != nil {
			for _, te := range c.From.Tables {
				if tableExprMentionsSequenceMutation(te) {
					return true
				}
			}
		}
		for _, se := range c.Exprs {
			if exprMentionsSequenceMutation(se.Expr) {
				return true
			}
		}
		if c.Where != nil && exprMentionsSequenceMutation(c.Where.Expr) {
			return true
		}
		if c.Having != nil && exprMentionsSequenceMutation(c.Having.Expr) {
			return true
		}
		if c.GroupBy != nil {
			for _, exprs := range c.GroupBy.GroupByExprsList {
				for _, e := range exprs {
					if exprMentionsSequenceMutation(e) {
						return true
					}
				}
			}
			for _, e := range c.GroupBy.GroupingSet {
				if exprMentionsSequenceMutation(e) {
					return true
				}
			}
		}
		for _, definition := range c.Windows {
			if definition != nil && windowSpecMentionsSequenceMutation(definition.Spec) {
				return true
			}
		}
		return false
	case *tree.UnionClause:
		return selectStatementMentionsSequenceMutation(c.Left) ||
			selectStatementMentionsSequenceMutation(c.Right)
	case *tree.ParenSelect:
		return selectMentionsSequenceMutation(c.Select)
	case *tree.Select:
		return selectMentionsSequenceMutation(c)
	case *tree.ValuesClause:
		for _, row := range c.Rows {
			for _, e := range row {
				if exprMentionsSequenceMutation(e) {
					return true
				}
			}
		}
	}
	return false
}

func tableExprMentionsSequenceMutation(te tree.TableExpr) bool {
	switch t := te.(type) {
	case nil:
		return false
	case *tree.AliasedTableExpr:
		return tableExprMentionsSequenceMutation(t.Expr)
	case *tree.JoinTableExpr:
		if tableExprMentionsSequenceMutation(t.Left) || tableExprMentionsSequenceMutation(t.Right) {
			return true
		}
		if on, ok := t.Cond.(*tree.OnJoinCond); ok && exprMentionsSequenceMutation(on.Expr) {
			return true
		}
		return false
	case *tree.ApplyTableExpr:
		return tableExprMentionsSequenceMutation(t.Left) || tableExprMentionsSequenceMutation(t.Right)
	case *tree.ParenTableExpr:
		return tableExprMentionsSequenceMutation(t.Expr)
	case *tree.Select:
		return selectMentionsSequenceMutation(t)
	case *tree.ParenSelect:
		return selectMentionsSequenceMutation(t.Select)
	case *tree.Subquery:
		return selectStatementMentionsSequenceMutation(t.Select)
	}
	return false
}

func orderByMentionsSequenceMutation(orderBy tree.OrderBy) bool {
	for _, order := range orderBy {
		if order != nil && exprMentionsSequenceMutation(order.Expr) {
			return true
		}
	}
	return false
}

func windowSpecMentionsSequenceMutation(spec *tree.WindowSpec) bool {
	if spec == nil {
		return false
	}
	for _, e := range spec.PartitionBy {
		if exprMentionsSequenceMutation(e) {
			return true
		}
	}
	if orderByMentionsSequenceMutation(spec.OrderBy) {
		return true
	}
	if spec.Frame != nil {
		frame := spec.Frame
		for _, bound := range []*tree.FrameBound{frame.Start, frame.End} {
			if bound != nil && exprMentionsSequenceMutation(bound.Expr) {
				return true
			}
		}
	}
	return false
}

// exprMentionsSequenceMutation mirrors remapDbInExpr (remap_db.go): every
// expression container a function call can hide in is descended, including
// nested sub-selects. Keep aligned with pkg/sql/parsers/tree/expr.go when an
// expression wrapper with children is added.
func exprMentionsSequenceMutation(expr tree.Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case *tree.Subquery:
		return selectStatementMentionsSequenceMutation(e.Select)
	case *tree.ExprList:
		for _, sub := range e.Exprs {
			if exprMentionsSequenceMutation(sub) {
				return true
			}
		}
		return false
	case *tree.ParenExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.NotExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsNullExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsNotNullExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsUnknownExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsNotUnknownExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsTrueExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsNotTrueExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsFalseExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.IsNotFalseExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.AndExpr:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.Right)
	case *tree.OrExpr:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.Right)
	case *tree.XorExpr:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.Right)
	case *tree.ComparisonExpr:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.Right) ||
			exprMentionsSequenceMutation(e.Escape)
	case *tree.RangeCond:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.From) ||
			exprMentionsSequenceMutation(e.To)
	case *tree.UnaryExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.BinaryExpr:
		return exprMentionsSequenceMutation(e.Left) || exprMentionsSequenceMutation(e.Right)
	case *tree.CastExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.CollateExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.BitCastExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.SerialExtractExpr:
		return exprMentionsSequenceMutation(e.SerialExpr) || exprMentionsSequenceMutation(e.IndexExpr)
	case *tree.IntervalExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.DefaultVal:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.VarExpr:
		return exprMentionsSequenceMutation(e.Expr)
	case *tree.FuncExpr:
		if sequenceFuncExprMutates(e) {
			return true
		}
		for _, sub := range e.Exprs {
			if exprMentionsSequenceMutation(sub) {
				return true
			}
		}
		if orderByMentionsSequenceMutation(e.OrderBy) {
			return true
		}
		return windowSpecMentionsSequenceMutation(e.WindowSpec)
	case *tree.Tuple:
		for _, sub := range e.Exprs {
			if exprMentionsSequenceMutation(sub) {
				return true
			}
		}
		return false
	case *tree.CaseExpr:
		if exprMentionsSequenceMutation(e.Expr) {
			return true
		}
		for _, when := range e.Whens {
			if when == nil {
				continue
			}
			if exprMentionsSequenceMutation(when.Cond) || exprMentionsSequenceMutation(when.Val) {
				return true
			}
		}
		return exprMentionsSequenceMutation(e.Else)
	case *tree.SampleExpr:
		columns, _ := e.GetColumns()
		for _, sub := range columns {
			if exprMentionsSequenceMutation(sub) {
				return true
			}
		}
		return false
	case *tree.FullTextMatchExpr:
		for _, keyPart := range e.KeyParts {
			if keyPart != nil && exprMentionsSequenceMutation(keyPart.Expr) {
				return true
			}
		}
		return exprMentionsSequenceMutation(e.Pattern)
	}
	return false
}

// sequenceFuncExprMutates reports whether a function call names a builtin
// that persists sequence state, matched unqualified or schema-qualified,
// mirroring how the builtin registry resolves names.
func sequenceFuncExprMutates(fn *tree.FuncExpr) bool {
	if fn.FuncName != nil {
		_, mutates := sequenceMutationFunctions[strings.ToLower(fn.FuncName.Origin())]
		if mutates {
			return true
		}
	}
	if name, ok := fn.Func.FunctionReference.(*tree.UnresolvedName); ok {
		_, mutates := sequenceMutationFunctions[strings.ToLower(name.ColName())]
		return mutates
	}
	return false
}

// classifyUpdateWrite determines whether an UPDATE writes a persistent table
// or needs a catalog transaction before its SET owners can be identified.
// Written tables derive from SET targets, which resolve through the source
// alias map — an explicitly aliased source
// by its alias, an unaliased one by its object name, WITH-clause names never
// resolve. A qualified target the sources cannot name never writes (the
// planner reports it as an unknown table before execution). An unqualified
// column targets the unique source owning it; the gate resolves column
// ownership through the session compile context so temporary-owned columns
// stay writable, while genuinely ambiguous assignments and unresolvable
// persistent sources keep the conservative gate.
func classifyUpdateWrite(ses FeSession, st *tree.Update, defaultDatabase string) writeDisposition {
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

	// Deferral is determined before inspecting qualified assignments: a later
	// unqualified column may need the post-Create catalog snapshot.
	unqualified := 0
	for _, assignment := range st.Exprs {
		if assignment != nil && !assignment.Tuple && len(assignment.Names) == 1 && assignment.Names[0] != nil && assignment.Names[0].NumParts <= 1 {
			unqualified++
		}
	}
	if len(sources) > 1 && unqualified > 0 && !catalogOwnershipResolvable(ses) {
		return writeDeferred
	}
	var resolvedColumns map[string]map[string]struct{}
	if len(sources) > 1 && unqualified > 1 {
		resolvedColumns = make(map[string]map[string]struct{}, len(sources))
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
				return writePersistent
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
					return writePersistent
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
		var owner [2]string
		ownerCount := 0
		unresolvedPersistent := false
		for alias, base := range sources {
			columns, cached := resolvedColumns[alias]
			var err error
			if !cached {
				columns, err = resolveSourceColumnNames(ses, base[0], base[1], defaultDatabase)
				if err == nil && resolvedColumns != nil {
					resolvedColumns[alias] = columns
				}
			}
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
				owner = base
				ownerCount++
			}
		}
		switch ownerCount {
		case 0:
			// No source owns the column: the planner rejects the statement
			// (or an unresolvable source owns it) — gate only what cannot be
			// proven temporary.
			if unresolvedPersistent {
				return writePersistent
			}
		case 1:
			if !isSessionTemporaryTable(ses, tree.NewTableName(
				tree.Identifier(owner[1]),
				tree.ObjectNamePrefix{SchemaName: tree.Identifier(owner[0])},
				nil,
			), defaultDatabase) {
				return writePersistent
			}
		default:
			// Genuine ambiguity: the planner errors, but the write target is
			// unprovable statically, so keep the conservative gate.
			return writePersistent
		}
	}
	return writeAllowed
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
