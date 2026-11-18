// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// showMetadataTarget follows the planner's database qualification precedence.
// Historical metadata and view definitions retain their existing database gate.
func showMetadataTarget(stmt tree.Statement) (database, table string, baseTableOnly, supported bool) {
	switch st := stmt.(type) {
	case *tree.ShowTables:
		return st.DBName, "", false, st.AtTsExpr == nil
	case *tree.ShowColumns:
		if st.Table == nil {
			return st.DBName, "", false, false
		}
		return st.TargetDatabase(), st.Table.GetTableName(), false, st.Table.GetTableName() != ""
	case *tree.ShowCreateTable:
		if st.Name == nil {
			return "", "", true, false
		}
		return st.Name.GetDBName(), st.Name.GetTableName(), true, st.AtTsExpr == nil && st.Name.GetTableName() != ""
	}
	return "", "", false, false
}

func showMetadataObjectAdmission(stmt tree.Statement) *bool {
	switch st := stmt.(type) {
	case *tree.ShowColumns:
		return &st.ObjectAdmitted
	case *tree.ShowCreateTable:
		return &st.ObjectAdmitted
	}
	return nil
}

// A prepared compile can outlive the snapshot used to build it. SHOW TABLES
// also rebuilds its admission route on execution.
func canCachePreparedMetadataCompile(stmt tree.Statement) bool {
	stmt = authorizationStatement(stmt)
	if _, ok := stmt.(*tree.ShowTables); ok {
		return false
	}
	admission := showMetadataObjectAdmission(stmt)
	return admission == nil || !*admission
}

// Bind the default on the AST so planning and both privilege checks use the
// same target, even when a prepared statement outlives a subsequent USE.
func bindShowMetadataDatabase(stmt tree.Statement, defaultDatabase func() string) {
	switch st := stmt.(type) {
	case *tree.ShowTables:
		if st.DBName == "" {
			st.DBName = defaultDatabase()
		}
	case *tree.ShowColumns:
		if st.TargetDatabase() == "" {
			st.DBName = defaultDatabase()
		}
	case *tree.ShowCreateTable:
		if st.Name != nil && st.Name.GetDBName() == "" {
			if db := defaultDatabase(); db != "" {
				st.Name = tree.NewUnresolvedObjectName(db, st.Name.GetTableName())
			}
		}
	}
}

func showMetadataVisibilitySQL(stmt tree.Statement, currentDatabase string, protocolVersion int64) (string, string) {
	database, table, baseTableOnly, supported := showMetadataTarget(stmt)
	if !supported {
		return "", ""
	}
	if database == "" {
		database = currentDatabase
	}
	if database == "" {
		return "", ""
	}
	dbLiteral := escapeSQLString(database)
	local := sysview.MetadataVisibilityCTE(protocolVersion, database) +
		"SELECT 1 FROM __mo_visible_tables WHERE reldatabase = " + dbLiteral
	if table != "" {
		local += " AND relname = " + escapeSQLString(table)
	}
	if baseTableOnly {
		local += " AND relkind != '" + catalog.SystemViewRel + "'"
	}
	local += " LIMIT 1"
	// Subscription tables have no local catalog row. Only broad grants on the
	// subscriber database apply; never compare subscriber roles to publisher IDs.
	subscription := sysview.SubscriptionMetadataVisibilitySQL(dbLiteral, protocolVersion) +
		fmt.Sprintf(" WHERE EXISTS (SELECT 1 FROM mo_catalog.mo_database d WHERE d.account_id = current_account_id() AND d.datname = %s AND d.dat_type = '%s') LIMIT 1", dbLiteral, catalog.SystemDBTypeSubscription)
	return local, subscription
}

func showSubscriptionBaseTableSQL(database, table string) string {
	return fmt.Sprintf("SHOW FULL TABLES FROM %s WHERE %s = %s AND Table_type != 'VIEW'",
		sqlquote.Ident(database), sqlquote.Ident("Tables_in_"+database), escapeSQLString(table))
}

func authenticateShowMetadata(ctx context.Context, ses *Session, stmt tree.Statement) (allowed bool, stats statistic.StatsArray, err error) {
	stats.Reset()
	if !showMetadataPredicateSafe(stmt) {
		return false, stats, moerr.NewNotSupported(ctx, "object-authorized SHOW requires a row-local predicate")
	}
	protocol := currentProtocolVersion(ses.GetProc())
	if ses.GetProc() == nil || ses.GetProc().Base == nil {
		protocol = defines.MORPCVersion11
	}
	// SHOW TABLES object-level admission and its per-table filter must be
	// enabled together. Older protocols retain the existing database gate;
	// their role SQL cannot represent the full inherited-role closure.
	if _, ok := stmt.(*tree.ShowTables); ok && protocol < defines.MORPCVersion12 {
		return false, stats, nil
	}
	localSQL, subscriptionSQL := showMetadataVisibilitySQL(stmt, ses.GetDatabaseName(), protocol)
	if localSQL == "" {
		return false, stats, nil
	}
	var options BackgroundExecOption
	options.forcePessimisticRC = true
	options.readSnapshot, err = authorizationReadSnapshot(ctx, ses)
	if err != nil {
		return false, stats, err
	}
	if proc := ses.GetProc(); proc != nil && proc.Base != nil {
		options.currentRoleContext = &proc.Base.SessionInfo.CurrentRoleContext
	}
	bh := ses.GetBackgroundExec(ctx, &options)
	defer func() { stats = bh.GetExecStatsArray(); bh.Close() }()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return false, stats, err
	}
	defer func() { err = finishTxn(ctx, bh, err) }()
	hasRows := func(sql string) (bool, error) {
		bh.ClearExecResultSet()
		if execErr := bh.Exec(ctx, sql); execErr != nil {
			return false, execErr
		}
		results, resultErr := getResultSet(ctx, bh)
		return execResultArrayHasData(results), resultErr
	}
	allowed, err = hasRows(localSQL)
	if err != nil || allowed {
		return allowed, stats, err
	}
	allowed, err = hasRows(subscriptionSQL)
	if err != nil || !allowed {
		return allowed, stats, err
	}
	database, table, baseTableOnly, _ := showMetadataTarget(stmt)
	if baseTableOnly {
		if database == "" {
			database = ses.GetDatabaseName()
		}
		// A subscriber-local broad grant cannot authorize publisher view
		// definitions. Reuse subscription resolution and the publication
		// allowlist in SHOW TABLES, in this already initialized background txn.
		allowed, err = hasRows(showSubscriptionBaseTableSQL(database, table))
	}
	return allowed, stats, err
}

// Use the statement's authorization view for object-admitted metadata. A
// snapshot transaction owns these catalog reads and never advances the user's
// transaction snapshot. The caller's execution scope releases it after the
// compiled query finishes or on any planning/execution error.
func (execCtx *ExecCtx) useShowMetadataAuthorizationView(ctx context.Context, ses *Session, namedObject bool) error {
	if execCtx.showMetadataRead != nil {
		return nil
	}
	snapshot, err := authorizationReadSnapshot(ctx, ses)
	if err != nil {
		return err
	}
	if snapshot.IsEmpty() {
		return moerr.NewInternalError(ctx, "authorization snapshot unavailable for SHOW metadata")
	}
	proc := execCtx.proc
	if proc == nil || proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(ctx, "transaction unavailable for SHOW metadata")
	}
	// A named object must resolve outside the user's workspace, which can
	// contain catalog changes even when its committed timestamp matches.
	if !namedObject && proc.GetTxnOperator().SnapshotTS().Equal(snapshot) {
		return nil
	}

	bh := ses.InitBackExec(nil, "", backSesOutputCallback, &BackgroundExecOption{readSnapshot: snapshot}).(*backExec)
	defer func() {
		if execCtx.showMetadataRead != bh {
			bh.Close()
		}
	}()
	readCtx := ExecCtx{
		reqCtx: ctx,
		ses:    bh.backSes,
		txnOpt: FeTxnOption{byBegin: true, autoCommit: true},
	}
	if err := bh.backSes.GetTxnHandler().Create(&readCtx); err != nil {
		return err
	}
	execCtx.showMetadataRead = bh
	proc.Base.TxnOperator = bh.GetTxnOperator()
	return nil
}

func (execCtx *ExecCtx) closeShowMetadataAuthorizationView(ses *Session) {
	if bh := execCtx.showMetadataRead; bh != nil {
		execCtx.showMetadataRead = nil
		execCtx.proc.Base.TxnOperator = ses.GetTxnHandler().GetTxn()
		bh.Close()
	}
}

// showMetadataPredicateSafe restricts the new fallback to row-local operators.
// Unknown forms (including subqueries and function calls) fail closed; the
// existing database-authorized path retains its expression support.
func showMetadataPredicateSafe(stmt tree.Statement) bool {
	var where *tree.Where
	var like *tree.ComparisonExpr
	switch st := stmt.(type) {
	case *tree.ShowTables:
		where, like = st.Where, st.Like
	case *tree.ShowColumns:
		where, like = st.Where, st.Like
	default:
		return true
	}
	if where != nil && !showMetadataRowLocalExpr(where.Expr) {
		return false
	}
	return like == nil || showMetadataRowLocalExpr(like)
}

func showMetadataRowLocalExpr(expr tree.Expr) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case *tree.NumVal, *tree.UnresolvedName:
		return true
	case *tree.ComparisonExpr:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.Right) && showMetadataRowLocalExpr(e.Escape)
	case *tree.BinaryExpr:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.Right)
	case *tree.AndExpr:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.Right)
	case *tree.OrExpr:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.Right)
	case *tree.XorExpr:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.Right)
	case *tree.NotExpr:
		return showMetadataRowLocalExpr(e.Expr)
	case *tree.ParenExpr:
		return showMetadataRowLocalExpr(e.Expr)
	case *tree.UnaryExpr:
		return showMetadataRowLocalExpr(e.Expr)
	case *tree.IsNullExpr:
		return showMetadataRowLocalExpr(e.Expr)
	case *tree.IsNotNullExpr:
		return showMetadataRowLocalExpr(e.Expr)
	case *tree.RangeCond:
		return showMetadataRowLocalExpr(e.Left) && showMetadataRowLocalExpr(e.From) && showMetadataRowLocalExpr(e.To)
	case *tree.Tuple:
		for _, item := range e.Exprs {
			if !showMetadataRowLocalExpr(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
