// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"reflect"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func bindAndOptimizeSelectQuery(stmtType plan.Query_StatementType, ctx CompilerContext, stmt *tree.Select, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	return bindAndOptimizeSelectQueryWithValidator(stmtType, ctx, stmt, isPrepareStmt, skipStats, nil)
}

func bindAndOptimizeSelectQueryWithValidator(
	stmtType plan.Query_StatementType,
	ctx CompilerContext,
	stmt *tree.Select,
	isPrepareStmt bool,
	skipStats bool,
	validate func(*Query) error,
) (*Plan, error) {
	return bindAndOptimizeSelectQueryWithValidatorAndCapture(
		stmtType, ctx, stmt, isPrepareStmt, skipStats, validate, nil, false, "",
	)
}

func bindAndOptimizeSelectQueryWithValidatorAndCapture(
	stmtType plan.Query_StatementType,
	ctx CompilerContext,
	stmt *tree.Select,
	isPrepareStmt bool,
	skipStats bool,
	validate func(*Query) error,
	capture func(*BindContext),
	restoreViewMySQLSpecialTypes bool,
	persistedViewTarget string,
) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildSelectHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(stmtType, ctx, isPrepareStmt, true)
	builder.sqlCalcFoundRows = selectHasSQLCalcFoundRows(stmt)
	builder.persistedViewTarget = persistedViewTarget
	builder.sessionSelectLimitMayStopEarly = sessionSelectLimitMayStopEarly(
		ctx, stmt, isPrepareStmt,
	)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.restoreViewMySQLSpecialTypes = restoreViewMySQLSpecialTypes
	if capture != nil {
		bindCtx.captureViewStarExpansion = true
		bindCtx.expandedSelectLists = make(map[*tree.SelectClause]tree.SelectExprs)
	}
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindSelect(stmt, bindCtx, true)
	if err != nil {
		return nil, err
	}
	builder.skipStats = skipStats
	// Shared-computation rewrites happen before createQuery, so parse the
	// service-level rollback hint before entering either rewrite.
	builder.parseOptimizeHints()
	if !builder.sharedComputationDisabled() {
		rootId = builder.reuseMultiReferenceCTEs(rootId)
		rootId = builder.sharePendingGroupingSetInputs(rootId)
	}
	ctx.SetViews(bindCtx.views)
	if capture != nil {
		capture(bindCtx)
	}

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if validate != nil {
		if err = validate(builder.qry); err != nil {
			return nil, err
		}
	}
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func sessionSelectLimitMayStopEarly(
	ctx CompilerContext,
	stmt *tree.Select,
	isPrepareStmt bool,
) bool {
	if ctx == nil || stmt == nil || stmt.IsPerform || selectHasExplicitTopLevelLimit(stmt) {
		return false
	}
	proc := ctx.GetProcess()
	if proc == nil || proc.Base == nil || !proc.Base.SessionInfo.ApplySQLSelectLimit ||
		proc.GetResolveVariableFunc() == nil {
		return false
	}
	// A prepared plan resolves this dynamic session variable at every EXECUTE;
	// even an unlimited value during PREPARE is not a proof for later runs.
	if isPrepareStmt {
		return true
	}
	value, err := ctx.ResolveVariable(SQLSelectLimitVariable, true, false)
	if err != nil {
		return true
	}
	limit, ok := value.(uint64)
	return !ok || limit != ^uint64(0)
}

func bindAndOptimizeInsertQuery(ctx CompilerContext, stmt *tree.Insert, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	builder.parseOptimizeHints()
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}
	if err := validateReturningSyntax(builder, stmt); err != nil {
		return nil, err
	}
	builder.returningRequested = stmt.HasReturning()

	rootId, err := builder.bindInsert(stmt, bindCtx)
	if err != nil {
		if stmt.HasReturning() {
			if feature := returningFallbackFeature(err, "legacy INSERT path"); feature != "" {
				return nil, returningNotSupported(builder, feature)
			}
		}
		// ON DUPLICATE KEY UPDATE is fully handled by the modern path; it must
		// never fall back to the legacy ODKU operator. Two exceptions still fall
		// back: plain INSERT (e.g. inserting into a system index table); and the
		// degenerate ODKU on a table with no primary/unique key (no dedup key to
		// represent the upsert; legacy treats it as a plain INSERT and preserves
		// the prepared-statement parameters).
		if !stmt.HasReturning() && moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) &&
			(len(stmt.OnDuplicateUpdate) == 0 ||
				err.Error() == noPkOnDupUpdateMsg) {
			return buildInsert(stmt, ctx, false, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if err = builder.appendReturningProjection(stmt.Returning, bindCtx); err != nil {
		return nil, err
	}
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// Append synchronous IVF/fulltext index maintenance (modern path, no legacy
	// fallback) from the materialized new-row image; no-op without such indexes.
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}

	// Enforce foreign key constraints for the modern insert path. The child→parent
	// parent-existence check is row-scoped and in-plan for every conflict action:
	// plain INSERT / ON DUPLICATE KEY UPDATE assert over the materialized image (see
	// modernInsertFkCheckEnabled / appendForeignConstrantPlan), and INSERT IGNORE
	// drops the offending rows (see buildInsertIgnoreFkFilter). Only self-referencing
	// FKs still need a post-execution DetectSql, generated here for all of them.
	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, stmt.With, nil, "insert")
	if err != nil {
		return nil, err
	}
	if len(tblInfo.tableDefs) == 1 && len(tblInfo.tableDefs[0].Fkeys) > 0 {
		// The in-plan child checks and self-reference DetectSqls depend on the
		// session's foreign_key_checks value. Keep prepared INSERT plans sensitive
		// even while checks are disabled, so a later EXECUTE rebuilds the plan
		// after either an OFF->ON or ON->OFF transition.
		query.HasForeignKeyAction = true

		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled {
			sqls, err := genSqlsForCheckFKSelfRefer(ctx.GetContext(), tblInfo.objRef[0].SchemaName,
				tblInfo.tableDefs[0].Name, tblInfo.tableDefs[0].Cols, tblInfo.tableDefs[0].Fkeys)
			if err != nil {
				return nil, err
			}
			query.DetectSqls = sqls
		}
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeReplaceQuery(ctx CompilerContext, stmt *tree.Replace, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	builder.parseOptimizeHints()
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindReplace(stmt, bindCtx)
	if err != nil {
		// REPLACE is the one DML entry point with no legacy-planner fallback:
		// map the resolver's external-table fallback sentinel (raised for
		// writable external tables) to the user-facing error every other DML
		// kind produces, instead of leaking the internal signal to the client.
		if moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) {
			switch err.Error() {
			case icebergRowLevelDMLUnsupportedMsg:
				return nil, moerr.NewNotSupported(ctx.GetContext(), "Iceberg row-level DML is not implemented")
			case externalTableUnsupportedDMLMsg:
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot insert/update/delete from external table")
			}
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// Append synchronous IVF/fulltext index maintenance (delete the conflicting
	// rows' old entries + insert the new ones) from the materialized image; no-op
	// without such indexes. Fixes issue #25000.
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}

	// Generate DetectSqls for self-referencing FK constraint checks.
	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "replace")
	if err != nil {
		return nil, err
	}
	// FK checks/actions are all disabled when foreign_key_checks is off, the
	// same way MySQL skips foreign-key enforcement. Gate every FK SQL below
	// (self-referencing checks, the RESTRICT pre-check, and the non-self
	// parent-side actions) under one guard so the behavior is consistent.
	fkChecksEnabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return nil, err
	}
	if len(tblInfo.tableDefs) == 1 &&
		(len(tblInfo.tableDefs[0].Fkeys) > 0 || len(tblInfo.tableDefs[0].RefChildTbls) > 0) {
		// The presence or absence of DetectSqls depends on the session's
		// foreign_key_checks value. Keep the plan FK-sensitive even when the
		// variable is currently off, otherwise a cached plan built without the
		// checks could survive after they are enabled.
		query.HasForeignKeyAction = true
	}
	if fkChecksEnabled && len(tblInfo.tableDefs) == 1 {
		if len(tblInfo.tableDefs[0].RefChildTbls) > 0 {
			// Parent-side actions are part of the modern REPLACE plan. Keep a
			// marker solely for the optimistic-transaction fail-closed guard.
			query.DetectSqls = append(query.DetectSqls, "REPLACE_PARENT_PLAN:")
		}
		sqls, err := genSqlsForCheckFKSelfRefer(
			ctx.GetContext(),
			tblInfo.objRef[0].SchemaName,
			tblInfo.tableDefs[0].Name,
			tblInfo.tableDefs[0].Cols,
			tblInfo.tableDefs[0].Fkeys,
		)
		if err != nil {
			return nil, err
		}
		query.DetectSqls = append(query.DetectSqls, sqls...)

		// Generate pre-check SQLs for parent→child safety (RESTRICT).
		preCheckSqls, err := genPreCheckSqlsForReplaceFKSelfRefer(
			ctx.GetContext(),
			tblInfo.objRef[0].SchemaName,
			tblInfo.tableDefs[0].Name,
			tblInfo.tableDefs[0].Cols,
			tblInfo.tableDefs[0].Fkeys,
			stmt,
		)
		if err != nil {
			return nil, err
		}
		for _, sql := range preCheckSqls {
			query.DetectSqls = append(query.DetectSqls, "REPLACE_PARENT_CHK:"+sql)
		}
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeLoadQuery(ctx CompilerContext, stmt *tree.Load, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	// return buildLoad(stmt, ctx, isPrepareStmt)
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindLoad(stmt, bindCtx)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) {
			return buildLoad(stmt, ctx, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// Append synchronous IVF/fulltext index maintenance (modern path, no legacy
	// fallback) from the materialized new-row image; no-op without such indexes.
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeDeleteQuery(ctx CompilerContext, stmt *tree.Delete, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildDeleteHistogram.Observe(time.Since(start).Seconds())
	}()
	if err := validateSingleTableDMLLimitOffset(ctx, "DELETE", stmt.Limit); err != nil {
		return nil, err
	}

	builder := NewQueryBuilder(plan.Query_DELETE, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}
	if err := validateReturningSyntax(builder, stmt); err != nil {
		return nil, err
	}
	builder.returningRequested = stmt.HasReturning()

	rootId, err := builder.bindDelete(ctx, stmt, bindCtx)
	if err != nil {
		if stmt.HasReturning() {
			if feature := returningFallbackFeature(err, "legacy DELETE path"); feature != "" {
				return nil, returningNotSupported(builder, feature)
			}
		}
		if !stmt.HasReturning() && moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) {
			if err.Error() == icebergRowLevelDMLUnsupportedMsg {
				return buildIcebergDeletePlan(stmt, ctx, isPrepareStmt)
			}
			return buildDelete(stmt, ctx, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if err = builder.appendReturningProjection(stmt.Returning, bindCtx); err != nil {
		return nil, err
	}
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeUpdateQuery(ctx CompilerContext, stmt *tree.Update, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildUpdateHistogram.Observe(time.Since(start).Seconds())
	}()
	if err := validateMultiTableUpdateClauses(ctx, stmt); err != nil {
		return nil, err
	}
	if err := validateSingleTableDMLLimitOffset(ctx, "UPDATE", stmt.Limit); err != nil {
		return nil, err
	}

	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}
	if err := validateReturningSyntax(builder, stmt); err != nil {
		return nil, err
	}
	builder.returningRequested = stmt.HasReturning()

	rootId, err := builder.bindUpdate(stmt, bindCtx)
	if err != nil {
		route, reason, routedErr := classifyUpdatePlannerError(err)
		switch route {
		case updatePlannerSpecialized:
			recordUpdatePlannerRoute(route, reason, "selected")
			if stmt.HasReturning() {
				if reason == updateRouteReasonIceberg {
					return nil, returningNotSupported(builder, "Iceberg table")
				}
				return nil, returningNotSupported(builder, "specialized UPDATE path")
			}
			return buildIcebergUpdatePlan(stmt, ctx, isPrepareStmt)
		case updatePlannerRejected, updatePlannerUnknown:
			recordUpdatePlannerRoute(route, reason, "rejected")
			if stmt.HasReturning() {
				switch reason {
				case updateRouteReasonExternalTable:
					return nil, returningNotSupported(builder, "external table")
				case updateRouteReasonTableForm:
					return nil, returningNotSupported(builder, "internal table")
				}
			}
			return nil, routedErr
		}
		return nil, routedErr
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if err = builder.appendReturningProjection(stmt.Returning, bindCtx); err != nil {
		return nil, err
	}
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}

	enabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return nil, err
	}
	if enabled && query.HasForeignKeyAction {
		tblInfo, resolveErr := getUpdateTableInfo(ctx, stmt)
		if resolveErr != nil {
			return nil, resolveErr
		}
		for i, tableDef := range tblInfo.tableDefs {
			if len(tblInfo.updateKeys[i]) == 0 {
				continue
			}
			selfFkeys := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
			for _, fk := range tableDef.Fkeys {
				if fk.ForeignTbl == 0 {
					selfFkeys = append(selfFkeys, fk)
				}
			}
			if len(selfFkeys) == 0 {
				continue
			}
			sqls, genErr := genSqlsForCheckFKSelfRefer(
				ctx.GetContext(),
				tblInfo.objRef[i].SchemaName,
				tableDef.Name,
				tableDef.Cols,
				selfFkeys,
			)
			if genErr != nil {
				return nil, genErr
			}
			query.DetectSqls = append(query.DetectSqls, sqls...)
		}
	}
	recordUpdatePlannerRoute(updatePlannerModern, updateRouteReasonNone, "selected")
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func buildExplainPlan(ctx CompilerContext, stmt tree.Statement, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildExplainHistogram.Observe(time.Since(start).Seconds())
	}()

	//get query optimizer and execute Optimize
	plan, err := BuildPlan(ctx, stmt, isPrepareStmt)
	if err != nil {
		return nil, err
	}

	//if it is the plan of the EXECUTE, replace it by the plan generated by the PREPARE.
	//At the same time, replace the param var by the param val
	if plan.GetDcl() != nil && plan.GetDcl().GetExecute() != nil {
		execPlan := plan.GetDcl().GetExecute()
		replaced, _, err := ctx.InitExecuteStmtParam(execPlan)
		if err != nil {
			return nil, err
		}
		plan = replaced
	}

	// Ensure that the plan includes a query section
	if plan.GetQuery() == nil {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "the sql query plan does not support explain.")
	}

	return plan, nil
}

func buildExplainAnalyze(ctx CompilerContext, stmt *tree.ExplainAnalyze, isPrepareStmt bool) (*Plan, error) {
	return buildExplainPlan(ctx, stmt.Statement, isPrepareStmt)
}

func buildExplainPhyPlan(ctx CompilerContext, stmt *tree.ExplainPhyPlan, isPrepareStmt bool) (*Plan, error) {
	return buildExplainPlan(ctx, stmt.Statement, isPrepareStmt)
}

func selectHasExportParam(stmt tree.SelectStatement) bool {
	return selectTreeHasExportParam(reflect.ValueOf(stmt))
}

// selectTreeHasExportParam follows the complete parser tree because SELECT
// nodes can also be nested in CTEs, table expressions, and scalar predicates.
// The parser's expression visitor cannot be used here because Subquery.Accept
// is intentionally unimplemented.
func selectTreeHasExportParam(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		if value.CanInterface() {
			if selectStmt, ok := value.Interface().(*tree.Select); ok && selectStmt.Ep != nil {
				return true
			}
		}
		return selectTreeHasExportParam(value.Elem())
	}

	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if selectTreeHasExportParam(value.Field(i)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if selectTreeHasExportParam(value.Index(i)) {
				return true
			}
		}
	}
	return false
}

func BuildPlan(ctx CompilerContext, stmt tree.Statement, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildPlanHistogram.Observe(time.Since(start).Seconds())
	}()
	_, task := gotrace.NewTask(context.TODO(), "plan.BuildPlan")
	defer task.End()
	switch stmt := stmt.(type) {
	case *tree.Select:
		if stmt.IsPerform && selectHasExportParam(stmt) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "PERFORM SELECT INTO OUTFILE")
		}
		queryPlan, err := bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, isPrepareStmt, false)
		if err != nil {
			return nil, err
		}
		applySQLSelectLimit(stmt, queryPlan)
		return queryPlan, nil
	case *tree.ParenSelect:
		queryPlan, err := bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt.Select, isPrepareStmt, false)
		if err != nil {
			return nil, err
		}
		applySQLSelectLimit(stmt.Select, queryPlan)
		return queryPlan, nil
	case *tree.ExplainStmt:
		return buildExplainPlan(ctx, stmt.Statement, isPrepareStmt)
	case *tree.ExplainAnalyze:
		return buildExplainAnalyze(ctx, stmt, isPrepareStmt)
	case *tree.ExplainPhyPlan:
		return buildExplainPhyPlan(ctx, stmt, isPrepareStmt)
	case *tree.Insert:
		return bindAndOptimizeInsertQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.MultiInsert:
		return bindAndOptimizeMultiInsertQuery(ctx, stmt, isPrepareStmt)
	case *tree.Replace:
		if stmt.HasReturning() {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "DML RETURNING does not support REPLACE")
		}
		return bindAndOptimizeReplaceQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Update:
		return bindAndOptimizeUpdateQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Delete:
		return bindAndOptimizeDeleteQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Merge:
		if stmt.HasReturning() {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "DML RETURNING does not support MERGE")
		}
		return bindAndOptimizeMergeQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.BeginTransaction:
		return buildBeginTransaction(stmt, ctx)
	case *tree.CommitTransaction:
		return buildCommitTransaction(stmt, ctx)
	case *tree.RollbackTransaction:
		return buildRollbackTransaction(stmt, ctx)
	case *tree.CreateDatabase:
		return buildCreateDatabase(stmt, ctx)
	case *tree.DropDatabase:
		return buildDropDatabase(stmt, ctx)
	case *tree.CreateTable:
		return buildCreateTable(ctx, stmt, nil, isPrepareStmt)
	case *tree.CreatePitr:
		return buildCreatePitr(stmt, ctx)
	case *tree.CreateCDC:
		return buildCreateCDC(stmt, ctx)
	case *tree.DropPitr:
		return buildDropPitr(stmt, ctx)
	case *tree.DropCDC:
		return buildDropCDC(stmt, ctx)
	case *tree.DropTable:
		return buildDropTable(stmt, ctx)
	case *tree.TruncateTable:
		return buildTruncateTable(stmt, ctx)
	case *tree.CreateSequence:
		return buildCreateSequence(stmt, ctx)
	case *tree.DropSequence:
		return buildDropSequence(stmt, ctx)
	case *tree.AlterSequence:
		return buildAlterSequence(stmt, ctx)
	case *tree.DropView:
		return buildDropView(stmt, ctx)
	case *tree.CreateView:
		return buildCreateView(stmt, ctx)
	case *tree.AlterView:
		return buildAlterView(stmt, ctx)
	case *tree.AlterTable:
		return buildAlterTable(stmt, ctx)
	case *tree.RenameTable:
		return buildRenameTable(stmt, ctx)
	case *tree.CreateIndex:
		return buildCreateIndex(stmt, ctx)
	case *tree.DropIndex:
		return buildDropIndex(stmt, ctx)
	case *tree.ShowCreateDatabase:
		return buildShowCreateDatabase(stmt, ctx)
	case *tree.ShowCreateTable:
		return buildShowCreateTable(stmt, ctx)
	case *tree.ShowCreateView:
		return buildShowCreateView(stmt, ctx)
	case *tree.ShowDatabases:
		return buildShowDatabases(stmt, ctx)
	case *tree.ShowTables:
		return buildShowTables(stmt, ctx)
	case *tree.ShowSequences:
		return buildShowSequences(stmt, ctx)
	case *tree.ShowColumns:
		return buildShowColumns(stmt, ctx)
	case *tree.ShowTableStatus:
		return buildShowTableStatus(stmt, ctx)
	case *tree.ShowTarget:
		return buildShowTarget(stmt, ctx)
	case *tree.ShowIndex:
		return buildShowIndex(stmt, ctx)
	case *tree.ShowGrants:
		return buildShowGrants(stmt, ctx)
	case *tree.ShowVariables:
		return buildShowVariables(stmt, ctx)
	case *tree.ShowStatus:
		return buildShowStatus(stmt, ctx)
	case *tree.ShowProcessList:
		return buildShowProcessList(ctx)
	case *tree.ShowLocks:
		return buildShowLocks(stmt, ctx)
	case *tree.ShowNodeList:
		return buildShowNodeList(stmt, ctx)
	case *tree.ShowFunctionOrProcedureStatus:
		return buildShowFunctionOrProcedureStatus(stmt, ctx)
	case *tree.ShowTableNumber:
		return buildShowTableNumber(stmt, ctx)
	case *tree.ShowColumnNumber:
		return buildShowColumnNumber(stmt, ctx)
	case *tree.ShowTableValues:
		return buildShowTableValues(stmt, ctx)
	case *tree.ShowRolesStmt:
		return buildShowRoles(stmt, ctx)
	case *tree.SetVar:
		return buildSetVariables(stmt, ctx, isPrepareStmt)
	case *tree.Execute:
		return buildExecute(stmt, ctx)
	case *tree.Deallocate:
		return buildDeallocate(stmt, ctx)
	case *tree.Load:
		return bindAndOptimizeLoadQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.PrepareStmt, *tree.PrepareString:
		return buildPrepare(stmt, ctx)
	case *tree.CallStmt:
		if isIcebergBuiltinCall(stmt) {
			return buildIcebergBuiltinCall(stmt, ctx)
		}
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	case *tree.Do, *tree.Declare:
		return nil, moerr.NewNotSupported(ctx.GetContext(), tree.String(stmt, dialect.MYSQL))
	case *tree.ValuesStatement:
		return buildValues(stmt, ctx, isPrepareStmt)
	case *tree.LockTableStmt:
		return buildLockTables(stmt, ctx)
	case *tree.UnLockTableStmt:
		return buildUnLockTables(stmt, ctx)
	case *tree.ShowCreatePublications:
		return buildShowCreatePublications(stmt, ctx)
	case *tree.ShowPublicationCoverage:
		return buildShowPublicationCoverage(stmt, ctx)
	case *tree.ShowStages:
		return buildShowStages(stmt, ctx)
	case *tree.ShowSnapShots:
		return buildShowSnapShots(stmt, ctx)
	case *tree.CreateAccount:
		return buildCreateAccount(stmt, ctx, isPrepareStmt)
	case *tree.AlterAccount:
		return buildAlterAccount(stmt, ctx, isPrepareStmt)
	case *tree.DropAccount:
		return buildDropAccount(stmt, ctx, isPrepareStmt)
	case *tree.ShowAccountUpgrade:
		return buildShowAccountUpgrade(stmt, ctx)
	case *tree.ShowPitr:
		return buildShowPitr(stmt, ctx)
	case *tree.CloneTable:
		return buildCloneTable(stmt, ctx)
	default:
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
}

// applySQLSelectLimit marks top-level SELECTs whose final result pipeline must
// enforce the session row cap. The compiler materializes ordinary finite caps
// only after optimization, avoiding changes to estimates and rewrites while
// still making the cap visible to offload serialization.
func applySQLSelectLimit(stmt *tree.Select, queryPlan *Plan) {
	query := queryPlan.GetQuery()
	if query != nil {
		query.ApplySqlSelectLimit = stmt != nil && !stmt.IsPerform &&
			!selectHasExplicitTopLevelLimit(stmt)
	}
}

func selectHasSQLCalcFoundRows(stmt *tree.Select) bool {
	for stmt != nil {
		switch body := stmt.Select.(type) {
		case *tree.SelectClause:
			return body.Option&tree.QuerySpecOptionSqlCalcFoundRows != 0
		case *tree.ParenSelect:
			stmt = body.Select
		case *tree.UnionClause:
			return selectStatementHasSQLCalcFoundRows(body.Left)
		default:
			return false
		}
	}
	return false
}

func selectStatementHasSQLCalcFoundRows(stmt tree.SelectStatement) bool {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return selectHasSQLCalcFoundRows(stmt)
	case *tree.ParenSelect:
		return selectHasSQLCalcFoundRows(stmt.Select)
	default:
		return false
	}
}

// Parenthesized SELECTs are flattened by bindSelect. A LIMIT on any wrapper in
// that chain is the top-level explicit LIMIT and takes precedence over the
// session sql_select_limit value. Limits inside UNION arms and subqueries are
// deliberately not considered top-level limits.
func selectHasExplicitTopLevelLimit(stmt *tree.Select) bool {
	for stmt != nil {
		if stmt.Limit != nil && stmt.Limit.Count != nil {
			return true
		}
		paren, ok := stmt.Select.(*tree.ParenSelect)
		if !ok {
			return false
		}
		stmt = paren.Select
	}
	return false
}

// GetResultColumnsFromPlan
func GetResultColumnsFromPlan(p *Plan) []*ColDef {
	getResultColumnsByProjectionlist := func(query *Query) []*ColDef {
		step := len(query.Steps) - 1
		if query.HasReturning {
			if query.ReturningStep < 0 || int(query.ReturningStep) >= len(query.Steps) {
				return nil
			}
			step = int(query.ReturningStep)
		}
		lastNode := query.Nodes[query.Steps[step]]
		if query.HasReturning && len(query.Headings) != len(lastNode.ProjectList) {
			return nil
		}
		columns := make([]*ColDef, len(lastNode.ProjectList))
		for idx, expr := range lastNode.ProjectList {
			columns[idx] = &ColDef{
				Name: query.Headings[idx],
				Typ:  expr.Typ,
			}

			if exprCol, ok := expr.Expr.(*plan.Expr_Col); ok {
				if col := exprCol.Col; col != nil {
					columns[idx].TblName = col.TblName
					columns[idx].DbName = col.DbName
				}
			}

			if source := findResultColumnSource(query, query.Steps[step], expr); source != nil {
				if columns[idx].TblName == "" {
					columns[idx].TblName = source.tableName
				}
				if columns[idx].DbName == "" {
					columns[idx].DbName = source.dbName
				}
				columns[idx].OriginTblName = source.tableName
				columns[idx].OriginName = source.columnName
				columns[idx].Primary = source.primary
				columns[idx].Unique = source.unique
				columns[idx].NotNull = source.notNull
				if source.nullExtended {
					columns[idx].Typ.NotNullable = false
				} else {
					columns[idx].Typ.NotNullable = columns[idx].Typ.NotNullable || source.notNull
				}
				columns[idx].Typ.AutoIncr = columns[idx].Typ.AutoIncr || source.autoIncr
			}

		}

		return columns
	}

	switch logicPlan := p.Plan.(type) {
	case *plan.Plan_Query:
		switch logicPlan.Query.StmtType {
		case plan.Query_SELECT:
			return getResultColumnsByProjectionlist(logicPlan.Query)
		case plan.Query_INSERT, plan.Query_UPDATE, plan.Query_DELETE:
			if logicPlan.Query.HasReturning {
				return getResultColumnsByProjectionlist(logicPlan.Query)
			}
			return nil
		default:
			// insert/update/delete statement will return nil
			return nil
		}
	case *plan.Plan_Tcl:
		// begin/commmit/rollback statement will return nil
		return nil
	case *plan.Plan_Ddl:
		switch logicPlan.Ddl.DdlType {
		case plan.DataDefinition_SHOW_VARIABLES:
			typ := makeGeneratedPlan2Type(types.T_varchar, 1024, 0, false)
			return []*ColDef{
				{Typ: typ, Name: "Variable_name"},
				{Typ: typ, Name: "Value"},
			}
		case plan.DataDefinition_CREATE_TABLE:
			return nil
		default:
			// show statement(except show variables) will return a query
			if logicPlan.Ddl.Query != nil {
				return getResultColumnsByProjectionlist(logicPlan.Ddl.Query)
			}
			return nil
		}
	}
	return nil
}

type resultColumnSource struct {
	dbName       string
	tableName    string
	columnName   string
	primary      bool
	unique       bool
	notNull      bool
	autoIncr     bool
	nullExtended bool
}

// findResultColumnSource follows a result projection through transparent plan
// nodes and JOIN remappings until it reaches the table column that produced
// it. Expressions such as arithmetic, aggregation, and DISTINCT intentionally
// do not produce source metadata: marking those outputs as key columns would
// be less compatible than leaving the flags unset.
func findResultColumnSource(query *plan.Query, nodeID int32, expr *plan.Expr) *resultColumnSource {
	if query == nil || expr == nil || expr.GetCol() == nil {
		return nil
	}
	return findResultColumnSourceAtNode(query, nodeID, expr.GetCol(), make(map[int32]bool))
}

func findResultColumnSourceAtNode(
	query *plan.Query,
	nodeID int32,
	ref *plan.ColRef,
	visited map[int32]bool,
) *resultColumnSource {
	if query == nil || ref == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
		return nil
	}
	visited[nodeID] = true
	node := query.Nodes[nodeID]
	if node == nil {
		return nil
	}

	if node.TableDef != nil && isResultColumnSourceNode(node.NodeType) {
		if len(node.ProjectList) > 0 {
			if sourceExpr := resultColumnProjectionAtNode(node, ref); sourceExpr != nil {
				if sourceRef := sourceExpr.GetCol(); sourceRef != nil {
					return resultColumnSourceFromTableDef(node.TableDef, sourceRef.ColPos)
				}
			}
			return nil
		}
		if source := resultColumnSourceFromTableDef(node.TableDef, ref.ColPos); source != nil {
			return source
		}
	}

	if node.NodeType == plan.Node_JOIN {
		return findResultColumnSourceAtJoin(query, node, ref, visited)
	}

	if !isResultColumnTransparentNode(node.NodeType) || len(node.Children) == 0 {
		return nil
	}

	projected := resultColumnProjectionAtNode(node, ref)
	if projected == nil {
		return nil
	}
	projectedRef := projected.GetCol()
	if projectedRef == nil {
		return nil
	}

	var found *resultColumnSource
	for _, childID := range node.Children {
		candidate := findResultColumnSourceAtNode(query, childID, projectedRef, cloneVisitedResultColumnNodes(visited))
		if candidate == nil {
			continue
		}
		if found != nil && *found != *candidate {
			// The reference is ambiguous across children. Do not claim a key
			// flag when the plan no longer identifies one source column.
			return nil
		}
		found = candidate
	}
	return found
}

func findResultColumnSourceAtJoin(
	query *plan.Query,
	node *plan.Node,
	ref *plan.ColRef,
	visited map[int32]bool,
) *resultColumnSource {
	if query == nil || node == nil || ref == nil || len(node.Children) == 0 {
		return nil
	}

	projectedRef := ref
	childIdx := -1
	if projected := resultColumnProjectionAtNode(node, ref); projected != nil {
		if col := projected.GetCol(); col != nil {
			projectedRef = col
			// JOIN ProjectList entries use RelPos 0/1 to identify the
			// corresponding child and ColPos to identify that child's output
			// slot. This is a local remapping, not a source-table position.
			if projectedRef.RelPos >= 0 && int(projectedRef.RelPos) < len(node.Children) {
				childIdx = int(projectedRef.RelPos)
				if childRef := resultColumnJoinChildProjectionRef(query, node, childIdx, projectedRef.ColPos); childRef != nil {
					projectedRef = childRef
				}
			}
		}
	}

	if childIdx < 0 {
		childIdx = resultColumnJoinChildByBinding(query, node, projectedRef)
	}
	if childIdx >= 0 && childIdx < len(node.Children) {
		candidate := findResultColumnSourceAtNode(
			query,
			node.Children[childIdx],
			projectedRef,
			cloneVisitedResultColumnNodes(visited),
		)
		return resultColumnSourceAfterJoin(candidate, node, childIdx)
	}

	// Plans from earlier optimization stages may not yet have the local JOIN
	// projection. Fall back to tracing each child by the source identity, but
	// reject an ambiguous match rather than assigning metadata from one side.
	var found *resultColumnSource
	foundChild := -1
	for childIdx, childID := range node.Children {
		candidate := findResultColumnSourceAtNode(
			query,
			childID,
			projectedRef,
			cloneVisitedResultColumnNodes(visited),
		)
		if candidate == nil {
			continue
		}
		if found != nil {
			return nil
		}
		found = candidate
		foundChild = childIdx
	}
	return resultColumnSourceAfterJoin(found, node, foundChild)
}

func resultColumnJoinChildProjectionRef(
	query *plan.Query,
	node *plan.Node,
	childIdx int,
	colPos int32,
) *plan.ColRef {
	if query == nil || node == nil || childIdx < 0 || childIdx >= len(node.Children) || colPos < 0 {
		return nil
	}
	childID := node.Children[childIdx]
	if childID < 0 || int(childID) >= len(query.Nodes) || query.Nodes[childID] == nil {
		return nil
	}
	child := query.Nodes[childID]
	if int(colPos) >= len(child.ProjectList) {
		return nil
	}
	return child.ProjectList[colPos].GetCol()
}

func resultColumnJoinChildByBinding(query *plan.Query, node *plan.Node, ref *plan.ColRef) int {
	if query == nil || node == nil || ref == nil || len(node.Children) == 0 {
		return -1
	}
	childIdx := -1
	for i, childID := range node.Children {
		if !resultColumnNodeHasBindingTag(query, childID, ref.RelPos, make(map[int32]bool)) {
			continue
		}
		if childIdx >= 0 {
			return -1
		}
		childIdx = i
	}
	return childIdx
}

func resultColumnNodeHasBindingTag(query *plan.Query, nodeID, tag int32, visited map[int32]bool) bool {
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
		return false
	}
	visited[nodeID] = true
	node := query.Nodes[nodeID]
	if node == nil {
		return false
	}
	for _, bindingTag := range node.BindingTags {
		if bindingTag == tag {
			return true
		}
	}
	for _, childID := range node.Children {
		if resultColumnNodeHasBindingTag(query, childID, tag, visited) {
			return true
		}
	}
	return false
}

func resultColumnSourceAfterJoin(source *resultColumnSource, node *plan.Node, childIdx int) *resultColumnSource {
	if source == nil || !nodeNullExtendsChild(node, childIdx) {
		return source
	}
	result := *source
	result.notNull = false
	result.nullExtended = true
	return &result
}

func cloneVisitedResultColumnNodes(visited map[int32]bool) map[int32]bool {
	clone := make(map[int32]bool, len(visited)+1)
	for nodeID, seen := range visited {
		clone[nodeID] = seen
	}
	return clone
}

func resultColumnProjectionAtNode(node *plan.Node, ref *plan.ColRef) *plan.Expr {
	if node == nil || ref == nil {
		return nil
	}
	// ColPos identifies the source column, not the position of the expression
	// in this node's projection.  A projection can reorder columns (for example
	// SELECT unique_value, id), so looking up ProjectList[ref.ColPos] can attach
	// the metadata of a different source column.  Resolve the source identity
	// across the whole projection first.
	for _, expr := range node.ProjectList {
		col := expr.GetCol()
		if col == nil {
			continue
		}
		if resultColumnRefsHaveSamePosition(ref, col) {
			return expr
		}
	}

	// Some plan nodes omit relation/column positions while retaining names.
	// Use names only after the identity lookup, and require the names that are
	// available on both refs to agree so an ambiguous table-only match cannot
	// claim key metadata.
	for _, expr := range node.ProjectList {
		col := expr.GetCol()
		if col == nil {
			continue
		}
		if resultColumnRefsMatchByName(ref, col) {
			return expr
		}
	}
	return nil
}

func resultColumnRefsHaveSamePosition(left, right *plan.ColRef) bool {
	if left == nil || right == nil {
		return false
	}
	return left.RelPos == right.RelPos && left.ColPos == right.ColPos
}

func resultColumnRefsMatchByName(left, right *plan.ColRef) bool {
	if left == nil || right == nil {
		return false
	}
	if left.Name != "" && right.Name != "" {
		if !strings.EqualFold(left.Name, right.Name) {
			return false
		}
		if left.TblName != "" && right.TblName != "" &&
			!strings.EqualFold(left.TblName, right.TblName) {
			return false
		}
		if left.DbName != "" && right.DbName != "" &&
			!strings.EqualFold(left.DbName, right.DbName) {
			return false
		}
		return true
	}
	if left.TblName == "" || right.TblName == "" ||
		!strings.EqualFold(left.TblName, right.TblName) {
		return false
	}
	if left.DbName != "" && right.DbName != "" &&
		!strings.EqualFold(left.DbName, right.DbName) {
		return false
	}
	return left.Name == "" && right.Name == ""
}

func isResultColumnSourceNode(nodeType plan.Node_NodeType) bool {
	switch nodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_VECTOR_INDEX_SCAN:
		return true
	default:
		return false
	}
}

func isResultColumnTransparentNode(nodeType plan.Node_NodeType) bool {
	switch nodeType {
	case plan.Node_PROJECT,
		plan.Node_FILTER,
		plan.Node_SORT,
		plan.Node_SAMPLE,
		plan.Node_MATERIAL,
		plan.Node_PARTITION,
		plan.Node_GATHER:
		return true
	default:
		return false
	}
}

func resultColumnSourceFromTableDef(tableDef *plan.TableDef, colPos int32) *resultColumnSource {
	if tableDef == nil || colPos < 0 || int(colPos) >= len(tableDef.Cols) {
		return nil
	}
	col := tableDef.Cols[colPos]
	if col == nil {
		return nil
	}
	primary := col.Primary
	if !primary && tableDef.Pkey != nil {
		primary = resultColumnNameInList(tableDef.Pkey.Names, col.Name)
	}
	unique := col.Unique && !primary
	if !primary && !unique {
		for _, index := range tableDef.Indexes {
			if index == nil || !index.Unique {
				continue
			}
			if resultColumnNameInList(index.Parts, col.Name) {
				unique = true
				break
			}
		}
	}
	tableName := tableDef.OriginalName
	if tableName == "" {
		tableName = tableDef.Name
	}
	return &resultColumnSource{
		dbName:     tableDef.DbName,
		tableName:  tableName,
		columnName: col.GetOriginCaseName(),
		primary:    primary,
		unique:     unique,
		notNull:    primary || col.NotNull || col.Typ.NotNullable,
		autoIncr:   col.Typ.AutoIncr,
	}
}

func resultColumnNameInList(names []string, name string) bool {
	for _, candidate := range names {
		if strings.EqualFold(candidate, name) {
			return true
		}
	}
	return false
}
