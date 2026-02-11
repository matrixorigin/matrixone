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

package compile

import (
	"context"
	"encoding/hex"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

type runSQLCoordinator interface {
	CancelAndWaitRunningSQL(ctx context.Context, keepToken uint64) error
}

type runSQLCoordinatorWithSQL interface {
	CancelAndWaitRunningSQLWithSQL(ctx context.Context, keepToken uint64, currentSQL string) error
}

// I create this file to store the two most important entry functions for the Compile struct and their helper functions.
// These functions are used to build the pipeline from the query plan and execute the pipeline respectively.
//
// The reason I put these two functions into separate files is that the original file contained too much code about
// how to create a pipeline and how to determine certain flags from the Compile struct.
// Such a huge file is hard to read and understand for developers who are not familiar with the codebase.

// Compile generates the node level execution pipeline from the query plan,
// and the final pipeline will be stored in the attribute `scope` of a Compile object.
func (c *Compile) Compile(
	execTopContext context.Context,
	queryPlan *plan.Plan,
	resultWriteBack func(batch *batch.Batch, crs *perfcounter.CounterSet) error) (err error) {
	// clear the last query context to avoid process reuse.
	c.proc.ResetQueryContext()

	// clear the clone txn operator to avoid reuse.
	c.proc.ResetCloneTxnOperator()

	// statistical information record and trace.
	compileStart := time.Now()
	_, task := gotrace.NewTask(context.TODO(), "pipeline.Compile")
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(execTopContext, e)
			c.proc.Error(execTopContext, "panic in compile",
				zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
				zap.String("error", err.Error()))
		}
		task.End()
		v2.TxnStatementCompileDurationHistogram.Observe(time.Since(compileStart).Seconds())
	}()

	// trace for pessimistic txn and check if it needs to lock meta table.
	if txnOperator := c.proc.GetTxnOperator(); txnOperator != nil && txnOperator.Txn().IsPessimistic() {
		seq := txnOperator.NextSequence()
		txnTrace.GetService(c.proc.GetService()).AddTxnDurationAction(
			txnOperator,
			client.CompileEvent,
			seq,
			0,
			0,
			err)
		defer func() {
			txnTrace.GetService(c.proc.GetService()).AddTxnDurationAction(
				txnOperator,
				client.CompileEvent,
				seq,
				0,
				time.Since(compileStart),
				err)
		}()

		// check if it needs to lock meta table.
		if qry, ok := queryPlan.Plan.(*plan.Plan_Query); ok {
			switch qry.Query.StmtType {
			case plan.Query_SELECT:
				for _, n := range qry.Query.Nodes {
					if n.NodeType == plan.Node_LOCK_OP {
						c.needLockMeta = true
						break
					}
				}
			case plan.Query_INSERT:
				for _, n := range qry.Query.Nodes {
					if n.NodeType == plan.Node_TABLE_SCAN {
						n.ObjRef.NotLockMeta = true
					}
				}
				c.needLockMeta = true
			default:
				c.needLockMeta = true
			}
		}
	}

	// initialize some attributes for Compile.
	c.fill = resultWriteBack
	c.pn = queryPlan

	// combine top context with some values and replace.
	topContext := context.WithValue(execTopContext, defines.EngineKey{}, c.e)
	topContext = perfcounter.WithCounterSet(topContext, c.counterSet)
	c.proc.ReplaceTopCtx(topContext)

	// from plan to scope.
	if c.scopes, err = c.compileScope(queryPlan); err != nil {
		return err
	}
	// todo: this is redundant.
	for _, s := range c.scopes {
		if len(s.NodeInfo.Addr) == 0 {
			s.NodeInfo.Addr = c.addr
		}
	}

	return c.proc.GetQueryContextError()
}

// Run executes the pipeline and returns the result.
func (c *Compile) Run(_ uint64) (queryResult *util2.RunResult, err error) {
	var txnOperator = c.proc.GetTxnOperator()

	// init context for pipeline.
	c.proc.ResetQueryContext()
	c.InitPipelineContextToExecuteQuery()

	// record this query to compile service.
	MarkQueryRunning(c, txnOperator)
	defer func() {
		MarkQueryDone(c, txnOperator)
	}()

	// the runC is the final object for executing the query, it's not always the same as c because of retry.
	var runC = c

	var executeSQL = c.originSQL
	if len(executeSQL) == 0 {
		executeSQL = c.sql
	}

	// track the entire execution lifecycle and release memory after it ends.
	var sequence = uint64(0)
	var writeOffset = uint64(0)
	if txnOperator != nil {
		sequence = txnOperator.NextSequence()
		writeOffset = uint64(txnOperator.GetWorkspace().GetSnapshotWriteOffset())
		txnOperator.GetWorkspace().IncrSQLCount()
	}

	var isExplainPhyPlan = false
	var option *ExplainOption
	if explainStmt, ok := c.stmt.(*tree.ExplainPhyPlan); ok {
		isExplainPhyPlan = true
		option = getExplainOption(explainStmt.Options)
	}

	defer func() {
		// if a rerun occurs, it differs from the original c, so we need to release it.
		if runC != c {
			runC.Release()
		}
	}()

	// update the top context with some trace information and values.
	execTopContext, span := trace.Start(c.proc.GetTopContext(), "Compile.Run", trace.WithKind(trace.SpanKindStatement))

	// statistical information record and trace.
	runStart := time.Now()
	v2.TxnStatementExecuteLatencyDurationHistogram.Observe(runStart.Sub(c.startAt).Seconds())
	sp := c.proc.GetStmtProfile()
	_, task := gotrace.NewTask(context.TODO(), "pipeline.Run")

	stats := statistic.StatsInfoFromContext(execTopContext)
	isInExecutor := perfcounter.IsInternalExecutor(execTopContext)
	if !isInExecutor {
		stats.ExecutionStart()
	}

	c.counterSet.Reset()
	execTopContext = perfcounter.AttachExecPipelineKey(execTopContext, c.counterSet)
	c.proc.ReplaceTopCtx(execTopContext)
	txnTrace.GetService(c.proc.GetService()).TxnStatementStart(txnOperator, executeSQL, sequence)
	defer func() {
		task.End()
		span.End(trace.WithStatementExtra(sp.GetTxnId(), sp.GetStmtId(), sp.GetSqlOfStmt()))
		if !isInExecutor {
			if err != nil {
				resetStatsInfoPreRun(stats, isInExecutor)
			}
			stats.ExecutionEnd()
		}

		timeCost := time.Since(runStart)
		v2.TxnStatementExecuteDurationHistogram.Observe(timeCost.Seconds())

		affectRows := 0
		if queryResult != nil {
			affectRows = int(queryResult.AffectRows)
		}
		txnTrace.GetService(c.proc.GetService()).TxnStatementCompleted(
			txnOperator, executeSQL, timeCost, sequence, affectRows, err)

		if _, ok := c.pn.Plan.(*plan.Plan_Ddl); ok {
			c.setHaveDDL(true)
		}
	}()

	// running and retry.
	var retryTimes = 0
	queryResult = &util2.RunResult{}
	v2.TxnStatementTotalCounter.Inc()
	for {
		// Record the time from the beginning of Run to just before runOnce().
		preRunOnceStart := time.Now()
		// Before compile.runOnce, Reset the 'StatsInfo' execution related resources in context
		resetStatsInfoPreRun(stats, isInExecutor)

		// running.
		if err = runC.prePipelineInitializer(); err == nil {
			runC.MessageBoard.BeforeRunonce()
			// Calculate time spent between the start and runOnce execution
			if !isInExecutor {
				stats.StoreCompilePreRunOnceDuration(time.Since(preRunOnceStart))
			}

			if err = runC.runOnce(); err == nil {
				if runC.anal != nil {
					runC.anal.retryTimes = retryTimes
				}
				break
			}
		}

		c.fatalLog(retryTimes, err)
		if !c.canRetry(err) {
			if c.proc.GetTxnOperator().Txn().IsRCIsolation() &&
				moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				orphan, e := c.proc.Base.LockService.IsOrphanTxn(
					execTopContext,
					txnOperator.Txn().ID,
				)
				if e != nil {
					getLogger(c.proc.GetService()).Error("failed to convert dup to orphan txn error",
						zap.String("txn", hex.EncodeToString(txnOperator.Txn().ID)),
						zap.Error(err),
					)
				}
				if e == nil && orphan {
					getLogger(c.proc.GetService()).Warn("convert dup to orphan txn error",
						zap.String("txn", hex.EncodeToString(txnOperator.Txn().ID)),
					)
					err = moerr.NewCannotCommitOrphan(execTopContext)
				}
			}
			return nil, err
		}

		retryTimes++
		if runC != c {
			runC.Release()
		}
		c.retryTimes = retryTimes
		defChanged := moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
		forcePreMode := moerr.IsMoErrCode(err, moerr.ErrVectorNeedRetryWithPreMode)
		if forcePreMode {
			// NOTE: This in-place modification of the AST will persist if the statement
			// is part of a prepared statement. This is generally desirable as it
			// avoids re-triggering adaptive mode logic on subsequent executions.
			updated := rewriteAutoModeToPre(c.stmt)
			if !updated {
				// If no explicit 'auto' was rewritten, but we got a retry request,
				// it means it was implicit auto mode (from session variable).
				// We force the AST to 'pre' mode to rebuild the plan correctly.
				if !forceModePre(c.stmt) {
					logutil.Warnf("Failed to force 'pre' mode on AST during retry: SQL=%s", c.sql)
				}
			}
			// Force rebuild of physical plan for explain analyze after rewrite.
			if c.anal != nil {
				c.anal.phyPlan = nil
				c.anal.remotePhyPlans = nil
				c.anal.explainPhyBuffer = nil
			}
		}

		if runC, err = c.prepareRetry(defChanged || forcePreMode); err != nil {
			return nil, err
		}

		// rebuild context for the retry.
		runC.InitPipelineContextToRetryQuery()
	}

	if err = runC.proc.GetQueryContextError(); err != nil {
		return nil, err
	}
	queryResult.AffectRows = runC.getAffectedRows()
	if c.uid != "mo_logger" &&
		strings.Contains(strings.ToLower(c.sql), "insert") &&
		(strings.Contains(c.sql, "{MO_TS =") ||
			strings.Contains(c.sql, "{SNAPSHOT =")) {
		getLogger(c.proc.GetService()).Info(
			"insert into with snapshot",
			zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
			zap.Uint64("affectRows", queryResult.AffectRows),
		)
	}
	if txnOperator != nil {
		err = txnOperator.GetWorkspace().Adjust(writeOffset)
	}

	//if !isInExecutor {
	c.AnalyzeExecPlan(runC, queryResult, stats, isExplainPhyPlan, option)
	//}

	return queryResult, err
}

// rewriteAutoModeToPre recursively traverses the AST and rewrites 'mode=auto' to 'mode=pre'
// in the RankOption of vector search queries.
// NOTE: RankOption is configured at the top-level SQL, so deep traversal here is defensive.
//
// This function is called when the adaptive mode (auto) determines that post-filter mode
// returns empty results and a retry with pre-filter mode is needed.
//
// The rewrite is performed in-place on the AST, so the same statement can be re-compiled
// with the updated mode setting.
//
// Parameters:
//   - stmt: The SQL statement AST to rewrite
//
// Returns:
//   - true if any 'mode=auto' was found and rewritten to 'mode=pre'
//   - false if no rewrite was performed (no explicit 'auto' mode found)
func rewriteAutoModeToPre(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.Select:
		return rewriteAutoModeInSelect(s)
	case *tree.ExplainStmt:
		return rewriteAutoModeToPre(s.Statement)
	case *tree.ExplainAnalyze:
		return rewriteAutoModeToPre(s.Statement)
	case *tree.ExplainPhyPlan:
		return rewriteAutoModeToPre(s.Statement)
	case *tree.ExplainFor:
		return rewriteAutoModeToPre(s.Statement)
	case *tree.Insert:
		return rewriteAutoModeInSelect(s.Rows)
	case *tree.Replace:
		return rewriteAutoModeInSelect(s.Rows)
	default:
		return false
	}
}

// forceModePre forces the AST to use 'pre' mode even when no explicit mode was specified.
//
// This function is called when auto mode was enabled via session variable (implicit)
// rather than explicit SQL option, and we need to rebuild the plan with pre-filter mode.
// Unlike rewriteAutoModeToPre, this function will set 'mode=pre' regardless of the
// current mode value, creating the RankOption structure if it doesn't exist.
//
// Parameters:
//   - stmt: The SQL statement AST to modify
//
// Returns:
//   - true if the mode was successfully set to 'pre'
//   - false if the statement type doesn't support RankOption
func forceModePre(stmt tree.Statement) bool {
	var sel *tree.Select
	switch s := stmt.(type) {
	case *tree.Select:
		sel = s
	case *tree.ExplainStmt:
		return forceModePre(s.Statement)
	case *tree.ExplainAnalyze:
		return forceModePre(s.Statement)
	case *tree.ExplainPhyPlan:
		return forceModePre(s.Statement)
	case *tree.ExplainFor:
		return forceModePre(s.Statement)
	case *tree.Insert:
		sel = s.Rows
	case *tree.Replace:
		sel = s.Rows
	default:
		return false
	}

	if sel == nil {
		return false
	}
	if sel.RankOption == nil {
		sel.RankOption = &tree.RankOption{
			Option: map[string]string{"mode": "pre"},
		}
	} else {
		if sel.RankOption.Option == nil {
			sel.RankOption.Option = map[string]string{"mode": "pre"}
		} else {
			sel.RankOption.Option["mode"] = "pre"
		}
	}
	return true
}

// rewriteAutoModeInSelect rewrites 'mode=auto' to 'mode=pre' in a Select statement.
// It checks both the top-level RankOption and recursively processes nested subqueries.
func rewriteAutoModeInSelect(sel *tree.Select) bool {
	if sel == nil {
		return false
	}
	updated := false
	// Check and rewrite the RankOption at the current Select level
	if sel.RankOption != nil && sel.RankOption.Option != nil {
		if mode, ok := sel.RankOption.Option["mode"]; ok && strings.EqualFold(mode, "auto") {
			sel.RankOption.Option["mode"] = "pre"
			updated = true
		}
	}
	// Recursively process nested select statements (subqueries)
	if sel.Select != nil {
		if rewriteAutoModeInSelectStatement(sel.Select) {
			updated = true
		}
	}
	return updated
}

// rewriteAutoModeInSelectStatement recursively processes different types of SelectStatement nodes.
// This handles Select, ParenSelect (parenthesized selects), UnionClause, and SelectClause.
func rewriteAutoModeInSelectStatement(stmt tree.SelectStatement) bool {
	switch s := stmt.(type) {
	case *tree.Select:
		return rewriteAutoModeInSelect(s)
	case *tree.ParenSelect:
		return rewriteAutoModeInSelect(s.Select)
	case *tree.UnionClause:
		// Process both sides of UNION/INTERSECT/EXCEPT
		updated := rewriteAutoModeInSelectStatement(s.Left)
		if rewriteAutoModeInSelectStatement(s.Right) {
			updated = true
		}
		return updated
	case *tree.SelectClause:
		return rewriteAutoModeInSelectClause(s)
	default:
		return false
	}
}

// rewriteAutoModeInSelectClause processes a SelectClause by checking its FROM clause
// for subqueries that may contain vector search with auto mode.
func rewriteAutoModeInSelectClause(clause *tree.SelectClause) bool {
	if clause == nil || clause.From == nil {
		return false
	}
	updated := false
	for _, tbl := range clause.From.Tables {
		if rewriteAutoModeInTableExpr(tbl) {
			updated = true
		}
	}
	return updated
}

// rewriteAutoModeInTableExpr recursively processes table expressions to find subqueries.
// This handles Subquery, JoinTableExpr, ApplyTableExpr, ParenTableExpr, AliasedTableExpr,
// and StatementSource (for derived tables).
func rewriteAutoModeInTableExpr(expr tree.TableExpr) bool {
	switch t := expr.(type) {
	case *tree.Subquery:
		return rewriteAutoModeInSelectStatement(t.Select)
	case *tree.JoinTableExpr:
		updated := false
		if t.Left != nil {
			updated = rewriteAutoModeInTableExpr(t.Left)
		}
		if t.Right != nil {
			updated = rewriteAutoModeInTableExpr(t.Right) || updated
		}
		return updated
	case *tree.ApplyTableExpr:
		updated := false
		if t.Left != nil {
			updated = rewriteAutoModeInTableExpr(t.Left)
		}
		if t.Right != nil {
			updated = rewriteAutoModeInTableExpr(t.Right) || updated
		}
		return updated
	case *tree.ParenTableExpr:
		return rewriteAutoModeInTableExpr(t.Expr)
	case *tree.AliasedTableExpr:
		return rewriteAutoModeInTableExpr(t.Expr)
	case *tree.StatementSource:
		return rewriteAutoModeToPre(t.Statement)
	default:
		return false
	}
}

// prepareRetry rebuild a new Compile object for retrying the query.
func (c *Compile) prepareRetry(defChanged bool) (*Compile, error) {
	v2.TxnStatementRetryCounter.Inc()
	c.proc.GetTxnOperator().GetWorkspace().IncrSQLCount()

	topContext := c.proc.GetTopContext()
	if txnOp := c.proc.GetTxnOperator(); txnOp != nil {
		if coordinator, ok := txnOp.(runSQLCoordinatorWithSQL); ok {
			sqlText := c.originSQL
			if sqlText == "" {
				sqlText = c.sql
			}
			if err := coordinator.CancelAndWaitRunningSQLWithSQL(topContext, c.runSqlToken, sqlText); err != nil {
				return nil, err
			}
		} else if coordinator, ok := txnOp.(runSQLCoordinator); ok {
			if err := coordinator.CancelAndWaitRunningSQL(topContext, c.runSqlToken); err != nil {
				return nil, err
			}
		}
	}

	// clear the workspace of the failed statement
	if e := c.proc.GetTxnOperator().GetWorkspace().RollbackLastStatement(topContext); e != nil {
		return nil, e
	}

	// increase the statement id
	if e := c.proc.GetTxnOperator().GetWorkspace().IncrStatementID(topContext, false); e != nil {
		return nil, e
	}

	// clear PostDmlSqlList
	c.proc.GetPostDmlSqlList().Clear()
	// clear stage cache
	c.proc.GetStageCache().Clear()

	// FIXME: the current retry method is quite bad, the overhead is relatively large, and needs to be
	// improved to refresh expression in the future.

	var e error
	runC := NewCompile(c.addr, c.db, c.sql, c.tenant, c.uid, c.e, c.proc, c.stmt, c.isInternal, c.cnLabel, c.startAt)
	runC.SetOriginSQL(c.originSQL)
	defer func() {
		if e != nil {
			runC.Release()
		}
	}()
	if defChanged {
		var pn *plan2.Plan
		pn, e = c.buildPlanFunc(topContext)
		if e != nil {
			return nil, e
		}
		c.pn = pn
		// Update c.anal.qry to point to the new plan's Query
		// This ensures fillPlanNodeAnalyzeInfo uses the correct nodes
		if qry, ok := pn.Plan.(*plan.Plan_Query); ok && c.anal != nil {
			c.anal.qry = qry.Query
		}
	}
	if e = runC.Compile(topContext, c.pn, c.fill); e != nil {
		return nil, e
	}
	return runC, nil
}

// InitPipelineContextToExecuteQuery initializes the context for each pipeline tree.
//
// the entire process must follow these rules:
// 1. the query context can control the context of all pipelines.
// 2. if there's a data transfer between two pipelines, the lifecycle of the sender's context ends with the receiver's termination.
func (c *Compile) InitPipelineContextToExecuteQuery() {
	contextBase := c.proc.Base.GetContextBase()
	contextBase.BuildQueryCtx(c.proc.GetTopContext())
	contextBase.SaveToQueryContext(defines.EngineKey{}, c.e)
	queryContext := contextBase.WithCounterSetToQueryContext(c.counterSet)

	// build pipeline context.
	currentContext := c.proc.BuildPipelineContext(queryContext)
	for _, pipeline := range c.scopes {
		if pipeline.Proc == nil {
			continue
		}
		pipeline.buildContextFromParentCtx(currentContext)
	}
}

// InitPipelineContextToRetryQuery initializes the context for each pipeline tree.
// the only place diff to InitPipelineContextToExecuteQuery is this function build query context from the last query.
func (c *Compile) InitPipelineContextToRetryQuery() {
	lastQueryCtx, _ := process.GetQueryCtxFromProc(c.proc)
	contextBase := c.proc.Base.GetContextBase()
	contextBase.BuildQueryCtx(lastQueryCtx)
	contextBase.SaveToQueryContext(defines.EngineKey{}, c.e)
	queryContext := contextBase.WithCounterSetToQueryContext(c.counterSet)

	// build pipeline context.
	currentContext := c.proc.BuildPipelineContext(queryContext)
	for _, pipeline := range c.scopes {
		if pipeline.Proc == nil {
			continue
		}
		pipeline.buildContextFromParentCtx(currentContext)
	}
}

// CleanPipelineChannelToNextQuery cleans the channel between each pipeline tree for recall / rerun.
// todo: this has not implement now.
//func (c *Compile) CleanPipelineChannelToNextQuery() {
//	// do nothing now.
//}

// buildContextFromParentCtx build the context for the pipeline tree.
// the input parameter is the whole tree's parent context.
func (s *Scope) buildContextFromParentCtx(parentCtx context.Context) {
	receiverCtx := s.Proc.BuildPipelineContext(parentCtx)

	// build context for receiver.
	for _, prePipeline := range s.PreScopes {
		prePipeline.buildContextFromParentCtx(receiverCtx)
	}
}

// setContextForParallelScope set the context for the parallel scope.
// the difference between this function and the buildContextFromParentCtx is we won't rebuild the context for top scope.
//
// parallel scope is a special scope generated by the scope.ParallelRun.
func setContextForParallelScope(parallelScope *Scope, originalContext context.Context, originalCancel context.CancelCauseFunc) {
	process.ReplacePipelineCtx(parallelScope.Proc, originalContext, originalCancel)

	// build context for data entry.
	for _, prePipeline := range parallelScope.PreScopes {
		prePipeline.buildContextFromParentCtx(parallelScope.Proc.Ctx)
	}
}

func (c *Compile) AnalyzeExecPlan(runC *Compile, queryResult *util2.RunResult, stats *statistic.StatsInfo, isExplainPhy bool, option *ExplainOption) {
	switch planType := c.pn.Plan.(type) {
	case *plan.Plan_Query:
		if planType.Query.StmtType != plan.Query_REPLACE {
			c.handleQueryPlanAnalyze(runC, queryResult, stats, isExplainPhy, option)
		}
	case *plan.Plan_Ddl:
		handleDdlPlanAnalyze(runC, stats)
	}
}

func handleDdlPlanAnalyze(runC *Compile, stats *statistic.StatsInfo) {
	if len(runC.scopes) > 0 {
		for i := range runC.scopes {
			if runC.scopes[i].ScopeAnalyzer != nil {
				stats.AddScopePrepareDuration(runC.scopes[i].ScopeAnalyzer.TimeConsumed)
			}
		}
	}
}

func (c *Compile) handleQueryPlanAnalyze(runC *Compile, queryResult *util2.RunResult, stats *statistic.StatsInfo, isExplainPhy bool, option *ExplainOption) {
	if c.anal.phyPlan == nil || !c.UpdatePreparePhyPlan(runC) {
		c.GenPhyPlan(runC)
	}

	c.fillPlanNodeAnalyzeInfo(stats)

	if isExplainPhy {
		topContext := c.proc.GetTopContext()

		statsInfo := statistic.StatsInfoFromContext(topContext)
		// Use the final (retry) scopes for explain analyze output.
		scopes := c.scopes
		if runC != nil && runC != c && len(runC.scopes) > 0 {
			scopes = runC.scopes
		}
		scopeInfo := makeExplainPhyPlanBuffer(scopes, queryResult, statsInfo, c.anal, option)

		// Ensure explain analyze always exposes the final (retry) plan buffer.
		c.anal.explainPhyBuffer = scopeInfo
		if runC.anal != nil && runC != c {
			runC.anal.explainPhyBuffer = scopeInfo
		}
	}
}

// Reset the 'StatsInfo' execution related resources in the SQL context before compiling. runOnce
func resetStatsInfoPreRun(stats *statistic.StatsInfo, isInExecutor bool) {
	if !isInExecutor {
		stats.ResetIOAccessTimeConsumption()
		stats.ResetIOMergerTimeConsumption()
		stats.ResetBuildReaderTimeConsumption()
		stats.ResetCompilePreRunOnceDuration()
		stats.ResetCompilePreRunOnceWaitLock()
		stats.ResetScopePrepareDuration()
	}
}
