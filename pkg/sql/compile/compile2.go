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
	"errors"
	"math"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func statementHasSQLCalcFoundRows(stmt tree.Statement) bool {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || selectStmt == nil {
		return false
	}
	for selectStmt != nil {
		switch body := selectStmt.Select.(type) {
		case *tree.SelectClause:
			return body.Option&tree.QuerySpecOptionSqlCalcFoundRows != 0
		case *tree.ParenSelect:
			selectStmt = body.Select
		case *tree.UnionClause:
			return selectStatementHasSQLCalcFoundRows(body.Left)
		default:
			return false
		}
	}
	return false
}

func statementHasSQLCalcFoundRowsPagination(stmt tree.Statement) bool {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || selectStmt == nil {
		return false
	}
	hasPagination := false
	for selectStmt != nil {
		hasPagination = hasPagination || selectStmt.Limit != nil
		switch body := selectStmt.Select.(type) {
		case *tree.SelectClause:
			return hasPagination && body.Option&tree.QuerySpecOptionSqlCalcFoundRows != 0
		case *tree.ParenSelect:
			selectStmt = body.Select
		case *tree.UnionClause:
			return hasPagination && selectStatementHasSQLCalcFoundRows(body.Left)
		default:
			return false
		}
	}
	return false
}

func selectStatementHasSQLCalcFoundRows(stmt tree.SelectStatement) bool {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return statementHasSQLCalcFoundRows(stmt)
	case *tree.ParenSelect:
		return statementHasSQLCalcFoundRows(stmt.Select)
	default:
		return false
	}
}

func markInsertTableScansNotLockMeta(query *plan.Query) {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_TABLE_SCAN || node.ObjRef == nil {
			continue
		}

		// INSERT plans can share an ObjectRef between a target-table scan and
		// the write context. NotLockMeta is local to the scan: the write target
		// must still contribute its shared metadata lock so concurrent DDL waits.
		node.ObjRef = plan2.DeepCopyObjectRef(node.ObjRef)
		node.ObjRef.NotLockMeta = true
	}
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
	c.proc.BeginFoundRowsStatement(statementHasSQLCalcFoundRows(c.stmt))
	c.beginSchedulingTraceAttempt()

	// clear the last query context to avoid process reuse.
	c.proc.ResetQueryContext()

	// clear the clone txn operator to avoid reuse.
	c.proc.ResetCloneTxnOperator()

	// Bind a new plan to the transaction snapshot before any pipeline or
	// pre-pipeline lock can advance an RC transaction's mutable snapshot. A
	// normal data retry reuses the same plan and therefore keeps its binding.
	c.bindPlanSnapshotForCompile()
	// Freeze the owner mapping before any Shuffle is constructed. A retry
	// inherits this execution value even if the deployment gate changes.
	c.bindStringShuffleHashAlgorithmForCompile()

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
				markInsertTableScansNotLockMeta(qry.Query)
				c.needLockMeta = true
			default:
				c.needLockMeta = true
			}
		}
	}

	// initialize some attributes for Compile.
	c.fill = resultWriteBack
	c.pn = queryPlan
	c.prepareLoadUniqueIndexPromotion(queryPlan)

	// combine top context with some values and replace.
	topContext := context.WithValue(execTopContext, defines.EngineKey{}, c.e)
	topContext = perfcounter.WithCounterSet(topContext, c.counterSet)
	c.proc.ReplaceTopCtx(topContext)
	// Resolve an ordinary statement's session row cap only after optimization,
	// but before Sirius can export the logical plan. This preserves optimizer
	// estimates while ensuring both native and offloaded execution see the same
	// finite top-level LIMIT.
	c.materializedSQLSelectLimitOwner = nil
	defer func() {
		c.materializedSQLSelectLimitOwner = nil
	}()
	materialization, materializeErr := c.materializeSQLSelectLimit(queryPlan)
	if materializeErr != nil {
		return materializeErr
	}
	if statementHasSQLCalcFoundRows(c.stmt) {
		c.materializedSQLSelectLimitOwner = materialization.root
	}
	if materialization.query != nil {
		defer materialization.restore()
	}
	if offloaded, offloadErr := c.tryCompileSiriusRead(execTopContext, queryPlan); offloadErr != nil {
		return offloadErr
	} else if offloaded {
		return c.proc.GetQueryContextError()
	}

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
	if err = TryMarkQueryRunning(c, txnOperator); err != nil {
		return nil, err
	}
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
	resourceRecorder := newExecutionResourceRecorder(
		execTopContext,
		c.resourceAttemptOwnerEligible,
	)
	defer resourceRecorder.publish()

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
	if c.siriusRead != nil {
		err = c.runSiriusRead(execTopContext)
		return queryResult, err
	}
	attemptStart := time.Now()
	attemptOpen := true
	var attemptPreRunWall time.Duration
	var attemptRemoteWait time.Duration
	attemptScopes := runC.scopes
	attemptAnal := runC.anal
	sinkAttemptOpen := false
	var coordinatorPhaseStart time.Time
	var coordinatorPhaseBase time.Duration
	var allocationAttempt *statementAllocationAttempt
	finishAllocationAttempt := func() error {
		if allocationAttempt == nil {
			return nil
		}
		attempt := allocationAttempt
		allocationAttempt = nil
		if runC != nil && runC.allocationAttempt == attempt {
			runC.allocationAttempt = nil
		}
		_, finishErr := attempt.finish()
		return finishErr
	}
	finishCurrentAttempt := func(retried bool) {
		if !attemptOpen {
			return
		}
		if !coordinatorPhaseStart.IsZero() {
			attemptPreRunWall = coordinatorPhaseBase + time.Since(coordinatorPhaseStart)
		} else if attemptPreRunWall == 0 {
			attemptPreRunWall = time.Since(attemptStart)
		}
		resourceRecorder.finishAttempt(
			uint64(retryTimes), attemptStart, attemptPreRunWall, attemptRemoteWait, stats,
			attemptScopes, attemptAnal, c.addr, retried,
		)
		attemptOpen = false
	}
	abortSinkAttempt := func(cause error) error {
		if !sinkAttemptOpen || c.resultSink == nil {
			return cause
		}
		sinkAttemptOpen = false
		return errors.Join(cause, c.resultSink.AbortAttempt(c.executionGeneration, cause))
	}
	if c.resultSink != nil {
		c.executionGeneration = 0
		runC.executionGeneration = 0
		if err = c.resultSink.BeginAttempt(execTopContext, 0, c.proc); err != nil {
			finishCurrentAttempt(false)
			return nil, err
		}
		sinkAttemptOpen = true
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			var panicErr error = moerr.NewInternalError(execTopContext, "panic while executing DML RETURNING")
			if c.resultSink != nil {
				panicErr = errors.Join(panicErr, c.cancelAndWaitRunningSQL(&attemptRemoteWait))
			}
			panicErr = joinAllocationLifecycleErrors(panicErr, finishAllocationAttempt())
			err = abortSinkAttempt(panicErr)
			finishCurrentAttempt(false)
			panic(recovered)
		}
	}()
	var carriedPreRunWall time.Duration
	resetStatsInfoPreRun(stats, isInExecutor)
	for {
		coordinatorPhaseStart = time.Time{}
		coordinatorPhaseBase = 0
		// Record the time from the beginning of Run to just before runOnce().
		preRunOnceStart := time.Now()
		coordinatorPhaseStart = preRunOnceStart
		coordinatorPhaseBase = carriedPreRunWall
		var preRunWall time.Duration
		// Before compile.runOnce, Reset the 'StatsInfo' execution related resources in context

		// running.
		if runC.remoteFragmentCounts == nil {
			runC.remoteFragmentCounts = collectRemoteFragmentCounts(runC.scopes, runC.addr)
		}
		// A retry is a new physical execution generation. Reusing the previous
		// ID could attach late RPCs from the failed generation to the new
		// generation's shared board and terminal-account group.
		if len(runC.remoteFragmentCounts) > 0 {
			runC.remoteExecutionID = newRemoteExecutionID()
		} else {
			runC.remoteExecutionID = uuid.Nil
		}
		exporter := func(snapshot mpool.AllocationAccountTerminalSnapshot) {
			if resourceRecorder != nil {
				resourceRecorder.recordAllocationAccountTerminal(snapshot)
			}
		}
		err = runC.ensureAllocationAccountLifecycle(exporter)
		if err == nil {
			allocationAttempt, err = runC.beginAllocationAccountAttempt()
		}
		if err == nil {
			err = runC.runPipelineAttempt(func() error {
				preRunWall = carriedPreRunWall + time.Since(preRunOnceStart)
				attemptPreRunWall = preRunWall
				runC.MessageBoard.BeforeRunonce()
				// Calculate time spent between the start and runOnce execution
				if !isInExecutor {
					stats.StoreCompilePreRunOnceDuration(time.Since(preRunOnceStart))
				}
				coordinatorPhaseStart = time.Time{}
				coordinatorPhaseBase = 0

				if runErr := runC.runOnce(); runErr != nil {
					return runErr
				}
				return runC.proc.GetQueryContextError()
			})
			if err == nil {
				if runC.anal != nil {
					runC.anal.retryTimes = retryTimes
				}
				break
			}
		}
		if preRunWall == 0 {
			preRunWall = carriedPreRunWall + time.Since(preRunOnceStart)
		}
		attemptPreRunWall = preRunWall
		coordinatorPhaseStart = time.Time{}
		coordinatorPhaseBase = 0
		if terminalErr := finishAllocationAttempt(); terminalErr != nil {
			err = joinAllocationLifecycleErrors(err, terminalErr)
			err = abortSinkAttempt(err)
			resourceRecorder.finishAttempt(
				uint64(retryTimes), attemptStart, preRunWall, attemptRemoteWait, stats,
				attemptScopes, attemptAnal, c.addr, false,
			)
			attemptOpen = false
			return nil, err
		}

		c.fatalLog(retryTimes, err)
		if !c.canRetry(err) {
			// runOnce may return after a local or coordinator branch fails while a
			// remote branch is still unwinding. Quiesce every producer before the
			// attempt-owned sink releases its file and reservations; a generation
			// check is a safety net for late callbacks, not a substitute for the
			// pipeline ownership barrier.
			if c.resultSink != nil {
				err = errors.Join(err, c.cancelAndWaitRunningSQL(&attemptRemoteWait))
			}
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
			resourceRecorder.finishAttempt(
				uint64(retryTimes), attemptStart, preRunWall, attemptRemoteWait, stats,
				attemptScopes, attemptAnal, c.addr, false,
			)
			attemptOpen = false
			err = abortSinkAttempt(err)
			return nil, err
		}
		defChanged := moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
		forcePreMode := moerr.IsMoErrCode(err, moerr.ErrVectorNeedRetryWithPreMode)
		c.onLoadUniqueIndexPromotionRetry(defChanged || forcePreMode)
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

		retryTransitionStart := time.Now()
		coordinatorPhaseStart = retryTransitionStart
		coordinatorPhaseBase = preRunWall
		transitionErr := c.prepareRetryTransition(&attemptRemoteWait)
		transitionWall := time.Since(retryTransitionStart)
		coordinatorPhaseStart = time.Time{}
		coordinatorPhaseBase = 0
		attemptPreRunWall = preRunWall + transitionWall
		if transitionErr != nil {
			err = abortSinkAttempt(transitionErr)
			resourceRecorder.finishAttempt(
				uint64(retryTimes), attemptStart, attemptPreRunWall, attemptRemoteWait, stats,
				attemptScopes, attemptAnal, c.addr, false,
			)
			attemptOpen = false
			return nil, err
		}
		if c.resultSink != nil {
			if abortErr := c.resultSink.AbortAttempt(c.executionGeneration, err); abortErr != nil {
				err = errors.Join(err, abortErr)
				finishCurrentAttempt(false)
				return nil, err
			}
			sinkAttemptOpen = false
		}
		resourceRecorder.finishAttempt(
			uint64(retryTimes), attemptStart, attemptPreRunWall, attemptRemoteWait, stats,
			attemptScopes, attemptAnal, c.addr, true,
		)
		attemptOpen = false
		if runC != c {
			releaseRetryCompile(runC)
		}
		runC = c

		retryTimes++
		c.retryTimes = retryTimes
		c.executionGeneration = uint64(retryTimes)
		attemptStart = time.Now()
		attemptOpen = true
		attemptPreRunWall = 0
		attemptRemoteWait = 0
		attemptScopes = nil
		attemptAnal = nil
		coordinatorPhaseStart = attemptStart
		coordinatorPhaseBase = 0
		stats.ResetRetryAttemptResource()
		resetStatsInfoPreRun(stats, isInExecutor)

		nextRunC, buildErr := c.buildRetryCompile(defChanged || forcePreMode)
		carriedPreRunWall = time.Since(attemptStart)
		attemptPreRunWall = carriedPreRunWall
		if buildErr != nil {
			err = buildErr
			resourceRecorder.finishAttempt(
				uint64(retryTimes), attemptStart, attemptPreRunWall, attemptRemoteWait, stats,
				attemptScopes, attemptAnal, c.addr, false,
			)
			attemptOpen = false
			return nil, err
		}
		runC = nextRunC
		runC.executionGeneration = c.executionGeneration
		attemptScopes = runC.scopes
		attemptAnal = runC.anal
		if c.resultSink != nil {
			if err = c.resultSink.BeginAttempt(execTopContext, c.executionGeneration, c.proc); err != nil {
				finishCurrentAttempt(false)
				return nil, err
			}
			sinkAttemptOpen = true
		}

		// rebuild context for the retry.
		runC.InitPipelineContextToRetryQuery()
		carriedPreRunWall = time.Since(attemptStart)
		attemptPreRunWall = carriedPreRunWall
		coordinatorPhaseStart = time.Time{}
		coordinatorPhaseBase = 0
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
		if err != nil {
			err = joinAllocationLifecycleErrors(err, finishAllocationAttempt())
			err = abortSinkAttempt(err)
			finishCurrentAttempt(false)
			return nil, err
		}
	}

	// Keep the attempt open through plan analysis. Adjust can fail and analysis
	// can panic, so neither path may inherit a prematurely sealed success
	// outcome. The panic defer above remains the single terminal owner until
	// this call returns.
	c.AnalyzeExecPlan(runC, queryResult, stats, isExplainPhyPlan, option)
	if terminalErr := finishAllocationAttempt(); terminalErr != nil {
		err = joinAllocationLifecycleErrors(err, terminalErr)
		err = abortSinkAttempt(err)
		finishCurrentAttempt(false)
		return nil, err
	}
	if c.resultSink != nil {
		if err = c.resultSink.SealAttempt(c.executionGeneration); err != nil {
			err = abortSinkAttempt(err)
			finishCurrentAttempt(false)
			return nil, err
		}
		sinkAttemptOpen = false
	}

	resourceRecorder.finishAttempt(
		uint64(retryTimes), attemptStart, attemptPreRunWall, attemptRemoteWait, stats,
		attemptScopes, attemptAnal, c.addr, false,
	)
	attemptOpen = false
	resourceRecorder.publish()
	// AnalyzeExecPlan builds the physical plan before execution resources are
	// published. Refresh its live snapshot after publication; the frontend
	// replaces this with the terminal sealed summary before persistence.
	if c.anal != nil {
		c.attachResourceSummary(c.anal.phyPlan)
	}
	if isExplainPhyPlan {
		c.refreshExplainPhyPlanBuffer(runC, queryResult, option)
	}

	return queryResult, err
}

func releaseRetryCompile(c *Compile) {
	proc := c.proc
	prepareParams := proc.DetachPrepareParams()
	defer proc.RestorePrepareParams(prepareParams)
	c.Release()
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
	case *tree.MultiInsert:
		return rewriteAutoModeInSelect(s.Source)
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
	case *tree.MultiInsert:
		sel = s.Source
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

// prepareRetryTransition quiesces the failed generation and advances its
// transaction workspace. It is charged to the attempt that requested retry.
func (c *Compile) prepareRetryTransition(remoteWait *time.Duration) error {
	v2.TxnStatementRetryCounter.Inc()
	c.proc.GetTxnOperator().GetWorkspace().IncrSQLCount()
	if err := c.cancelAndWaitRunningSQL(remoteWait); err != nil {
		return err
	}

	topContext := c.proc.GetTopContext()
	// clear the workspace of the failed statement
	if e := c.proc.GetTxnOperator().GetWorkspace().RollbackLastStatement(topContext); e != nil {
		return e
	}

	// increase the statement id
	if e := c.proc.GetTxnOperator().GetWorkspace().IncrStatementID(topContext, false); e != nil {
		return e
	}
	// A retry is a new statement execution generation. Do not let a generated
	// value from the rolled-back attempt win the new attempt's result, and
	// restore the session-visible LAST_INSERT_ID baseline until the retry
	// generates a replacement value.
	c.proc.SetStatementLastInsertID(0)
	c.proc.SetLastInsertID(c.proc.GetSessionInfo().LastInsertID)

	// clear PostDmlSqlList
	c.proc.GetPostDmlSqlList().Clear()
	// clear stage cache
	c.proc.GetStageCache().Clear()
	return nil
}

func (c *Compile) cancelAndWaitRunningSQL(remoteWait *time.Duration) error {
	topContext := c.proc.GetTopContext()
	if txnOp := c.proc.GetTxnOperator(); txnOp != nil {
		if coordinator, ok := txnOp.(runSQLCoordinatorWithSQL); ok {
			sqlText := c.originSQL
			if sqlText == "" {
				sqlText = c.sql
			}
			if err := measureRetryRemoteWait(remoteWait, func() error {
				return coordinator.CancelAndWaitRunningSQLWithSQL(topContext, c.runSqlToken, sqlText)
			}); err != nil {
				return err
			}
		} else if coordinator, ok := txnOp.(runSQLCoordinator); ok {
			if err := measureRetryRemoteWait(remoteWait, func() error {
				return coordinator.CancelAndWaitRunningSQL(topContext, c.runSqlToken)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func measureRetryRemoteWait(total *time.Duration, wait func() error) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if total == nil || elapsed <= 0 {
			return
		}
		if *total > time.Duration(math.MaxInt64)-elapsed {
			*total = time.Duration(math.MaxInt64)
			return
		}
		*total += elapsed
	}()
	return wait()
}

// buildRetryCompile starts the next generation. A build or compile failure is
// therefore a terminal outcome of that new attempt instead of disappearing
// into the previous attempt's closing phase.
func (c *Compile) buildRetryCompile(rebuildPlan bool) (*Compile, error) {
	topContext := c.proc.GetTopContext()
	// Invalidate a completed proof before a different logical generation can be
	// built or observed.
	c.onLoadUniqueIndexPromotionRetry(rebuildPlan)

	// FIXME: the current retry method is quite bad, the overhead is relatively large, and needs to be
	// improved to refresh expression in the future.

	var e error
	runC := NewCompile(c.addr, c.db, c.sql, c.tenant, c.uid, c.e, c.proc, c.stmt, c.isInternal, c.cnLabel, c.startAt)
	runC.inheritLoadUniqueIndexPromotion(c)
	c.bindRetryPlanGeneration(runC, rebuildPlan)
	c.bindLoadUniqueIndexPromotionSnapshot(runC, rebuildPlan)
	runC.resultSink = c.resultSink
	runC.executionGeneration = c.executionGeneration
	c.copyAllocationAccountLifecycleTo(runC)
	runC.SetQuerySchedulingIntent(c.querySchedulingIntent)
	runC.SetSchedulingTraceRecorder(c.schedulingTrace)
	runC.SetOriginSQL(c.originSQL)
	defer func() {
		if recovered := recover(); recovered != nil {
			runC.Release()
			panic(recovered)
		}
		if e != nil {
			runC.Release()
		}
	}()
	planForRetry := c.pn
	if rebuildPlan {
		planForRetry, e = c.buildPlanFunc(topContext)
		if e != nil {
			return nil, e
		}
		if e = c.validateRetryResultMetadata(topContext, planForRetry); e != nil {
			return nil, e
		}
	}
	if e = runC.Compile(topContext, planForRetry, c.fill); e != nil {
		return nil, e
	}
	if rebuildPlan {
		// Publish the rebuilt logical plan and its immutable binding together,
		// after physical compilation succeeds. A subsequent ordinary retry must
		// inherit this generation rather than the one that first hit the fence.
		c.pn = planForRetry
		c.inheritPlanSnapshot(runC)
		// Update c.anal.qry to point to the new plan's Query. This ensures
		// fillPlanNodeAnalyzeInfo uses the correct nodes.
		if qry, ok := planForRetry.Plan.(*plan.Plan_Query); ok && c.anal != nil {
			c.anal.qry = qry.Query
		}
	}
	return runC, nil
}

func (c *Compile) validateRetryResultMetadata(
	ctx context.Context,
	rebuilt *plan.Plan,
) error {
	if selectStmt, ok := c.stmt.(*tree.Select); ok && len(selectStmt.IntoVars) > 0 &&
		len(plan2.GetResultColumnsFromPlan(rebuilt)) != len(selectStmt.IntoVars) {
		// SELECT INTO validates arity before the first attempt. Repeat that
		// validation at the definition-retry boundary because an empty rebuilt
		// result never reaches the row callback that also checks arity.
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	if !c.resultMetadataFrozen || sameResultMetadata(c.pn, rebuilt) {
		return nil
	}
	// Returning the definition-change error from buildRetryCompile is terminal
	// for this Run invocation (it is not fed back through canRetry). This avoids
	// executing rows with a schema different from metadata already sent while
	// preserving the client-visible request to retry/reprepare.
	return moerr.NewTxnNeedRetryWithDefChanged(ctx)
}

func sameResultMetadata(left, right *plan.Plan) bool {
	leftColumns := plan2.GetResultColumnsFromPlan(left)
	rightColumns := plan2.GetResultColumnsFromPlan(right)
	if len(leftColumns) != len(rightColumns) {
		return false
	}
	for i := range leftColumns {
		if !proto.Equal(leftColumns[i], rightColumns[i]) {
			return false
		}
	}
	return true
}

func (c *Compile) bindRetryPlanGeneration(runC *Compile, rebuildPlan bool) {
	runC.inheritStringShuffleHashAlgorithm(c)
	if !rebuildPlan {
		runC.inheritPlanSnapshot(c)
		return
	}
	// The retry's rebuilt plan and physical topology form a new generation.
	// Frontend prepared state from the old generation is now ineligible even if
	// rebuilding or running the retry later fails. This applies equally to a
	// cached prepared Compile and to an uncached prepared logical plan.
	c.planGenerationRebuilt = true
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
	switch c.pn.Plan.(type) {
	case *plan.Plan_Query:
		c.handleQueryPlanAnalyze(runC, stats)
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

func (c *Compile) handleQueryPlanAnalyze(runC *Compile, stats *statistic.StatsInfo) {
	if c.anal.phyPlan == nil || !c.UpdatePreparePhyPlan(runC) {
		c.GenPhyPlan(runC)
	}

	c.fillPlanNodeAnalyzeInfo(stats)

}

func (c *Compile) refreshExplainPhyPlanBuffer(runC *Compile, queryResult *util2.RunResult, option *ExplainOption) {
	statsInfo := statistic.StatsInfoFromContext(c.proc.GetTopContext())
	scopes := c.scopes
	if runC != nil && runC != c && len(runC.scopes) > 0 {
		scopes = runC.scopes
	}
	scopeInfo := makeExplainPhyPlanBuffer(scopes, queryResult, statsInfo, c.anal, option)
	c.anal.explainPhyBuffer = scopeInfo
	if runC != nil && runC.anal != nil && runC != c {
		runC.anal.explainPhyBuffer = scopeInfo
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
