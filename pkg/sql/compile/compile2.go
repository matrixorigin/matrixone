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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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
	resultWriteBack func(batch *batch.Batch) error) (err error) {
	// clear the last query context to avoid process reuse.
	c.proc.ResetQueryContext()

	// statistical information record and trace.
	compileStart := time.Now()
	_, task := gotrace.NewTask(context.TODO(), "pipeline.Compile")
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(execTopContext, e)
			c.proc.Error(execTopContext, "panic in compile",
				zap.String("sql", c.sql),
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

			default:
				c.needLockMeta = true
			}
		}
	}

	// initialize some attributes for Compile.
	c.fill = resultWriteBack
	c.pn = queryPlan

	// replace the original top context with the input one to avoid any value modification.
	c.proc.ReplaceTopCtx(execTopContext)

	// with values.
	topContext := c.proc.SaveToTopContext(defines.EngineKey{}, c.e)
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
	// clear the last query context to avoid process reuse.
	c.proc.ResetQueryContext()

	// the runC is the final object for executing the query, it's not always the same as c because of retry.
	var runC = c

	var executeSQL = c.originSQL
	if len(executeSQL) == 0 {
		executeSQL = c.sql
	}

	// track the entire execution lifecycle and release memory after it ends.
	var txnOperator = c.proc.GetTxnOperator()
	var seq = uint64(0)
	var writeOffset = uint64(0)
	if txnOperator != nil {
		seq = txnOperator.NextSequence()
		writeOffset = uint64(txnOperator.GetWorkspace().GetSnapshotWriteOffset())
		txnOperator.GetWorkspace().IncrSQLCount()
		txnOperator.EnterRunSql()
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
		if txnOperator != nil {
			txnOperator.ExitRunSql()
		}
		c.proc.CleanValueScanBatchs()
		c.proc.SetPrepareBatch(nil)
		c.proc.SetPrepareExprList(nil)
	}()

	// update the top context with some trace information and values.
	execTopContext, span := trace.Start(c.proc.GetTopContext(), "Compile.Run", trace.WithKind(trace.SpanKindStatement))
	c.proc.ReplaceTopCtx(execTopContext)

	// statistical information record and trace.
	runStart := time.Now()
	v2.TxnStatementExecuteLatencyDurationHistogram.Observe(runStart.Sub(c.startAt).Seconds())
	sp := c.proc.GetStmtProfile()
	_, task := gotrace.NewTask(context.TODO(), "pipeline.Run")

	stats := statistic.StatsInfoFromContext(execTopContext)
	stats.ExecutionStart()
	txnTrace.GetService(c.proc.GetService()).TxnStatementStart(txnOperator, executeSQL, seq)
	defer func() {
		task.End()
		span.End(trace.WithStatementExtra(sp.GetTxnId(), sp.GetStmtId(), sp.GetSqlOfStmt()))
		stats.ExecutionEnd()

		timeCost := time.Since(runStart)
		v2.TxnStatementExecuteDurationHistogram.Observe(timeCost.Seconds())

		affectRows := 0
		if queryResult != nil {
			affectRows = int(queryResult.AffectRows)
		}
		txnTrace.GetService(c.proc.GetService()).TxnStatementCompleted(
			txnOperator, executeSQL, timeCost, seq, affectRows, err)

		if _, ok := c.pn.Plan.(*plan.Plan_Ddl); ok {
			c.setHaveDDL(true)
		}
	}()

	// running and retry.
	var retryTimes = 0
	queryResult = &util2.RunResult{}
	v2.TxnStatementTotalCounter.Inc()
	for {
		// Before compile.runOnce, reset `StatsInfo` IO resources which in sql context
		stats.ResetIOAccessTimeConsumption()
		stats.ResetIOMergerTimeConsumption()
		stats.ResetBuildReaderTimeConsumption()

		// build query context and pipeline contexts for the current run.
		runC.InitPipelineContextToExecuteQuery()
		runC.MessageBoard.BeforeRunonce()
		if err = runC.runOnce(); err == nil {
			if runC.anal != nil {
				runC.anal.retryTimes = retryTimes
			}
			break
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

		defChanged := moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
		if runC, err = c.prepareRetry(defChanged); err != nil {
			return nil, err
		}
	}

	if err = runC.proc.GetQueryContextError(); err != nil {
		return nil, err
	}
	queryResult.AffectRows = runC.getAffectedRows()
	if c.uid != "mo_logger" && strings.Contains(strings.ToLower(c.sql), "insert") && (strings.Contains(c.sql, "{MO_TS =") || strings.Contains(c.sql, "{SNAPSHOT =")) {
		getLogger(c.proc.GetService()).Info("insert into with snapshot", zap.String("sql", c.sql), zap.Uint64("affectRows", queryResult.AffectRows))
	}
	if txnOperator != nil {
		err = txnOperator.GetWorkspace().Adjust(writeOffset)
	}

	if c.hasValidQueryPlan() {
		c.handlePlanAnalyze(runC, isExplainPhyPlan, queryResult, option)
	}

	return queryResult, err
}

// prepareRetry rebuild a new Compile object for retrying the query.
func (c *Compile) prepareRetry(defChanged bool) (*Compile, error) {
	v2.TxnStatementRetryCounter.Inc()
	c.proc.GetTxnOperator().GetWorkspace().IncrSQLCount()

	topContext := c.proc.GetTopContext()

	// clear the workspace of the failed statement
	if e := c.proc.GetTxnOperator().GetWorkspace().RollbackLastStatement(topContext); e != nil {
		return nil, e
	}

	// increase the statement id
	if e := c.proc.GetTxnOperator().GetWorkspace().IncrStatementID(topContext, false); e != nil {
		return nil, e
	}

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
	contextBase.BuildQueryCtx()
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
func setContextForParallelScope(parallelScope *Scope, originalContext context.Context, originalCancel context.CancelFunc) {
	process.ReplacePipelineCtx(parallelScope.Proc, originalContext, originalCancel)

	// build context for data entry.
	for _, prePipeline := range parallelScope.PreScopes {
		prePipeline.buildContextFromParentCtx(parallelScope.Proc.Ctx)
	}
}

func (c *Compile) handlePlanAnalyze(runC *Compile, isExplainPhy bool, queryResult *util2.RunResult, option *ExplainOption) {
	c.GenPhyPlan(runC)
	c.fillPlanNodeAnalyzeInfo()

	if isExplainPhy {
		topContext := c.proc.GetTopContext()

		statsInfo := statistic.StatsInfoFromContext(topContext)
		scopeInfo := makeExplainPhyPlanBuffer(c.scopes, queryResult, statsInfo, c.anal, option)

		runC.anal.explainPhyBuffer = scopeInfo
	}
}
