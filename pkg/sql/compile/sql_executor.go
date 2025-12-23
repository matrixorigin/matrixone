// Copyright 2023 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

type sqlExecutor struct {
	addr        string
	eng         engine.Engine
	mp          *mpool.MPool
	txnClient   client.TxnClient
	fs          fileservice.FileService
	ls          lockservice.LockService
	qc          qclient.QueryClient
	hakeeper    logservice.CNHAKeeperClient
	us          udf.Service
	buf         *buffer.Buffer
	taskservice taskservice.TaskService
}

// NewSQLExecutor returns a internal used sql service. It can execute sql in current CN.
func NewSQLExecutor(
	addr string,
	eng engine.Engine,
	mp *mpool.MPool,
	txnClient client.TxnClient,
	fs fileservice.FileService,
	qc qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	us udf.Service,
	taskService taskservice.TaskService,
) executor.SQLExecutor {
	v, ok := runtime.ServiceRuntime(qc.ServiceID()).GetGlobalVariables(runtime.LockService)
	if !ok {
		panic("missing lock service")
	}
	return &sqlExecutor{
		addr:        addr,
		eng:         eng,
		txnClient:   txnClient,
		fs:          fs,
		ls:          v.(lockservice.LockService),
		qc:          qc,
		hakeeper:    hakeeper,
		us:          us,
		mp:          mp,
		buf:         buffer.New(),
		taskservice: taskService,
	}
}

func (s *sqlExecutor) NewTxnOperator(ctx context.Context) client.TxnOperator {
	var opts executor.Options

	ctx, opts, err := s.adjustOptions(ctx, opts)
	if err != nil {
		return nil
	}
	if !opts.ExistsTxn() {
		if err := s.eng.New(ctx, opts.Txn()); err != nil {
			return nil
		}
	}
	opts.Txn().GetWorkspace().StartStatement()
	opts.Txn().GetWorkspace().IncrStatementID(ctx, false)
	return opts.Txn()
}

func (s *sqlExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	ctx = perfcounter.AttachTxnExecutorKey(ctx)

	var (
		res executor.Result
	)

	// use an outer txn to execute this sql and DO NOT
	// commit or rollback after this execution finished.
	if opts.ExistsTxn() && opts.KeepTxnAlive() {
		var (
			err  error
			exec *txnExecutor
		)

		if exec, err = newTxnExecutor(ctx, s, opts); err != nil {
			return res, err
		}

		return exec.Exec(sql, opts.StatementOption())
	} else {
		err := s.ExecTxn(
			ctx,
			func(exec executor.TxnExecutor) error {
				v, err := exec.Exec(sql, opts.StatementOption())
				res = v
				return err
			},
			opts.WithSQL(sql))
		if err != nil {
			return executor.Result{}, err
		}
		return res, nil
	}
}

func (s *sqlExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	ctx = perfcounter.AttachTxnExecutorKey(ctx)
	exec, err := newTxnExecutor(ctx, s, opts)
	if err != nil {
		return err
	}
	err = execFunc(exec)
	if err != nil {
		logutil.Error("internal sql executor error",
			zap.Error(err),
			zap.String("sql", commonutil.Abbreviate(opts.SQL(), 500)),
			zap.String("txn", exec.Txn().Txn().DebugString()),
		)
		return exec.rollback(err)
	}
	if err = exec.commit(); err != nil {
		return err
	}
	s.maybeWaitCommittedLogApplied(exec.opts)
	return nil
}

func (s *sqlExecutor) maybeWaitCommittedLogApplied(opts executor.Options) {
	if !opts.WaitCommittedLogApplied() {
		return
	}
	ts := opts.Txn().Txn().CommitTS
	if !ts.IsEmpty() {
		s.txnClient.SyncLatestCommitTS(ts)
	}
}

func (s *sqlExecutor) getCompileContext(
	ctx context.Context,
	proc *process.Process,
	db string,
	lower int64) *compilerContext {
	cc := &compilerContext{
		ctx:       ctx,
		defaultDB: db,
		engine:    s.eng,
		proc:      proc,
		lower:     lower,
	}
	// For testing: check if a stats cache is provided in context
	if statsCache, ok := ctx.Value("test_stats_cache").(*plan.StatsCache); ok {
		cc.statsCache = statsCache
	}
	return cc
}

func (s *sqlExecutor) adjustOptions(
	ctx context.Context,
	opts executor.Options,
) (context.Context, executor.Options, error) {
	if ctx.Value(defines.TenantIDKey{}) == nil {
		ctx = context.WithValue(
			ctx,
			defines.TenantIDKey{},
			uint32(0))
	}

	if opts.HasAccountID() {
		ctx = context.WithValue(
			ctx,
			defines.TenantIDKey{},
			opts.AccountID())
	}

	if !opts.HasExistsTxn() {
		txnOpts := opts.ExtraTxnOptions()
		txnOpts = append(txnOpts,
			client.WithTxnCreateBy(
				opts.AccountID(),
				"",
				"sql-executor",
				0),
			client.WithDisableTrace(!opts.EnableTrace()))
		txnOp, err := s.txnClient.New(
			ctx,
			opts.MinCommittedTS(),
			txnOpts...)
		if err != nil {
			return nil, executor.Options{}, err
		}
		opts = opts.SetupNewTxn(txnOp)
	}
	return ctx, opts, nil
}

type txnExecutor struct {
	s        *sqlExecutor
	ctx      context.Context
	opts     executor.Options
	database string
}

func newTxnExecutor(
	ctx context.Context,
	s *sqlExecutor,
	opts executor.Options,
) (*txnExecutor, error) {
	ctx = perfcounter.AttachTxnExecutorKey(ctx)
	ctx, opts, err := s.adjustOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	if !opts.ExistsTxn() {
		if err := s.eng.New(ctx, opts.Txn()); err != nil {
			return nil, err
		}
	}
	return &txnExecutor{s: s, ctx: ctx, opts: opts}, nil
}

func (exec *txnExecutor) Use(db string) {
	exec.database = db
}

func (exec *txnExecutor) Exec(
	sql string,
	statementOption executor.StatementOption,
) (executor.Result, error) {
	// NOTE: This code is to restore tenantID information in the Context when temporarily switching tenants
	// so that it can be restored to its original state after completing the task.
	var originCtx context.Context
	if statementOption.HasAccountID() {
		// save the current context
		originCtx = exec.ctx
		// switch tenantID
		exec.ctx = context.WithValue(exec.ctx, defines.TenantIDKey{}, statementOption.AccountID())
		if statementOption.HasUserID() {
			exec.ctx = context.WithValue(exec.ctx, defines.UserIDKey{}, statementOption.UserID())
		}
		if statementOption.HasRoleID() {
			exec.ctx = context.WithValue(exec.ctx, defines.RoleIDKey{}, statementOption.RoleID())
		}
		defer func() {
			// restore context at the end of the function
			exec.ctx = originCtx
		}()
	}
	//------------------------------------------------------------------------------------------------------------------
	if statementOption.IgnoreForeignKey() {
		exec.ctx = context.WithValue(exec.ctx,
			defines.IgnoreForeignKey{},
			true)
	}

	if v := statementOption.AlterCopyDedupOpt(); v != nil {
		exec.ctx = context.WithValue(exec.ctx,
			defines.AlterCopyOpt{}, v)
	}

	if logicalId := statementOption.KeepLogicalId(); logicalId != 0 {
		exec.ctx = context.WithValue(exec.ctx,
			defines.LogicalIdKey{},
			logicalId)
	}

	exec.ctx = context.WithValue(
		exec.ctx,
		defines.InternalExecutorKey{},
		true,
	)

	receiveAt := time.Now()
	lower := exec.opts.LowerCaseTableNames()
	stmts, err := parsers.Parse(exec.ctx, dialect.MYSQL, sql, lower)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if err != nil {
		return executor.Result{}, err
	}

	// TODO(volgariver6): we got a duplicate code logic in `func (cwft *TxnComputationWrapper) Compile`,
	// maybe we should fix it.
	txnOp := exec.opts.Txn()
	if txnOp != nil && !exec.opts.DisableIncrStatement() {
		txnOp.GetWorkspace().StartStatement()
		defer func() {
			txnOp.GetWorkspace().EndStatement()
		}()

		err := txnOp.GetWorkspace().IncrStatementID(exec.ctx, false)
		if err != nil {
			return executor.Result{}, err
		}
	}

	proc := process.NewTopProcess(
		exec.ctx,
		exec.s.mp,
		exec.s.txnClient,
		exec.opts.Txn(),
		exec.s.fs,
		exec.s.ls,
		exec.s.qc,
		exec.s.hakeeper,
		exec.s.us,
		nil,
		exec.s.taskservice,
	)
	proc.SetResolveVariableFunc(exec.opts.ResolveVariableFunc())

	if exec.opts.ResolveVariableFunc() != nil {
		proc.SetResolveVariableFunc(exec.opts.ResolveVariableFunc())
	}

	prepared := false
	if statementOption.HasParams() {
		vec := statementOption.Params(exec.s.mp)
		proc.SetPrepareParams(vec)
		prepared = true
		defer vec.Free(proc.Mp())
	}

	proc.Base.WaitPolicy = statementOption.WaitPolicy()
	proc.Base.SessionInfo.TimeZone = exec.opts.GetTimeZone()
	proc.Base.SessionInfo.Buf = exec.s.buf
	proc.Base.SessionInfo.StorageEngine = exec.s.eng
	proc.Base.QueryClient = exec.s.qc
	defer func() {
		proc.Free()
	}()

	compileContext := exec.s.getCompileContext(exec.ctx, proc, exec.getDatabase(), lower)
	compileContext.SetRootSql(sql)

	var pn *plan.Plan

	switch stmt := stmts[0].(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		//*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber,
		*tree.ShowTableNumber, *tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:

		opt := plan.NewBaseOptimizer(compileContext)
		optimized, err := opt.Optimize(stmt, prepared)
		if err == nil {
			pn = &plan.Plan{
				Plan: &plan.Plan_Query{
					Query: optimized,
				},
			}
		} else {
			return executor.Result{}, err
		}
	default:
		pn, err = plan.BuildPlan(compileContext, stmt, prepared)
	}

	if err != nil {
		return executor.Result{}, err
	}

	if prepared {
		_, _, err := plan.ResetPreparePlan(compileContext, pn)
		if err != nil {
			return executor.Result{}, err
		}
	}

	c := NewCompile(
		exec.s.addr,
		exec.getDatabase(),
		sql,
		"",
		"",
		exec.s.eng,
		proc,
		stmts[0],
		false,
		nil,
		receiveAt,
	)
	c.SetOriginSQL(sql)
	c.adjustTableExtraFunc = exec.opts.AdjustTableExtraFunc()
	c.disableDropAutoIncrement = statementOption.DisableDropIncrStatement()
	c.keepAutoIncrement = statementOption.KeepAutoIncrement()
	c.disableRetry = exec.opts.DisableIncrStatement()
	c.ignorePublish = statementOption.IgnorePublish()
	c.ignoreCheckExperimental = statementOption.IgnoreCheckExperimental()
	c.disableLock = statementOption.DisableLock()

	defer c.Release()

	if prepared {
		c.SetBuildPlanFunc(func(ctx context.Context) (*plan.Plan, error) {
			pn, err := plan.BuildPlan(
				exec.s.getCompileContext(ctx, proc, exec.getDatabase(), lower),
				stmts[0], true,
			)
			if err != nil {
				return pn, err
			}
			_, _, err = plan.ResetPreparePlan(compileContext, pn)
			if err != nil {
				return pn, err
			}
			return pn, nil
		})
	} else {
		c.SetBuildPlanFunc(func(ctx context.Context) (*plan.Plan, error) {
			return plan.BuildPlan(
				exec.s.getCompileContext(ctx, proc, exec.getDatabase(), lower),
				stmts[0], false)
		})
	}

	result := executor.NewResult(exec.s.mp)

	stream_chan, err_chan, streaming := exec.opts.Streaming()

	if exec.opts.ForceRebuildPlan() {
		pn, err = c.buildPlanFunc(proc.Ctx)
		if err != nil {
			return executor.Result{}, err
		}
	}

	var batches []*batch.Batch
	err = c.Compile(
		exec.ctx,
		pn,
		func(bat *batch.Batch, crs *perfcounter.CounterSet) error {
			if bat != nil {
				// the bat is valid only in current method. So we need copy data.
				// FIXME: add a custom streaming apply handler to consume readed data. Now
				// our current internal sql will never read too much data.
				rows, err := bat.Clone(exec.s.mp, streaming)
				if err != nil {
					return err
				}
				if streaming {
					stream_result := executor.NewResult(exec.s.mp)
					for len(stream_chan) == cap(stream_chan) {
						select {
						case <-proc.Ctx.Done():
							err_chan <- moerr.NewInternalError(proc.Ctx, "context cancelled")
							return moerr.NewInternalError(proc.Ctx, "context cancelled")
						case <-exec.ctx.Done():
							err_chan <- exec.ctx.Err()
							return exec.ctx.Err()
						default:
							time.Sleep(1 * time.Millisecond)
						}
					}
					stream_result.Batches = []*batch.Batch{rows}
					stream_chan <- stream_result
				} else {
					batches = append(batches, rows)
				}
			}
			return nil

		})
	if err != nil {
		if streaming {
			err_chan <- err
		}
		return executor.Result{}, err
	}
	var runResult *util.RunResult
	runResult, err = c.Run(0)
	if err != nil {
		if streaming {
			err_chan <- err
		}
		for _, bat := range batches {
			if bat != nil {
				bat.Clean(exec.s.mp)
			}
		}
		return executor.Result{}, err
	}

	if !statementOption.DisableLog() {
		logutil.Info("sql_executor exec",
			zap.String("sql", commonutil.Abbreviate(sql, 500)),
			zap.String("txn-id", hex.EncodeToString(exec.opts.Txn().Txn().ID)),
			zap.Duration("duration", time.Since(receiveAt)),
			zap.Int("BatchSize", len(batches)),
			zap.Int("retry-times", c.retryTimes),
			zap.Uint64("AffectedRows", runResult.AffectRows),
		)
	}

	result.LastInsertID = proc.GetLastInsertID()
	result.Batches = batches
	result.AffectedRows = runResult.AffectRows
	result.LogicalPlan = pn.GetQuery()
	return result, nil
}

func (exec *txnExecutor) LockTable(table string) error {
	txnOp := exec.opts.Txn()
	ctx := exec.ctx

	dbSource, err := exec.s.eng.Database(ctx, exec.opts.Database(), txnOp)
	if err != nil {
		return err
	}
	rel, err := dbSource.Relation(ctx, table, nil)
	if err != nil {
		return err
	}
	proc := process.NewTopProcess(
		ctx,
		exec.s.mp,
		exec.s.txnClient,
		txnOp,
		exec.s.fs,
		exec.s.ls,
		exec.s.qc,
		exec.s.hakeeper,
		exec.s.us,
		nil,
		exec.s.taskservice,
	)
	proc.Base.SessionInfo.TimeZone = exec.opts.GetTimeZone()
	proc.Base.SessionInfo.Buf = exec.s.buf
	defer func() {
		proc.Free()
	}()
	return doLockTable(exec.s.eng, proc, rel, false)
}

func (exec *txnExecutor) Txn() client.TxnOperator {
	return exec.opts.Txn()
}

func (exec *txnExecutor) commit() error {
	if exec.opts.ExistsTxn() {
		return nil
	}
	return exec.opts.Txn().Commit(exec.ctx)
}

func (exec *txnExecutor) rollback(err error) error {
	if exec.opts.ExistsTxn() {
		return err
	}
	return errors.Join(err, exec.opts.Txn().Rollback(exec.ctx))
}

func (exec *txnExecutor) getDatabase() string {
	if exec.database != "" {
		return exec.database
	}
	return exec.opts.Database()
}
