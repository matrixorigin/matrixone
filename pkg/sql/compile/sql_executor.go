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

	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"go.uber.org/zap"
)

type sqlExecutor struct {
	addr      string
	eng       engine.Engine
	mp        *mpool.MPool
	txnClient client.TxnClient
	fs        fileservice.FileService
	ls        lockservice.LockService
	qc        qclient.QueryClient
	hakeeper  logservice.CNHAKeeperClient
	us        udf.Service
	aicm      *defines.AutoIncrCacheManager
	buf       *buffer.Buffer
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
	aicm *defines.AutoIncrCacheManager) executor.SQLExecutor {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.LockService)
	if !ok {
		panic("missing lock service")
	}
	return &sqlExecutor{
		addr:      addr,
		eng:       eng,
		txnClient: txnClient,
		fs:        fs,
		ls:        v.(lockservice.LockService),
		qc:        qc,
		hakeeper:  hakeeper,
		us:        us,
		aicm:      aicm,
		mp:        mp,
		buf:       buffer.New(),
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
	opts executor.Options) (executor.Result, error) {
	var res executor.Result
	err := s.ExecTxn(
		ctx,
		func(exec executor.TxnExecutor) error {
			v, err := exec.Exec(sql, opts.StatementOption())
			res = v
			return err
		},
		opts)
	if err != nil {
		return executor.Result{}, err
	}
	return res, nil
}

func (s *sqlExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options) error {
	exec, err := newTxnExecutor(ctx, s, opts)
	if err != nil {
		return err
	}
	err = execFunc(exec)
	if err != nil {
		logutil.Errorf("internal sql executor error: %v", err)
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
	db string) *compilerContext {
	return newCompilerContext(
		ctx,
		db,
		s.eng,
		proc)
}

func (s *sqlExecutor) adjustOptions(
	ctx context.Context,
	opts executor.Options) (context.Context, executor.Options, error) {
	if opts.HasAccountID() {
		ctx = context.WithValue(
			ctx,
			defines.TenantIDKey{},
			opts.AccountID())
	} else if ctx.Value(defines.TenantIDKey{}) == nil {
		ctx = context.WithValue(
			ctx,
			defines.TenantIDKey{},
			uint32(0))
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
	opts executor.Options) (*txnExecutor, error) {
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
	statementOption executor.StatementOption) (executor.Result, error) {

	//-----------------------------------------------------------------------------------------
	// NOTE: This code is to restore tenantID information in the Context when temporarily switching tenants
	// so that it can be restored to its original state after completing the task.
	recoverAccount := func(exec *txnExecutor, accId uint32) {
		exec.ctx = context.WithValue(exec.ctx, defines.TenantIDKey{}, accId)
	}

	if statementOption.HasAccountID() {
		originAccountID := catalog.System_Account
		if v := exec.ctx.Value(defines.TenantIDKey{}); v != nil {
			originAccountID = v.(uint32)
		}

		exec.ctx = context.WithValue(exec.ctx,
			defines.TenantIDKey{},
			statementOption.AccountID())
		// NOTE: Restore AccountID information in context.Context
		defer recoverAccount(exec, originAccountID)
	}
	//-----------------------------------------------------------------------------------------

	receiveAt := time.Now()

	stmts, err := parsers.Parse(exec.ctx, dialect.MYSQL, sql, 1, 0)
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

	proc := process.New(
		exec.ctx,
		exec.s.mp,
		exec.s.txnClient,
		exec.opts.Txn(),
		exec.s.fs,
		exec.s.ls,
		exec.s.qc,
		exec.s.hakeeper,
		exec.s.us,
		exec.s.aicm,
	)
	proc.WaitPolicy = statementOption.WaitPolicy()
	proc.SetVectorPoolSize(0)
	proc.SessionInfo.TimeZone = exec.opts.GetTimeZone()
	proc.SessionInfo.Buf = exec.s.buf
	proc.SessionInfo.StorageEngine = exec.s.eng
	defer func() {
		proc.CleanValueScanBatchs()
		proc.FreeVectors()
	}()

	compileContext := exec.s.getCompileContext(exec.ctx, proc, exec.getDatabase())
	compileContext.SetRootSql(sql)

	pn, err := plan.BuildPlan(compileContext, stmts[0], false)
	if err != nil {
		return executor.Result{}, err
	}

	c := NewCompile(exec.s.addr, exec.getDatabase(), sql, "", "", exec.ctx, exec.s.eng, proc, stmts[0], false, nil, receiveAt)
	defer c.Release()
	c.disableRetry = exec.opts.DisableIncrStatement()
	c.SetBuildPlanFunc(func() (*plan.Plan, error) {
		return plan.BuildPlan(
			exec.s.getCompileContext(exec.ctx, proc, exec.getDatabase()),
			stmts[0], false)
	})

	result := executor.NewResult(exec.s.mp)
	var batches []*batch.Batch
	err = c.Compile(
		exec.ctx,
		pn,
		func(bat *batch.Batch) error {
			if bat != nil {
				// the bat is valid only in current method. So we need copy data.
				// FIXME: add a custom streaming apply handler to consume readed data. Now
				// our current internal sql will never read too much data.
				rows, err := bat.Dup(exec.s.mp)
				if err != nil {
					return err
				}
				batches = append(batches, rows)
			}
			return nil
		})
	if err != nil {
		return executor.Result{}, err
	}
	var runResult *util.RunResult
	runResult, err = c.Run(0)
	if err != nil {
		for _, bat := range batches {
			if bat != nil {
				bat.Clean(exec.s.mp)
			}
		}
		return executor.Result{}, err
	}

	logutil.Info("sql_executor exec",
		zap.String("sql", sql),
		zap.String("txn-id", hex.EncodeToString(exec.opts.Txn().Txn().ID)))
	result.LastInsertID = proc.GetLastInsertID()
	result.Batches = batches
	result.AffectedRows = runResult.AffectRows
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
	proc := process.New(
		ctx,
		exec.s.mp,
		exec.s.txnClient,
		txnOp,
		exec.s.fs,
		exec.s.ls,
		exec.s.qc,
		exec.s.hakeeper,
		exec.s.us,
		exec.s.aicm,
	)
	proc.SetVectorPoolSize(0)
	proc.SessionInfo.TimeZone = exec.opts.GetTimeZone()
	proc.SessionInfo.Buf = exec.s.buf
	defer func() {
		proc.CleanValueScanBatchs()
		proc.FreeVectors()
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
