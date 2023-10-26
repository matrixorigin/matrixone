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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/logservice"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type sqlExecutor struct {
	addr      string
	eng       engine.Engine
	mp        *mpool.MPool
	txnClient client.TxnClient
	fs        fileservice.FileService
	ls        lockservice.LockService
	qs        queryservice.QueryService
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
	qs queryservice.QueryService,
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
		qs:        qs,
		hakeeper:  hakeeper,
		us:        us,
		aicm:      aicm,
		mp:        mp,
		buf:       buffer.New(),
	}
}

func (s *sqlExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options) (executor.Result, error) {
	var res executor.Result
	err := s.ExecTxn(
		ctx,
		func(exec executor.TxnExecutor) error {
			v, err := exec.Exec(sql)
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
	opts executor.Options) *compilerContext {
	return newCompilerContext(
		ctx,
		opts.Database(),
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
	}

	if !opts.HasExistsTxn() {
		txnOp, err := s.txnClient.New(ctx, opts.MinCommittedTS(),
			client.WithTxnCreateBy("sql-executor"))
		if err != nil {
			return nil, executor.Options{}, err
		}
		opts = opts.SetupNewTxn(txnOp)
	}
	return ctx, opts, nil
}

type txnExecutor struct {
	s    *sqlExecutor
	ctx  context.Context
	opts executor.Options
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

func (exec *txnExecutor) Exec(sql string) (executor.Result, error) {
	stmts, err := parsers.Parse(exec.ctx, dialect.MYSQL, sql, 1)
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
		exec.s.qs,
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

	pn, err := plan.BuildPlan(
		exec.s.getCompileContext(exec.ctx, proc, exec.opts),
		stmts[0], false)
	if err != nil {
		return executor.Result{}, err
	}

	c := New(exec.s.addr, exec.opts.Database(), sql, "", "", exec.ctx, exec.s.eng, proc, stmts[0], false, nil)

	result := executor.NewResult(exec.s.mp)
	var batches []*batch.Batch
	err = c.Compile(
		exec.ctx,
		pn,
		nil,
		func(a any, bat *batch.Batch) error {
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

	result.Batches = batches
	result.AffectedRows = runResult.AffectRows
	return result, nil
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
