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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/multierr"
)

type sqlExecutor struct {
	addr      string
	eng       engine.Engine
	mp        *mpool.MPool
	txnClient client.TxnClient
	fs        fileservice.FileService
	ls        lockservice.LockService
	aicm      *defines.AutoIncrCacheManager
}

// NewSQLExecutor returns a internal used sql service. It can execute sql in current CN.
func NewSQLExecutor(
	addr string,
	eng engine.Engine,
	mp *mpool.MPool,
	txnClient client.TxnClient,
	fs fileservice.FileService,
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
		aicm:      aicm,
		mp:        mp,
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
		return exec.rollback()
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
		s.txnClient.(client.TxnClientWithCtl).SetLatestCommitTS(ts)
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

	proc := process.New(
		exec.ctx,
		exec.s.mp,
		exec.s.txnClient,
		exec.opts.Txn(),
		exec.s.fs,
		exec.s.ls,
		exec.s.aicm,
	)

	pn, err := plan.BuildPlan(
		exec.s.getCompileContext(exec.ctx, proc, exec.opts),
		stmts[0])
	if err != nil {
		return executor.Result{}, err
	}

	c := New(
		exec.s.addr,
		exec.opts.Database(),
		sql,
		"",
		"",
		exec.ctx,
		exec.s.eng,
		proc,
		stmts[0],
		false,
		nil)

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
	if err := c.Run(0); err != nil {
		return executor.Result{}, err
	}

	result.Batches = batches
	result.AffectedRows = c.GetAffectedRows()
	return result, nil
}

func (exec *txnExecutor) commit() error {
	if exec.opts.ExistsTxn() {
		return nil
	}
	if err := exec.s.eng.Commit(
		exec.ctx,
		exec.opts.Txn()); err != nil {
		return err
	}
	return exec.opts.Txn().Commit(exec.ctx)
}

func (exec *txnExecutor) rollback() error {
	if exec.opts.ExistsTxn() {
		return nil
	}
	err := exec.s.eng.Rollback(
		exec.ctx,
		exec.opts.Txn())
	return multierr.Append(err,
		exec.opts.Txn().Rollback(exec.ctx))
}
