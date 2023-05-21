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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/multierr"
)

// WithTxn exec sql in a exists txn
func (opts Options) WithTxn(txnOp client.TxnOperator) Options {
	opts.txnOp = txnOp
	return opts
}

// WithDatabase exec sql in database
func (opts Options) WithDatabase(database string) Options {
	opts.database = database
	return opts
}

// WithAccountID execute sql in account
func (opts Options) WithAccountID(accoundID uint32) Options {
	opts.accoundID = accoundID
	return opts
}

// WithMinCommittedTS use minCommittedTS to exec sql. It will set txn's snapshot to
// minCommittedTS+1, so the txn can see the data which committed at minCommittedTS.
// It's not work if txn operator is set.
func (opts Options) WithMinCommittedTS(ts timestamp.Timestamp) Options {
	opts.minCommittedTS = ts
	return opts
}

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
	aicm *defines.AutoIncrCacheManager) SQLExecutor {
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
	opts Options) (Result, error) {
	var res Result
	err := s.ExecTxn(
		ctx,
		func(exec TxnExecutor) error {
			v, err := exec.Exec(sql)
			res = v
			return err
		},
		opts)
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func (s *sqlExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(TxnExecutor) error,
	opts Options) error {
	exec, err := newTxnExecutor(ctx, s, opts)
	if err != nil {
		return err
	}
	err = execFunc(exec)
	if err != nil {
		return exec.rollback()
	}
	return exec.commit()
}

func (s *sqlExecutor) getCompileContext(
	ctx context.Context,
	proc *process.Process,
	opts Options) *compilerContext {
	return newCompilerContext(
		ctx,
		opts.database,
		s.eng,
		proc)
}

func (s *sqlExecutor) adjustOptions(
	ctx context.Context,
	opts Options) (context.Context, Options, error) {
	if opts.accoundID > 0 {
		ctx = context.WithValue(
			ctx,
			defines.TenantIDKey{},
			opts.accoundID)
	}

	if opts.txnOp == nil {
		txnOp, err := s.txnClient.New(ctx, opts.minCommittedTS)
		if err != nil {
			return nil, Options{}, err
		}
		opts.txnOp = txnOp
		opts.innerTxn = true
	}
	return ctx, opts, nil
}

type txnExecutor struct {
	s    *sqlExecutor
	ctx  context.Context
	opts Options
}

func newTxnExecutor(
	ctx context.Context,
	s *sqlExecutor,
	opts Options) (*txnExecutor, error) {
	ctx, opts, err := s.adjustOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	if opts.innerTxn {
		if err := s.eng.New(ctx, opts.txnOp); err != nil {
			return nil, err
		}
	}
	return &txnExecutor{s: s, ctx: ctx, opts: opts}, nil
}

func (exec *txnExecutor) Exec(sql string) (Result, error) {
	stmts, err := parsers.Parse(exec.ctx, dialect.MYSQL, sql, 1)
	if err != nil {
		return Result{}, err
	}

	proc := process.New(
		exec.ctx,
		exec.s.mp,
		exec.s.txnClient,
		exec.opts.txnOp,
		exec.s.fs,
		exec.s.ls,
		exec.s.aicm,
	)

	pn, err := plan.BuildPlan(
		exec.s.getCompileContext(exec.ctx, proc, exec.opts),
		stmts[0])
	if err != nil {
		return Result{}, err
	}

	c := New(
		exec.s.addr,
		exec.opts.database,
		sql,
		"",
		"",
		exec.ctx,
		exec.s.eng,
		proc,
		stmts[0],
		false,
		nil)

	var result Result
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
		return Result{}, err
	}
	if err := c.Run(0); err != nil {
		return Result{}, err
	}

	result.batches = batches
	result.affectedRows = c.GetAffectedRows()
	return result, nil
}

func (exec *txnExecutor) commit() error {
	if !exec.opts.innerTxn {
		return nil
	}
	if err := exec.s.eng.Commit(
		exec.ctx,
		exec.opts.txnOp); err != nil {
		return err
	}
	return exec.opts.txnOp.Commit(exec.ctx)
}

func (exec *txnExecutor) rollback() error {
	if !exec.opts.innerTxn {
		return nil
	}
	err := exec.s.eng.Rollback(
		exec.ctx,
		exec.opts.txnOp)
	return multierr.Append(err,
		exec.opts.txnOp.Rollback(exec.ctx))
}

func (res Result) GetAffectedRows() uint64 {
	return res.affectedRows
}

func (res Result) Close() {
	for _, rows := range res.batches {
		rows.Clean(res.mp)
	}
}

// ReadRows read all rows, apply is used to read cols data in a row. If apply return false, stop
// reading. If the query has a lot of data, apply will be called multiple times, giving a batch of
// rows for each call.
func (res Result) ReadRows(apply func(cols []*vector.Vector) bool) {
	for _, rows := range res.batches {
		if !apply(rows.Vecs) {
			return
		}
	}
}

// GetFixedRows get fixed rows, int, float, etc.
func GetFixedRows[T any](vec *vector.Vector) []T {
	return vector.MustFixedCol[T](vec)
}

// GetBytesRows get bytes rows, varchar, varbinary, text, json, etc.
func GetBytesRows(vec *vector.Vector) [][]byte {
	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	rows := make([][]byte, 0, n)
	for idx := range data {
		rows = append(rows, data[idx].GetByteSlice(area))
	}
	return rows
}

// GetStringRows get bytes rows, varchar, varbinary, text, json, etc.
func GetStringRows(vec *vector.Vector) []string {
	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	rows := make([]string, 0, n)
	for idx := range data {
		rows = append(rows, string(data[idx].GetByteSlice(area)))
	}
	return rows
}
