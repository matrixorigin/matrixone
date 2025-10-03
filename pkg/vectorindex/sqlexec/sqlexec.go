// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"context"
	"errors"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// SqlContext stores required information for background SQLInternalExecutor
type SqlContext struct {
	Ctx         context.Context
	CNUuid      string
	TxnOperator client.TxnOperator
	AccountId   uint32
}

func NewSqlContext(ctx context.Context, cnuuid string, txnOperator client.TxnOperator, accountid uint32) *SqlContext {
	return &SqlContext{Ctx: ctx, CNUuid: cnuuid, TxnOperator: txnOperator, AccountId: accountid}
}

func (s *SqlContext) GetService() string {
	return s.CNUuid
}

func (s *SqlContext) Txn() client.TxnOperator {
	return s.TxnOperator
}

// SqlProcess is the wrapper for both process.Process and background SQLContext
// SqlProcess enable the API to run in both frontend and background with InternalSQLExecutor
// process.Process always exists in frontend.
// However, process.Process does not exist in background job.
// SqlContext with required infos such as context.Context, CNUUID, TxnOperator and AccountId enable
// to run SQL with InternalSQLExecutor.
// Either process.Process or SqlContext is used in SqlProcess.
// We will look for process.Process first before SqlContext
type SqlProcess struct {
	Proc   *process.Process
	SqlCtx *SqlContext
}

func NewSqlProcess(proc *process.Process) *SqlProcess {
	return &SqlProcess{Proc: proc}
}

func NewSqlProcessWithContext(ctx *SqlContext) *SqlProcess {
	return &SqlProcess{SqlCtx: ctx}
}

func (s *SqlProcess) GetContext() context.Context {
	if s.Proc != nil {
		return s.Proc.Ctx
	}
	return s.SqlCtx.Ctx
}

func (s *SqlProcess) GetTopContext() context.Context {
	if s.Proc != nil {
		return s.Proc.GetTopContext()
	}
	return s.SqlCtx.Ctx
}

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func RunSql(sqlproc *SqlProcess, sql string) (executor.Result, error) {
	if sqlproc.Proc != nil {
		proc := sqlproc.Proc
		v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		//-------------------------------------------------------
		topContext := proc.GetTopContext()
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return executor.Result{}, err
		}
		//-------------------------------------------------------

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithDatabase(proc.GetSessionInfo().Database).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithAccountID(accountId)
		return exec.Exec(topContext, sql, opts)
	} else {

		sqlctx := sqlproc.SqlCtx
		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlctx.AccountId

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithAccountID(accountId)
		return exec.Exec(sqlctx.Ctx, sql, opts)

	}
}

// run SQL in WithStreaming() and pass the channel to SQL executor
func RunStreamingSql(
	ctx context.Context,
	sqlproc *SqlProcess,
	sql string,
	stream_chan chan executor.Result,
	error_chan chan error,
) (executor.Result, error) {

	if sqlproc.Proc != nil {
		proc := sqlproc.Proc
		v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		//-------------------------------------------------------
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return executor.Result{}, err
		}
		//-------------------------------------------------------
		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithDatabase(proc.GetSessionInfo().Database).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithAccountID(accountId).
			WithStreaming(stream_chan, error_chan)
		return exec.Exec(ctx, sql, opts)
	} else {

		sqlctx := sqlproc.SqlCtx

		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlctx.AccountId

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithAccountID(accountId).
			WithStreaming(stream_chan, error_chan)
		return exec.Exec(ctx, sql, opts)

	}

}

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func RunTxn(sqlproc *SqlProcess, execFunc func(executor.TxnExecutor) error) error {
	if sqlproc.Proc != nil {
		proc := sqlproc.Proc

		v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		//-------------------------------------------------------
		topContext := proc.GetTopContext()
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return err
		}
		//-------------------------------------------------------

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithDatabase(proc.GetSessionInfo().Database).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithAccountID(accountId)
		return exec.ExecTxn(topContext, execFunc, opts)
	} else {

		sqlctx := sqlproc.SqlCtx
		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlctx.AccountId

		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithAccountID(accountId)
		return exec.ExecTxn(sqlctx.Ctx, execFunc, opts)
	}
}

func getTxn(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	info string,
) (client.TxnOperator, error) {
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		info,
		0)
	op, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		return nil, err
	}
	err = cnEngine.New(ctx, op)
	if err != nil {
		return nil, err
	}
	return op, nil
}

// run SQL with SqlContext
func RunTxnWithSqlContext(ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cnUUID string,
	accountId uint32,
	duration time.Duration,
	f func(sqlproc *SqlProcess) error) (err error) {

	newctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()

	txnOp, err := getTxn(newctx, cnEngine, cnTxnClient, "runTxnWithSqlContext")
	if err != nil {
		return err
	}

	sqlproc := NewSqlProcessWithContext(NewSqlContext(newctx, cnUUID, txnOp, accountId))
	err = f(sqlproc)
	if err != nil {
		err = errors.Join(err, txnOp.Rollback(sqlproc.GetContext()))
	} else {
		err = txnOp.Commit(sqlproc.GetContext())
	}
	return
}
