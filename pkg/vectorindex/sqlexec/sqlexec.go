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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// SqlContext stores required information for background SQLInternalExecutor
type SqlContext struct {
	Ctx                 context.Context
	CNUuid              string
	TxnOperator         client.TxnOperator
	AccountId           uint32
	ResolveVariableFunc func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
}

func NewSqlContext(ctx context.Context,
	cnuuid string,
	txnOperator client.TxnOperator,
	accountid uint32,
	resolveVariableFunc func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error),
) *SqlContext {
	return &SqlContext{Ctx: ctx, CNUuid: cnuuid, TxnOperator: txnOperator, AccountId: accountid, ResolveVariableFunc: resolveVariableFunc}
}

func (s *SqlContext) GetService() string {
	return s.CNUuid
}

func (s *SqlContext) Txn() client.TxnOperator {
	return s.TxnOperator
}

func (s *SqlContext) GetResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return s.ResolveVariableFunc
}

func (s *SqlContext) SetResolveVariableFunc(f func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)) {
	s.ResolveVariableFunc = f
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

	// Optional RuntimeFilterSpec
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	// Optional doc_id membership-filter bytes (tagged docfilter payload) for the ivf entries scan.
	IvfMembershipFilter []byte
	// Optional doc_id membership-filter bytes (tagged docfilter payload) for the fulltext index scan.
	FulltextMembershipFilter []byte

	// Optional raw runtime-filter payload from the build side for IVF probe path.
	// This contains serialized unique join keys and must be converted by IVF code
	// before it is exposed to entries table scans.
	IvfRuntimeFilterData []byte
	// Optional exact primary-key filter list (SQL literals, comma-separated).
	// When set, ivf_search uses it to build "pk IN (...)" and skip centroid filtering.
	ExactPkFilter string
	// Optional IndexReaderParam attached by vector index runtime.
	// Used to drive additional filtering in internal SQL executor (e.g. ivf entries scan).
	IndexReaderParam *plan.IndexReaderParam

	// Optional trusted execution identity for planner-generated internal SQL.
	// SQL/table-function arguments must never populate these fields.
	AccountIDOverride *uint32
	DatabaseOverride  string
}

func NewSqlProcess(proc *process.Process) *SqlProcess {
	return &SqlProcess{Proc: proc}
}

func NewSqlProcessWithContext(ctx *SqlContext) *SqlProcess {
	return &SqlProcess{SqlCtx: ctx}
}

func (s *SqlProcess) WithExecutionIdentity(accountID uint32, database string) *SqlProcess {
	s.AccountIDOverride = &accountID
	s.DatabaseOverride = database
	return s
}

func (s *SqlProcess) executionAccountID(defaultAccountID uint32) uint32 {
	if s.AccountIDOverride != nil {
		return *s.AccountIDOverride
	}
	return defaultAccountID
}

func (s *SqlProcess) executionDatabase(defaultDatabase string) string {
	if s.DatabaseOverride != "" {
		return s.DatabaseOverride
	}
	return defaultDatabase
}

func (s *SqlProcess) executionContext(ctx context.Context) context.Context {
	if s.AccountIDOverride != nil {
		return defines.AttachAccountId(ctx, *s.AccountIDOverride)
	}
	return ctx
}

func (s *SqlProcess) executionStatementOption() executor.StatementOption {
	option := executor.StatementOption{}.WithDisableLog()
	if s.AccountIDOverride != nil {
		option = option.WithAccountID(*s.AccountIDOverride)
	}
	return option
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

func (s *SqlProcess) GetResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if s.Proc != nil {
		return s.Proc.GetResolveVariableFunc()
	}
	if s.SqlCtx != nil {
		return s.SqlCtx.GetResolveVariableFunc()
	}
	return nil
}

// GetService returns the CN UUID (service name) of the underlying proc or SqlContext.
// Captured at load so a later background query (e.g. the cache IsStale check) can reach
// the internal SQL executor by service name.
func (s *SqlProcess) GetService() string {
	if s.Proc != nil {
		return s.Proc.GetService()
	}
	return s.SqlCtx.GetService()
}

// GetAccountID returns the tenant account id of the underlying proc or SqlContext.
func (s *SqlProcess) GetAccountID() (uint32, error) {
	if s.Proc != nil {
		return defines.GetAccountId(s.Proc.Ctx)
	}
	return s.SqlCtx.AccountId, nil
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
		topContext := sqlproc.executionContext(proc.GetTopContext())
		// Attach optional membership filter payload to context for internal executor.
		if len(sqlproc.IvfMembershipFilter) > 0 {
			topContext = context.WithValue(topContext, defines.IvfMembershipFilter{}, sqlproc.IvfMembershipFilter)
		}
		if len(sqlproc.FulltextMembershipFilter) > 0 {
			topContext = context.WithValue(topContext, defines.FulltextMembershipFilter{}, sqlproc.FulltextMembershipFilter)
		}
		// Attach optional DistRange to context for internal executor.
		if sqlproc.IndexReaderParam != nil {
			topContext = context.WithValue(topContext, defines.IvfReaderParam{}, sqlproc.IndexReaderParam)
		}
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return executor.Result{}, err
		}
		//-------------------------------------------------------

		accountId = sqlproc.executionAccountID(accountId)
		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithDatabase(sqlproc.executionDatabase(proc.GetSessionInfo().Database)).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithAccountID(accountId).
			WithResolveVariableFunc(proc.GetResolveVariableFunc()).
			WithFrontend(proc.Base.IsFrontend).
			WithStatementOption(sqlproc.executionStatementOption())
		return exec.Exec(topContext, sql, opts)
	} else {

		sqlctx := sqlproc.SqlCtx
		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlproc.executionAccountID(sqlctx.AccountId)
		execCtx := sqlproc.executionContext(sqlctx.Ctx)

		exec := v.(executor.SQLExecutor)
		// SqlCtx is the background entry point (no frontend session) —
		// inherits the default IsFrontend=false (i.e. background).
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithDatabase(sqlproc.executionDatabase("")).
			WithAccountID(accountId).
			WithResolveVariableFunc(sqlctx.GetResolveVariableFunc()).
			WithStatementOption(sqlproc.executionStatementOption())
		return exec.Exec(execCtx, sql, opts)

	}
}

// RunSqlAutoCommit runs a read-only SQL in a BACKGROUND context with an executor-managed
// (auto-commit) txn — no caller sqlproc/txn required, only the CN UUID + tenant. The
// internal SQL executor holds the engine/txnClient, so omitting WithTxn makes it
// create+commit its own txn (Options.ExistsTxn()==false). Used by the vector-index cache's
// IsStale freshness check, which runs on the housekeeping goroutine long after the loading
// query's txn is gone; it captures cnUUID+accountID at load and re-queries here. The caller
// owns ctx (deadline/cancel) and must Close the returned Result.
func RunSqlAutoCommit(ctx context.Context, cnUUID string, accountID uint32, db, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("RunSqlAutoCommit: missing internal sql executor")
	}
	exec := v.(executor.SQLExecutor)
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithAccountID(accountID).
		WithStatementOption(executor.StatementOption{}.WithDisableLog())
	if db != "" {
		opts = opts.WithDatabase(db)
	}
	return exec.Exec(ctx, sql, opts)
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
		ctx = sqlproc.executionContext(ctx)
		// Attach optional membership filter payload to context for internal executor.
		if len(sqlproc.IvfMembershipFilter) > 0 {
			ctx = context.WithValue(ctx, defines.IvfMembershipFilter{}, sqlproc.IvfMembershipFilter)
		}
		if len(sqlproc.FulltextMembershipFilter) > 0 {
			ctx = context.WithValue(ctx, defines.FulltextMembershipFilter{}, sqlproc.FulltextMembershipFilter)
		}
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return executor.Result{}, err
		}
		//-------------------------------------------------------
		accountId = sqlproc.executionAccountID(accountId)
		exec := v.(executor.SQLExecutor)
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithDatabase(sqlproc.executionDatabase(proc.GetSessionInfo().Database)).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithAccountID(accountId).
			WithStreaming(stream_chan, error_chan).
			WithResolveVariableFunc(proc.GetResolveVariableFunc()).
			WithFrontend(proc.Base.IsFrontend).
			WithStatementOption(sqlproc.executionStatementOption())
		return exec.Exec(ctx, sql, opts)
	} else {

		sqlctx := sqlproc.SqlCtx

		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlproc.executionAccountID(sqlctx.AccountId)
		ctx = sqlproc.executionContext(ctx)

		exec := v.(executor.SQLExecutor)
		// SqlCtx is the background entry point (no frontend session) —
		// inherits the default IsFrontend=false (i.e. background).
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithDatabase(sqlproc.executionDatabase("")).
			WithAccountID(accountId).
			WithStreaming(stream_chan, error_chan).
			WithResolveVariableFunc(sqlctx.GetResolveVariableFunc()).
			WithStatementOption(sqlproc.executionStatementOption())
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
			WithAccountID(accountId).
			WithResolveVariableFunc(proc.GetResolveVariableFunc()).
			WithFrontend(proc.Base.IsFrontend)
		return exec.ExecTxn(topContext, execFunc, opts)
	} else {

		sqlctx := sqlproc.SqlCtx
		v, ok := moruntime.ServiceRuntime(sqlctx.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		if !ok {
			panic("missing lock service")
		}

		accountId := sqlctx.AccountId

		exec := v.(executor.SQLExecutor)
		// SqlCtx is the background entry point (no frontend session) —
		// inherits the default IsFrontend=false (i.e. background).
		opts := executor.Options{}.
			// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
			// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
			WithDisableIncrStatement().
			WithTxn(sqlctx.Txn()).
			WithAccountID(accountId).
			WithResolveVariableFunc(sqlctx.GetResolveVariableFunc())
		return exec.ExecTxn(sqlctx.Ctx, execFunc, opts)
	}
}

func GetTxn(
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
	resolveVariableFunc func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error),
	cbdata any,
	f func(sqlproc *SqlProcess, data any) error) (err error) {

	newctx := context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()

	txnOp, err := GetTxn(newctx, cnEngine, cnTxnClient, "runTxnWithSqlContext")
	if err != nil {
		return err
	}

	sqlproc := NewSqlProcessWithContext(NewSqlContext(newctx, cnUUID, txnOp, accountId, resolveVariableFunc))
	err = f(sqlproc, cbdata)
	return finishTxnWithCleanupContext(accountId, err, txnOp.Commit, txnOp.Rollback)
}

func finishTxnWithCleanupContext(
	accountId uint32,
	err error,
	commit func(context.Context) error,
	rollback func(context.Context) error,
) error {
	cleanupCtx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	cleanupCtx, cleanupCancel := context.WithTimeout(cleanupCtx, time.Minute)
	defer cleanupCancel()
	if err != nil {
		return errors.Join(err, rollback(cleanupCtx))
	}
	return commit(cleanupCtx)
}
