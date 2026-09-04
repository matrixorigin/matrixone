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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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
	// True when the runtime-filter producer sent an exact key set. It is
	// separate from the payload because an empty set is semantically different
	// from no filter (RF PASS).
	IvfHasMembershipFilter bool
	// Optional exact primary-key filter list (SQL literals, comma-separated).
	// When set, the legacy SQL search adapter uses it to build "pk IN (...)".
	ExactPkFilter string
	// Optional IndexReaderParam attached by vector index runtime.
	// Used to drive additional filtering in internal SQL executor (e.g. ivf entries scan).
	IndexReaderParam *plan.IndexReaderParam

	// RelationScanner is installed by VECTOR_INDEX_SCAN. Query-time index
	// reads use it instead of generating SQL and invoking a nested planner.
	RelationScanner RelationScanExecutor

	// Optional trusted execution identity for planner-generated internal SQL.
	// SQL/table-function arguments must never populate these fields.
	AccountIDOverride *uint32
	DatabaseOverride  string

	// Optional named-snapshot read timestamp. When set (and historical), the
	// internal SQL runs against a txn cloned at this TS, so index-table reads
	// return the snapshot's historical state instead of the current one
	// (fulltext/fulltext2 MATCH on a named snapshot, #27941). nil => current TS.
	SnapshotTS *timestamp.Timestamp
}

// EffectiveSnapshotTS returns the historical read timestamp this SqlProcess will
// actually time-travel to -- i.e. the TS txnForRun clones the read txn at -- or nil
// when the read runs at the current txn (no SnapshotTS, an empty TS, or a TS not
// earlier than the current one). It is the SINGLE source of truth for "is this a
// historical read": any caller that keys a cache by the snapshot (e.g. the
// fulltext2 TS-suffixed cache key) MUST derive that key from this, so the key can
// never disagree with the clone decision. A disagreement would cache a historical
// index under the current key and serve it to current queries (#27941).
func (s *SqlProcess) EffectiveSnapshotTS() *timestamp.Timestamp {
	if s.SnapshotTS == nil || s.Proc == nil {
		return nil
	}
	txnOp := s.Proc.GetTxnOperator()
	if txnOp == nil {
		return nil
	}
	ts := *s.SnapshotTS
	if ts.IsEmpty() || !ts.Less(txnOp.Txn().SnapshotTS) {
		return nil
	}
	return s.SnapshotTS
}

// BuildSnapshotTS returns the transaction SnapshotTS (physical) this process reads at -- the
// base-table version an index generation built here reflects, recorded in the metadata's build_ts.
//
// It is deliberately NOT the wall clock the metadata's "timestamp" column carries: that one only
// orders generations, is skewable across CNs, and cannot be compared against a named snapshot's
// TS to decide whether a generation actually covers the data a {snapshot = ...} read wants.
//
// 0 when there is no transaction to ask, which readers treat as unknown.
func (s *SqlProcess) BuildSnapshotTS() int64 {
	if s == nil {
		return 0
	}
	var op client.TxnOperator
	switch {
	case s.Proc != nil:
		op = s.Proc.GetTxnOperator()
	case s.SqlCtx != nil:
		op = s.SqlCtx.TxnOperator
	}
	if op == nil {
		return 0
	}
	return op.Txn().SnapshotTS.PhysicalTime
}

// ApplyScanSnapshot threads a planner-resolved named snapshot onto this SqlProcess and returns
// the effective historical read timestamp, or nil when the snapshot is not historical relative
// to the current txn -- in which case nothing is bound and the read proceeds as an ordinary
// current-state read.
//
// It carries BOTH halves of the snapshot identity, which is the reason this is one helper rather
// than a field assignment at each call site: the timestamp, so index-table reads run on a txn
// cloned at it, and the snapshot's owning TENANT, so those reads resolve under the account that
// owns the data. Binding the timestamp alone is a correctness bug for a cross-account snapshot:
// an account-level snapshot carries Tenant.TenantID = the snapshot's account (see
// planSnapshotFromRecord), so a sys session reading acc1's snapshot would scan the base table as
// acc1 while resolving __mo_index_secondary_... as account 0 -- table-not-found, or silently
// empty results. The compile layer binds the same pair under the same condition; see the
// ScanSnapshot branch in Compile's table-scan path.
//
// snap comes from the PLANNER (plan.TableFunction.ScanSnapshot), never from a table-function
// argument, which is what makes setting the trusted AccountIDOverride here legitimate.
func (s *SqlProcess) ApplyScanSnapshot(snap *plan.Snapshot) *timestamp.Timestamp {
	if snap == nil {
		return nil
	}
	s.SnapshotTS = snap.TS
	ets := s.EffectiveSnapshotTS()
	if ets == nil {
		return nil
	}
	if snap.Tenant != nil {
		id := snap.Tenant.TenantID
		s.AccountIDOverride = &id
	}
	return ets
}

// txnForRun returns the txn operator the internal SQL should run under: a clone
// pinned at the historical snapshot TS when EffectiveSnapshotTS reports one, else
// the process's current txn.
func (s *SqlProcess) txnForRun(proc *process.Process) client.TxnOperator {
	txnOp := proc.GetTxnOperator()
	if ets := s.EffectiveSnapshotTS(); ets != nil && txnOp != nil {
		return txnOp.CloneSnapshotOp(*ets)
	}
	return txnOp
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

// EffectiveAccountID is the account this SqlProcess actually EXECUTES as: the snapshot's
// owning tenant when ApplyScanSnapshot bound one, else the caller's own account.
//
// GetAccountID reads the original process context and therefore answers "who asked", which is
// the wrong owner for a cross-account snapshot read: SYS reading tenant 42's index runs its
// index-table SQL as 42 (executionContext / executionStatementOption both honour the override),
// so anything attributing the resulting resident state -- the cache's byte governor -- must
// attribute it to 42 as well, not to 0.
func (s *SqlProcess) EffectiveAccountID() (uint32, error) {
	if s == nil {
		return 0, moerr.NewInternalErrorNoCtx("nil SqlProcess")
	}
	if s.AccountIDOverride != nil {
		return *s.AccountIDOverride, nil
	}
	return s.GetAccountID()
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
			WithTxn(sqlproc.txnForRun(proc)).
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
			WithTxn(sqlproc.txnForRun(proc)).
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
			WithTxn(sqlproc.txnForRun(proc)).
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
