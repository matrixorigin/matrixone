// Copyright 2026 Matrix Origin
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

package function

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// txnIdentity matches a transaction operator argument by object identity.
// gomock's default Eq falls back to reflect.DeepEqual, under which two mock
// transaction operators built by the same controller compare equal (the
// shared controller and the recorder back-references make the deep
// comparison converge to true); the routing assertions must tell the caller
// transaction and the autonomous attempt's transaction apart.
type txnIdentity struct{ txn client.TxnOperator }

func (m txnIdentity) Matches(x interface{}) bool { return x == m.txn }
func (m txnIdentity) String() string             { return "is the expected transaction object" }

// These tests pin the sequence allocation routing of design
// docs/design/sequence_ownership_state_consistency.md §4 D: every nextval /
// setval resolves the sequence relation inside the caller transaction, probes
// committed visibility through a fresh autonomous transaction, and then
// either allocates/repositions through that autonomous transaction
// (publication only after its commit) or falls back to the caller
// transaction under the pass-scoped obligation protocol (§9 D/K1/J1/J2
// fake-executor oracles).

// seqMetadataState is the fake sequence data row behind the fake internal
// executor. Autonomous attempts reach it through ExecTxn, the
// transaction-local route through Exec; the state machine is route-agnostic.
type seqMetadataState struct {
	mu      sync.Mutex
	last    int64
	minv    int64
	maxv    int64
	start   int64
	incr    int64
	cycle   bool
	called  bool
	selects int
	updates []string
}

var seqRowColumnTypes = []types.Type{
	types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	types.T_int64.ToType(), types.T_int64.ToType(), types.T_bool.ToType(),
	types.T_bool.ToType(),
}

func newSeqMetadataState(last, minv, maxv, start, incr int64, cycle, called bool) *seqMetadataState {
	return &seqMetadataState{last: last, minv: minv, maxv: maxv, start: start, incr: incr, cycle: cycle, called: called}
}

// rewind restores the row to an earlier position, simulating a statement-level
// rollback (RollbackLastStatement) truncating the caller workspace's counter
// writes while emitted values survive on the client.
func (s *seqMetadataState) rewind(last int64, called bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last, s.called = last, called
}

// execSQL serves exactly the two SQL shapes the sequence functions issue:
// the 7-column metadata SELECT ... FOR UPDATE and the counter UPDATE.
func (s *seqMetadataState) execSQL(mp *mpool.MPool, sql string) (executor.Result, error) {
	normalized := strings.ToLower(strings.TrimSpace(sql))
	if strings.HasPrefix(normalized, "select last_seq_num") {
		s.mu.Lock()
		s.selects++
		last, minv, maxv, start, incr := s.last, s.minv, s.maxv, s.start, s.incr
		cycle, called := s.cycle, s.called
		s.mu.Unlock()
		mem := executor.NewMemResult(seqRowColumnTypes, mp)
		mem.NewBatchWithRowCount(1)
		for col, values := range []any{
			[]int64{last}, []int64{minv}, []int64{maxv}, []int64{start}, []int64{incr},
			[]bool{cycle}, []bool{called},
		} {
			switch v := values.(type) {
			case []int64:
				if err := executor.AppendFixedRows(mem, col, v); err != nil {
					return executor.Result{}, err
				}
			case []bool:
				if err := executor.AppendFixedRows(mem, col, v); err != nil {
					return executor.Result{}, err
				}
			}
		}
		return mem.GetResult(), nil
	}
	if strings.HasPrefix(normalized, "update ") {
		setIdx := strings.Index(normalized, " set ")
		if setIdx < 0 {
			return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, piece := range strings.Split(normalized[setIdx+len(" set "):], ",") {
			kv := strings.SplitN(strings.TrimSpace(piece), "=", 2)
			if len(kv) != 2 {
				return executor.Result{}, fmt.Errorf("unexpected sequence assignment %q: %s", piece, sql)
			}
			switch key, rawVal := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1]); key {
			case "last_seq_num":
				value, err := strconv.ParseInt(rawVal, 10, 64)
				if err != nil {
					return executor.Result{}, err
				}
				s.last = value
			case "is_called":
				s.called = rawVal == "true"
			default:
				return executor.Result{}, fmt.Errorf("unexpected sequence assignment %q: %s", piece, sql)
			}
		}
		s.updates = append(s.updates, normalized)
		return executor.Result{}, nil
	}
	return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
}

func (s *seqMetadataState) recordedUpdates() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.updates...)
}

func (s *seqMetadataState) selectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selects
}

// seqRouteExec fakes the internal SQL executor for the routing design.
// ExecTxn opens one fresh committed transaction per attempt (autoTxn) and
// lets the test own the commit outcome per attempt: beforeCommit is a
// barrier point between the attempt body and the commit decision, and
// attemptOutcome decides the commit result (nil commit, non-retryable abort,
// retryable/ambiguous failure). Exec serves the caller transaction
// (transaction-local route); inspectExec observes its options.
// inspectTxnCtx observes the exact context handed to ExecTxn (the bounded
// retry wrapper must propagate its budget deadline into execution, not only
// check it between attempts), and blockOnCtx makes the attempt itself block
// until that context ends — an in-flight-blocked attempt oracle.
//
// Expiry-probe transactions (the decision's tuple round and lock checks,
// the baseline samples — all marked by markProbeCtx) are counted apart from
// allocation attempts so a decision can never be mistaken for an attempt,
// never block on blockOnCtx, and probeLockHeld fails only THEIR for-update
// read. allocLockBlocked fails the allocation attempt's OWN for-update read
// with the honest lock-wait-abort shape: the body dies at its first SELECT,
// so the committed row behind the fixture stays exactly as the test armed
// it (an attempt aborted by a real lock wait never applies its UPDATE).
type seqRouteExec struct {
	autoTxn          *mock_frontend.MockTxnOperator
	state            *seqMetadataState
	mp               *mpool.MPool
	beforeCommit     func(attempt int)
	attemptOutcome   func(attempt int) error
	inspectExec      func(opts executor.Options)
	inspectTxnCtx    func(ctx context.Context)
	blockOnCtx       bool
	probeLockHeld    bool
	allocLockBlocked bool
	attempts         int
	probeAttempts    int
	execs            int
}

var _ executor.SQLExecutor = (*seqRouteExec)(nil)

func (e *seqRouteExec) Exec(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.execs++
	if e.inspectExec != nil {
		e.inspectExec(opts)
	}
	return e.state.execSQL(e.mp, sql)
}

func (e *seqRouteExec) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	if ctx.Value(seqProbeCtxMarkKey{}) != nil {
		e.probeAttempts++
		te := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			if e.probeLockHeld && strings.Contains(strings.ToLower(sql), "for update") {
				return executor.Result{}, moerr.NewLockWaitTimeoutNoCtx()
			}
			return e.state.execSQL(e.mp, sql)
		}, e.autoTxn)
		return fn(te)
	}
	e.attempts++
	if e.inspectTxnCtx != nil {
		e.inspectTxnCtx(ctx)
	}
	if e.blockOnCtx {
		<-ctx.Done()
		return ctx.Err()
	}
	te := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if e.allocLockBlocked && strings.Contains(strings.ToLower(sql), "for update") {
			return executor.Result{}, moerr.NewLockWaitTimeoutNoCtx()
		}
		return e.state.execSQL(e.mp, sql)
	}, e.autoTxn)
	if err := fn(te); err != nil {
		return err
	}
	if e.beforeCommit != nil {
		e.beforeCommit(e.attempts)
	}
	if e.attemptOutcome != nil {
		return e.attemptOutcome(e.attempts)
	}
	return nil
}

// seqRouteFixture wires one test process to a mock engine exposing the same
// sequence relation twice: once through the caller transaction and once
// through the autonomous attempt's own transaction. The routing contract
// compares the committed-attempt database and relation ids against the IDs
// the caller resolved, so the fixture pins one committed database id; the
// shadow/stale oracles override the attempt-side ids.
type seqRouteFixture struct {
	proc      *process.Process
	eng       *mock_frontend.MockEngine
	callerDB  *mock_frontend.MockDatabase
	callerRel *mock_frontend.MockRelation
	callerTxn *mock_frontend.MockTxnOperator
	autoTxn   *mock_frontend.MockTxnOperator
	autoDB    *mock_frontend.MockDatabase
	autoRel   *mock_frontend.MockRelation
	exec      *seqRouteExec
	state     *seqMetadataState
	relID     uint64
	dbID      string
}

func sequenceRelDef() []engine.TableDef {
	return []engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
	}
}

func newSeqRouteFixture(t *testing.T, relID uint64, state *seqMetadataState) *seqRouteFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	callerDB := mock_frontend.NewMockDatabase(ctrl)
	callerRel := mock_frontend.NewMockRelation(ctrl)
	autoDB := mock_frontend.NewMockDatabase(ctrl)
	autoRel := mock_frontend.NewMockRelation(ctrl)
	callerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	autoTxn := mock_frontend.NewMockTxnOperator(ctrl)

	proc := testutil.NewProcess(t)
	proc.InitSeq()
	proc.Base.SessionInfo.Database = "db"
	proc.Base.SessionInfo.SeqObligations = process.NewSequenceObligationSet()
	proc.Base.TxnOperator = callerTxn
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)

	exec := &seqRouteExec{autoTxn: autoTxn, state: state, mp: proc.Mp()}
	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		rt.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, exec)
		if hadPrevious {
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
		}
	})

	// Caller-transaction resolution happens on every call, on either route.
	eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{callerTxn}).Return(callerDB, nil).AnyTimes()
	callerDB.EXPECT().Relation(gomock.Any(), "seq", nil).Return(callerRel, nil).AnyTimes()
	callerDB.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid-1").AnyTimes()
	callerRel.EXPECT().TableDefs(gomock.Any()).Return(sequenceRelDef(), nil).AnyTimes()
	callerRel.EXPECT().GetTableID(gomock.Any()).Return(relID).AnyTimes()

	return &seqRouteFixture{
		proc: proc, eng: eng, callerDB: callerDB, callerRel: callerRel, callerTxn: callerTxn,
		autoTxn: autoTxn, autoDB: autoDB, autoRel: autoRel,
		exec: exec, state: state, relID: relID, dbID: "dbid-1",
	}
}

// expectCommittedRoute admits the autonomous attempt's resolution through its
// own fresh transaction: the caller-resolved identity is visible in committed
// state (same committed database id, same relation id).
func (f *seqRouteFixture) expectCommittedRoute() {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
	f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return(f.dbID).AnyTimes()
	f.autoDB.EXPECT().Relation(gomock.Any(), "seq", nil).Return(f.autoRel, nil).AnyTimes()
	f.autoRel.EXPECT().TableDefs(gomock.Any()).Return(sequenceRelDef(), nil).AnyTimes()
	f.autoRel.EXPECT().GetTableID(gomock.Any()).Return(f.relID).AnyTimes()
}

// expectCommittedNonSequence makes the autonomous attempt's identity
// revalidation fail: the committed relation is not a sequence.
func (f *seqRouteFixture) expectCommittedNonSequence() {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
	f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return(f.dbID).AnyTimes()
	f.autoDB.EXPECT().Relation(gomock.Any(), "seq", nil).Return(f.autoRel, nil).AnyTimes()
	f.autoRel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
	}, nil).AnyTimes()
	f.autoRel.EXPECT().GetTableID(gomock.Any()).Return(f.relID).AnyTimes()
}

// expectLocalDatabaseMiss fails the committed-visibility probe at the
// database lookup, sending the call down the transaction-local route.
func (f *seqRouteFixture) expectLocalDatabaseMiss(probeErr error) {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(nil, probeErr).AnyTimes()
}

// expectLocalRelationMiss fails the committed-visibility probe at the
// relation lookup, sending the call down the transaction-local route.
func (f *seqRouteFixture) expectLocalRelationMiss(probeErr error) {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
	f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return(f.dbID).AnyTimes()
	f.autoDB.EXPECT().Relation(gomock.Any(), "seq", nil).Return(nil, probeErr).AnyTimes()
}

// expectWorkspaceShadowedDatabase matches the shape of an uncommitted
// DROP+CLONE shadow: the committed view still holds the OLD database
// generation (different id), so the caller's identity is transaction-local
// even though the name resolves in committed state.
func (f *seqRouteFixture) expectWorkspaceShadowedDatabase() {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
	f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid-old-committed").AnyTimes()
}

// expectStaleCommittedRelation resolves the same committed database but a
// DIFFERENT committed relation id: the generation changed underneath the
// caller's resolution (a committed drop+recreate between resolution and the
// attempt). Routing must retry bounded with re-resolution and must never
// send this committed-generation shape to the in-caller route.
func (f *seqRouteFixture) expectStaleCommittedRelation() {
	f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
	f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return(f.dbID).AnyTimes()
	f.autoDB.EXPECT().Relation(gomock.Any(), "seq", nil).Return(f.autoRel, nil).AnyTimes()
	f.autoRel.EXPECT().GetTableID(gomock.Any()).Return(f.relID + 5000).AnyTimes()
}

func nextvalRow(proc *process.Process) (string, error) {
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 1, proc.Mp())
	if err != nil {
		return "", err
	}
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	if err := result.PreExtendAndReset(1); err != nil {
		return "", err
	}
	if err := Nextval([]*vector.Vector{input}, result, proc, 1, nil); err != nil {
		return "", err
	}
	return string(result.GetResultVector().GetBytesAt(0)), nil
}

func nextvalRows(proc *process.Process, n int) ([]string, error) {
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), n, proc.Mp())
	if err != nil {
		return nil, err
	}
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	if err := result.PreExtendAndReset(n); err != nil {
		return nil, err
	}
	if err := Nextval([]*vector.Vector{input}, result, proc, n, nil); err != nil {
		return nil, err
	}
	rv := result.GetResultVector()
	outs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		outs = append(outs, string(rv.GetBytesAt(i)))
	}
	return outs, nil
}

func setvalRow(proc *process.Process, num string, iscalled bool) (string, error) {
	table, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 1, proc.Mp())
	if err != nil {
		return "", err
	}
	defer table.Free(proc.Mp())
	value, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(num), 1, proc.Mp())
	if err != nil {
		return "", err
	}
	defer value.Free(proc.Mp())
	called, err := vector.NewConstFixed[bool](types.T_bool.ToType(), iscalled, 1, proc.Mp())
	if err != nil {
		return "", err
	}
	defer called.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	if err := result.PreExtendAndReset(1); err != nil {
		return "", err
	}
	if err := Setval([]*vector.Vector{table, value, called}, result, proc, 1, nil); err != nil {
		return "", err
	}
	return string(result.GetResultVector().GetBytesAt(0)), nil
}

func requireObligation(t *testing.T, proc *process.Process, relID uint64, passStart, floor uint64, minus bool) {
	t.Helper()
	ob, ok := proc.GetSequenceObligationSet().Get(relID)
	require.True(t, ok, "expected a pass-scoped obligation for relation %d", relID)
	require.Equal(t, relID, ob.RelID)
	require.Equal(t, "db", ob.Db)
	require.Equal(t, "seq", ob.Table)
	require.Equal(t, passStart, ob.PassStart)
	require.Equal(t, floor, ob.Floor)
	require.False(t, ob.Unsigned)
	require.Equal(t, minus, ob.Minus)
}

// Publication matrix, success arm (design §4 D / §9 D): the emission leaves
// the session cache untouched until the autonomous transaction commits; on
// commit it is replicated to the session cache (SeqAddValues) and to the
// caller-visible last value (SeqLastValue) inside the gate interval.
func TestNextvalAutonomousPublishesAfterCommit(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	bodyDone := make(chan struct{})
	release := make(chan struct{})
	var bodyOnce sync.Once
	f.exec.beforeCommit = func(int) {
		bodyOnce.Do(func() { close(bodyDone) })
		<-release
	}

	type outcome struct {
		value string
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := nextvalRow(f.proc)
		done <- outcome{value: value, err: err}
	}()
	select {
	case <-bodyDone:
	case <-time.After(time.Second):
		t.Fatal("autonomous nextval did not reach its commit point")
	}
	// The allocation is computed and written but the autonomous transaction
	// has not committed: no session publication may leak ahead of it.
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
	select {
	case out := <-done:
		t.Fatalf("nextval published before the autonomous commit: %+v", out)
	default:
	}
	close(release)

	var out outcome
	select {
	case out = <-done:
	case <-time.After(time.Second):
		t.Fatal("autonomous nextval did not finish after commit release")
	}
	require.NoError(t, out.err)
	require.Equal(t, "1", out.value)
	require.Equal(t, "1", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
	require.Equal(t, "1", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, 1, f.exec.attempts)
	require.Zero(t, f.exec.execs, "the autonomous route runs no caller-transaction metadata SQL")
	require.Equal(t, []string{"update `db`.`seq` set is_called = true"}, f.state.recordedUpdates())
}

// Publication matrix, definite-abort arm: an aborted autonomous transaction
// publishes nothing, is not retried, and leaves the gate usable (defer
// cleanup on the error path).
func TestNextvalAutonomousAbortPublishesNothing(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	abortErr := errors.New("testing: autonomous commit abort")
	f.exec.attemptOutcome = func(int) error { return abortErr }

	value, err := nextvalRow(f.proc)
	require.ErrorIs(t, err, abortErr)
	require.Empty(t, value)
	require.Equal(t, 1, f.exec.attempts, "a definite abort is not retryable")
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])

	release, gateErr := f.proc.AcquireSequence(context.Background())
	require.NoError(t, gateErr, "the sequence gate must be released after a failed call")
	release()
}

// Publication matrix, ambiguous-commit arm: attempt 1 commits ambiguously
// (its update does land), so attempt 2 re-reads through its own fresh
// committed view and the re-verified state decides the published emission.
// The ambiguous attempt's value is a deliberate gap — never published.
func TestNextvalAutonomousAmbiguousCommitGap(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	f.exec.attemptOutcome = func(attempt int) error {
		if attempt == 1 {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		return nil
	}

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "2", value, "the fresh committed probe observes attempt 1's landed update")
	require.Equal(t, 2, f.exec.attempts)
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, []string{
		"update `db`.`seq` set is_called = true",
		"update `db`.`seq` set last_seq_num = 2",
	}, f.state.recordedUpdates())
}

// Classification must see through the internal executor's wrapping:
// txnExecutor.rollback returns errors.Join(attemptErr, rollbackErr), whose
// dynamic type defeats moerr.IsMoErrCode's direct assertion (the observed
// 2-CN contention failure mode — a raw 20628 leaking to the client instead
// of engaging the bounded retry).
func TestSeqErrClassifiersTraverseExecutorJoin(t *testing.T) {
	conflict := errors.Join(moerr.NewTxnNeedRetryNoCtx(), nil)
	require.True(t, retryableSeqTxnErr(conflict))
	require.True(t, retryableSeqTxnErr(
		errors.Join(moerr.NewTxnNeedRetryWithDefChangedNoCtx(), errors.New("testing: rollback note"))))
	require.True(t, retryableSeqTxnErr(
		fmt.Errorf("attempt: %w", moerr.NewLockWaitTimeoutNoCtx())))

	pinned := errors.Join(moerr.NewInternalErrorNoCtx("testing: range exhausted"), nil)
	require.False(t, retryableSeqTxnErr(pinned))
	require.False(t, retryableSeqTxnErr(
		errors.Join(errors.New("testing: plain abort"), nil)))

	require.True(t, isSeqNotVisibleErr(errors.Join(moerr.NewNoSuchTableNoCtx("db", "s"), nil)))
	require.True(t, isSeqNotVisibleErr(fmt.Errorf("probe: %w", moerr.GetOkExpectedEOB())))
	require.False(t, isSeqNotVisibleErr(errors.Join(moerr.NewTxnNeedRetryNoCtx(), nil)))
}

// Ambiguous-commit arm with the executor's real join wrapping: attempt 1's
// conflict lands joined with the rollback outcome and must still engage the
// bounded retry, exactly as the live 2-CN 16-client path requires.
func TestNextvalAutonomousJoinedConflictRetries(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	f.exec.attemptOutcome = func(attempt int) error {
		if attempt == 1 {
			return errors.Join(moerr.NewTxnNeedRetryNoCtx(), nil)
		}
		return nil
	}

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "2", value, "the fresh committed probe observes attempt 1's landed update")
	require.Equal(t, 2, f.exec.attempts)
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
}

// Retry exhaustion is bounded and surfaces as one named error carrying the
// sequence identity; there is no unbounded loop and no publication. This
// shape runs WITHOUT a per-statement tier-2 retry state, which is exactly
// the design's default-deny envelope (background/internal statements), so
// it pins that no conversion happens there (tier-2 admission oracles live
// in seqfunc_tier2_test.go).
func TestNextvalAutonomousRetryExhaustionIsBounded(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	f.exec.attemptOutcome = func(int) error { return moerr.NewLockWaitTimeoutNoCtx() }

	_, err := nextvalRow(f.proc)
	require.ErrorContains(t, err,
		"sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
	require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
}

// Deadline oracle (v12-review P3): an in-flight attempt that returns past the
// budget deadline ends the loop with the named exhaustion error — the
// effective deadline forbids a second attempt, nothing is published, and the
// sequence gate is released for the next caller.
func TestNextvalAutonomousDeadlineStopsAfterBlockedAttempt(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	oldBudget := seqAutonomousBudgetDur
	seqAutonomousBudgetDur = 200 * time.Millisecond
	t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

	f.exec.attemptOutcome = func(int) error {
		time.Sleep(400 * time.Millisecond) // in-flight attempt blocks past the budget
		return moerr.NewTxnNeedRetryNoCtx()
	}

	_, err := nextvalRow(f.proc)
	// Even when the residue is a transient conflict, exhaustion is the
	// terminal named error (approved v12 §4 D/§10): escaping into a
	// compile-level retry would re-enter with a fresh budget, the rejected
	// unbounded retry.
	require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
	require.Equal(t, 1, f.exec.attempts, "the effective deadline must forbid a second attempt")
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])

	release, gateErr := f.proc.AcquireSequence(context.Background())
	require.NoError(t, gateErr, "the sequence gate must be released after the deadline-bound failure")
	release()
}

// Exhaustion surface matrix: the reviewed bound is terminal (approved v12
// §4 D/§10). Every residue — transient commit conflict, definition-changed,
// stale committed generation, lock-wait timeout, deadlock — exhausts into
// the named exhaustion error; escaping into a compile-level statement retry
// would re-enter the wrapper with a fresh budget per compiler retry, the
// unbounded retry §10 rejects. A stuck holder or self-deadlock fails, and a
// contention queue that outlasts the reviewed budget fails deterministically
// with the sequence named.
func TestNextvalAutonomousExhaustionSurfaceMatrix(t *testing.T) {
	cases := []struct {
		name       string
		residue    func() error
		assertCode func(t *testing.T, err error)
	}{
		{"transient-conflict", func() error { return moerr.NewTxnNeedRetryNoCtx() }, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		}},
		{"definition-changed", func() error { return moerr.NewTxnNeedRetryWithDefChangedNoCtx() }, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		}},
		{"lock-wait-timeout", func() error { return moerr.NewLockWaitTimeoutNoCtx() }, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		}},
		{"deadlock", func() error { return moerr.NewDeadLockDetectedNoCtx() }, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
			f.expectCommittedRoute()
			f.exec.attemptOutcome = func(int) error { return tc.residue() }

			_, err := nextvalRow(f.proc)
			require.Error(t, err)
			tc.assertCode(t, err)
			require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)
			require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
			require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
		})
	}
}

// Cancellation-during-backoff oracle (v12-review P3): after a retryable first
// attempt the caller context is canceled while the loop sleeps between
// attempts; the loop returns the context error, starts no further attempt,
// publishes nothing, and releases the sequence gate.
func TestNextvalAutonomousCancelDuringBackoff(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	oldStep := seqAutonomousBackoffStepDur
	seqAutonomousBackoffStepDur = func(int) time.Duration { return 5 * time.Second }
	t.Cleanup(func() { seqAutonomousBackoffStepDur = oldStep })

	ctx, cancel := context.WithCancel(f.proc.Ctx)
	f.proc.Ctx = ctx
	f.exec.attemptOutcome = func(int) error { return moerr.NewTxnNeedRetryNoCtx() }

	done := make(chan error, 1)
	go func() {
		_, err := nextvalRow(f.proc)
		done <- err
	}()
	time.Sleep(300 * time.Millisecond) // attempt 1 has returned; the loop sleeps in backoff
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation during backoff did not unblock the retry loop")
	}
	require.Equal(t, 1, f.exec.attempts, "no attempt may follow the canceled backoff")
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])

	release, gateErr := f.proc.AcquireSequence(context.Background())
	require.NoError(t, gateErr, "the sequence gate must be released after cancellation")
	release()
}

// Cancellation arm: a canceled caller context aborts the retry engine before
// any autonomous attempt and before any engine-side work.
func TestNextvalAutonomousCancelledContextSkipsAttempts(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	ctx, cancel := context.WithCancel(f.proc.Ctx)
	cancel()
	f.proc.Ctx = ctx

	_, err := nextvalRow(f.proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, f.exec.attempts)
	require.Zero(t, f.exec.execs)
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
}

// Identity revalidation inside the autonomous attempt (design §4 D): a
// committed relation that fails requireSequence is deterministic, fails
// without consuming retry budget, and publishes nothing.
func TestNextvalAutonomousIdentityRevalidationFailsOnce(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedNonSequence()

	_, err := nextvalRow(f.proc)
	require.EqualError(t, err, "internal error: Table input is not a sequence")
	require.Equal(t, 1, f.exec.attempts)
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
}

// Probe-miss → sentinel → local route (design §4 D, L1): the generation that
// is invisible to the committed probe allocates inside the caller
// transaction, and the pass-scoped obligation is raised at allocation time
// with floor = the first issued value, before the value leaves the function.
func TestNextvalTransactionLocalProbeMissRaisesObligation(t *testing.T) {
	run := func(t *testing.T, f *seqRouteFixture) {
		sawCallerTxn := false
		f.exec.inspectExec = func(opts executor.Options) {
			require.Same(t, f.proc.GetTxnOperator(), opts.Txn(),
				"the local route must execute inside the caller transaction")
			require.True(t, opts.DisableIncrStatement())
			require.True(t, opts.HasAccountID())
			require.Equal(t, uint32(42), opts.AccountID())
			sawCallerTxn = true
		}

		value, err := nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "10", value)
		require.True(t, sawCallerTxn)
		require.Equal(t, 1, f.exec.attempts, "the sentinel aborts autonomous retries immediately")
		require.Equal(t, 2, f.exec.execs, "one SELECT FOR UPDATE + one UPDATE in the caller transaction")
		requireObligation(t, f.proc, 7, 10, 10, false)
		require.Equal(t, "10", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
		require.Equal(t, "10", f.proc.GetSessionInfo().SeqLastValue[0])
		require.Equal(t, []string{"update `db`.`seq` set is_called = true"}, f.state.recordedUpdates())
	}

	t.Run("database probe miss ErrNoSuchTable", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
		f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))
		run(t, f)
	})
	t.Run("relation probe miss OkExpectedEOB", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
		f.expectLocalRelationMiss(moerr.GetOkExpectedEOB())
		run(t, f)
	})
}

// K1 oracle (design §4 D step 2): after a statement rewind reclaims the
// workspace counter, the next allocation repairs forward from the pass floor
// and never re-issues an emitted value of the current pass.
func TestNextvalLocalRouteRepairsStatementRewind(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
	f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "10", value)

	// Statement rollback: the workspace counter returns to the pre-allocation
	// state while the emitted 10 stands in the obligation.
	f.state.rewind(10, false)

	value, err = nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "13", value, "floor+step — the rewound emission 10 is never re-issued")
	requireObligation(t, f.proc, 7, 10, 13, false)
	require.Equal(t, "13", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
	require.Equal(t, []string{
		"update `db`.`seq` set is_called = true",
		"update `db`.`seq` set last_seq_num = 13, is_called = true",
	}, f.state.recordedUpdates())
}

// J1 oracle (design §4 D step 3): a descending CYCLE pass wraps deliberately;
// the pinned stream 100,60,100 is preserved on the local route, the wrap
// reseeds the obligation pass, and a rewind across the wrap re-wraps into a
// fresh pass rather than resurrecting the retired pass's floor.
func TestNextvalLocalRouteCycleWrapReseedsPass(t *testing.T) {
	// Pinned s1 shape from seq_func.result: increment -40, min 30, max 100.
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(100, 30, 100, 100, -40, true, false))
	f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

	var stream []string
	for i := 0; i < 3; i++ {
		value, err := nextvalRow(f.proc)
		require.NoError(t, err)
		stream = append(stream, value)
	}
	require.Equal(t, []string{"100", "60", "100"}, stream)
	requireObligation(t, f.proc, 7, 100, 100, true)

	// Rewind across the wrap: the reclaimed wrap re-opens a fresh pass whose
	// start-side value is re-emitted deliberately; the retired pass's values
	// stay re-issuable only through a wrap like this one.
	f.state.rewind(60, true)
	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "100", value)
	requireObligation(t, f.proc, 7, 100, 100, true)
	require.Equal(t, []string{
		"update `db`.`seq` set is_called = true",
		"update `db`.`seq` set last_seq_num = 60",
		"update `db`.`seq` set last_seq_num = 100",
		"update `db`.`seq` set last_seq_num = 100",
	}, f.state.recordedUpdates())
}

// Autonomous SETVAL splits routing identically to nextval and keeps the
// historical stored-called normalization: storedCalled = prevCalled ||
// !iscalled — the third argument can mark a fresh sequence called only by
// asking for it; it cannot reset an already-called one.
func TestSetvalAutonomousIsCalledNormalization(t *testing.T) {
	for _, test := range []struct {
		name       string
		prevCalled bool
		iscalled   bool
		wantStored bool
	}{
		{name: "called seq set called", prevCalled: true, iscalled: true, wantStored: true},
		{name: "fresh seq set called stays uncalled", prevCalled: false, iscalled: true, wantStored: false},
		{name: "called seq set uncalled keeps called", prevCalled: true, iscalled: false, wantStored: true},
		{name: "fresh seq set uncalled marks called", prevCalled: false, iscalled: false, wantStored: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, test.prevCalled))
			f.expectCommittedRoute()
			f.proc.GetSessionInfo().SeqLastValue[0] = "7"

			value, err := setvalRow(f.proc, "60", test.iscalled)
			require.NoError(t, err)
			require.Equal(t, "60", value)
			require.Equal(t, 1, f.exec.attempts)
			require.Equal(t, []string{fmt.Sprintf(
				"update `db`.`seq` set last_seq_num = 60, is_called = %t", test.wantStored)},
				f.state.recordedUpdates())
			if test.iscalled {
				require.Equal(t, "60", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
				require.Equal(t, "60", f.proc.GetSessionInfo().SeqLastValue[0])
			} else {
				require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
				require.Equal(t, "7", f.proc.GetSessionInfo().SeqLastValue[0],
					"an uncalled assignment publishes no session state")
			}
		})
	}

	t.Run("out of range fails without consuming retry budget", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, true))
		f.expectCommittedRoute()

		_, err := setvalRow(f.proc, "200", true)
		require.ErrorContains(t, err, "Set value is not in range")
		require.Equal(t, 1, f.exec.attempts)
		require.Empty(t, f.state.recordedUpdates())
		require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	})
}

// J2 oracle (design §4 D step 4): a successful SETVAL on a transaction-local
// generation is a deliberate boundary that clears the obligation instead of
// reseeding it; the repositioned-uncalled state then issues the assigned
// value exactly (the seq_func.result:49-58 shape on the local route). The
// boundary is staged at execution time and applied at the statement-success
// publication point, so a same-statement failure could still restore it.
func TestSetvalTransactionLocalBoundaryClearsObligation(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
	f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

	// A failed statement allocated 10 (obligation floor) and was rolled back:
	// the workspace is back to the uncalled start, the obligation stands.
	require.NoError(t, f.proc.GetSequenceObligationSet().Raise(process.SequenceObligation{
		RelID: 7, Db: "db", Table: "seq", PassStart: 10, Floor: 10,
	}))

	value, err := setvalRow(f.proc, "60", true)
	require.NoError(t, err)
	require.Equal(t, "60", value)
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 60, is_called = false"},
		f.state.recordedUpdates(), "a never-called sequence keeps is_called=false per the stored-called contract")
	_, ok := f.proc.GetSequenceObligationSet().Get(uint64(7))
	require.False(t, ok, "the staged boundary is hidden from enforcement immediately")
	require.Equal(t, "60", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])

	// Statement success: the publication point applies the staged boundary.
	f.proc.GetSequenceObligationSet().ApplyStaged()

	value, err = nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "60", value, "exactly the assigned value — no enforcement advance, no called-stamp")
}

// J2 statement-failure arm (v12-review F6): the SETVAL executed mid-statement
// and staged its boundary, then the statement failed. RollbackLastStatement
// rewinds the workspace (the reposition never persisted) and the statement
// rollback point restores the staged boundary, so the previously issued
// values stay protected and the next allocation repairs from the floor.
func TestSetvalLocalFailedStatementRestoresBoundary(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
	f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

	// First statement failed after issuing 10: obligation floor stands at 10
	// while the workspace counter is back at the uncalled start.
	require.NoError(t, f.proc.GetSequenceObligationSet().Raise(process.SequenceObligation{
		RelID: 7, Db: "db", Table: "seq", PassStart: 10, Floor: 10,
	}))

	// Second statement repositions successfully at execution time...
	value, err := setvalRow(f.proc, "60", true)
	require.NoError(t, err)
	require.Equal(t, "60", value)
	_, ok := f.proc.GetSequenceObligationSet().Get(uint64(7))
	require.False(t, ok, "the staged boundary hides the obligation while the statement runs")
	require.Empty(t, f.proc.GetSequenceObligationSet().Snapshot())

	// ...then fails: the workspace rewind reclaims the reposition, and the
	// staged boundary is restored with it.
	f.state.rewind(10, false)
	f.proc.GetSequenceObligationSet().RestoreStaged()
	requireObligation(t, f.proc, 7, 10, 10, false)

	value, err = nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "13", value, "the rewound SETVAL must not reclaim the issued 10")
	requireObligation(t, f.proc, 7, 10, 13, false)
	require.Equal(t, []string{
		"update `db`.`seq` set last_seq_num = 60, is_called = false",
		"update `db`.`seq` set last_seq_num = 13, is_called = true",
	}, f.state.recordedUpdates())
}

// J2 failed-SETVAL control: a statement-failed SETVAL leaves the obligation
// standing, so the next allocation repairs forward from the floor.
func TestSetvalLocalFailedBoundaryLeavesObligation(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
	f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

	require.NoError(t, f.proc.GetSequenceObligationSet().Raise(process.SequenceObligation{
		RelID: 7, Db: "db", Table: "seq", PassStart: 10, Floor: 10,
	}))

	_, err := setvalRow(f.proc, "200", true)
	require.ErrorContains(t, err, "Set value is not in range")
	requireObligation(t, f.proc, 7, 10, 10, false)

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "13", value, "floor+step — the failed SETVAL leaves the floor standing")
	requireObligation(t, f.proc, 7, 10, 13, false)
}

// Forward-floor invariant, no-prior-obligation arm (#30359 pre-PR review):
// one statement performs SETVAL then allocates (fresh obligation [60,60]),
// then FAILS. The workspace rewinds to the pre-statement pending position
// while the observed 60 stands in the obligation. The next allocation picks
// up the rewound candidate — never issued, so it may be emitted once — but
// it must NOT lower the floor: Extend widens the envelope to span both
// sides, so the following allocation catches the position walking back into
// issued values and repairs past the floor, and the commit stamp cannot
// persist the regressed position.
func TestNextvalLocalRewoundEmissionNeverLowersFloor(t *testing.T) {
	t.Run("ascending", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 1, false, false))
		f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

		value, err := setvalRow(f.proc, "60", true)
		require.NoError(t, err)
		require.Equal(t, "60", value)
		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "60", value)
		requireObligation(t, f.proc, 7, 60, 60, false)

		// The SETVAL+allocation statement failed: the workspace rewinds,
		// the staged boundary restores (a no-op — no pre-statement
		// obligation existed), and the observed 60 stays protected.
		f.state.rewind(10, false)
		f.proc.GetSequenceObligationSet().RestoreStaged()
		requireObligation(t, f.proc, 7, 60, 60, false)

		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "10", value, "the rewound position was never issued; it may be emitted once")
		// [10,60] — lowering the floor to 10 would collapse the envelope
		// (PassStart 60 ahead of Floor 10 protects nothing) and let later
		// allocations march back toward the observed 60.
		requireObligation(t, f.proc, 7, 10, 60, false)

		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "61", value,
			"the position walking back into the envelope repairs past the floor; 60 is never re-issued")
		requireObligation(t, f.proc, 7, 10, 61, false)

		// COMMIT continuation: without the widened envelope the surviving
		// (10, called) would persist at commit and the next transaction
		// would re-trace toward 60. The stamp decision reads it as unsafe.
		ob, ok := f.proc.GetSequenceObligationSet().Get(uint64(7))
		require.True(t, ok)
		require.True(t, ob.UnsafeToPreserve(10, true),
			"COMMIT restores past the floor instead of persisting the regressed position")

		require.Equal(t, []string{
			"update `db`.`seq` set last_seq_num = 60, is_called = false",
			"update `db`.`seq` set is_called = true",
			"update `db`.`seq` set last_seq_num = 10, is_called = true",
			"update `db`.`seq` set last_seq_num = 61, is_called = true",
		}, f.state.recordedUpdates())
	})

	t.Run("descending", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, -100, 100, 10, -1, false, false))
		f.expectLocalDatabaseMiss(moerr.NewNoSuchTableNoCtx("db", "seq"))

		value, err := setvalRow(f.proc, "2", true)
		require.NoError(t, err)
		require.Equal(t, "2", value)
		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "2", value)
		requireObligation(t, f.proc, 7, 2, 2, true)

		f.state.rewind(10, false)
		f.proc.GetSequenceObligationSet().RestoreStaged()
		requireObligation(t, f.proc, 7, 2, 2, true)

		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "10", value)
		requireObligation(t, f.proc, 7, 10, 2, true)

		value, err = nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "1", value,
			"repair past the descending floor; the observed 2 is never walked back to")
		requireObligation(t, f.proc, 7, 10, 1, true)

		ob, ok := f.proc.GetSequenceObligationSet().Get(uint64(7))
		require.True(t, ok)
		require.True(t, ob.UnsafeToPreserve(10, true),
			"descending COMMIT continuation: the rewound position is unsafe to persist")

		require.Equal(t, []string{
			"update `db`.`seq` set last_seq_num = 2, is_called = false",
			"update `db`.`seq` set is_called = true",
			"update `db`.`seq` set last_seq_num = 10, is_called = true",
			"update `db`.`seq` set last_seq_num = 1, is_called = true",
		}, f.state.recordedUpdates())
	})
}

// Workspace-shadow oracle (v12-review F4, DROP+CLONE inside BEGIN): the
// committed view resolves the name to a DIFFERENT database generation than
// the caller resolved. Routing by name existence would allocate from the old
// committed generation (and self-wait on the caller's own lifecycle locks);
// the database-id probe routes in-caller instead, keyed by identity.
func TestNextvalWorkspaceShadowedDatabaseRoutesLocal(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, false))
	f.expectWorkspaceShadowedDatabase()

	sawCallerTxn := false
	f.exec.inspectExec = func(opts executor.Options) {
		require.Same(t, f.proc.GetTxnOperator(), opts.Txn(),
			"the local route must execute inside the caller transaction")
		sawCallerTxn = true
	}

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "10", value)
	require.True(t, sawCallerTxn)
	require.Equal(t, 1, f.exec.attempts, "the identity sentinel aborts autonomous retries immediately")
	require.Equal(t, 2, f.exec.execs, "one SELECT FOR UPDATE + one UPDATE in the caller transaction")
	requireObligation(t, f.proc, 7, 10, 10, false)
	require.Equal(t, []string{"update `db`.`seq` set is_called = true"}, f.state.recordedUpdates())
}

// SETVAL follows the same identity routing: the shadowed database generation
// assigns in the caller transaction.
func TestSetvalWorkspaceShadowedDatabaseRoutesLocal(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 10, 3, false, true))
	f.expectWorkspaceShadowedDatabase()

	value, err := setvalRow(f.proc, "60", true)
	require.NoError(t, err)
	require.Equal(t, "60", value)
	require.Equal(t, 1, f.exec.attempts, "the identity sentinel aborts autonomous retries immediately")
	require.Equal(t, 2, f.exec.execs, "one SELECT FOR UPDATE + one UPDATE in the caller transaction")
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 60, is_called = true"},
		f.state.recordedUpdates())
}

// Stale-identity oracle (v12-review F4): the committed database matches but
// the committed relation id differs from the caller's — a committed
// drop+recreate raced the caller's resolution. This is a genuinely stale
// identity, never transaction-local: the call retries the bounded budget,
// exhausts into the terminal named exhaustion error (approved v12 §4 D/§10
// keep the reviewed bound wrapper-owned end to end; a recreate racing every
// attempt is exceptional and must fail deterministically), and must not
// route in-caller (caller rollback cannot be allowed to reclaim a committed
// value).
func TestStaleCommittedRelationRetriesBoundedNeverLocal(t *testing.T) {
	oldStep := seqAutonomousBackoffStepDur
	seqAutonomousBackoffStepDur = func(int) time.Duration { return 0 }
	t.Cleanup(func() { seqAutonomousBackoffStepDur = oldStep })

	t.Run("nextval", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		f.expectStaleCommittedRelation()

		_, err := nextvalRow(f.proc)
		require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)
		require.Zero(t, f.exec.execs, "a stale committed generation never routes in-caller")
		require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
		_, ok := f.proc.GetSequenceObligationSet().Get(uint64(7))
		require.False(t, ok, "no in-caller obligation may be raised for a committed generation")
	})
	t.Run("setval", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, true))
		f.expectStaleCommittedRelation()

		_, err := setvalRow(f.proc, "60", true)
		require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)
		require.Zero(t, f.exec.execs, "a stale committed generation never routes in-caller")
		require.Zero(t, f.state.selectCount(),
			"the attempt fails at the identity probe before touching sequence metadata rows")
	})
}

// In-flight budget oracle (v12-review F8): the effective deadline
// min(statement, budget) is propagated into execution — the context handed
// to ExecTxn carries it, so an attempt blocked inside the engine observes the
// deadline and is released. The loop must not start another attempt and the
// caller sees the documented named exhaustion error.
func TestNextvalAutonomousBudgetDeadlineCancelsBlockedAttempt(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	oldBudget := seqAutonomousBudgetDur
	seqAutonomousBudgetDur = 500 * time.Millisecond
	t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

	start := time.Now()
	var gotDeadline time.Time
	f.exec.inspectTxnCtx = func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "the retry wrapper must bind its budget deadline into the attempt context")
		gotDeadline = deadline
	}
	f.exec.blockOnCtx = true // the attempt blocks in execution until the context ends

	_, err := nextvalRow(f.proc)
	elapsed := time.Since(start)
	require.ErrorContains(t, err,
		"sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
	require.Equal(t, 1, f.exec.attempts, "a deadline-aborted attempt consumes the whole call")
	require.False(t, gotDeadline.Before(start.Add(seqAutonomousBudgetDur)),
		"the attempt context deadline must cover the full budget")
	require.True(t, gotDeadline.Before(start.Add(seqAutonomousBudgetDur+5*time.Second)),
		"the attempt context deadline must not exceed the budget envelope")
	require.GreaterOrEqual(t, elapsed, seqAutonomousBudgetDur-time.Millisecond,
		"the blocked attempt ran until the propagated deadline fired")
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)

	release, gateErr := f.proc.AcquireSequence(context.Background())
	require.NoError(t, gateErr, "the sequence gate must be released after the in-flight deadline failure")
	release()
}

// Attempt-context engine-resolution oracle (design §4 D H2 + the
// deadline-propagation disposition): the autonomous attempt's engine
// resolution runs on the ATTEMPT's bounded context. Catalog misses in
// disttae run metadata SQL on the context they receive, so a resolution
// bound to the caller's deadline-less statement context can block past
// the per-attempt budget and retain the sequence gate. Every arm blocks
// the named resolution on the context the production code passes, with a
// watchdog well past the compressed budget: pre-propagation code hands the
// engine the caller's (deadline-less) statement context, so the watchdog
// fires and the arm fails; with propagation the budget fires first.
// Caller cancellation keeps its precedence through the same context.
func TestSeqAutonomousAttemptCtxBoundsEngineResolution(t *testing.T) {
	errWatchdog := errors.New("engine resolution watchdog fired — the attempt context never fired")

	const budget = 400 * time.Millisecond
	// blockResolution captures the context the engine op received, asserts
	// it carries the attempt's budget deadline, and blocks until it fires
	// (or the watchdog exposes a leaked caller context).
	blockResolution := func(t *testing.T, start time.Time, want time.Duration, received *time.Time) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			require.True(t, ok, "the engine resolution must receive the attempt's bounded context")
			require.WithinDuration(t, start.Add(want), deadline, 250*time.Millisecond,
				"the received deadline must be the attempt budget min(statement, budget)")
			*received = deadline
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(4 * time.Second):
				return errWatchdog
			}
		}
	}

	t.Run("nextval blocked Database resolution", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		oldBudget := seqAutonomousBudgetDur
		seqAutonomousBudgetDur = budget
		t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

		start := time.Now()
		var received time.Time
		f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).DoAndReturn(
			func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				return nil, blockResolution(t, start, budget, &received)(ctx)
			}).AnyTimes()

		_, err := nextvalRow(f.proc)
		elapsed := time.Since(start)
		require.ErrorContains(t, err,
			"sequence db.seq: allocation could not acquire the sequence row within 8 attempts",
			"a budget-aborted resolution keeps the named surface, not the watchdog sentinel")
		require.False(t, errors.Is(err, errWatchdog), "the resolution aborted at the attempt deadline")
		require.Equal(t, 1, f.exec.attempts, "one bounded attempt, no overshoot")
		require.False(t, received.IsZero())
		require.Less(t, elapsed, 3*time.Second,
			"the blocked resolution completed at the budget, not at the watchdog")

		release, gateErr := f.proc.AcquireSequence(context.Background())
		require.NoError(t, gateErr, "the sequence gate must be released after the budget-bounded failure")
		release()
	})

	t.Run("nextval blocked Relation resolution", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		oldBudget := seqAutonomousBudgetDur
		seqAutonomousBudgetDur = budget
		t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

		start := time.Now()
		var received time.Time
		f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).Return(f.autoDB, nil).AnyTimes()
		f.autoDB.EXPECT().GetDatabaseId(gomock.Any()).Return(f.dbID).AnyTimes()
		f.autoDB.EXPECT().Relation(gomock.Any(), "seq", nil).DoAndReturn(
			func(ctx context.Context, _ string, _ interface{}) (engine.Relation, error) {
				return nil, blockResolution(t, start, budget, &received)(ctx)
			}).AnyTimes()

		_, err := nextvalRow(f.proc)
		elapsed := time.Since(start)
		require.ErrorContains(t, err,
			"sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
		require.False(t, errors.Is(err, errWatchdog), "the resolution aborted at the attempt deadline")
		require.Equal(t, 1, f.exec.attempts)
		require.False(t, received.IsZero())
		require.Less(t, elapsed, 3*time.Second)

		release, gateErr := f.proc.AcquireSequence(context.Background())
		require.NoError(t, gateErr, "the sequence gate must be released after the budget-bounded failure")
		release()
	})

	t.Run("setval blocked Database resolution", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, true))
		oldBudget := seqAutonomousBudgetDur
		seqAutonomousBudgetDur = budget
		t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

		start := time.Now()
		var received time.Time
		f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).DoAndReturn(
			func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				return nil, blockResolution(t, start, budget, &received)(ctx)
			}).AnyTimes()

		_, err := setvalRow(f.proc, "60", true)
		elapsed := time.Since(start)
		require.ErrorContains(t, err,
			"sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
		require.False(t, errors.Is(err, errWatchdog), "the resolution aborted at the attempt deadline")
		require.Equal(t, 1, f.exec.attempts)
		require.False(t, received.IsZero())
		require.Less(t, elapsed, 3*time.Second)

		release, gateErr := f.proc.AcquireSequence(context.Background())
		require.NoError(t, gateErr, "the sequence gate must be released after the budget-bounded failure")
		release()
	})

	t.Run("caller cancellation keeps precedence over the attempt budget", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		// The default envelope: no budget compression — the caller's own
		// cancel arrives long before the 10 s attempt budget.
		stmtCtx, cancel := context.WithCancel(f.proc.Ctx)
		f.proc.Ctx = stmtCtx
		defer cancel()

		start := time.Now()
		var received time.Time
		f.eng.EXPECT().Database(gomock.Any(), "db", txnIdentity{f.autoTxn}).DoAndReturn(
			func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				return nil, blockResolution(t, start, seqAutonomousBudgetDur, &received)(ctx)
			}).AnyTimes()
		time.AfterFunc(120*time.Millisecond, cancel)

		_, err := nextvalRow(f.proc)
		elapsed := time.Since(start)
		require.True(t, errors.Is(err, context.Canceled),
			"caller cancellation surfaces unchanged, got %v", err)
		require.False(t, errors.Is(err, errWatchdog))
		require.Equal(t, 1, f.exec.attempts)
		require.False(t, received.IsZero(),
			"the received context still carries the attempt deadline even when the caller fires first")
		require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
		require.Less(t, elapsed, 3*time.Second,
			"caller cancellation unblocked the resolution at once")
	})
}

// Burst oracles (design §6 bulk gate): the vector's rows share one identity,
// so the whole burst commits in ONE autonomous transaction whose composed
// values and final stored state equal n serialized single-value transactions
// — one metadata SELECT, one counter UPDATE, one commit for n emissions.
func TestNextvalCommittedBurstSingleCommittedUnit(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 1, 1, false, true))
	f.expectCommittedRoute()

	vals, err := nextvalRows(f.proc, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"11", "12", "13"}, vals)
	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, 1, f.state.selectCount())
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 13"}, f.state.recordedUpdates())
	require.Equal(t, int64(13), f.state.last)
	require.True(t, f.state.called)
	require.Equal(t, "13", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, "13", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
	require.Zero(t, f.exec.execs, "the committed route runs no caller-transaction metadata SQL")
}

// Fresh-sequence burst: the first emission returns the stored start;
// subsequent emissions advance; the single persist marks the row called.
func TestNextvalCommittedBurstUncalledFirstRow(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(5, 1, 100, 5, 1, false, false))
	f.expectCommittedRoute()

	vals, err := nextvalRows(f.proc, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"5", "6", "7"}, vals)
	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 7, is_called = true"}, f.state.recordedUpdates())
	require.Equal(t, "7", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, "7", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
}

// CYCLE wrap inside a burst: the pass resets in memory exactly as serialized
// transactions would, and the stored counter lands on the last emission.
func TestNextvalCommittedBurstCycleWrap(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(9, 1, 10, 1, 1, true, true))
	f.expectCommittedRoute()

	vals, err := nextvalRows(f.proc, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"10", "1", "2"}, vals)
	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 2"}, f.state.recordedUpdates())
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
}

// NO CYCLE exhaustion mid-burst: the burst commits nothing (the serialized
// equivalent would have burned the prefix — both are failure-atomic shapes
// I4 permits for a failed statement) and publishes nothing to the session.
func TestNextvalCommittedBurstMaxExhaustionIsAtomic(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(9, 1, 10, 1, 1, false, true))
	f.expectCommittedRoute()

	_, err := nextvalRows(f.proc, 3)
	require.ErrorContains(t, err, "Reached maximum value of sequence")
	require.Equal(t, 1, f.exec.attempts, "range exhaustion is not retryable")
	require.Empty(t, f.state.recordedUpdates(), "the burst writes nothing when one emission overflows")
	require.Equal(t, int64(9), f.state.last)
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
	require.Empty(t, f.proc.GetSessionInfo().SeqLastValue[0])
}

// Single-row calls keep the historical SQL shapes byte-identically: the
// batch implementation specializes to the single-emission arms.
func TestNextvalCommittedSingleRowSQLShapeUnchanged(t *testing.T) {
	t.Run("called", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 1, 1, false, true))
		f.expectCommittedRoute()
		value, err := nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "11", value)
		require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 11"}, f.state.recordedUpdates())
	})
	t.Run("uncalled", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(5, 1, 100, 5, 1, false, false))
		f.expectCommittedRoute()
		value, err := nextvalRow(f.proc)
		require.NoError(t, err)
		require.Equal(t, "5", value)
		require.Equal(t, []string{"update `db`.`seq` set is_called = true"}, f.state.recordedUpdates())
	})
}

// Row-order failure oracle: a resolution error on a later row flushes the
// earlier rows' committed burst first, then reports the error — identical
// order to row-by-row allocation.
func TestNextvalBatchFlushesCommittedRowsBeforeResolutionError(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(10, 1, 100, 1, 1, false, true))
	f.expectCommittedRoute()
	f.callerDB.EXPECT().Relation(gomock.Any(), "other", nil).
		Return(nil, moerr.NewNoSuchTable(f.proc.Ctx, "db", "other")).AnyTimes()

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(f.proc.Mp())
	for _, name := range []string{"seq", "seq", "other"} {
		require.NoError(t, vector.AppendBytes(input, []byte(name), false, f.proc.Mp()))
	}
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), f.proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(3))

	err := Nextval([]*vector.Vector{input}, result, f.proc, 3, nil)
	require.Error(t, err)
	require.Equal(t, []string{"update `db`.`seq` set last_seq_num = 12"}, f.state.recordedUpdates(),
		"the two earlier rows' burst commits before the third row's error surfaces")
	require.Equal(t, "12", f.proc.GetSessionInfo().SeqLastValue[0])
	require.Equal(t, "12", f.proc.GetSessionInfo().SeqAddValues[uint64(7)])
}

// TestCheckSequenceWatermarkDomainArms pins the #30359 pre-PR review
// domain-conflation contract on the decision side: the sequence's own
// metadata picks the comparison arm, an arm no captured value fit is
// inert (no transition, no range guard), and unrelated arms never
// participate. Signed -1 and unsigned MaxUint64 share one bit pattern, so
// a decision that folded them would false-exhaust or mis-reconcile the
// merge watermark.
func TestCheckSequenceWatermarkDomainArms(t *testing.T) {
	signedRow := func(last, minv, maxv, start, incr int64, cycle, called bool) []interface{} {
		return []interface{}{last, minv, maxv, start, incr, cycle, called}
	}
	unsignedRow := func(last, minv, maxv, start uint64, incr int64, cycle, called bool) []interface{} {
		return []interface{}{last, minv, maxv, start, incr, cycle, called}
	}
	uint16Row := func(last, minv, maxv, start uint16, incr int64, cycle, called bool) []interface{} {
		return []interface{}{last, minv, maxv, start, incr, cycle, called}
	}
	int16Row := func(last, minv, maxv, start int16, incr int64, cycle, called bool) []interface{} {
		return []interface{}{last, minv, maxv, start, incr, cycle, called}
	}
	// armExtrema builds the 6-arm decision input with the listed arms set.
	armExtrema := func(set map[SequenceDomain]SequenceWatermarkDomainExtrema) SequenceWatermarkExtrema {
		var ext SequenceWatermarkExtrema
		for d, e := range set {
			ext.Arms[d] = e
		}
		return ext
	}

	t.Run("unsigned sequence ignores foreign signed bits", func(t *testing.T) {
		// The signed arm carries the MaxUint64 bit pattern (folded -1
		// under the pre-fix capture); the sequence is unsigned, reads only
		// the uint arm, and reconciles to 2 without tripping the NO CYCLE
		// guard.
		dec, err := CheckSequenceWatermark(context.Background(),
			unsignedRow(1, 1, math.MaxUint64, 1, 1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint64: {Max: 2, Min: 2, Has: true},
				SeqDomainInt64:  {Max: math.MaxUint64, Min: math.MaxUint64, Has: true},
			}), "s")
		require.NoError(t, err)
		require.True(t, dec.Transition)
		require.Equal(t, "2", dec.Literal)
	})

	t.Run("empty unsigned arm is inert and never runs the guard", func(t *testing.T) {
		// Only negative keys were captured for a NO CYCLE unsigned
		// sequence: no transition and no exhaustion error, even though a
		// folded -1 bit pattern would exceed every guard.
		dec, err := CheckSequenceWatermark(context.Background(),
			unsignedRow(5, 1, 10, 1, 1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainInt64: {Max: uint64(math.MaxInt64) + 1, Min: sequenceWatermarkNegBitsForTest(-9), Has: true},
			}), "s")
		require.NoError(t, err)
		require.False(t, dec.Transition)
	})

	t.Run("signed sequence ignores foreign unsigned bits", func(t *testing.T) {
		// The uint arm carries MaxUint64 (folded -1 pre-fix); the signed
		// descending sequence reads only the signed arm {3} and
		// reconciles to it.
		dec, err := CheckSequenceWatermark(context.Background(),
			signedRow(100, math.MinInt64, 100, 100, -1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint64: {Max: math.MaxUint64, Min: math.MaxUint64, Has: true},
				SeqDomainInt64:  {Max: uint64(3), Min: uint64(3), Has: true},
			}), "s")
		require.NoError(t, err)
		require.True(t, dec.Transition)
		require.Equal(t, "3", dec.Literal)
		require.True(t, dec.Descending)
	})

	t.Run("empty signed arm is inert for a descending signed sequence", func(t *testing.T) {
		// Only a MaxUint64 key was captured: pre-fix it folded to -1 and
		// would have dragged the descending watermark below the position.
		dec, err := CheckSequenceWatermark(context.Background(),
			signedRow(100, math.MinInt64, 100, 100, -1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint64: {Max: math.MaxUint64, Min: math.MaxUint64, Has: true},
			}), "s")
		require.NoError(t, err)
		require.False(t, dec.Transition)
	})

	t.Run("guard still fails a genuine NO CYCLE overflow", func(t *testing.T) {
		// A representable watermark that genuinely exceeds a NO CYCLE
		// unsigned sequence's bounds still aborts failure-atomically.
		_, err := CheckSequenceWatermark(context.Background(),
			unsignedRow(1, 1, 10, 1, 1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint64: {Max: 10, Min: 10, Has: true},
			}), "s")
		require.ErrorContains(t, err, "Reached maximum value of sequence")
	})

	t.Run("narrow unsigned sequence ignores wider foreign-domain keys", func(t *testing.T) {
		// N2 regression (pre-PR review): a SMALLINT UNSIGNED sequence
		// backing a VARCHAR default whose merged keys are '2' and
		// '18446744073709551615'. The MaxUint64 key is not representable
		// in the sequence's uint16 storage domain and leaves the Uint16
		// arm at {2}; pre-fix the width-blind uint-magnitude arm read
		// MaxUint64 and its narrowing wrap false-exhausted the NO CYCLE
		// sequence at the merge. The decision reconciles to exactly 2.
		dec, err := CheckSequenceWatermark(context.Background(),
			uint16Row(1, 1, math.MaxUint16, 1, 1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint16: {Max: 2, Min: 2, Has: true},
				SeqDomainUint32: {Max: 2, Min: 2, Has: true},
				SeqDomainUint64: {Max: math.MaxUint64, Min: 2, Has: true},
				SeqDomainInt16:  {Max: 2, Min: 2, Has: true},
				SeqDomainInt32:  {Max: 2, Min: 2, Has: true},
				SeqDomainInt64:  {Max: 2, Min: 2, Has: true},
			}), "s")
		require.NoError(t, err)
		require.True(t, dec.Transition)
		require.Equal(t, "2", dec.Literal)
	})

	t.Run("narrow unsigned arm at the type edge still runs the genuine overflow guard", func(t *testing.T) {
		// The representable watermark 65535 on a NO CYCLE SMALLINT
		// UNSIGNED sequence (maxv 65535) can never issue a next value:
		// the guard aborts failure-atomically. Width filtering filters
		// foreign keys; it never weakens in-domain exhaustion.
		_, err := CheckSequenceWatermark(context.Background(),
			uint16Row(1, 1, math.MaxUint16, 1, 1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainUint16: {Max: math.MaxUint16, Min: math.MaxUint16, Has: true},
			}), "s")
		require.ErrorContains(t, err, "Reached maximum value of sequence")
	})

	t.Run("narrow signed descending sequence reads only its own arm", func(t *testing.T) {
		// A SMALLINT descending sequence with merged keys -5 and the
		// foreign-domain MaxUint64: the Int16 arm carries {-5}; the
		// wider arms must not drag the descending reconcile below or
		// above its domain. Transition installs -5.
		dec, err := CheckSequenceWatermark(context.Background(),
			int16Row(100, math.MinInt16, 100, 100, -1, false, true),
			armExtrema(map[SequenceDomain]SequenceWatermarkDomainExtrema{
				SeqDomainInt16:  {Max: sequenceWatermarkNegBitsForTest(-5), Min: sequenceWatermarkNegBitsForTest(-5), Has: true},
				SeqDomainInt32:  {Max: sequenceWatermarkNegBitsForTest(-5), Min: sequenceWatermarkNegBitsForTest(-5), Has: true},
				SeqDomainInt64:  {Max: sequenceWatermarkNegBitsForTest(-5), Min: sequenceWatermarkNegBitsForTest(-5), Has: true},
				SeqDomainUint64: {Max: math.MaxUint64, Min: math.MaxUint64, Has: true},
			}), "s")
		require.NoError(t, err)
		require.True(t, dec.Transition)
		require.Equal(t, "-5", dec.Literal)
		require.True(t, dec.Descending)
	})
}

// TestSequenceDomainContainmentBoundaries pins the 6 storage-domain
// representability table at its exact edges (#30359 pre-PR review N2):
// the type minimum and maximum are representable, one beyond either edge
// is not, and no negative value is representable in an unsigned domain.
// These are the filters that keep a foreign-width key from moving the
// reconcile watermark's per-domain arm.
func TestSequenceDomainContainmentBoundaries(t *testing.T) {
	t.Run("signed domains include their edges and reject one beyond", func(t *testing.T) {
		// Type min and max are representable; one beyond either edge is not.
		require.True(t, SeqDomainInt16.ContainsInt64(math.MinInt16))
		require.True(t, SeqDomainInt16.ContainsInt64(math.MaxInt16))
		require.False(t, SeqDomainInt16.ContainsInt64(math.MinInt16-1))
		require.False(t, SeqDomainInt16.ContainsInt64(math.MaxInt16+1))
		require.False(t, SeqDomainInt16.ContainsUint64(math.MaxInt16+1))
		require.True(t, SeqDomainInt16.ContainsUint64(math.MaxInt16))

		require.True(t, SeqDomainInt32.ContainsInt64(math.MinInt32))
		require.True(t, SeqDomainInt32.ContainsInt64(math.MaxInt32))
		require.False(t, SeqDomainInt32.ContainsInt64(math.MinInt32-1))
		require.False(t, SeqDomainInt32.ContainsInt64(math.MaxInt32+1))
		require.False(t, SeqDomainInt32.ContainsUint64(math.MaxInt32+1))
		require.True(t, SeqDomainInt32.ContainsUint64(math.MaxInt32))

		require.True(t, SeqDomainInt64.ContainsInt64(math.MinInt64))
		require.True(t, SeqDomainInt64.ContainsInt64(math.MaxInt64))
		require.False(t, SeqDomainInt64.ContainsUint64(math.MaxInt64+1))
		require.True(t, SeqDomainInt64.ContainsUint64(math.MaxInt64))
	})

	t.Run("unsigned domains include their edges and reject every negative", func(t *testing.T) {
		require.True(t, SeqDomainUint16.ContainsUint64(math.MaxUint16))
		require.False(t, SeqDomainUint16.ContainsUint64(math.MaxUint16+1))
		require.True(t, SeqDomainUint32.ContainsUint64(math.MaxUint32))
		require.False(t, SeqDomainUint32.ContainsUint64(math.MaxUint32+1))
		require.True(t, SeqDomainUint64.ContainsUint64(math.MaxUint64))
		for _, d := range []SequenceDomain{SeqDomainUint16, SeqDomainUint32, SeqDomainUint64} {
			require.True(t, d.UnsignedDomain())
			require.True(t, d.ContainsInt64(0))
			require.False(t, d.ContainsInt64(-1), "domain %d rejects -1", d)
			require.False(t, d.ContainsInt64(math.MinInt64), "domain %d rejects MinInt64", d)
		}
		for _, d := range []SequenceDomain{SeqDomainInt16, SeqDomainInt32, SeqDomainInt64} {
			require.False(t, d.UnsignedDomain())
		}
	})

	t.Run("wider domains are not a superset shortcut for narrower ones", func(t *testing.T) {
		// The N2 defect shape: MaxUint64 fits ONLY the Uint64 domain.
		require.False(t, SeqDomainUint16.ContainsUint64(math.MaxUint64))
		require.False(t, SeqDomainUint32.ContainsUint64(math.MaxUint64))
		require.False(t, SeqDomainInt64.ContainsUint64(math.MaxUint64))
		require.True(t, SeqDomainUint64.ContainsUint64(math.MaxUint64))
		// A negative int64 value never fits any unsigned domain, however wide.
		require.False(t, SeqDomainUint64.ContainsInt64(-1))
	})

	// The domain count is exactly the six storage types a sequence row can
	// carry; a new domain forces this file's proofs to grow with it.
	require.Equal(t, 6, SequenceDomainCount)
	require.Equal(t, int(SequenceDomainCount), len(seqDomainTable))
}

// sequenceWatermarkNegBitsForTest renders a negative value's sign-extended
// bit slot, matching the capture-side storage shape.
func sequenceWatermarkNegBitsForTest(v int64) uint64 {
	return uint64(v)
}
