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

package databranchutils

// This file is the shared test harness for the D186-D191 stage-owner
// cells (H.10)(n)/(o)/(q)/(t)/(u)/(v). Every stub mirrors ONE platform
// contract at its cited locus — the mirror is the cell's correspondence
// claim, and a platform change to any mirrored locus fails the cells
// loudly rather than passing spuriously:
//
//   - stageOperatorStub: pkg/txn/client/operator.go — the public terminal
//     gate (claimTerminalCall :2365-2382, rejection moerr.NewTxnClosedNoCtx
//     :1098-1102/:1164-1168), the private rollback (:1206-1251) holding
//     tc.mu across RollbackEvent (:1231), the unlock tail (:1240-1250) and
//     closeLocked (:1236-1240), closeLocked firing ClosedEvent exactly once
//     under the mutex (:2037-2060), scheduleRollbackAfterRunningSQL
//     (:2115-2138) sealing the gate and arming the drained callback, and
//     runSQLTracker exitToken launching the drained callback only at
//     sealed && activeTokens == 0 in a NEW goroutine (:456-478/:496-525).
//     Commit mirrors doWrite's defer stack (:1494-1526): the synchronous
//     unlock runs holding tc.mu BEFORE closeLocked publishes ClosedEvent.
//   - stubLockService: pkg/lockservice/service.go:758-775 — the unlock
//     retry rides the service-lifetime context; shutdown (Close) is the
//     one terminal cancellation owner, and Close owns remaining locks once
//     admission is sealed (:761-764).
//   - stubTxnClient: pkg/txn/client/client.go — the default ClosedEvent
//     consumer (closeTxn :1230-1246) signalling markAllActiveTxnAborted on
//     ErrCannotCommitOnInvalidCN (:1241-1242), the abort task walking
//     generations into addFlagIfGeneration (client.go:1537-1539 ->
//     operator.go:2488-2489, which acquires tc.mu), and Close joining the
//     task stopper (client.go:898).
//   - execStubStageSQL: pkg/sql/compile/sql_executor.go:384-393 — ONE
//     defer arms RollbackAfterRunningSQL on a MustRollbackTxn error and
//     then exits the SQL token; the token exits strictly before return.
//   - stubStopRPCs: pkg/cnservice/server.go stopRPCs — the D190(B.1)(i)
//     segment order (lockService.Close BEFORE the txn client stopper
//     join), orderable for the RED twins.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// stageJournal is the cells' ordered edge log; every asserted chain is an
// index comparison over it.
type stageJournal struct {
	mu    sync.Mutex
	edges []string
}

func (j *stageJournal) add(edge string) {
	j.mu.Lock()
	j.edges = append(j.edges, edge)
	j.mu.Unlock()
}

func (j *stageJournal) has(edge string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range j.edges {
		if e == edge {
			return true
		}
	}
	return false
}

func (j *stageJournal) count(edge string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, e := range j.edges {
		if e == edge {
			n++
		}
	}
	return n
}

func (j *stageJournal) index(edge string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.edges {
		if e == edge {
			return i
		}
	}
	return -1
}

// requireOrder asserts every edge exists and their indices ascend.
func (j *stageJournal) requireOrder(t *testing.T, edges ...string) {
	t.Helper()
	prev := -1
	for _, e := range edges {
		idx := j.index(e)
		if idx < 0 {
			t.Fatalf("journal missing edge %q; have %v", e, j.edges)
		}
		if idx <= prev {
			t.Fatalf("journal edge %q at %d not after previous at %d; have %v", e, idx, prev, j.edges)
		}
		prev = idx
	}
}

// stageGauges are the cells' baseline oracles: active transactions
// (created minus closed) and held locks (granted minus unlocked).
type stageGauges struct {
	activeTxns atomic.Int64
	heldLocks  atomic.Int64
}

// waitForCondition polls pred at 1ms until it holds or the budget ends;
// it NEVER uses a fixed sleep as proof.
func waitForCondition(t *testing.T, budget time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if pred() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition %q not met within %s", what, budget)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// stubLockService (lockservice/service.go:758-775 mirror).
// ---------------------------------------------------------------------------

type stubLockService struct {
	gauges  *stageGauges
	journal *stageJournal

	serviceCtx context.Context
	cancel     context.CancelFunc
	closed     atomic.Bool

	// The unlock-block injection: stallAll stalls every Unlock, or
	// stallTargets stalls per transaction id (the cells arm the exact
	// stage transaction whose deferred tail they hold). A stalled Unlock
	// retries until the service-lifetime cancellation discharges it (the
	// serviceCtx gate of :769-775).
	stallAll     atomic.Bool
	stallTargets sync.Map
	stallErrs    sync.Map
	stallEntered atomic.Bool
	unlockCalls  atomic.Int64
}

// ArmStallForErr injects the unlock block for one transaction id and
// makes the discharged tail return err (the injected-error variant of
// the blocked-commit schedule).
func (ls *stubLockService) ArmStallForErr(id []byte, err error) {
	ls.stallTargets.Store(string(id), true)
	ls.stallErrs.Store(string(id), err)
}

func newStubLockService(gauges *stageGauges, journal *stageJournal) *stubLockService {
	ctx, cancel := context.WithCancel(context.Background())
	return &stubLockService{gauges: gauges, journal: journal, serviceCtx: ctx, cancel: cancel}
}

// ArmStallFor injects the unlock block for one transaction id.
func (ls *stubLockService) ArmStallFor(id []byte) {
	ls.stallTargets.Store(string(id), true)
}

// Unlock mirrors the production retry contract; locks is the held count
// the caller owned, decremented once the tail completes (discharged by
// remote success OR by shutdown sealing admission — Close then owns the
// remainder).
func (ls *stubLockService) Unlock(_ context.Context, id []byte, locks int64) error {
	ls.unlockCalls.Add(1)
	var retErr error
	_, targeted := ls.stallTargets.Load(string(id))
	if ls.stallAll.Load() || targeted {
		if ls.stallEntered.CompareAndSwap(false, true) {
			ls.journal.add("unlock-stall-entered")
		}
		<-ls.serviceCtx.Done()
		ls.journal.add("unlock-discharged")
		ls.stallTargets.Delete(string(id))
	}
	if e, ok := ls.stallErrs.LoadAndDelete(string(id)); ok {
		retErr = e.(error)
	}
	if locks > 0 {
		ls.gauges.heldLocks.Add(-locks)
	}
	return retErr
}

// Close is the service-lifetime cancellation segment of stopRPCs.
func (ls *stubLockService) Close() {
	if ls.closed.CompareAndSwap(false, true) {
		ls.journal.add("ls-closed")
		ls.cancel()
	}
}

// ---------------------------------------------------------------------------
// stageOperatorStub (txn/client/operator.go mirror).
// ---------------------------------------------------------------------------

const (
	stubTerminalIdle   = 0
	stubTerminalActive = 1
	stubTerminalSealed = 2
)

type stageOperatorStub struct {
	// Embedded for the unimplemented surface; every method the cells
	// exercise is overridden below.
	client.TxnOperator

	id      []byte
	cli     *stubTxnClient
	ls      *stubLockService
	gauges  *stageGauges
	journal *stageJournal

	mu      sync.Mutex // mirror tc.mu
	closed  bool
	aborted bool
	locks   int64

	// terminalAction: 0 none, 1 rollback-after-running-SQL armed
	// (operator.go:2121).
	terminalAction int32
	terminalCall   atomic.Int32

	// waitSQLBudget overrides the running-SQL wait bound (harness scale);
	// zero uses the 50ms default.
	waitSQLBudget time.Duration

	// runSQLTracker mirror (operator.go:440-525).
	tmu           sync.Mutex
	tokens        map[uint64]struct{}
	tokenSeq      atomic.Uint64
	trackerSealed atomic.Bool
	onDrained     func()
	drainLaunched atomic.Int64

	cbs map[client.EventType][]client.TxnEventCallback
}

func newStageOperatorStub(cli *stubTxnClient, locks int64) *stageOperatorStub {
	op := &stageOperatorStub{
		id:      []byte(fmt.Sprintf("stub-txn-%d", time.Now().UnixNano())),
		cli:     cli,
		ls:      cli.ls,
		gauges:  cli.gauges,
		journal: cli.journal,
		locks:   locks,
		tokens:  make(map[uint64]struct{}),
		cbs:     make(map[client.EventType][]client.TxnEventCallback),
	}
	return op
}

func (op *stageOperatorStub) Txn() txn.TxnMeta { return txn.TxnMeta{ID: op.id} }

// AppendEventCallback mirrors operator_events.go:54-78: legal only while
// the operator is open (production panics on a closed append :65-67).
func (op *stageOperatorStub) AppendEventCallback(event client.EventType, callbacks ...client.TxnEventCallback) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.closed {
		panic("append event callback on closed stub operator")
	}
	op.cbs[event] = append(op.cbs[event], callbacks...)
}

// fireLocked mirrors triggerEventLocked: callbacks run UNDER tc.mu and
// must be non-blocking; registration order is preserved (the txn client's
// defaults were installed at construction, owner/appended callbacks after).
func (op *stageOperatorStub) fireLocked(event client.EventType, err error) {
	for _, cb := range op.cbs[event] {
		_ = cb.Func(context.Background(), op,
			client.TxnEvent{Event: event, Txn: op.Txn(), Err: err}, cb.Value)
	}
}

// claimTerminalCall mirrors operator.go:2365-2382: exactly one public
// terminal call owns the transition; internal cleanup bypasses the gate.
func (op *stageOperatorStub) claimTerminalCall() error {
	if op.terminalCall.CompareAndSwap(stubTerminalIdle, stubTerminalActive) {
		return nil
	}
	return moerr.NewTxnClosedNoCtx(op.id)
}

func (op *stageOperatorStub) releaseTerminalCall() {
	op.terminalCall.Store(stubTerminalSealed)
}

// closeLocked mirrors operator.go:2037-2060: seal the gate, seal the
// tracker, publish ClosedEvent EXACTLY once. Caller holds op.mu.
func (op *stageOperatorStub) closeLocked(commitErr error) {
	op.terminalCall.CompareAndSwap(stubTerminalIdle, stubTerminalSealed)
	if op.closed {
		return
	}
	op.trackerSealed.Store(true)
	op.closed = true
	op.fireLocked(client.ClosedEvent, commitErr)
}

// Commit mirrors the doWrite commit defer stack (operator.go:1494-1526):
// the synchronous unlock runs HOLDING tc.mu BEFORE closeLocked publishes
// ClosedEvent, so a stalled unlock blocks the commit's return, the close
// publication, and every later same-mutex walk, in that order.
func (op *stageOperatorStub) Commit(ctx context.Context) error {
	if err := op.claimTerminalCall(); err != nil {
		return err
	}
	defer op.releaseTerminalCall()
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.closed {
		return moerr.NewTxnClosedNoCtx(op.id)
	}
	op.journal.add("commit-entry")
	locks := op.locks
	op.locks = 0
	if err := op.ls.Unlock(ctx, op.id, locks); err != nil {
		op.closeLocked(err)
		return err
	}
	op.closeLocked(nil)
	return nil
}

// Rollback is the public terminal call (operator.go:1163-1174): the gate
// claim (whose rejection is the D187/D188 recorded contract the cells
// assert), then the running-SQL wait whose failure schedules the deferred
// cleanup and returns the wait error (:1169-1174).
func (op *stageOperatorStub) Rollback(ctx context.Context) error {
	if err := op.claimTerminalCall(); err != nil {
		return err
	}
	defer op.releaseTerminalCall()
	if err := op.sealAndWaitRunningSQL(); err != nil {
		op.scheduleRollbackAfterRunningSQL(err)
		return err
	}
	return op.rollback(ctx)
}

// sealAndWaitRunningSQL mirrors sealAndWaitRunningSQLWithSQL: seal the
// tracker against new tokens and wait, bounded, for active tokens to
// drain. The budget is the harness-scale stand-in for
// runningSQLWaitTimeout.
func (op *stageOperatorStub) sealAndWaitRunningSQL() error {
	op.trackerSealed.Store(true)
	budget := op.waitSQLBudget
	if budget <= 0 {
		budget = 50 * time.Millisecond
	}
	deadline := time.Now().Add(budget)
	for {
		if op.activeTokenCount() == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return moerr.NewInternalErrorNoCtx("stub: running SQL wait timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

// closeWithCommitErr mirrors the commit-failure close that publishes
// ClosedEvent carrying the commit error (the commitErr-carrying
// publication of operator.go:2050-2061): e.g. an
// ErrCannotCommitOnInvalidCN commit resolves the operator closed with
// that error as the event's Err.
func (op *stageOperatorStub) closeWithCommitErr(err error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	op.closeLocked(err)
}

// rollback mirrors rollbackOwned (operator.go:1206-1251): workspace
// rollback outside the mutex, then tc.mu held across the RollbackEvent
// publication (:1231), the unlock tail (:1240-1250, LIFO before
// closeLocked), and closeLocked's ClosedEvent (:1236-1240).
func (op *stageOperatorStub) rollback(ctx context.Context) error {
	op.journal.add("workspace-rollback")
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.closed {
		return nil
	}
	op.fireLocked(client.RollbackEvent, nil)
	locks := op.locks
	op.locks = 0
	err := op.ls.Unlock(ctx, op.id, locks)
	op.closeLocked(nil)
	return err
}

// RollbackAfterRunningSQL mirrors the DeferredTxnRollback surface
// (operator.go:2112-2115 -> 2116-2138): seal the terminal gate and arm
// the drained callback; cleanup starts only when the last SQL token
// exits, in a NEW goroutine (:476-477).
func (op *stageOperatorStub) RollbackAfterRunningSQL(err error) {
	op.scheduleRollbackAfterRunningSQL(err)
}

func (op *stageOperatorStub) scheduleRollbackAfterRunningSQL(err error) {
	op.mu.Lock()
	if op.closed || op.terminalAction != 0 {
		op.mu.Unlock()
		return
	}
	op.terminalAction = 1
	op.terminalCall.Store(stubTerminalSealed)
	op.mu.Unlock()
	op.sealAndRunWhenDrained(func() {
		_ = op.rollback(context.Background())
	})
}

// EnterRunSqlWithToken mirrors enterToken: register one active SQL token.
func (op *stageOperatorStub) EnterRunSqlWithToken() uint64 {
	token := op.tokenSeq.Add(1)
	op.tmu.Lock()
	op.tokens[token] = struct{}{}
	op.tmu.Unlock()
	return token
}

// ExitRunSqlWithToken mirrors exitToken (operator.go:455-478): the
// drained callback launches ONLY at sealed && activeTokens == 0, and in
// its own goroutine.
func (op *stageOperatorStub) ExitRunSqlWithToken(token uint64) {
	if token == 0 {
		return
	}
	var callback func()
	op.tmu.Lock()
	delete(op.tokens, token)
	if op.trackerSealed.Load() && len(op.tokens) == 0 {
		callback = op.onDrained
		op.onDrained = nil
	}
	op.tmu.Unlock()
	if callback != nil {
		op.drainLaunched.Add(1)
		go callback()
	}
}

// sealAndRunWhenDrained mirrors operator.go:496-525: single-shot arm;
// with zero tokens the callback launches immediately (still async).
func (op *stageOperatorStub) sealAndRunWhenDrained(fn func()) {
	if fn == nil {
		return
	}
	op.tmu.Lock()
	if op.trackerSealed.Load() && op.onDrained != nil {
		op.tmu.Unlock()
		return
	}
	op.trackerSealed.Store(true)
	if len(op.tokens) == 0 {
		op.tmu.Unlock()
		op.drainLaunched.Add(1)
		go fn()
		return
	}
	op.onDrained = fn
	op.tmu.Unlock()
}

func (op *stageOperatorStub) activeTokenCount() int {
	op.tmu.Lock()
	defer op.tmu.Unlock()
	return len(op.tokens)
}

// markAbortedIfGeneration mirrors addFlagIfGeneration
// (operator.go:2483-2496): the abort-marking walk acquires tc.mu — the
// mutex dependency the D190(B.1) reorder discharges.
func (op *stageOperatorStub) markAbortedIfGeneration() bool {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.closed {
		return false
	}
	op.aborted = true
	op.journal.add("abort-marked")
	return true
}

// ---------------------------------------------------------------------------
// stubTxnClient (txn/client/client.go mirror).
// ---------------------------------------------------------------------------

type stubTxnClient struct {
	gauges  *stageGauges
	journal *stageJournal
	ls      *stubLockService

	mu  sync.Mutex
	ops map[*stageOperatorStub]struct{}

	abortC       chan struct{}
	abortCtx     context.Context
	abortCancel  context.CancelFunc
	abortJobDone chan struct{}
	closeOnce    sync.Once
}

func newStubTxnClient(gauges *stageGauges, journal *stageJournal, ls *stubLockService) *stubTxnClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &stubTxnClient{
		gauges:       gauges,
		journal:      journal,
		ls:           ls,
		ops:          make(map[*stageOperatorStub]struct{}),
		abortC:       make(chan struct{}, 1),
		abortCtx:     ctx,
		abortCancel:  cancel,
		abortJobDone: make(chan struct{}),
	}
	go c.abortTask()
	return c
}

// New registers one active operator and installs the client's default
// ClosedEvent consumer (client.go:1230-1246) BEFORE any appended callback,
// mirroring defaultTxnEventCallbacks.
func (c *stubTxnClient) New(locks int64) *stageOperatorStub {
	op := newStageOperatorStub(c, locks)
	c.mu.Lock()
	c.ops[op] = struct{}{}
	c.mu.Unlock()
	c.gauges.activeTxns.Add(1)
	// The scratch's lock tail is held from creation until its Unlock tail
	// completes (stall included): the gauge's increment lives HERE, the
	// decrement in stubLockService.Unlock.
	c.gauges.heldLocks.Add(locks)
	op.cbs[client.ClosedEvent] = append(op.cbs[client.ClosedEvent],
		client.TxnEventCallback{Func: func(_ context.Context, _ client.TxnOperator, ev client.TxnEvent, _ any) error {
			c.noteClosedTxn(ev)
			return nil
		}})
	return op
}

// noteClosedTxn mirrors closeTxn (client.go:1230-1247): the invalid-CN
// error signals the abort task (non-blocking), then the operator leaves
// the active registry and the gauge drops.
func (c *stubTxnClient) noteClosedTxn(ev client.TxnEvent) {
	if moerr.IsMoErrCode(ev.Err, moerr.ErrCannotCommitOnInvalidCN) {
		c.markAllActiveTxnAborted()
	}
	c.mu.Lock()
	for op := range c.ops {
		if string(op.id) == string(ev.Txn.ID) {
			delete(c.ops, op)
			break
		}
	}
	c.mu.Unlock()
	c.gauges.activeTxns.Add(-1)
}

// markAllActiveTxnAborted mirrors the non-blocking abortC signal
// (client.go:1242).
func (c *stubTxnClient) markAllActiveTxnAborted() {
	select {
	case c.abortC <- struct{}{}:
	default:
	}
}

// abortTask mirrors the "mark active txn aborted task" (client.go:
// 1517-1557): each signal walks every active generation into
// addFlagIfGeneration — acquiring each operator's mu — inside the task's
// run, so a blocked walk pins the stopper join at Close.
func (c *stubTxnClient) abortTask() {
	defer close(c.abortJobDone)
	for {
		select {
		case <-c.abortCtx.Done():
			return
		case <-c.abortC:
			c.journal.add("abort-walk-begin")
			c.mu.Lock()
			ops := make([]*stageOperatorStub, 0, len(c.ops))
			for op := range c.ops {
				ops = append(ops, op)
			}
			c.mu.Unlock()
			for _, op := range ops {
				op.markAbortedIfGeneration()
			}
			c.journal.add("abort-walk-done")
		}
	}
}

// Close mirrors client.go:870-908's terminal segment: cancel the task
// context, then JOIN the stopper. The production join is unbounded; the
// harness's budget arm exists only to OBSERVE the D190(B.1) reverted-order
// deadline witness — with joinBudget == 0 the join is unbounded.
func (c *stubTxnClient) Close(joinBudget time.Duration) (joined bool) {
	c.closeOnce.Do(func() { c.abortCancel() })
	if joinBudget <= 0 {
		<-c.abortJobDone
		c.journal.add("txn-stopper-joined")
		return true
	}
	select {
	case <-c.abortJobDone:
		c.journal.add("txn-stopper-joined")
		return true
	case <-time.After(joinBudget):
		return false
	}
}

// ---------------------------------------------------------------------------
// stubStopRPCs (cnservice/server.go stopRPCs segment mirror, D190(B.1)(i)).
// ---------------------------------------------------------------------------

func stubStopRPCsNormal(ls *stubLockService, cli *stubTxnClient) {
	ls.Close()
	cli.Close(0)
}

// stubStopRPCsReverted runs the txn-client stopper join FIRST with a
// bounded observation window (the deadline witness), then discharges the
// lockservice cancellation segment, then completes the join: the twin
// asserts the join CANNOT complete before the discharge.
func stubStopRPCsReverted(ls *stubLockService, cli *stubTxnClient, witness time.Duration) (blockedAtDeadline bool) {
	if cli.Close(witness) {
		return false
	}
	ls.Close()
	cli.Close(0)
	return true
}

// ---------------------------------------------------------------------------
// execStubStageSQL (sql_executor.go:384-393 mirror).
// ---------------------------------------------------------------------------

// execStubStageSQL is the executor's arm-then-exit defer: ONE defer arms
// RollbackAfterRunningSQL on a MustRollbackTxn error, THEN exits the SQL
// token — the token exits strictly before this function returns.
func execStubStageSQL(op *stageOperatorStub, stmt func() error) (err error) {
	token := op.EnterRunSqlWithToken()
	defer func() {
		if moerr.MustRollbackTxn(err) {
			op.RollbackAfterRunningSQL(err)
		}
		op.ExitRunSqlWithToken(token)
	}()
	return stmt()
}

// stubFatalStageErr is the fatal internal-SQL error class: deadlock
// detector refusal is a whole-txn rollback code (MustRollbackTxn true).
func stubFatalStageErr() error { return moerr.NewDeadLockDetectedNoCtx() }
