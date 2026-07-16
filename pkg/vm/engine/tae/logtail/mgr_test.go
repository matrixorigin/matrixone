// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

// goSubmit runs fn in a fresh goroutine; used as submit in tests to avoid
// pulling in the ants pool dependency.
func goSubmit(fn func()) { go fn() }

// syncSubmit runs fn synchronously; exercises the degenerate case where
// collect effectively runs on the caller.
func syncSubmit(fn func()) { fn() }

func TestOrderedCollectAndPublish_Empty(t *testing.T) {
	called := false
	orderedCollectAndPublish(
		0,
		func(int) bool { return false },
		goSubmit,
		func(int) *txnWithLogtails { called = true; return nil },
		func(*txnWithLogtails) { called = true },
	)
	require.False(t, called, "neither collect nor publish should run for n=0")
}

func TestOrderedCollectAndPublish_AllSkip(t *testing.T) {
	var collectCalls, publishCalls atomic.Int32
	orderedCollectAndPublish(
		3,
		func(int) bool { return true }, // skip all
		goSubmit,
		func(int) *txnWithLogtails {
			collectCalls.Add(1)
			return &txnWithLogtails{}
		},
		func(*txnWithLogtails) { publishCalls.Add(1) },
	)
	require.Zero(t, collectCalls.Load())
	require.Zero(t, publishCalls.Load())
}

func TestOrderedCollectAndPublish_HappyPath(t *testing.T) {
	const n = 5
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 1, 2, 3, 4}, publishOrder,
		"publish must follow strictly ascending index order")
}

func TestOrderedCollectAndPublish_ReverseReadyOrder(t *testing.T) {
	// Slot n-1 completes first (no sleep); earlier slots sleep longer the
	// lower their index. Publisher must still observe 0, 1, ..., n-1.
	const n = 4
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails {
			// slot 0 sleeps longest, slot n-1 returns immediately.
			time.Sleep(time.Duration(n-1-i) * 20 * time.Millisecond)
			return tails[i]
		},
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 1, 2, 3}, publishOrder,
		"publish must be in index order even when later slots finish first")
}

func TestOrderedCollectAndPublish_NilResultSkipped(t *testing.T) {
	// Slot 1 returns nil (simulating rollback); only slots 0 and 2 should publish.
	const n = 3
	tails := make([]*txnWithLogtails, n)
	tails[0] = &txnWithLogtails{}
	tails[1] = nil
	tails[2] = &txnWithLogtails{}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt != nil && tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown or nil txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 2}, publishOrder,
		"slot returning nil must be skipped without breaking order")
}

// TestOrderedCollectAndPublish_RollbackReleasesCloseCBInCollect exercises
// the rollback contract used by onTxnLogTails: CollectLogtail has already
// produced batches (and a closeCB to release them) by the time the txn's
// final state is inspected, so the collect callback must release closeCB
// explicitly before returning nil. Failing to do so leaks the batches, and
// publishing the tail would leak rolled-back mutations to subscribers.
func TestOrderedCollectAndPublish_RollbackReleasesCloseCBInCollect(t *testing.T) {
	const n = 3
	rolledBack := map[int]bool{1: true} // slot 1 rolls back

	var closeCBCalls atomic.Int32
	mkCloseCB := func() func() {
		return func() { closeCBCalls.Add(1) }
	}

	tails := make([]*txnWithLogtails, n)
	for i := 0; i < n; i++ {
		tails[i] = &txnWithLogtails{closeCB: mkCloseCB()}
	}

	var mu sync.Mutex
	var published []*txnWithLogtails
	var publishedCloseCBCalls atomic.Int32

	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails {
			t := tails[i]
			if rolledBack[i] {
				// Mirror onTxnLogTails: rollback must release closeCB
				// and skip publish.
				t.closeCB()
				return nil
			}
			return t
		},
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			published = append(published, v)
			v.closeCB()
			publishedCloseCBCalls.Add(1)
		},
	)

	require.Equal(t, []*txnWithLogtails{tails[0], tails[2]}, published,
		"rollback slot must not be published")
	require.Equal(t, int32(3), closeCBCalls.Load(),
		"closeCB must fire exactly once per slot (rollback: direct; committed: via publish)")
	require.Equal(t, int32(2), publishedCloseCBCalls.Load(),
		"publish-path closeCB count must match committed slots only")
}

func TestOrderedCollectAndPublish_SkipInterleaved(t *testing.T) {
	// Even slots are skipped; odd slots publish. Publisher sees 1, 3.
	const n = 5
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var collectCalls atomic.Int32
	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(i int) bool { return i%2 == 0 },
		goSubmit,
		func(i int) *txnWithLogtails {
			collectCalls.Add(1)
			return tails[i]
		},
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, int32(2), collectCalls.Load(), "skip(i) must prevent collect")
	require.Equal(t, []int{1, 3}, publishOrder)
}

// antsLikeSubmit mimics ants by recovering panics in the submitted fn
// so they do not crash the test process. This is the failure mode the
// helper must tolerate: a silently-recovered panic inside collect would
// otherwise leave the publisher waiting on a channel that never receives.
func antsLikeSubmit(fn func()) {
	go func() {
		defer func() { _ = recover() }()
		fn()
	}()
}

func TestOrderedCollectAndPublish_CollectPanicDoesNotBlock(t *testing.T) {
	// Slot 1 panics inside collect; slots 0 and 2 succeed. Publisher must
	// still make progress and publish the two healthy slots. Without the
	// defer-recover inside the helper, slot 1's channel would never
	// receive a value and the publisher would block forever.
	const n = 3
	tails := make([]*txnWithLogtails, n)
	tails[0] = &txnWithLogtails{}
	tails[1] = nil // panic slot
	tails[2] = &txnWithLogtails{}

	var mu sync.Mutex
	var publishOrder []int

	done := make(chan struct{})
	go func() {
		orderedCollectAndPublish(
			n,
			func(int) bool { return false },
			antsLikeSubmit,
			func(i int) *txnWithLogtails {
				if i == 1 {
					panic("simulated collect panic")
				}
				return tails[i]
			},
			func(v *txnWithLogtails) {
				mu.Lock()
				defer mu.Unlock()
				for idx, tt := range tails {
					if tt != nil && tt == v {
						publishOrder = append(publishOrder, idx)
						return
					}
				}
				t.Errorf("published unknown txnWithLogtails")
			},
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("orderedCollectAndPublish blocked after collect panic")
	}

	require.Equal(t, []int{0, 2}, publishOrder,
		"panicking slot must not block progress of surrounding slots")
}

// ---------------------------------------------------------------------
// Tests for collectOneTxn: the real per-slot contract onTxnLogTails
// depends on. These assert the invariants reviewers asked for:
//   - DoneEvent(TailCollecting) fires on every path (committed,
//     rollback, panic) — otherwise WaitWalAndTail hangs
//   - closeCB fires exactly once in every non-publishing path
//   - committed txns return a non-nil tail and do NOT run closeCB
//     locally (it is handed to the publisher)
//   - rolled-back txns return nil
//   - a panic inside collect propagates (so the pool's PanicHandler
//     re-panics and the process is terminated); we still run both
//     defers before leaving
// ---------------------------------------------------------------------

// fakeTxnStore embeds NoopTxnStore and only tracks the balance of
// TailCollecting Add/Done events. WaitEvent(WalPreparing) is a no-op
// here because the test injects collect directly.
type fakeTxnStore struct {
	txnbase.NoopTxnStore
	tailCollecting atomic.Int32
}

func (s *fakeTxnStore) WaitEvent(int) {}
func (s *fakeTxnStore) AddEvent(typ int) {
	if typ == txnif.TailCollecting {
		s.tailCollecting.Add(1)
	}
}
func (s *fakeTxnStore) DoneEvent(typ int) {
	if typ == txnif.TailCollecting {
		s.tailCollecting.Add(-1)
	}
}

// fakeAsyncTxn embeds a nil AsyncTxn so only the methods we override
// are reachable. Any other call from collectOneTxn would nil-panic the
// test loudly, which is actually the safety behavior we want.
type fakeAsyncTxn struct {
	txnif.AsyncTxn
	store       *fakeTxnStore
	replay      bool
	twoPC       bool
	prepare     types.TS
	state       txnif.TxnState
	stateFn     func() txnif.TxnState // overrides state when set
	stateWaitFn func(bool) txnif.TxnState
}

func (t *fakeAsyncTxn) IsReplay() bool           { return t.replay }
func (t *fakeAsyncTxn) Is2PC() bool              { return t.twoPC }
func (t *fakeAsyncTxn) GetPrepareTS() types.TS   { return t.prepare }
func (t *fakeAsyncTxn) GetStore() txnif.TxnStore { return t.store }
func (t *fakeAsyncTxn) GetTxnState(wait bool) txnif.TxnState {
	if t.stateWaitFn != nil {
		return t.stateWaitFn(wait)
	}
	if t.stateFn != nil {
		return t.stateFn()
	}
	return t.state
}

func TestManagerStopCancelsUnresolvedReplayed2PC(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	mgr.Start()

	queried := make(chan struct{})
	releaseLegacyWait := make(chan struct{})
	var queryOnce sync.Once
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   true,
		prepare: types.BuildTS(20, 0),
		stateWaitFn: func(wait bool) txnif.TxnState {
			queryOnce.Do(func() { close(queried) })
			if wait {
				<-releaseLegacyWait
				return txnif.TxnStateRollbacked
			}
			return txnif.TxnStatePrepared
		},
	}
	mgr.OnEndPrepareWAL(txn)
	select {
	case <-queried:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed 2PC collector did not start")
	}

	stopped := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		// Let the old blocking implementation terminate before failing so this
		// regression test never leaks its queue goroutine.
		close(releaseLegacyWait)
		<-stopped
		t.Fatal("Manager.Stop blocked on an unresolved replayed 2PC")
	}
	require.Zero(t, txn.store.tailCollecting.Load())
}

func TestCollectReplayed2PCPanicsOnUnknownState(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	defer mgr.Stop()
	var calls atomic.Int32
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   true,
		prepare: types.BuildTS(20, 0),
		stateWaitFn: func(bool) txnif.TxnState {
			if calls.Add(1) == 1 {
				return txnif.TxnStateUnknown
			}
			return txnif.TxnStateRollbacked
		},
	}
	txn.store.AddEvent(txnif.TailCollecting)
	require.Panics(t, func() { mgr.collectReplayed2PC(txn) })
	require.Zero(t, txn.store.tailCollecting.Load())
}

func TestCollectReplayed2PCWaitsFromPreparingToCommitted(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	defer mgr.Stop()
	states := []txnif.TxnState{
		txnif.TxnStatePreparing,
		txnif.TxnStatePrepared,
		txnif.TxnStateCommitted,
	}
	var calls atomic.Int32
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   true,
		prepare: types.BuildTS(20, 0),
		stateWaitFn: func(bool) txnif.TxnState {
			idx := int(calls.Add(1)) - 1
			return states[idx]
		},
	}
	txn.store.AddEvent(txnif.TailCollecting)
	require.NotNil(t, mgr.collectReplayed2PC(txn))
	require.Equal(t, int32(3), calls.Load())
	require.Zero(t, txn.store.tailCollecting.Load())
}

func TestOnTxnLogTails_Replayed2PCCommitHoldsOrderUntilDecision(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	defer mgr.Stop()

	release := make(chan struct{})
	enteredStateWait := make(chan struct{}, 1)
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   true,
		prepare: types.BuildTS(20, 0),
		stateFn: func() txnif.TxnState {
			enteredStateWait <- struct{}{}
			<-release
			return txnif.TxnStateCommitted
		},
	}
	txn.store.AddEvent(txnif.TailCollecting)

	var callbacks atomic.Int32
	require.NoError(t, mgr.RegisterCallback(func(
		_, _ timestamp.Timestamp, closeCB func(), _ ...logtail.TableLogtail,
	) error {
		callbacks.Add(1)
		closeCB()
		return nil
	}))

	done := make(chan struct{})
	go func() {
		mgr.onTxnLogTails(txn)
		close(done)
	}()

	select {
	case <-enteredStateWait:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed prepared 2PC never entered final-state wait")
	}
	select {
	case <-done:
		t.Fatal("replayed prepared 2PC was released before its final decision")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed committed 2PC did not release its logtail slot")
	}
	require.Positive(t, callbacks.Load(), "committed replay must advance the ordered logtail stream")
	require.Zero(t, txn.store.tailCollecting.Load())
}

func TestOnTxnLogTails_Replayed2PCRollbackHoldsThenDrops(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	defer mgr.Stop()

	release := make(chan struct{})
	enteredStateWait := make(chan struct{}, 1)
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   true,
		prepare: types.BuildTS(20, 0),
		stateFn: func() txnif.TxnState {
			enteredStateWait <- struct{}{}
			<-release
			return txnif.TxnStateRollbacked
		},
	}
	txn.store.AddEvent(txnif.TailCollecting)

	var callbacks atomic.Int32
	require.NoError(t, mgr.RegisterCallback(func(
		_, _ timestamp.Timestamp, closeCB func(), _ ...logtail.TableLogtail,
	) error {
		callbacks.Add(1)
		closeCB()
		return nil
	}))

	done := make(chan struct{})
	go func() {
		mgr.onTxnLogTails(txn)
		close(done)
	}()

	select {
	case <-enteredStateWait:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed prepared 2PC never entered final-state wait")
	}
	select {
	case <-done:
		t.Fatal("replayed prepared 2PC rollback was released before its final decision")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed rolled-back 2PC did not release its logtail slot")
	}
	require.Zero(t, callbacks.Load(), "rolled-back replay must not publish a logtail")
	require.Zero(t, txn.store.tailCollecting.Load())
}

func TestOnTxnLogTails_Replayed1PCRemainsSkipped(t *testing.T) {
	mgr := NewManager(nil, nil, 10, func() types.TS { return types.BuildTS(100, 0) })
	defer mgr.Stop()

	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		replay:  true,
		twoPC:   false,
		prepare: types.BuildTS(20, 0),
		stateFn: func() txnif.TxnState {
			t.Fatal("replayed 1PC must not be recollected")
			return txnif.TxnStateCommitted
		},
	}
	var callbacks atomic.Int32
	require.NoError(t, mgr.RegisterCallback(func(
		_, _ timestamp.Timestamp, closeCB func(), _ ...logtail.TableLogtail,
	) error {
		callbacks.Add(1)
		closeCB()
		return nil
	}))

	mgr.onTxnLogTails(txn)
	require.Zero(t, callbacks.Load())
}

func newFakeCommittedTxn() *fakeAsyncTxn {
	return &fakeAsyncTxn{
		store: &fakeTxnStore{},
		state: txnif.TxnStateCommitted,
	}
}

func newFakeRollbackTxn() *fakeAsyncTxn {
	return &fakeAsyncTxn{
		store: &fakeTxnStore{},
		state: txnif.TxnStateRollbacked,
	}
}

// makeCollectStub returns a txnLogtailCollector that records whether
// its returned closeCB was invoked.
func makeCollectStub() (txnLogtailCollector, *atomic.Int32) {
	var closeCalls atomic.Int32
	fn := func(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
		tails := &[]logtail.TableLogtail{}
		return tails, func() { closeCalls.Add(1) }
	}
	return fn, &closeCalls
}

func TestCollectOneTxn_Committed_HandsCloseCBToPublisher(t *testing.T) {
	txn := newFakeCommittedTxn()
	// Mirror OnEndPrepareWAL which balances with DoneEvent later.
	txn.store.AddEvent(txnif.TailCollecting)
	collect, closeCalls := makeCollectStub()

	tail := collectOneTxn(txn, collect)

	require.NotNil(t, tail, "committed txn must return non-nil tail")
	require.Equal(t, int32(0), closeCalls.Load(),
		"committed path must NOT call closeCB locally; it is handed to publisher")
	require.Equal(t, int32(0), txn.store.tailCollecting.Load(),
		"DoneEvent(TailCollecting) must balance AddEvent")
}

func TestCollectOneTxn_Rollback_ReleasesCloseCBAndReturnsNil(t *testing.T) {
	txn := newFakeRollbackTxn()
	txn.store.AddEvent(txnif.TailCollecting)
	collect, closeCalls := makeCollectStub()

	tail := collectOneTxn(txn, collect)

	require.Nil(t, tail, "rollback must NOT be published")
	require.Equal(t, int32(1), closeCalls.Load(),
		"rollback path must release closeCB locally")
	require.Equal(t, int32(0), txn.store.tailCollecting.Load(),
		"DoneEvent(TailCollecting) must balance AddEvent even on rollback")
}

func TestCollectOneTxn_Panic_StillCleansUpAndPropagates(t *testing.T) {
	// Simulate GetTxnState returning an unexpected state, which
	// collectOneTxn handles with a panic. DoneEvent fires inline
	// before GetTxnState, and the deferred closeCB still runs during
	// panic unwind. The panic must propagate so the pool's
	// PanicHandler can terminate the process.
	txn := &fakeAsyncTxn{
		store:   &fakeTxnStore{},
		stateFn: func() txnif.TxnState { return txnif.TxnState(999) },
	}
	txn.store.AddEvent(txnif.TailCollecting)
	collect, closeCalls := makeCollectStub()

	require.Panics(t, func() {
		collectOneTxn(txn, collect)
	}, "unknown txn state must panic (propagates to PanicHandler)")

	require.Equal(t, int32(1), closeCalls.Load(),
		"closeCB must still run during panic unwind")
	require.Equal(t, int32(0), txn.store.tailCollecting.Load(),
		"DoneEvent(TailCollecting) must still fire during panic unwind")
}

func TestCollectOneTxn_DoneEventFiresBeforeGetTxnState(t *testing.T) {
	// The real apply path does: WaitWalAndTail (waits TailCollecting)
	// then DoneApply (flips state). If collectOneTxn calls
	// GetTxnState(true) before DoneEvent(TailCollecting), the commit
	// goroutine is blocked on our event and we wait forever for the
	// state it was about to flip — deadlock.
	store := &fakeTxnStore{}
	store.AddEvent(txnif.TailCollecting)

	var observedEventAtGetState int32
	txn := &fakeAsyncTxn{
		store: store,
		stateFn: func() txnif.TxnState {
			observedEventAtGetState = store.tailCollecting.Load()
			return txnif.TxnStateCommitted
		},
	}
	collect, _ := makeCollectStub()

	tail := collectOneTxn(txn, collect)
	require.NotNil(t, tail)
	require.Equal(t, int32(0), observedEventAtGetState,
		"DoneEvent(TailCollecting) must fire before GetTxnState(true); "+
			"otherwise apply is blocked on the event and the state never flips")
}

// panicCollector panics inside collect(), before the inline
// DoneEvent call. The deferred fallback must still balance AddEvent.
func TestCollectOneTxn_PanicInCollect_StillFiresDoneEvent(t *testing.T) {
	store := &fakeTxnStore{}
	store.AddEvent(txnif.TailCollecting)
	txn := &fakeAsyncTxn{store: store, state: txnif.TxnStateCommitted}

	panicCollector := func(txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
		panic("simulated CollectLogtail panic")
	}

	require.Panics(t, func() {
		collectOneTxn(txn, panicCollector)
	})
	require.Equal(t, int32(0), store.tailCollecting.Load(),
		"DoneEvent(TailCollecting) must fire during unwind when collect panics")
}

func TestOrderedCollectAndPublish_SyncSubmit(t *testing.T) {
	// Exercise the path where submit runs fn inline. This guards against
	// regressions where readyCh[i] capacity assumption breaks if the
	// publisher happens to drain a slot while the producer is still
	// enqueueing later slots.
	const n = 3
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}
	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		syncSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
		},
	)
	require.Equal(t, []int{0, 1, 2}, publishOrder)
}
