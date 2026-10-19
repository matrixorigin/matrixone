// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

type countingAdmissionLimiter struct{ calls atomic.Uint64 }

func (l *countingAdmissionLimiter) Wait(context.Context, context.Context) error {
	l.calls.Add(1)
	return nil
}

func admissionTestClient(t *testing.T, opts ...TxnClientCreateOption) (*txnClient, *countingAdmissionLimiter) {
	t.Helper()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := NewTxnClient("", newTestTxnSender(), append(opts, WithTimestampWaiter(immediateTimestampWaiter{}))...).(*txnClient)
	c.Resume()
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	limiter := &countingAdmissionLimiter{}
	c.limiter = limiter
	return c, limiter
}

// A snapshot is exclusive: readiness at A means the logtail is applied through
// A.Prev(). Reusing A must not require a subsequent logtail notification.
func TestSnapshotAdmissionReusesAppliedFrontier(t *testing.T) {
	for _, authorization := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "authorization"}[authorization], func(t *testing.T) {
			c, limiter := admissionTestClient(t, WithEnableSacrificingFreshness())
			waiter := NewTimestampWaiter(c.logger).(*timestampWaiter)
			defer waiter.Close()
			applied := newTestTimestamp(10)
			waiter.latestTS.Store(&applied)
			c.timestampWaiter = waiter
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			defer a.End()
			option := WithSnapshotAdmission(a)
			if authorization {
				option = WithAuthorizationSnapshot(a)
			}
			// No further NotifyLatestCommitTS is sent: creation must consume the
			// frontier already proven by AcquireSnapshotAdmission.
			op, err := c.New(ctx, timestamp.Timestamp{}, option)
			require.NoError(t, err)
			require.Equal(t, applied.Next(), op.SnapshotTS())
			expectedAdmissions := uint64(1)
			if authorization {
				expectedAdmissions++
			}
			require.Equal(t, expectedAdmissions, limiter.calls.Load())
			require.NoError(t, op.Rollback(ctx))
		})
	}
}

func TestSnapshotAdmissionSingleUseAndRestart(t *testing.T) {
	c, limiter := admissionTestClient(t)
	ctx := context.Background()
	a, supported, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	require.True(t, supported)
	defer a.End()
	snapshot, err := a.Snapshot(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	op, err := c.New(ctx, timestamp.Timestamp{}, WithSnapshotAdmission(a))
	require.NoError(t, err)
	require.Equal(t, snapshot, op.Txn().SnapshotTS)
	require.EqualValues(t, 1, limiter.calls.Load(), "data New consumes the completed admission")
	require.Nil(t, op.(*txnOperator).opts.snapshotAdmission)
	_, err = c.New(ctx, timestamp.Timestamp{}, WithSnapshotAdmission(a))
	require.Error(t, err)
	available, err := a.Available(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	require.False(t, available)
	require.NoError(t, op.Rollback(ctx))
	a.End()
	restarted, err := c.RestartTxn(ctx, op.(*txnOperator), timestamp.Timestamp{})
	require.NoError(t, err)
	require.EqualValues(t, 2, limiter.calls.Load(), "a restart admits normally and retains no statement lifetime")
	require.NoError(t, restarted.Rollback(ctx))
}

func TestAuthorizationSnapshotExactAndOptionOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "auth-first", true: "auth-last"}[reverse], func(t *testing.T) {
			c, limiter := admissionTestClient(t)
			ctx := context.Background()
			a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			defer a.End()
			snapshot, err := a.Snapshot(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			// A later notification may be observed during creation, but must not
			// advance the independent authorization reader beyond its fixed A.
			waiter := NewTimestampWaiter(c.logger).(*timestampWaiter)
			defer waiter.Close()
			later := snapshot.Next()
			waiter.latestTS.Store(&later)
			c.timestampWaiter = waiter
			opts := []TxnOption{WithAuthorizationSnapshot(a), WithTxnIsolation(txn.TxnIsolation_RC)}
			if reverse {
				opts[0], opts[1] = opts[1], opts[0]
			}
			op, err := c.New(ctx, timestamp.Timestamp{}, opts...)
			require.NoError(t, err)
			require.Equal(t, txn.TxnIsolation_SI, op.Txn().Isolation)
			require.True(t, op.TxnOptions().ReadOnly())
			require.Equal(t, snapshot, op.Txn().SnapshotTS, "reader must use exactly the admitted A")
			require.EqualValues(t, 2, limiter.calls.Load(), "real authorization read pays admission")
			require.NoError(t, op.Rollback(ctx))
			available, err := a.Available(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			require.True(t, available)
		})
	}
}

func TestSnapshotAdmissionRejectsConflictsAndInvalidation(t *testing.T) {
	for _, kind := range []string{"explicit-first", "explicit-last", "both", "write-cache", "foreign", "minimum", "ended", "cancelled", "zero", "nil", "nil-auth", "auth-explicit-first", "auth-explicit-last"} {
		t.Run(kind, func(t *testing.T) {
			c, limiter := admissionTestClient(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			defer a.End()
			snapshot, err := a.Snapshot(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			opts := make([]TxnOption, 1, 4)
			opts[0] = WithSnapshotAdmission(a)
			minimum := timestamp.Timestamp{}
			switch kind {
			case "explicit-first":
				opts = append(opts[:0], WithSnapshotTS(snapshot.Prev()), WithSnapshotAdmission(a))
			case "explicit-last":
				opts = append(opts, WithSnapshotTS(snapshot.Prev()))
			case "both":
				opts = append(opts, WithAuthorizationSnapshot(a))
			case "write-cache":
				opts = append(opts[:0], WithTxnCacheWrite(), WithAuthorizationSnapshot(a))
			case "foreign":
				other, _ := admissionTestClient(t)
				c = other
			case "minimum":
				minimum = snapshot
			case "ended":
				a.End()
			case "cancelled":
				cancel()
			case "nil":
				opts = append(opts[:0], WithSnapshotAdmission(nil))
			case "nil-auth":
				opts = append(opts[:0], WithAuthorizationSnapshot(nil))
			case "auth-explicit-first":
				opts = append(opts[:0], WithSnapshotTS(snapshot.Prev()), WithAuthorizationSnapshot(a))
			case "auth-explicit-last":
				opts = append(opts[:0], WithAuthorizationSnapshot(a), WithSnapshotTS(snapshot.Prev()))
			case "zero":
				opts = append(opts[:0], WithSnapshotAdmission(&SnapshotAdmission{}))
			}
			op, err := c.New(context.Background(), minimum, opts...)
			require.Error(t, err)
			require.Nil(t, op)
			require.Zero(t, c.atomic.activeTxnCount.Load())
			require.EqualValues(t, 1, limiter.calls.Load())
		})
	}
}

func TestSnapshotAdmissionEndWhileQueued(t *testing.T) {
	c, _ := admissionTestClient(t, WithMaxActiveTxn(1))
	ctx := context.Background()
	first, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn())
	require.NoError(t, err)
	defer first.Rollback(ctx)
	a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	defer a.End()
	waiter := &admissionNotifyingWaiter{entered: make(chan struct{}, 1)}
	c.timestampWaiter = waiter
	result := make(chan txnCreateResult, 1)
	go func() {
		op, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn(), WithSnapshotAdmission(a))
		result <- txnCreateResult{op, err}
	}()
	select {
	case <-waiter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second generation did not reach its snapshot barrier")
	}
	a.End()
	select {
	case got := <-result:
		require.Error(t, got.err)
		require.Nil(t, got.op)
	case <-time.After(5 * time.Second):
		t.Fatal("ended statement retained a queued transaction")
	}
	require.EqualValues(t, 1, c.atomic.activeTxnCount.Load())
	c.mu.RLock()
	require.Zero(t, c.mu.waitActiveTxns.size)
	c.mu.RUnlock()
}

type admissionNotifyingWaiter struct {
	immediateTimestampWaiter
	entered chan struct{}
}

// Hiding the parent's private cancel key makes context.AfterFunc use this
// observer. It detects materialization of token cancellation without Go internals.
type admissionCancellationObserver struct {
	context.Context
	registrations atomic.Uint64
}

func (*admissionCancellationObserver) Value(any) any { return nil }
func (c *admissionCancellationObserver) AfterFunc(f func()) func() bool {
	c.registrations.Add(1)
	return context.AfterFunc(c.Context, f)
}

func TestSnapshotAdmissionDirectCreationContext(t *testing.T) {
	for _, kind := range []string{"native", "nil", "custom", "authorization"} {
		t.Run(kind, func(t *testing.T) {
			c, limiter := admissionTestClient(t)
			ctx := t.Context()
			a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			t.Cleanup(a.End)
			observer := &admissionCancellationObserver{Context: a.ctx}
			a.ctx = observer
			option := WithSnapshotAdmission(a)
			switch kind {
			case "native", "authorization":
				waiter := NewTimestampWaiter(c.logger).(*timestampWaiter)
				t.Cleanup(waiter.Close)
				later := a.snapshot.Next()
				waiter.latestTS.Store(&later)
				c.timestampWaiter = waiter
				if kind == "authorization" {
					option = WithAuthorizationSnapshot(a)
				}
			case "nil":
				c.timestampWaiter = nil
			}
			op, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn(), option)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, op.Rollback(ctx)) })
			require.Equal(t, a.snapshot, op.SnapshotTS(), "a later native frontier must not replace A")
			wantLifetimeChildren, wantAdmissions := uint64(0), uint64(1)
			if kind == "custom" || kind == "authorization" {
				wantLifetimeChildren = 1
			}
			if kind == "authorization" {
				wantAdmissions++
			}
			require.Equal(t, wantLifetimeChildren, observer.registrations.Load())
			a.mu.RLock()
			materialized := a.cancel != nil
			a.mu.RUnlock()
			require.Equal(t, wantLifetimeChildren != 0, materialized)
			require.Equal(t, wantAdmissions, limiter.calls.Load())
			a.End()
			require.Equal(t, a.snapshot, op.SnapshotTS(), "End does not own the created transaction")
		})
	}
}

func TestSnapshotAdmissionDirectOpenPredicates(t *testing.T) {
	for _, kind := range []string{"native", "nil", "custom", "unapplied", "paused", "abort barrier", "capacity", "canceled", "closed"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := admissionTestClient(t, WithMaxActiveTxn(1))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			op := newTxnOperator(c.sid, c.logger, c.clock, c.sender, c.newTxnMeta(), c.getTxnOptions([]TxnOption{WithUserTxn()})...)
			op.setDefaultEventCallbacks(&c.sharedEventCallbacks)
			frontier := newTestTimestamp(10)
			waiter := NewTimestampWaiter(c.logger).(*timestampWaiter)
			t.Cleanup(waiter.Close)
			waiter.latestTS.Store(&frontier)
			op.timestampWaiter = waiter
			switch kind {
			case "nil":
				op.timestampWaiter = nil
			case "custom":
				op.timestampWaiter = immediateTimestampWaiter{}
			case "unapplied":
				older := frontier.Prev()
				waiter.latestTS.Store(&older)
			case "paused":
				c.Pause()
			case "abort barrier":
				c.mu.Lock()
				c.mu.waitMarkAllActiveAbortedC = make(chan struct{})
				c.mu.Unlock()
			case "capacity":
				first, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, first.Rollback(ctx)) })
			case "canceled":
				cancel()
			case "closed":
				require.NoError(t, c.Close())
			}
			users, active, queued := txnAdmissionCounts(c)
			opened := c.tryOpenAdmittedTxn(ctx, op, frontier.Next())
			require.Equal(t, kind == "native" || kind == "nil", opened)
			if opened {
				require.NoError(t, op.Rollback(t.Context()))
			} else {
				u, a, q := txnAdmissionCounts(c)
				require.Equal(t, users, u)
				require.Equal(t, active, a)
				require.Equal(t, queued, q, "failed eligibility transfers no ownership")
				op.closeUnadmitted(context.Canceled)
			}
		})
	}
}

type admissionEndAtOpenContext struct {
	context.Context
	checks int
	end    func()
}

func (c *admissionEndAtOpenContext) Err() error {
	c.checks++
	if c.checks == 2 { // claim has finished; locked direct-open check precedes publication
		c.end()
	}
	return c.Context.Err()
}

func TestSnapshotAdmissionEndDuringDirectOpen(t *testing.T) {
	c, limiter := admissionTestClient(t)
	a, _, err := AcquireSnapshotAdmission(t.Context(), c, timestamp.Timestamp{})
	require.NoError(t, err)
	t.Cleanup(a.End)
	observer := &admissionCancellationObserver{Context: a.ctx}
	a.ctx = observer
	c.timestampWaiter = nil
	ctx := &admissionEndAtOpenContext{Context: t.Context(), end: a.End}
	op, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn(), WithSnapshotAdmission(a))
	require.Error(t, err)
	require.Nil(t, op)
	require.Zero(t, observer.registrations.Load(), "exercise actual non-waiting creation")
	users, active, queued := txnAdmissionCounts(c)
	require.Zero(t, users)
	require.Zero(t, active)
	require.Zero(t, queued)
	require.EqualValues(t, 1, limiter.calls.Load(), "failure cannot mint a second admission")
}

func TestSnapshotAdmissionEndDuringReadinessFallback(t *testing.T) {
	for _, pausedState := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort barrier", true: "paused"}[pausedState], func(t *testing.T) {
			c, _ := admissionTestClient(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)
			a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
			require.NoError(t, err)
			t.Cleanup(a.End)
			observer := &admissionCancellationObserver{Context: a.ctx}
			a.ctx = observer
			c.timestampWaiter = nil
			c.mu.Lock()
			if pausedState {
				c.mu.state = paused
			} else {
				c.mu.waitMarkAllActiveAbortedC = make(chan struct{})
			}
			c.mu.Unlock()
			result := make(chan txnCreateResult, 1)
			go func() {
				op, err := c.New(ctx, timestamp.Timestamp{}, WithUserTxn(), WithSnapshotAdmission(a))
				result <- txnCreateResult{op, err}
			}()
			// Rendezvous with the actual readiness select while holding its
			// reacquisition lock. Keep the client unready on a fresh barrier;
			// only token End (the caller stays live) can release creation.
			observed := false
			for !observed && ctx.Err() == nil {
				c.mu.Lock()
				barrier := c.mu.waitMarkAllActiveAbortedC
				if pausedState {
					barrier = c.mu.pausedC
				}
				select {
				case barrier <- struct{}{}:
					if !pausedState {
						c.mu.waitMarkAllActiveAbortedC = make(chan struct{})
					}
					a.End()
					observed = true
				default:
				}
				c.mu.Unlock()
				goruntime.Gosched()
			}
			require.True(t, observed, "New never reached its readiness wait")
			select {
			case got := <-result:
				require.Error(t, got.err)
				require.Nil(t, got.op)
			case <-ctx.Done():
				t.Fatal("token End did not release readiness wait")
			}
			require.NoError(t, ctx.Err(), "caller cancellation must not release this test")
			require.EqualValues(t, 1, observer.registrations.Load())
			users, active, queued := txnAdmissionCounts(c)
			require.Zero(t, users)
			require.Zero(t, active)
			require.Zero(t, queued)
		})
	}
}

func TestAuthorizationSnapshotEndDuringLimiterWait(t *testing.T) {
	c, _ := admissionTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	t.Cleanup(a.End)
	limiter := &blockingTxnRateLimiter{entered: make(chan struct{}, 1)}
	c.limiter = limiter
	result := make(chan txnCreateResult, 1)
	go func() {
		op, err := c.New(ctx, timestamp.Timestamp{}, WithAuthorizationSnapshot(a))
		result <- txnCreateResult{op, err}
	}()
	select {
	case <-limiter.entered:
	case <-ctx.Done():
		t.Fatal("authorization reader did not enter ordinary admission")
	}
	a.End()
	select {
	case got := <-result:
		require.ErrorIs(t, got.err, context.Canceled)
		require.Nil(t, got.op)
	case <-ctx.Done():
		t.Fatal("End did not cancel ordinary authorization admission")
	}
	require.NoError(t, ctx.Err())
	users, active, queued := txnAdmissionCounts(c)
	require.Zero(t, users)
	require.Zero(t, active)
	require.Zero(t, queued)
}

func TestSnapshotAdmissionLazyCreationLifetime(t *testing.T) {
	for _, kind := range []string{"end before creation", "end during creation", "caller canceled", "parent canceled"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := admissionTestClient(t)
			bound, cancelBound := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancelBound)
			parent, cancelParent := context.WithCancel(bound)
			t.Cleanup(cancelParent)
			a, _, err := AcquireSnapshotAdmission(parent, c, timestamp.Timestamp{})
			require.NoError(t, err)
			t.Cleanup(a.End)
			require.Nil(t, a.cancel, "acquisition has no creation waiter to cancel")
			type key struct{}
			caller, cancelCaller := context.WithCancelCause(context.WithValue(bound, key{}, "caller"))
			t.Cleanup(func() { cancelCaller(nil) })
			if kind == "end before creation" {
				a.End()
			}
			child, finish := a.creationContext(caller)
			t.Cleanup(finish)
			require.Equal(t, "caller", child.Value(key{}))
			deadline, _ := caller.Deadline()
			gotDeadline, _ := child.Deadline()
			require.Equal(t, deadline, gotDeadline)
			want := error(context.Canceled)
			switch kind {
			case "end during creation":
				a.End()
			case "caller canceled":
				want = errors.New("caller cause")
				cancelCaller(want)
			case "parent canceled":
				cancelParent()
			}
			select {
			case <-child.Done():
				require.ErrorIs(t, context.Cause(child), want)
			case <-bound.Done():
				t.Fatal("creation cancellation did not propagate")
			}
		})
	}
}

func TestSnapshotAdmissionEndRacesLazyInitialization(t *testing.T) {
	c, _ := admissionTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	t.Cleanup(a.End)
	start, ended := make(chan struct{}), make(chan struct{})
	type creation struct {
		ctx    context.Context
		finish func()
	}
	created := make(chan creation, 1)
	go func() {
		<-start
		child, finish := a.creationContext(ctx)
		created <- creation{child, finish}
	}()
	go func() { <-start; a.End(); close(ended) }()
	close(start)
	select {
	case <-ended:
	case <-ctx.Done():
		t.Fatal("End did not finish during initialization")
	}
	var got creation
	select {
	case got = <-created:
	case <-ctx.Done():
		t.Fatal("initialization did not finish after End")
	}
	t.Cleanup(got.finish)
	select {
	case <-got.ctx.Done():
		require.ErrorIs(t, got.ctx.Err(), context.Canceled)
	case <-ctx.Done():
		t.Fatal("End/init race lost cancellation")
	}
	require.NoError(t, ctx.Err(), "token End cannot cancel its parent")
	_, err = a.Snapshot(ctx, c, timestamp.Timestamp{})
	require.Error(t, err)
}

func TestSnapshotAdmissionCreationAdaptersShareLifetime(t *testing.T) {
	c, _ := admissionTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	a, _, err := AcquireSnapshotAdmission(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err)
	t.Cleanup(a.End)
	observer := &admissionCancellationObserver{Context: a.ctx}
	a.ctx = observer
	first, finishFirst := a.creationContext(ctx)
	t.Cleanup(finishFirst)
	second, finishSecond := a.creationContext(ctx)
	t.Cleanup(finishSecond)
	require.EqualValues(t, 1, observer.registrations.Load(), "creations share one token cancellation child")
	finishFirst()
	require.ErrorIs(t, first.Err(), context.Canceled)
	require.NoError(t, second.Err(), "finishing one creation cannot cancel its sibling")
	_, err = a.Snapshot(ctx, c, timestamp.Timestamp{})
	require.NoError(t, err, "finishing creation cannot end the token")
	a.End()
	select {
	case <-second.Done():
		require.ErrorIs(t, second.Err(), context.Canceled)
	case <-ctx.Done():
		t.Fatal("End did not cancel the shared lifetime")
	}
	require.NoError(t, ctx.Err())
}

func (w *admissionNotifyingWaiter) GetTimestampWithClose(ctx context.Context, ts timestamp.Timestamp, closed <-chan struct{}) (timestamp.Timestamp, error) {
	w.entered <- struct{}{}
	return ts.Next(), nil
}
