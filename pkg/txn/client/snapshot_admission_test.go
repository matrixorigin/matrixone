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

func (w *admissionNotifyingWaiter) GetTimestampWithClose(ctx context.Context, ts timestamp.Timestamp, closed <-chan struct{}) (timestamp.Timestamp, error) {
	w.entered <- struct{}{}
	return ts.Next(), nil
}
