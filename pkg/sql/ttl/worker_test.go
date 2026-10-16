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

package ttl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// Deliberately fails before invoking the callback: retry must open a new whole
// transaction, be bounded, preserve permanent failures and respect cancellation.
type retryExecutor struct {
	executor.SQLExecutor
	calls        int
	succeedAfter int
	err          error
	cancel       context.CancelFunc
}

func (e *retryExecutor) ExecTxn(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
	e.calls++
	if e.succeedAfter > 0 && e.calls > e.succeedAfter {
		return nil
	}
	if e.cancel != nil {
		e.cancel()
	}
	return e.err
}
func validRetryCandidate(t *testing.T) Candidate {
	t.Helper()
	p := Policy{Version: 1, Column: "ts", Seconds: 1}
	c := engine.ConstraintDef{Cts: []engine.Constraint{&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: PropertyKey, Value: Encode(&p)}}}}}
	data, err := c.MarshalBinary()
	require.NoError(t, err)
	return Candidate{TableID: 1, Database: "d", Table: "t", Policy: p, Constraint: data}
}
func TestTTLRetryBoundsAndCancellation(t *testing.T) {
	c := validRetryCandidate(t)
	retry := &retryExecutor{err: errors.Join(moerr.NewTxnNeedRetryNoCtx())}
	_, err := DeleteBatch(t.Context(), retry, c, time.Now(), 1)
	require.Error(t, err)
	require.Equal(t, 8, retry.calls)
	permanent := errors.New("permanent error")
	fail := &retryExecutor{err: permanent}
	_, err = DeleteBatch(t.Context(), fail, c, time.Now(), 1)
	require.ErrorIs(t, err, permanent)
	require.Equal(t, 1, fail.calls)
	ctx, cancel := context.WithCancel(t.Context())
	stop := &retryExecutor{err: moerr.NewTxnNeedRetryNoCtx(), cancel: cancel}
	_, err = DeleteBatch(ctx, stop, c, time.Now(), 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, stop.calls)
	c.Policy.Seconds = 2
	_, err = DeleteBatch(t.Context(), fail, c, time.Now(), 1)
	require.Error(t, err)
	require.Equal(t, 1, fail.calls, "mismatched policy must not start a transaction")
	options := DefaultWorkerOptions()
	options.BatchSize = 0
	require.Error(t, Run(t.Context(), nil, time.Now(), options))
}

// Lock wait timeouts doom the current transaction, but a new transaction may
// progress after the competing owner releases its lock. Permanent errors must
// not be retried, and retries must retain their existing attempt/cancel bounds.
func TestTTLRetryLockWaitTimeout(t *testing.T) {
	c := validRetryCandidate(t)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"local", moerr.NewLockWaitTimeoutNoCtx()},
		{"remote", moerr.NewRemoteLockWaitTimeoutNoCtx()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			joined := errors.Join(tc.err, errors.New("rollback diagnostic"))
			require.True(t, isRetryable(joined))
			retry := &retryExecutor{err: joined, succeedAfter: 1}
			n, err := DeleteBatch(t.Context(), retry, c, time.Now(), 1)
			require.NoError(t, err)
			require.Zero(t, n)
			require.Equal(t, 2, retry.calls)
			exhausted := &retryExecutor{err: joined}
			_, err = DeleteBatch(t.Context(), exhausted, c, time.Now(), 1)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, 8, exhausted.calls)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stopped := &retryExecutor{err: joined, cancel: cancel}
			_, err = DeleteBatch(ctx, stopped, c, time.Now(), 1)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, stopped.calls)
		})
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, moerr.NewInvalidInputNoCtx("bad policy")} {
		require.False(t, isRetryable(err))
	}
}
