// Copyright 2024 Matrix Origin
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

package publication

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Result with mockResult ----

func TestResult_MockResult_Close(t *testing.T) {
	mock := &testMockResult{data: [][]interface{}{{"a"}}, currentRow: -1}
	r := &Result{mockResult: mock}
	err := r.Close()
	assert.NoError(t, err)
	assert.True(t, mock.closed)
}

func TestResult_MockResult_Next(t *testing.T) {
	mock := &testMockResult{data: [][]interface{}{{"a"}, {"b"}}, currentRow: -1}
	r := &Result{mockResult: mock}
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
}

func TestResult_MockResult_Scan(t *testing.T) {
	mock := &testMockResult{data: [][]interface{}{{"hello"}}, currentRow: -1}
	r := &Result{mockResult: mock}
	require.True(t, r.Next())
	var s string
	err := r.Scan(&s)
	assert.NoError(t, err)
	assert.Equal(t, "hello", s)
}

func TestResult_MockResult_Err(t *testing.T) {
	mock := &testMockResult{data: [][]interface{}{}, currentRow: -1}
	r := &Result{mockResult: mock}
	assert.NoError(t, r.Err())
}

// ---- UpstreamExecutor.ExecSQLInDatabase delegates to ExecSQL ----

func TestUpstreamExecutor_ExecSQLInDatabase_DelegatesToExecSQL(t *testing.T) {
	e := &UpstreamExecutor{}
	// useTxn=true should fail same as ExecSQL
	_, _, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", true, false, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support transactions")
}

// ---- UpstreamExecutor.execWithRetry retryDuration exceeded ----

func TestUpstreamExecutor_ExecWithRetry_RetryDurationExceeded(t *testing.T) {
	for _, timeout := range []time.Duration{0, 2 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := &UpstreamExecutor{retryTimes: 100, retryDuration: time.Second}
				e.initRetryPolicy(DefaultClassifier{})
				e.retryPolicy.Backoff = ExponentialBackoff{Base: time.Hour}
				ctx, stop := context.WithCancel(context.Background())
				defer stop()
				var attemptCtx context.Context
				calls := 0
				result, cancel, err := e.execWithRetry(ctx, nil, timeout, func(child context.Context) (*Result, error) {
					calls++
					attemptCtx = child
					return nil, errors.New("connection reset")
				})
				if cancel != nil {
					t.Cleanup(cancel)
				}
				require.ErrorContains(t, err, "retry limit exceeded")
				require.Nil(t, result)
				require.Nil(t, cancel)
				require.Equal(t, 1, calls)
				require.NoError(t, ctx.Err())
				if timeout > 0 {
					require.ErrorIs(t, attemptCtx.Err(), context.Canceled)
				} else {
					require.Same(t, ctx, attemptCtx)
				}
			})
		})
	}
}

// ---- UpstreamExecutor.execWithRetry success after retry ----

func TestUpstreamExecutor_ExecWithRetry_SuccessAfterRetry(t *testing.T) {
	for _, timeout := range []time.Duration{0, 2 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			e := &UpstreamExecutor{retryTimes: 5, retryDuration: 10 * time.Second}
			e.initRetryPolicy(DefaultClassifier{})
			e.retryPolicy.Backoff = nil
			ctx, stop := context.WithCancel(context.Background())
			t.Cleanup(stop)
			want := &Result{}
			var attemptContexts []context.Context
			var previousErrors []error
			result, cancel, err := e.execWithRetry(ctx, nil, timeout, func(child context.Context) (*Result, error) {
				if len(attemptContexts) > 0 {
					previousErrors = append(previousErrors, attemptContexts[len(attemptContexts)-1].Err())
				}
				attemptContexts = append(attemptContexts, child)
				if len(attemptContexts) < 3 {
					return nil, errors.New("connection reset")
				}
				return want, nil
			})
			if cancel != nil {
				t.Cleanup(cancel)
			}
			if result != nil {
				t.Cleanup(func() { require.NoError(t, result.Close()) })
			}
			require.NoError(t, err)
			require.Same(t, want, result)
			require.Len(t, attemptContexts, 3)
			require.Len(t, previousErrors, 2)
			for _, err := range previousErrors {
				if timeout > 0 {
					require.ErrorIs(t, err, context.Canceled, "previous attempt must be cancelled before replacement")
				} else {
					require.NoError(t, err)
				}
			}
			for _, child := range attemptContexts {
				_, hasDeadline := child.Deadline()
				require.Equal(t, timeout > 0, hasDeadline)
				if timeout == 0 {
					require.Same(t, ctx, child)
				}
			}
			require.NoError(t, attemptContexts[2].Err(), "successful attempt belongs to the caller")
			if timeout > 0 {
				require.NotNil(t, cancel)
				cancel()
				require.ErrorIs(t, attemptContexts[2].Err(), context.Canceled)
			} else {
				require.Nil(t, cancel)
			}
			require.NoError(t, ctx.Err(), "attempt cleanup must not cancel the borrowed parent")
		})
	}
}

func TestUpstreamExecutor_ExecWithRetry_AttemptTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := &UpstreamExecutor{retryDuration: 10 * time.Second}
		e.initRetryPolicy(&mockClassifier{retryable: false})
		e.retryPolicy.Backoff = nil
		calls := 0
		result, cancel, err := e.execWithRetry(context.Background(), nil, time.Second, func(ctx context.Context) (*Result, error) {
			calls++
			<-ctx.Done()
			return nil, ctx.Err()
		})
		if cancel != nil {
			t.Cleanup(cancel)
		}
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, result)
		require.Nil(t, cancel)
		require.Equal(t, 1, calls)
	})
}

// ---- UpstreamExecutor.ExecSQL no retry path with timeout ----

func TestUpstreamExecutor_ExecSQL_NoRetryWithTimeout(t *testing.T) {
	e := &UpstreamExecutor{}
	// conn is nil, will fail on ensureConnection
	_, _, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, time.Second)
	assert.Error(t, err)
}

// ---- UpstreamExecutor.calculateMaxAttempts edge cases ----

func TestUpstreamExecutor_CalculateMaxAttempts_NegativeOne(t *testing.T) {
	e := &UpstreamExecutor{retryTimes: -1}
	assert.Equal(t, int(2147483647), e.calculateMaxAttempts())
}

func TestUpstreamExecutor_CalculateMaxAttempts_Zero(t *testing.T) {
	e := &UpstreamExecutor{retryTimes: 0}
	assert.Equal(t, 1, e.calculateMaxAttempts())
}
