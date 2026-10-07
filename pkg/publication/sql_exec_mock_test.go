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
	e := &UpstreamExecutor{
		retryTimes:    100,
		retryDuration: time.Nanosecond,
	}
	e.initRetryPolicy(&mockClassifier{retryable: true})
	// A nil backoff keeps this boundary test immediate. ExponentialBackoff{}
	// applies its default 100 ms base delay.
	e.retryPolicy.Backoff = nil

	attempt := 0
	_, _, err := e.execWithRetry(context.Background(), nil, 0, func(ctx context.Context) (*Result, error) {
		attempt++
		return nil, errors.New("fail")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "retry limit exceeded")
	assert.Equal(t, 1, attempt)
}

// ---- UpstreamExecutor.execWithRetry success after retry ----

func TestUpstreamExecutor_ExecWithRetry_SuccessAfterRetry(t *testing.T) {
	e := &UpstreamExecutor{retryTimes: 5, retryDuration: 10 * time.Second}
	e.initRetryPolicy(&mockClassifier{retryable: true})
	e.retryPolicy.Backoff = nil

	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	want := &Result{}
	var attemptContexts []context.Context
	var previousErrors []error
	result, cancel, err := e.execWithRetry(ctx, nil, time.Second, func(ctx context.Context) (*Result, error) {
		if len(attemptContexts) > 0 {
			previousErrors = append(previousErrors, attemptContexts[len(attemptContexts)-1].Err())
		}
		attemptContexts = append(attemptContexts, ctx)
		if len(attemptContexts) < 3 {
			return nil, errors.New("transient")
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
		require.ErrorIs(t, err, context.Canceled, "previous attempt must be cancelled before replacement")
	}
	for _, ctx := range attemptContexts {
		_, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)
	}
	require.NoError(t, attemptContexts[2].Err(), "successful attempt belongs to the caller")
	require.NotNil(t, cancel)
	cancel()
	require.ErrorIs(t, attemptContexts[2].Err(), context.Canceled)
}

func TestUpstreamExecutor_ExecWithRetry_AttemptTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := &UpstreamExecutor{retryDuration: 10 * time.Second}
		e.initRetryPolicy(&mockClassifier{retryable: false})
		e.retryPolicy.Backoff = nil
		calls := 0
		start := time.Now()
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
		require.Equal(t, time.Second, time.Since(start))
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
