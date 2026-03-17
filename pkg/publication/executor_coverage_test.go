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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- createUpstreamExecutor error branches ----

func TestCreateUpstreamExecutor_EmptyConn(t *testing.T) {
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil, "", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upstream_conn is empty")
}

func TestCreateUpstreamExecutor_InternalSQLExecutor_InvalidFormat(t *testing.T) {
	// "internal_sql_executor:a:b:c" → len(parts) > 2
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil,
		"internal_sql_executor:a:b:c", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid upstream_conn format")
}

func TestCreateUpstreamExecutor_InternalSQLExecutor_InvalidAccountID(t *testing.T) {
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil,
		"internal_sql_executor:notanumber", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse account ID")
}

func TestCreateUpstreamExecutor_ExternalConn_InvalidFormat(t *testing.T) {
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil,
		"invalid-connection-string", nil, nil, nil,
	)
	require.Error(t, err)
}

func TestCreateUpstreamExecutor_ExternalConn_EmptyUser(t *testing.T) {
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil,
		"mysql://:password@127.0.0.1:6001", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user cannot be empty")
}

// ---- retryPublication ----

func TestRetryPublication_Success(t *testing.T) {
	called := 0
	err := retryPublication(context.Background(), func() error {
		called++
		return nil
	}, DefaultExecutorRetryOption())
	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func TestRetryPublication_NilOption(t *testing.T) {
	err := retryPublication(context.Background(), func() error {
		return nil
	}, nil)
	assert.NoError(t, err)
}

func TestRetryPublication_NoRetryOnNonClassified(t *testing.T) {
	// retryPublication creates Policy with Classifier: nil → never retries
	attempt := 0
	err := retryPublication(context.Background(), func() error {
		attempt++
		return moerr.NewInternalErrorNoCtx("fail")
	}, &ExecutorRetryOption{
		RetryTimes:    5,
		RetryInterval: time.Millisecond,
		RetryDuration: time.Second,
	})
	assert.Error(t, err)
	assert.Equal(t, 1, attempt) // no retry since classifier is nil
}

func TestRetryPublication_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryPublication(ctx, func() error {
		return moerr.NewInternalErrorNoCtx("fail")
	}, &ExecutorRetryOption{
		RetryTimes:    10,
		RetryInterval: time.Millisecond,
		RetryDuration: time.Second,
	})
	assert.Error(t, err)
}

func TestRetryPublication_ErrNonRetryable(t *testing.T) {
	attempt := 0
	err := retryPublication(context.Background(), func() error {
		attempt++
		if attempt > 1 {
			return ErrNonRetryable
		}
		return moerr.NewInternalErrorNoCtx("fail")
	}, &ExecutorRetryOption{
		RetryTimes:    10,
		RetryInterval: time.Millisecond,
		RetryDuration: time.Second,
	})
	assert.Error(t, err)
}
