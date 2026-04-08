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
	"github.com/tidwall/btree"
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

func TestCreateUpstreamExecutor_InternalSQLExecutor_RequiresHelperFactory(t *testing.T) {
	_, _, err := createUpstreamExecutor(
		context.Background(), "cn1", nil, nil, nil,
		"internal_sql_executor:0", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires upstreamSQLHelperFactory")
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

// --- fillDefaultOption tests ---

func TestFillDefaultOption_NonNil_ZeroValues(t *testing.T) {
	opt := fillDefaultOption(&PublicationExecutorOption{})
	require.NotNil(t, opt)
	assert.NotZero(t, opt.GCInterval)
	assert.NotZero(t, opt.GCTTL)
	assert.NotZero(t, opt.SyncTaskInterval)
	assert.NotNil(t, opt.RetryOption)
	assert.NotNil(t, opt.SQLExecutorRetryOpt)
}

func TestFillDefaultOption_PresetValues(t *testing.T) {
	customInterval := 42 * time.Second
	customTTL := 99 * time.Hour
	opt := fillDefaultOption(&PublicationExecutorOption{
		GCInterval:          customInterval,
		GCTTL:               customTTL,
		SyncTaskInterval:    5 * time.Minute,
		RetryOption:         &ExecutorRetryOption{RetryTimes: 7},
		SQLExecutorRetryOpt: &SQLExecutorRetryOption{},
	})
	assert.Equal(t, customInterval, opt.GCInterval)
	assert.Equal(t, customTTL, opt.GCTTL)
	assert.Equal(t, 5*time.Minute, opt.SyncTaskInterval)
	assert.Equal(t, 7, opt.RetryOption.RetryTimes)
}

// --- getTask not-found case ---

func TestGetTask_NotFound(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	task, ok := exec.getTask("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, TaskEntry{}, task)
}

func TestGetTask_Found(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "found-task", LSN: 42, State: IterationStateRunning})
	task, ok := exec.getTask("found-task")
	assert.True(t, ok)
	assert.Equal(t, uint64(42), task.LSN)
	assert.Equal(t, IterationStateRunning, task.State)
}

func TestGetTaskPublic_NotFound(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	task, ok := exec.GetTask("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, TaskEntry{}, task)
}

func TestGetTaskPublic_Found(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "pub-task", LSN: 7, SubscriptionState: SubscriptionStatePause})
	task, ok := exec.GetTask("pub-task")
	assert.True(t, ok)
	assert.Equal(t, uint64(7), task.LSN)
	assert.Equal(t, SubscriptionStatePause, task.SubscriptionState)
}

// ---- fillDefaultOption tests ----

func TestFillDefaultOption_NilInput(t *testing.T) {
	opt := fillDefaultOption(nil)
	require.NotNil(t, opt)
	assert.NotZero(t, opt.GCInterval)
	assert.NotZero(t, opt.GCTTL)
	assert.NotZero(t, opt.SyncTaskInterval)
	assert.NotNil(t, opt.RetryOption)
	assert.NotNil(t, opt.SQLExecutorRetryOpt)
}

func TestFillDefaultOption_PartialInput(t *testing.T) {
	opt := fillDefaultOption(&PublicationExecutorOption{
		GCInterval: 5 * time.Minute,
	})
	require.NotNil(t, opt)
	assert.Equal(t, 5*time.Minute, opt.GCInterval)
	assert.NotZero(t, opt.GCTTL)
	assert.NotNil(t, opt.RetryOption)
}

func TestFillDefaultOption_FullInput(t *testing.T) {
	retryOpt := &ExecutorRetryOption{RetryTimes: 10, RetryInterval: time.Second}
	sqlRetryOpt := &SQLExecutorRetryOption{MaxRetries: 5, RetryInterval: time.Second}
	opt := fillDefaultOption(&PublicationExecutorOption{
		GCInterval:          1 * time.Minute,
		GCTTL:               2 * time.Minute,
		SyncTaskInterval:    3 * time.Minute,
		RetryOption:         retryOpt,
		SQLExecutorRetryOpt: sqlRetryOpt,
	})
	assert.Equal(t, 1*time.Minute, opt.GCInterval)
	assert.Equal(t, 2*time.Minute, opt.GCTTL)
	assert.Equal(t, 3*time.Minute, opt.SyncTaskInterval)
	assert.Same(t, retryOpt, opt.RetryOption)
	assert.Same(t, sqlRetryOpt, opt.SQLExecutorRetryOpt)
}

// ---- deleteTaskEntry tests ----

func TestDeleteTaskEntry(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "task-1", LSN: 1})
	exec.setTask(TaskEntry{TaskID: "task-2", LSN: 2})

	exec.deleteTaskEntry(TaskEntry{TaskID: "task-1"})
	_, ok := exec.getTask("task-1")
	assert.False(t, ok)
	_, ok = exec.getTask("task-2")
	assert.True(t, ok)
}
