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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock helpers (local to this file, following existing patterns)
// ---------------------------------------------------------------------------

// mockSQLExecutorForJob implements SQLExecutor via a pluggable function.
type mockSQLExecutorForJob struct {
	execFn func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error)
}

func (m *mockSQLExecutorForJob) Close() error                           { return nil }
func (m *mockSQLExecutorForJob) Connect() error                         { return nil }
func (m *mockSQLExecutorForJob) EndTxn(_ context.Context, _ bool) error { return nil }
func (m *mockSQLExecutorForJob) ExecSQL(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return m.execFn(ctx, ar, accountID, query, useTxn, needRetry, timeout)
}
func (m *mockSQLExecutorForJob) ExecSQLInDatabase(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, _ string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return m.execFn(ctx, ar, accountID, query, useTxn, needRetry, timeout)
}

// mockScanner implements MockResultScanner for fine-grained control.
type mockScanner struct {
	nextFn  func() bool
	scanFn  func(dest ...interface{}) error
	closeFn func() error
	errFn   func() error
}

func (m *mockScanner) Next() bool                        { return m.nextFn() }
func (m *mockScanner) Scan(dest ...interface{}) error     { return m.scanFn(dest...) }
func (m *mockScanner) Close() error                       { return m.closeFn() }
func (m *mockScanner) Err() error                         { return m.errFn() }

// newMockResult wraps a mockScanner into a *Result.
func newMockResult(s MockResultScanner) *Result {
	return &Result{mockResult: s}
}

// noopCancel is a no-op cancel func used in tests.
func noopCancel() {}

// ---------------------------------------------------------------------------
// GetMetaJob.Execute() error-path tests
// ---------------------------------------------------------------------------

func TestGetMetaJob_Execute_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			t.Fatal("ExecSQL should not be called when context is cancelled")
			return nil, nil, nil
		},
	}
	job := NewGetMetaJob(ctx, mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.ErrorIs(t, res.Err, context.Canceled)
}

func TestGetMetaJob_Execute_NonRetryableExecError(t *testing.T) {
	// A non-retryable error (no "timeout", "connection reset", etc.)
	nonRetryErr := moerr.NewInternalErrorNoCtx("permission denied")
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, nonRetryErr
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to execute GETOBJECT query")
}

func TestGetMetaJob_Execute_NextFalse_WithError_NonRetryable(t *testing.T) {
	scanErr := moerr.NewInternalErrorNoCtx("data corruption")
	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return scanErr },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to read metadata")
}

func TestGetMetaJob_Execute_NextFalse_NoError_NoContent(t *testing.T) {
	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "no object content returned")
}

func TestGetMetaJob_Execute_ScanError_NonRetryable(t *testing.T) {
	callIdx := 0
	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			return moerr.NewInternalErrorNoCtx("column type mismatch")
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			callIdx++
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to scan offset 0")
}

func TestGetMetaJob_Execute_TotalChunksZero(t *testing.T) {
	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			// Assign: MetadataData, TotalSize, ChunkIndex, TotalChunks, IsComplete
			*dest[0].(*[]byte) = []byte("data")
			*dest[1].(*int64) = 100
			*dest[2].(*int64) = 0
			*dest[3].(*int64) = 0 // TotalChunks <= 0
			*dest[4].(*bool) = true
			return nil
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "invalid total_chunks")
}

func TestGetMetaJob_Execute_Success(t *testing.T) {
	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			*dest[0].(*[]byte) = []byte("metadata")
			*dest[1].(*int64) = 256
			*dest[2].(*int64) = 0
			*dest[3].(*int64) = 3
			*dest[4].(*bool) = false
			return nil
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	require.NoError(t, res.Err)
	assert.Equal(t, []byte("metadata"), res.MetadataData)
	assert.Equal(t, int64(256), res.TotalSize)
	assert.Equal(t, int64(3), res.TotalChunks)
	assert.False(t, res.IsComplete)
}

func TestGetMetaJob_Execute_AllRetriesExhausted(t *testing.T) {
	attempts := 0
	// Return a retryable error (contains "timeout") on every call
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			return nil, nil, moerr.NewInternalErrorNoCtx("connection timeout")
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

func TestGetMetaJob_Execute_RetryableThenSuccess(t *testing.T) {
	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			if attempts < 3 {
				return nil, nil, moerr.NewInternalErrorNoCtx("connection timeout")
			}
			scanner := &mockScanner{
				nextFn: func() bool { return true },
				scanFn: func(dest ...interface{}) error {
					*dest[0].(*[]byte) = []byte("data")
					*dest[1].(*int64) = 100
					*dest[2].(*int64) = 0
					*dest[3].(*int64) = 1
					*dest[4].(*bool) = true
					return nil
				},
				closeFn: func() error { return nil },
				errFn:   func() error { return nil },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	require.NoError(t, res.Err)
	assert.Equal(t, 3, attempts)
}

func TestGetMetaJob_Execute_NextFalse_WithRetryableError_ThenExhausted(t *testing.T) {
	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			scanner := &mockScanner{
				nextFn:  func() bool { return false },
				scanFn:  func(dest ...interface{}) error { return nil },
				closeFn: func() error { return nil },
				errFn:   func() error { return moerr.NewInternalErrorNoCtx("connection timeout") },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

func TestGetMetaJob_Execute_ScanError_Retryable_ThenExhausted(t *testing.T) {
	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			scanner := &mockScanner{
				nextFn: func() bool { return true },
				scanFn: func(dest ...interface{}) error {
					return moerr.NewInternalErrorNoCtx("connection timeout during scan")
				},
				closeFn: func() error { return nil },
				errFn:   func() error { return nil },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetMetaJob(context.Background(), mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

// ---------------------------------------------------------------------------
// GetChunkJob.Execute() error-path tests
// ---------------------------------------------------------------------------

func TestGetChunkJob_Execute_ContextCancelledDuringSemaphore(t *testing.T) {
	// Reset semaphore to ensure fresh state
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			t.Fatal("should not reach ExecSQL")
			return nil, nil, nil
		},
	}
	job := NewGetChunkJob(ctx, mock, "obj1", 1, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.ErrorIs(t, res.Err, context.Canceled)
	assert.Equal(t, int64(1), res.ChunkIndex)
}

func TestGetChunkJob_Execute_NonRetryableExecError(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, moerr.NewInternalErrorNoCtx("access denied")
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 2, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to execute GETOBJECT query for offset 2")
}

func TestGetChunkJob_Execute_NoChunkContent(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 3, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "no chunk content returned")
}

func TestGetChunkJob_Execute_AllRetriesExhausted(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			return nil, nil, moerr.NewInternalErrorNoCtx("network error")
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 4, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

func TestGetChunkJob_Execute_Success(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			*dest[0].(*[]byte) = []byte("chunk-data")
			*dest[1].(*int64) = 500
			*dest[2].(*int64) = 1
			*dest[3].(*int64) = 5
			*dest[4].(*bool) = false
			return nil
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 1, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	require.NoError(t, res.Err)
	assert.Equal(t, []byte("chunk-data"), res.ChunkData)
	assert.Equal(t, int64(1), res.ChunkIndex)
}

func TestGetChunkJob_Execute_ContextCancelledDuringRetry(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			if attempts == 1 {
				// First attempt: retryable error
				return nil, nil, moerr.NewInternalErrorNoCtx("network error")
			}
			// Cancel before second attempt check
			cancel()
			return nil, nil, moerr.NewInternalErrorNoCtx("network error")
		},
	}
	job := NewGetChunkJob(ctx, mock, "obj1", 0, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
}

func TestGetChunkJob_Execute_ScanError_NonRetryable(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			return moerr.NewInternalErrorNoCtx("column mismatch")
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 5, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to scan offset 5")
}

func TestGetChunkJob_Execute_ScanError_Retryable_ThenExhausted(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			scanner := &mockScanner{
				nextFn: func() bool { return true },
				scanFn: func(dest ...interface{}) error {
					return moerr.NewInternalErrorNoCtx("i/o timeout during scan")
				},
				closeFn: func() error { return nil },
				errFn:   func() error { return nil },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 6, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

func TestGetChunkJob_Execute_NextFalse_WithRetryableError(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			scanner := &mockScanner{
				nextFn:  func() bool { return false },
				scanFn:  func(dest ...interface{}) error { return nil },
				closeFn: func() error { return nil },
				errFn:   func() error { return moerr.NewInternalErrorNoCtx("backend unavailable") },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 7, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "after 3 retries")
	assert.Equal(t, 3, attempts)
}

func TestGetChunkJob_Execute_NextFalse_WithNonRetryableError(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return moerr.NewInternalErrorNoCtx("data corruption") },
	}
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 8, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "failed to read chunk 8")
}

// ---------------------------------------------------------------------------
// Result methods coverage (mock and nil paths in sql_executor.go)
// ---------------------------------------------------------------------------

func TestResult_Close_MockPath(t *testing.T) {
	closed := false
	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { closed = true; return nil },
		errFn:   func() error { return nil },
	}
	r := newMockResult(scanner)
	err := r.Close()
	assert.NoError(t, err)
	assert.True(t, closed)
}

func TestResult_Close_NilResult(t *testing.T) {
	r := &Result{}
	err := r.Close()
	assert.NoError(t, err)
}

func TestResult_Next_MockPath(t *testing.T) {
	calls := 0
	scanner := &mockScanner{
		nextFn:  func() bool { calls++; return calls <= 2 },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	r := newMockResult(scanner)
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
}

func TestResult_Next_NilResult(t *testing.T) {
	r := &Result{}
	assert.False(t, r.Next())
}

func TestResult_Scan_MockPath(t *testing.T) {
	scanner := &mockScanner{
		nextFn: func() bool { return true },
		scanFn: func(dest ...interface{}) error {
			*dest[0].(*string) = "hello"
			return nil
		},
		closeFn: func() error { return nil },
		errFn:   func() error { return nil },
	}
	r := newMockResult(scanner)
	var s string
	err := r.Scan(&s)
	assert.NoError(t, err)
	assert.Equal(t, "hello", s)
}

func TestResult_Scan_NilResult(t *testing.T) {
	r := &Result{}
	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "result is nil")
}

func TestResult_Err_MockPath(t *testing.T) {
	expectedErr := moerr.NewInternalErrorNoCtx("test err")
	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return nil },
		errFn:   func() error { return expectedErr },
	}
	r := newMockResult(scanner)
	assert.Equal(t, expectedErr, r.Err())
}

func TestResult_Err_NilResult(t *testing.T) {
	r := &Result{}
	assert.NoError(t, r.Err())
}

func TestResult_Close_MockError(t *testing.T) {
	scanner := &mockScanner{
		nextFn:  func() bool { return false },
		scanFn:  func(dest ...interface{}) error { return nil },
		closeFn: func() error { return fmt.Errorf("close failed") },
		errFn:   func() error { return nil },
	}
	r := newMockResult(scanner)
	err := r.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

// ---------------------------------------------------------------------------
// ActiveRoutine tests (sql_executor.go)
// ---------------------------------------------------------------------------

func TestNewActiveRoutine(t *testing.T) {
	ar := NewActiveRoutine()
	require.NotNil(t, ar)
	require.NotNil(t, ar.Pause)
	require.NotNil(t, ar.Cancel)
}

func TestActiveRoutine_ClosePause(t *testing.T) {
	ar := NewActiveRoutine()
	ar.ClosePause()
	// After close, receiving from Pause should not block
	select {
	case <-ar.Pause:
		// expected
	default:
		t.Fatal("Pause channel should be closed")
	}
}

func TestActiveRoutine_CloseCancel(t *testing.T) {
	ar := NewActiveRoutine()
	ar.CloseCancel()
	select {
	case <-ar.Cancel:
		// expected
	default:
		t.Fatal("Cancel channel should be closed")
	}
}

// ---------------------------------------------------------------------------
// getOrCreateChunkSemaphore additional coverage
// ---------------------------------------------------------------------------

func TestGetOrCreateChunkSemaphore_DefaultCapacity(t *testing.T) {
	// Reset to test default path
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	sem := getOrCreateChunkSemaphore()
	require.NotNil(t, sem)
	// Capacity should be > 0
	assert.Greater(t, cap(sem), 0)
}

func TestGetOrCreateChunkSemaphore_Idempotent(t *testing.T) {
	// Reset
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	sem1 := getOrCreateChunkSemaphore()
	sem2 := getOrCreateChunkSemaphore()
	// Should return the exact same channel
	assert.Equal(t, fmt.Sprintf("%p", sem1), fmt.Sprintf("%p", sem2))
}

// ---------------------------------------------------------------------------
// ParseUpstreamConn coverage (sql_executor.go)
// ---------------------------------------------------------------------------

func TestParseUpstreamConn_EmptyString(t *testing.T) {
	_, err := ParseUpstreamConn("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestParseUpstreamConn_InvalidPrefix(t *testing.T) {
	_, err := ParseUpstreamConn("postgres://user:pass@localhost:3306")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid connection string")
}

func TestParseUpstreamConn_ValidFormat(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://acc#user:pass@127.0.0.1:6001")
	require.NoError(t, err)
	assert.Equal(t, "acc", cfg.Account)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "pass", cfg.Password)
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Equal(t, 6001, cfg.Port)
}

func TestParseUpstreamConn_NoAccount(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://user:pass@127.0.0.1:6001")
	require.NoError(t, err)
	assert.Equal(t, "", cfg.Account)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "pass", cfg.Password)
}

func TestParseUpstreamConn_MissingAt(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://userpass")
	assert.Error(t, err)
}

func TestParseUpstreamConn_InvalidPort(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user:pass@host:notaport")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// SetGetParameterUnitWrapper coverage (sql_executor.go)
// ---------------------------------------------------------------------------

func TestSetGetParameterUnitWrapper(t *testing.T) {
	// Save original and restore after test
	getParameterUnitWrapperMu.RLock()
	orig := getParameterUnitWrapper
	getParameterUnitWrapperMu.RUnlock()
	defer SetGetParameterUnitWrapper(orig)

	called := false
	SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
		called = true
		return nil
	})

	getParameterUnitWrapperMu.RLock()
	fn := getParameterUnitWrapper
	getParameterUnitWrapperMu.RUnlock()
	require.NotNil(t, fn)
	fn("test-uuid")
	assert.True(t, called)
}

// ---------------------------------------------------------------------------
// FilterObjectJob.Execute() with nil ttlChecker (no TTL check)
// ---------------------------------------------------------------------------

func TestFilterObjectJob_Execute_NilTTLChecker(t *testing.T) {
	// With nil ttlChecker, Execute should proceed to FilterObject which will fail
	// on invalid objectStatsBytes, giving us coverage of the non-TTL path.
	job := NewFilterObjectJob(
		context.Background(),
		nil,               // objectStatsBytes
		types.TS{},        // snapshotTS
		nil,               // upstreamExecutor
		false,             // isTombstone
		nil,               // localFS
		nil,               // mp
		nil,               // getChunkWorker
		nil,               // writeObjectWorker
		"acc1",            // subscriptionAccountName
		"pub1",            // pubName
		nil,               // ccprCache
		nil,               // txnID
		nil,               // aobjectMap
		nil,               // ttlChecker (nil)
	)
	job.Execute()
	res := job.WaitDone().(*FilterObjectJobResult)
	// Should fail because objectStatsBytes is nil (FilterObject returns error)
	assert.Error(t, res.Err)
}

// ---------------------------------------------------------------------------
// InvalidAccountID constant
// ---------------------------------------------------------------------------

func TestInvalidAccountID(t *testing.T) {
	assert.Equal(t, uint32(0xFFFFFFFF), InvalidAccountID)
}

// ---------------------------------------------------------------------------
// UpstreamConnConfig struct
// ---------------------------------------------------------------------------

func TestUpstreamConnConfig_Fields(t *testing.T) {
	cfg := UpstreamConnConfig{
		Account:  "acc",
		User:     "user",
		Password: "pass",
		Host:     "localhost",
		Port:     3306,
		Timeout:  "10s",
	}
	assert.Equal(t, "acc", cfg.Account)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "pass", cfg.Password)
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 3306, cfg.Port)
	assert.Equal(t, "10s", cfg.Timeout)
}

// ---------------------------------------------------------------------------
// tryDecryptPassword coverage (short password path)
// ---------------------------------------------------------------------------

func TestTryDecryptPassword_ShortPassword(t *testing.T) {
	// Password shorter than 32 chars should be returned as-is
	result := tryDecryptPassword(context.Background(), "shortpass", nil, "")
	assert.Equal(t, "shortpass", result)
}

func TestTryDecryptPassword_InvalidHex(t *testing.T) {
	// A 32+ char string that is NOT valid hex
	longNonHex := "this_is_not_hex_and_it_is_quite_long_enough"
	result := tryDecryptPassword(context.Background(), longNonHex, nil, "")
	assert.Equal(t, longNonHex, result)
}

func TestTryDecryptPassword_NilExecutor(t *testing.T) {
	// Valid hex, 32+ chars, but nil executor -> returns as-is
	validHex := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	result := tryDecryptPassword(context.Background(), validHex, nil, "")
	assert.Equal(t, validHex, result)
}

// ---------------------------------------------------------------------------
// GetMetaJob.Execute() - context cancelled between retry loop iterations
// ---------------------------------------------------------------------------

func TestGetMetaJob_Execute_ContextCancelledBetweenRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			// Cancel context after first retryable failure
			cancel()
			return nil, nil, moerr.NewInternalErrorNoCtx("connection timeout")
		},
	}
	job := NewGetMetaJob(ctx, mock, "obj1", "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, res.Err)
}

// ---------------------------------------------------------------------------
// GetChunkJob.Execute() - retryable exec error then success
// ---------------------------------------------------------------------------

func TestGetChunkJob_Execute_RetryableThenSuccess(t *testing.T) {
	// Reset semaphore
	getChunkSemaphoreOnce = sync.Once{}
	getChunkSemaphore = nil

	attempts := 0
	mock := &mockSQLExecutorForJob{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			attempts++
			if attempts < 2 {
				return nil, nil, moerr.NewInternalErrorNoCtx("backend unavailable")
			}
			scanner := &mockScanner{
				nextFn: func() bool { return true },
				scanFn: func(dest ...interface{}) error {
					*dest[0].(*[]byte) = []byte("ok")
					*dest[1].(*int64) = 10
					*dest[2].(*int64) = 0
					*dest[3].(*int64) = 1
					*dest[4].(*bool) = true
					return nil
				},
				closeFn: func() error { return nil },
				errFn:   func() error { return nil },
			}
			return newMockResult(scanner), noopCancel, nil
		},
	}
	job := NewGetChunkJob(context.Background(), mock, "obj1", 0, "acc1", "pub1")
	job.Execute()
	res := job.WaitDone().(*GetChunkJobResult)
	require.NoError(t, res.Err)
	assert.Equal(t, 2, attempts)
}

// ---------------------------------------------------------------------------
// AObjectMap coverage
// ---------------------------------------------------------------------------

func TestAObjectMap_NewAndOperations(t *testing.T) {
	m := NewAObjectMap()
	require.NotNil(t, m)

	// Get on empty map
	_, exists := m.Get("nonexistent")
	assert.False(t, exists)

	// Set and Get
	mapping := &AObjectMapping{
		DownstreamStats: objectio.ObjectStats{},
		IsTombstone:     false,
		DBName:          "db1",
		TableName:       "tbl1",
		RowOffsetMap:    map[uint32]uint32{0: 1, 1: 0},
	}
	m.Set("upstream-obj-1", mapping)

	got, exists := m.Get("upstream-obj-1")
	require.True(t, exists)
	assert.Equal(t, "db1", got.DBName)
	assert.Equal(t, "tbl1", got.TableName)
	assert.False(t, got.IsTombstone)
	assert.Equal(t, uint32(1), got.RowOffsetMap[0])

	// Overwrite existing
	mapping2 := &AObjectMapping{
		IsTombstone: true,
		DBName:      "db2",
		TableName:   "tbl2",
	}
	m.Set("upstream-obj-1", mapping2)
	got2, exists := m.Get("upstream-obj-1")
	require.True(t, exists)
	assert.True(t, got2.IsTombstone)
	assert.Equal(t, "db2", got2.DBName)

	// Delete
	m.Delete("upstream-obj-1")
	_, exists = m.Get("upstream-obj-1")
	assert.False(t, exists)

	// Delete non-existent (no panic)
	m.Delete("nonexistent")
}

func TestAObjectMap_MultipleEntries(t *testing.T) {
	m := NewAObjectMap()
	for i := 0; i < 10; i++ {
		m.Set(fmt.Sprintf("obj-%d", i), &AObjectMapping{
			DBName:    fmt.Sprintf("db-%d", i),
			TableName: fmt.Sprintf("tbl-%d", i),
		})
	}
	for i := 0; i < 10; i++ {
		got, exists := m.Get(fmt.Sprintf("obj-%d", i))
		require.True(t, exists)
		assert.Equal(t, fmt.Sprintf("db-%d", i), got.DBName)
	}
}

// ---------------------------------------------------------------------------
// Additional util.go coverage
// ---------------------------------------------------------------------------

func TestParseUpstreamConn_PasswordWithSpecialChars(t *testing.T) {
	// Password containing colon - should be preserved via Join
	cfg, err := ParseUpstreamConn("mysql://acc#user:p4ss:w0rd@host:3306")
	require.NoError(t, err)
	assert.Equal(t, "p4ss:w0rd", cfg.Password)
	assert.Equal(t, "host", cfg.Host)
	assert.Equal(t, 3306, cfg.Port)
	assert.Equal(t, "acc", cfg.Account)
}
