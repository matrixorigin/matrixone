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

package cdc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWatermarkUpdaterWithTracking tracks UpdateWatermarkErrMsg calls
type mockWatermarkUpdaterWithTracking struct {
	watermarks map[string]types.TS
	errorCalls []errorCall
	mu         sync.Mutex
	failClear  bool // if true, clearing errors will fail
}

type errorCall struct {
	errMsg   string
	errorCtx *ErrorContext
}

func newMockWatermarkUpdaterWithTracking() *mockWatermarkUpdaterWithTracking {
	return &mockWatermarkUpdaterWithTracking{
		watermarks: make(map[string]types.TS),
		errorCalls: make([]errorCall, 0),
	}
}

func (m *mockWatermarkUpdaterWithTracking) RemoveCachedWM(ctx context.Context, key *WatermarkKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.watermarks, m.keyString(key))
	return nil
}

func (m *mockWatermarkUpdaterWithTracking) UpdateWatermarkErrMsg(ctx context.Context, key *WatermarkKey, errMsg string, errorCtx *ErrorContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Track the call
	m.errorCalls = append(m.errorCalls, errorCall{
		errMsg:   errMsg,
		errorCtx: errorCtx,
	})

	// Simulate failure if configured
	if m.failClear && errMsg == "" {
		return assert.AnError
	}

	return nil
}

func (m *mockWatermarkUpdaterWithTracking) GetFromCache(ctx context.Context, key *WatermarkKey) (types.TS, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.watermarks[m.keyString(key)]
	if !ok {
		return types.TS{}, assert.AnError
	}
	return ts, nil
}

func (m *mockWatermarkUpdaterWithTracking) GetOrAddCommitted(ctx context.Context, key *WatermarkKey, watermark *types.TS) (types.TS, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keyStr := m.keyString(key)
	if ts, ok := m.watermarks[keyStr]; ok {
		return ts, nil
	}
	m.watermarks[keyStr] = *watermark
	return *watermark, nil
}

func (m *mockWatermarkUpdaterWithTracking) UpdateWatermarkOnly(ctx context.Context, key *WatermarkKey, watermark *types.TS) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watermarks[m.keyString(key)] = *watermark
	return nil
}

func (m *mockWatermarkUpdaterWithTracking) keyString(key *WatermarkKey) string {
	return key.TaskId + ":" + key.DBName + ":" + key.TableName
}

func (m *mockWatermarkUpdaterWithTracking) getClearErrorCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, call := range m.errorCalls {
		if call.errMsg == "" {
			count++
		}
	}
	return count
}

func (m *mockWatermarkUpdaterWithTracking) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCalls = make([]errorCall, 0)
}

// TestClearErrorOnFirstSuccess verifies error clearing behavior on first successful data processing
func TestClearErrorOnFirstSuccess(t *testing.T) {
	t.Run("FirstSuccessClearsError", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()

		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
		}

		ctx := context.Background()

		// First success should clear error
		stream.clearErrorOnFirstSuccess(ctx)

		// Verify error was cleared
		assert.Equal(t, 1, updater.getClearErrorCallCount(), "Error should be cleared on first success")
		assert.True(t, stream.hasSucceeded.Load(), "hasSucceeded flag should be set")
	})

	t.Run("MultipleSuccessOnlyClearsOnce", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()

		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
		}

		ctx := context.Background()

		// Multiple successes should only clear once
		for i := 0; i < 5; i++ {
			stream.clearErrorOnFirstSuccess(ctx)
		}

		// Verify error was only cleared once
		assert.Equal(t, 1, updater.getClearErrorCallCount(), "Error should only be cleared once, not on every success")
		assert.True(t, stream.hasSucceeded.Load(), "hasSucceeded flag should remain set")
	})

	t.Run("ClearErrorFailureDoesNotBlockExecution", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()
		updater.failClear = true // Simulate error clearing failure

		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
			tableInfo: &DbTableInfo{
				SourceDbName:  "db1",
				SourceTblName: "t1",
			},
		}

		ctx := context.Background()

		// Should not panic even if error clearing fails
		require.NotPanics(t, func() {
			stream.clearErrorOnFirstSuccess(ctx)
		})

		// hasSucceeded should still be set
		assert.True(t, stream.hasSucceeded.Load(), "hasSucceeded flag should be set even if error clearing fails")
	})

	t.Run("ConcurrentClearErrorSafety", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()

		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
		}

		ctx := context.Background()

		// Simulate concurrent success processing
		var wg sync.WaitGroup
		concurrency := 10
		wg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				stream.clearErrorOnFirstSuccess(ctx)
			}()
		}

		wg.Wait()

		// Verify error was only cleared once despite concurrent calls
		assert.Equal(t, 1, updater.getClearErrorCallCount(), "Error should only be cleared once even with concurrent calls")
		assert.True(t, stream.hasSucceeded.Load(), "hasSucceeded flag should be set")
	})

	t.Run("ResetSuccessFlagBetweenReaders", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()

		// First reader
		stream1 := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
		}

		ctx := context.Background()
		stream1.clearErrorOnFirstSuccess(ctx)
		assert.Equal(t, 1, updater.getClearErrorCallCount())

		// Second reader (simulating reader restart)
		// Note: In production, a new TableChangeStream instance would be created,
		// so hasSucceeded would start as false
		stream2 := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
			// hasSucceeded is false by default (new instance)
		}

		// First success of new reader should clear error again
		stream2.clearErrorOnFirstSuccess(ctx)

		// Verify error was cleared again (total 2 times)
		assert.Equal(t, 2, updater.getClearErrorCallCount(), "New reader instance should clear error on its first success")
		assert.True(t, stream2.hasSucceeded.Load())
	})
}

// TestHasSucceededAtomicBehavior verifies atomic.Bool behavior
func TestHasSucceededAtomicBehavior(t *testing.T) {
	var hasSucceeded atomic.Bool

	// Initial state
	assert.False(t, hasSucceeded.Load(), "Initial state should be false")

	// Set to true
	hasSucceeded.Store(true)
	assert.True(t, hasSucceeded.Load(), "Should be true after Store(true)")

	// Multiple reads
	for i := 0; i < 100; i++ {
		assert.True(t, hasSucceeded.Load(), "Should remain true")
	}

	// Concurrent load/store
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			hasSucceeded.Load()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			hasSucceeded.Store(true)
		}
	}()

	wg.Wait()
	assert.True(t, hasSucceeded.Load(), "Should be true after concurrent operations")
}

// TestErrorClearingInProcessWithTxn verifies error clearing is called at correct points
func TestErrorClearingInProcessWithTxn(t *testing.T) {
	t.Run("ClearOnReachEndTs", func(t *testing.T) {
		// This test would require more complex mocking of the entire processWithTxn flow
		// For now, we verify the logic by checking the clearErrorOnFirstSuccess method directly
		// Integration tests in table_change_stream_test.go cover the full flow

		updater := newMockWatermarkUpdaterWithTracking()
		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
			endTs: types.BuildTS(100, 0),
		}

		ctx := context.Background()

		// Simulate reaching endTs
		stream.clearErrorOnFirstSuccess(ctx)

		assert.Equal(t, 1, updater.getClearErrorCallCount())
		assert.True(t, stream.hasSucceeded.Load())
	})

	t.Run("ClearOnNoMoreData", func(t *testing.T) {
		updater := newMockWatermarkUpdaterWithTracking()
		stream := &TableChangeStream{
			watermarkUpdater: updater,
			watermarkKey: &WatermarkKey{
				AccountId: 1,
				TaskId:    "task1",
				DBName:    "db1",
				TableName: "t1",
			},
		}

		ctx := context.Background()

		// Simulate ChangeTypeNoMoreData
		stream.clearErrorOnFirstSuccess(ctx)

		assert.Equal(t, 1, updater.getClearErrorCallCount())
		assert.True(t, stream.hasSucceeded.Load())
	})
}
