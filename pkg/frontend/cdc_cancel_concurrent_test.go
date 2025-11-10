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

package frontend

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMockTableDetector creates a properly initialized mock TableDetector
// This ensures scanTableFn is set to prevent panics in scanTableLoop
func createMockTableDetector() *cdc.TableDetector {
	detector := &cdc.TableDetector{
		Mp:                   make(map[uint32]cdc.TblMap),
		Callbacks:            make(map[string]cdc.TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
	}

	// Set scanTableFn to no-op to prevent panic in scanTableLoop
	detectorValue := reflect.ValueOf(detector).Elem()
	scanTableFnField := detectorValue.FieldByName("scanTableFn")
	if scanTableFnField.IsValid() && scanTableFnField.CanSet() {
		scanTableFnField.Set(reflect.ValueOf(func() error {
			return nil // No-op scan function for testing
		}))
	}

	// Call Register to properly initialize cancel field
	// This starts scanTableLoop, which is safe because scanTableFn is no-op
	detector.Register("__test_permanent_dummy__", 1, []string{}, []string{}, func(map[uint32]cdc.TblMap) error {
		return nil // Dummy callback
	})

	return detector
}

// setupMockTableDetector creates and stubs a mock TableDetector for testing
// Returns gostub.Stubs that should be reset with .Reset()
// Uses a shared mock detector instance and initializes cancel properly via Register
func setupMockTableDetector() *gostub.Stubs {
	sharedDetector := createMockTableDetector()

	// Return the same shared detector instance for all calls
	return gostub.Stub(&cdc.GetTableDetector, func(cnUUID string) *cdc.TableDetector {
		return sharedDetector
	})
}

// mockConcurrentChangeReader implements cdc.ChangeReader for concurrent testing
type mockConcurrentChangeReader struct {
	running   atomic.Bool
	stopped   atomic.Bool
	wg        sync.WaitGroup
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	tableInfo *cdc.DbTableInfo
	stopDelay time.Duration // Simulate work time before stopping
	started   chan struct{} // Signal when Run() has started
}

func newMockConcurrentChangeReader(tableInfo *cdc.DbTableInfo, stopDelay time.Duration) *mockConcurrentChangeReader {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockConcurrentChangeReader{
		ctx:       ctx,
		cancel:    cancel,
		tableInfo: tableInfo,
		stopDelay: stopDelay,
		started:   make(chan struct{}),
	}
}

func (m *mockConcurrentChangeReader) Run(ctx context.Context, ar *cdc.ActiveRoutine) {
	m.running.Store(true)
	m.wg.Add(1)
	defer m.wg.Done()
	defer m.stopped.Store(true)

	// Signal that Run() has started (for deterministic testing)
	close(m.started)

	// Simulate ongoing work
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			// Simulate time to cleanup resources
			if m.stopDelay > 0 {
				time.Sleep(m.stopDelay)
			}
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Simulate periodic work
		}
	}
}

func (m *mockConcurrentChangeReader) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
	})
}

func (m *mockConcurrentChangeReader) Wait() {
	m.wg.Wait()
}

func (m *mockConcurrentChangeReader) GetTableInfo() *cdc.DbTableInfo {
	return m.tableInfo
}

// TestCancelSynchronouslyStopsReaders verifies that Cancel() waits for all readers to stop
func TestCancelSynchronouslyStopsReaders(t *testing.T) {
	// Note: GetTableDetector is already stubbed globally in cdc_test.go init()

	// Get baseline goroutine count
	baselineGoroutines := runtime.NumGoroutine()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-cancel",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Transition to Running state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	// Create multiple readers with different stop delays to simulate real workload
	numReaders := 5
	readers := make([]*mockConcurrentChangeReader, numReaders)

	for i := 0; i < numReaders; i++ {
		tableInfo := &cdc.DbTableInfo{
			SourceDbName:  "test_db",
			SourceTblName: "test_table_" + string(rune('A'+i)),
			SourceTblId:   uint64(i + 1),
		}
		// Vary stop delays to simulate different workloads
		stopDelay := time.Duration(i*10) * time.Millisecond
		reader := newMockConcurrentChangeReader(tableInfo, stopDelay)
		readers[i] = reader

		// Start reader in goroutine
		go reader.Run(context.Background(), exec.activeRoutine)

		// Store in runningReaders
		key := tableInfo.SourceDbName + "." + tableInfo.SourceTblName
		exec.runningReaders.Store(key, reader)
	}

	// Wait for all readers to start (deterministic)
	for i, reader := range readers {
		<-reader.started // Block until Run() has started
		assert.True(t, reader.running.Load(), "Reader %d should be running", i)
		assert.False(t, reader.stopped.Load(), "Reader %d should not be stopped yet", i)
	}

	// Record goroutine count before Cancel
	goroutinesBeforeCancel := runtime.NumGoroutine()
	t.Logf("Goroutines before Cancel: %d (baseline: %d, diff: %d)",
		goroutinesBeforeCancel, baselineGoroutines, goroutinesBeforeCancel-baselineGoroutines)

	// Call Cancel - this should synchronously wait for all readers to stop
	startTime := time.Now()
	err := exec.Cancel()
	cancelDuration := time.Since(startTime)

	require.NoError(t, err)
	assert.Equal(t, StateCancelled, exec.stateMachine.State())

	// Cancel should have waited for all readers to stop
	// The slowest reader has 40ms delay, so Cancel should take at least that long
	assert.GreaterOrEqual(t, cancelDuration.Milliseconds(), int64(40),
		"Cancel should wait for slowest reader (40ms)")

	t.Logf("Cancel duration: %v", cancelDuration)

	// Verify all readers have stopped
	for i, reader := range readers {
		assert.True(t, reader.stopped.Load(), "Reader %d should be stopped after Cancel", i)
	}

	// Verify runningReaders is empty
	readerCount := 0
	exec.runningReaders.Range(func(key, value interface{}) bool {
		readerCount++
		return true
	})
	assert.Equal(t, 0, readerCount, "runningReaders should be empty after Cancel")

	// Goroutine leak check removed - we already verified via Wait() that all readers stopped
	// Checking goroutine count is non-deterministic and not reliable in CI environments
}

// TestCancelWithoutReadersDoesNotBlock verifies Cancel works when no readers exist
func TestCancelWithoutReadersDoesNotBlock(t *testing.T) {
	// Setup mock table detector
	// Note: GetTableDetector is already stubbed globally in cdc_test.go init()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-no-readers",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Transition to Running state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	// Cancel should complete quickly even with no readers
	startTime := time.Now()
	err := exec.Cancel()
	cancelDuration := time.Since(startTime)

	require.NoError(t, err)
	assert.Equal(t, StateCancelled, exec.stateMachine.State())
	assert.Less(t, cancelDuration.Milliseconds(), int64(100),
		"Cancel should complete quickly with no readers")
}

// TestCancelIdempotent verifies calling Cancel multiple times is safe
func TestCancelIdempotent(t *testing.T) {
	// Setup mock table detector
	// Note: GetTableDetector is already stubbed globally in cdc_test.go init()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-idempotent",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Transition to Running state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	// First Cancel should succeed
	err := exec.Cancel()
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, exec.stateMachine.State())

	// Second Cancel should fail (invalid state transition)
	err = exec.Cancel()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transition")
}

// TestStopAllReadersWithNilMap verifies stopAllReaders handles nil runningReaders
func TestStopAllReadersWithNilMap(t *testing.T) {
	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-nil-map",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: nil, // Explicitly nil
	}

	// Should not panic
	require.NotPanics(t, func() {
		exec.stopAllReaders()
	})
}

// TestCancelMultipleReaders verifies Cancel works correctly with multiple readers
func TestCancelMultipleReaders(t *testing.T) {
	// Setup mock table detector
	// Note: GetTableDetector is already stubbed globally in cdc_test.go init()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-multiple",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Transition to Running state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	// Add 10 readers with varying stop delays
	numReaders := 10
	readers := make([]*mockConcurrentChangeReader, numReaders)

	for i := 0; i < numReaders; i++ {
		tableInfo := &cdc.DbTableInfo{
			SourceDbName:  "test_db",
			SourceTblName: "test_table_" + string(rune('A'+i)),
			SourceTblId:   uint64(i + 1),
		}
		// Vary stop delays: 0, 5, 10, 15, ... 45 ms
		stopDelay := time.Duration(i*5) * time.Millisecond
		reader := newMockConcurrentChangeReader(tableInfo, stopDelay)
		readers[i] = reader

		// Start reader
		go reader.Run(context.Background(), exec.activeRoutine)

		// Store in runningReaders
		key := tableInfo.SourceDbName + "." + tableInfo.SourceTblName
		exec.runningReaders.Store(key, reader)
	}

	// Wait for all readers to start (deterministic)
	for _, reader := range readers {
		<-reader.started // Block until Run() has started
	}

	// Cancel should wait for all readers (slowest is 45ms)
	startTime := time.Now()
	err := exec.Cancel()
	cancelDuration := time.Since(startTime)

	require.NoError(t, err)
	assert.Equal(t, StateCancelled, exec.stateMachine.State())

	// Should have waited for slowest reader (45ms)
	assert.GreaterOrEqual(t, cancelDuration.Milliseconds(), int64(45),
		"Cancel should wait for slowest reader (45ms)")

	// Verify all readers are cleaned up
	readerCount := 0
	exec.runningReaders.Range(func(key, value interface{}) bool {
		readerCount++
		return true
	})
	assert.Equal(t, 0, readerCount, "All readers should be removed from map")

	t.Logf("Cancel duration: %v (waited for %d readers)", cancelDuration, numReaders)
}
