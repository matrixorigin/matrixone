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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMockTableDetector creates a mock TableDetector without a background
// scanner. Scanner behavior belongs to the cdc package tests.
func createMockTableDetector() *cdc.TableDetector {
	return createMockTableDetectorForTest()("test-cn")
}

// setupMockTableDetector creates and stubs a mock TableDetector for testing
// Returns gostub.Stubs that should be reset with .Reset()
// Uses a shared mock detector instance.
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

type heldCleanupReader struct {
	started      chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	releaseRun   chan struct{}
	done         chan struct{}
	closeCalls   atomic.Int32
}

func newHeldCleanupReader() *heldCleanupReader {
	return &heldCleanupReader{
		started:      make(chan struct{}),
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
		releaseRun:   make(chan struct{}),
		done:         make(chan struct{}),
	}
}

func (r *heldCleanupReader) Run(_ context.Context, ar *cdc.ActiveRoutine) {
	close(r.started)
	<-ar.Cancel
	<-r.releaseRun
	close(r.done)
}

func (r *heldCleanupReader) Close() {
	if r.closeCalls.Add(1) == 1 {
		close(r.closeStarted)
	}
	<-r.releaseClose
}
func (r *heldCleanupReader) Wait()                          { <-r.done }
func (r *heldCleanupReader) GetTableInfo() *cdc.DbTableInfo { return nil }

func newCancelDrainExecutor(t *testing.T) *CDCTaskExecutor {
	t.Helper()
	exec := &CDCTaskExecutor{
		spec:           &task.CreateCdcDetails{TaskId: "cancel-drain", TaskName: "cancel-drain"},
		stateMachine:   NewExecutorStateMachine(),
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		runningReaders: &sync.Map{},
		holdCh:         make(chan int, 1),
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	return exec
}

func TestCancelAndWaitRetainsBlockedReaderCleanup(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	reader := newHeldCleanupReader()
	var closeOnce, runOnce sync.Once
	t.Cleanup(func() {
		closeOnce.Do(func() { close(reader.releaseClose) })
		runOnce.Do(func() { close(reader.releaseRun) })
	})
	exec.runningReaders.Store("db.table", reader)
	exec.startReader(t.Context(), exec.activeRoutine, reader)
	<-reader.started

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- exec.CancelAndWait(ctx) }()
	select {
	case <-reader.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("reader close did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cleanup wait ignored cancellation while reader close was blocked")
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	require.ErrorIs(t, exec.CancelAndWait(canceled), context.Canceled)
	require.Equal(t, int32(1), reader.closeCalls.Load(), "retry must join the same cleanup")
	closeOnce.Do(func() { close(reader.releaseClose) })
	require.ErrorIs(t, exec.CancelAndWait(canceled), context.Canceled, "reader Run is still cleaning up")
	runOnce.Do(func() { close(reader.releaseRun) })
	ctx, stop = context.WithTimeout(t.Context(), time.Second)
	defer stop()
	require.NoError(t, exec.CancelAndWait(ctx))
	require.Error(t, exec.Cancel(), "direct repeated Cancel keeps its existing contract")
}

func TestCancelAndWaitDrainsWhenTransitionFails(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	// Idle rejects Cancel, but a previously scheduled reader still has to stop.
	exec.stateMachine = NewExecutorStateMachine()
	reader := newHeldCleanupReader()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(reader.releaseRun) }) })
	exec.startReader(t.Context(), exec.activeRoutine, reader)
	<-reader.started
	result := make(chan error, 1)
	go func() { result <- exec.CancelAndWait(t.Context()) }()
	select {
	case <-exec.activeRoutine.Cancel:
	case err := <-result:
		t.Fatalf("cleanup returned before signaling the reader: %v", err)
	case <-time.After(time.Second):
		t.Fatal("cleanup did not signal the reader")
	}
	select {
	case err := <-result:
		t.Fatalf("cleanup returned before the reader exited: %v", err)
	default:
	}
	release.Do(func() { close(reader.releaseRun) })
	select {
	case err := <-result:
		require.ErrorContains(t, err, "cannot cancel")
	case <-time.After(time.Second):
		t.Fatal("cleanup did not join the reader")
	}
}

func TestCancelAndWaitJoinsReadersScheduledByInFlightCallback(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	reader := newHeldCleanupReader()
	var runOnce sync.Once
	t.Cleanup(func() { runOnce.Do(func() { close(reader.releaseRun) }) })
	// Model a detector callback that passed its generation check before Cancel.
	exec.callbackMu.RLock()
	locked := true
	t.Cleanup(func() {
		if locked {
			exec.callbackMu.RUnlock()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- exec.CancelAndWait(ctx) }()
	require.Eventually(t, func() bool { return exec.stateMachine.State() == StateCancelled }, time.Second, time.Millisecond)
	// A concurrent restart may install its routine after Cancel closed the old
	// one, while this callback is still creating a reader.
	lateRoutine := cdc.NewCdcActiveRoutine()
	exec.setActiveRoutine(lateRoutine)
	exec.startReader(t.Context(), lateRoutine, reader)
	<-reader.started
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	exec.callbackMu.RUnlock()
	locked = false
	joined := make(chan error, 1)
	go func() { joined <- exec.CancelAndWait(t.Context()) }()
	select {
	case err := <-joined:
		t.Fatalf("cleanup returned before the callback's reader exited: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	runOnce.Do(func() { close(reader.releaseRun) })
	select {
	case err := <-joined:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after the reader exited")
	}
}

func TestCancelAndWaitCancelsDetectorCallbackContext(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	exec.spec.Accounts = []*task.Account{{Id: 1}}
	exec.bindLifecycleContext(t.Context())
	entered := make(chan struct{})
	stub := gostub.Stub(&cdc.GetTxnOp, func(ctx context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	defer stub.Reset()
	callbackDone := make(chan error, 1)
	go func() {
		callbackDone <- exec.handleNewTablesForGeneration(exec.callbackGeneration.Load(), nil)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("detector callback did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, exec.CancelAndWait(ctx))
	require.ErrorIs(t, <-callbackDone, context.Canceled)
}

func TestCancelAndWaitCleanupIsOwnedByRunner(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	runner := stopper.NewStopper("cdc-cleanup-test")
	exec.lifecycleTaskScheduler = runner.RunNamedTask
	reader := newHeldCleanupReader()
	var closeOnce, runOnce sync.Once
	t.Cleanup(func() {
		closeOnce.Do(func() { close(reader.releaseClose) })
		runOnce.Do(func() { close(reader.releaseRun) })
		runner.Stop()
	})
	exec.runningReaders.Store("db.table", reader)
	exec.startReader(t.Context(), exec.activeRoutine, reader)
	<-reader.started
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- exec.CancelAndWait(ctx) }()
	select {
	case <-reader.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("reader close did not start")
	}
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	stopped := make(chan struct{})
	go func() { runner.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("runner stopped before its cleanup task completed")
	case <-time.After(25 * time.Millisecond):
	}
	closeOnce.Do(func() { close(reader.releaseClose) })
	runOnce.Do(func() { close(reader.releaseRun) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("runner did not join completed cleanup")
	}
}

func TestCancelAndWaitAfterRunnerStops(t *testing.T) {
	exec := newCancelDrainExecutor(t)
	runner := stopper.NewStopper("cdc-stopped-cleanup-test")
	exec.lifecycleTaskScheduler = runner.RunNamedTask
	runner.Stop()
	require.NoError(t, exec.CancelAndWait(t.Context()))
	require.Equal(t, StateCancelled, exec.stateMachine.State())
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
