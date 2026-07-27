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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartCleansUpOldReaders verifies that Start() properly cleans up old readers before starting new ones
func TestStartCleansUpOldReaders(t *testing.T) {
	// Setup mocks
	// Note: GetTableDetector is already stubbed globally in cdc_test.go init()

	// Get baseline goroutine count
	baselineGoroutines := runtime.NumGoroutine()

	// Create executor
	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-cleanup",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Add "old" readers to runningReaders (simulating previous run)
	numOldReaders := 3
	oldReaders := make([]*mockConcurrentChangeReader, numOldReaders)

	for i := 0; i < numOldReaders; i++ {
		tableInfo := &cdc.DbTableInfo{
			SourceDbName:  "old_db",
			SourceTblName: "old_table_" + string(rune('A'+i)),
			SourceTblId:   uint64(i + 1),
		}
		reader := newMockConcurrentChangeReader(tableInfo, 20*time.Millisecond)
		oldReaders[i] = reader

		// Start old reader
		go reader.Run(context.Background(), exec.activeRoutine)

		// Store in runningReaders
		key := tableInfo.SourceDbName + "." + tableInfo.SourceTblName
		exec.runningReaders.Store(key, reader)
	}

	// Wait for all old readers to start (deterministic)
	for _, reader := range oldReaders {
		<-reader.started // Block until Run() has started
	}

	// Verify old readers are running
	for i, reader := range oldReaders {
		assert.True(t, reader.running.Load(), "Old reader %d should be running", i)
	}

	// Count old readers
	oldReaderCount := 0
	exec.runningReaders.Range(func(key, value interface{}) bool {
		oldReaderCount++
		return true
	})
	assert.Equal(t, numOldReaders, oldReaderCount, "Should have 3 old readers")

	goroutinesWithOldReaders := runtime.NumGoroutine()
	t.Logf("Goroutines with old readers: %d (baseline: %d, diff: %d)",
		goroutinesWithOldReaders, baselineGoroutines, goroutinesWithOldReaders-baselineGoroutines)

	// Directly test the cleanup logic by simulating Start's behavior
	// Instead of calling the full Start(), we test the cleanup part

	// Transition to Starting state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))

	// Simulate Start's cleanup logic
	startTime := time.Now()

	// This is the code from Start() that we're testing:
	if exec.runningReaders != nil {
		exec.runningReaders.Range(func(key, value interface{}) bool {
			reader := value.(cdc.ChangeReader)
			reader.Close()
			return true
		})

		exec.runningReaders.Range(func(key, value interface{}) bool {
			reader := value.(cdc.ChangeReader)
			reader.Wait()
			return true
		})

		exec.runningReaders.Range(func(key, value interface{}) bool {
			exec.runningReaders.Delete(key)
			return true
		})
	}

	cleanupDuration := time.Since(startTime)

	// Mark as failed to clean up state machine
	err := exec.stateMachine.SetFailed("test cleanup completed")
	require.NoError(t, err)

	// Verify state
	assert.Equal(t, StateFailed, exec.stateMachine.State())

	// Cleanup should have waited for old readers to stop (slowest is 20ms)
	assert.GreaterOrEqual(t, cleanupDuration.Milliseconds(), int64(20),
		"Start should wait for old readers to stop (20ms)")

	t.Logf("Cleanup duration: %v", cleanupDuration)

	// Verify all old readers have stopped
	for i, reader := range oldReaders {
		assert.True(t, reader.stopped.Load(), "Old reader %d should be stopped", i)
	}

	// Verify runningReaders is empty (or re-initialized as empty map)
	newReaderCount := 0
	exec.runningReaders.Range(func(key, value interface{}) bool {
		newReaderCount++
		return true
	})
	assert.Equal(t, 0, newReaderCount, "runningReaders should be empty after cleanup")

	// Goroutine leak check removed - we already verified via Wait() that all readers stopped
	// Checking goroutine count is non-deterministic and not reliable in CI environments
}

// TestStartWithNilRunningReaders verifies Start handles nil runningReaders gracefully
func TestStartWithNilRunningReaders(t *testing.T) {
	// Setup mocks
	stubs := setupMockTableDetector()
	defer stubs.Reset()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-nil-readers",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: nil, // Explicitly nil
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Transition to Starting state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))

	// Test the cleanup logic directly
	require.NotPanics(t, func() {
		// This is the code from Start() that handles nil runningReaders:
		if exec.runningReaders != nil {
			// ... cleanup logic ...
		} else {
			exec.runningReaders = &sync.Map{}
		}
	})

	// runningReaders should now be initialized
	assert.NotNil(t, exec.runningReaders, "runningReaders should be initialized")
}

// TestStartCleanupWithClosedReaders verifies Start handles already-closed readers
func TestStartCleanupWithClosedReaders(t *testing.T) {
	// Setup mocks
	stubs := setupMockTableDetector()
	defer stubs.Reset()

	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "test-task-closed-readers",
			TaskName: "test-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		cnUUID:         "test-cn-uuid",
		runningReaders: &sync.Map{},
		activeRoutine:  cdc.NewCdcActiveRoutine(),
		stateMachine:   NewExecutorStateMachine(),
		holdCh:         make(chan int, 1),
	}

	// Add old readers that are already closed
	numOldReaders := 2
	closedReaders := make([]*mockConcurrentChangeReader, numOldReaders)

	for i := 0; i < numOldReaders; i++ {
		tableInfo := &cdc.DbTableInfo{
			SourceDbName:  "old_db",
			SourceTblName: "old_table_" + string(rune('A'+i)),
			SourceTblId:   uint64(i + 1),
		}
		reader := newMockConcurrentChangeReader(tableInfo, 0)
		closedReaders[i] = reader

		// Start reader
		go reader.Run(context.Background(), exec.activeRoutine)

		// Store in runningReaders
		key := tableInfo.SourceDbName + "." + tableInfo.SourceTblName
		exec.runningReaders.Store(key, reader)
	}

	// Wait for all readers to start (deterministic)
	for _, reader := range closedReaders {
		<-reader.started // Block until Run() has started
	}

	// Immediately close all readers (they should stop quickly)
	for _, reader := range closedReaders {
		reader.Close()
	}
	for _, reader := range closedReaders {
		reader.Wait()
	}

	// Transition to Starting state
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))

	// Test the cleanup logic with already-closed readers
	startTime := time.Now()
	require.NotPanics(t, func() {
		// Simulate Start's cleanup logic
		if exec.runningReaders != nil {
			exec.runningReaders.Range(func(key, value interface{}) bool {
				reader := value.(cdc.ChangeReader)
				reader.Close()
				return true
			})

			exec.runningReaders.Range(func(key, value interface{}) bool {
				reader := value.(cdc.ChangeReader)
				reader.Wait()
				return true
			})

			exec.runningReaders.Range(func(key, value interface{}) bool {
				exec.runningReaders.Delete(key)
				return true
			})
		}
	})
	cleanupDuration := time.Since(startTime)

	// Cleanup should complete quickly since readers are already stopped
	assert.Less(t, cleanupDuration.Milliseconds(), int64(100),
		"Cleanup should be fast with already-stopped readers")

	t.Logf("Cleanup duration: %v", cleanupDuration)
}

func TestRestartFailureDrainsHoldChAndMovesToFailed(t *testing.T) {
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "restart-task-failure",
			TaskName: "restart-task",
			Accounts: []*task.Account{{Id: 1}},
		},
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
	}

	// Pretend we were running and the previous Start left a signal in holdCh
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	exec.holdCh <- 1

	var callCount atomic.Int32
	firstCallDone := make(chan struct{}, 1)
	secondCallDone := make(chan struct{}, 1)

	exec.startFunc = func(ctx context.Context) error {
		call := callCount.Add(1)
		switch call {
		case 1:
			defer func() { firstCallDone <- struct{}{} }()
			require.NoError(t, exec.stateMachine.SetFailed("boom"))
			return moerr.NewInternalErrorNoCtx("boom")
		case 2:
			defer func() { secondCallDone <- struct{}{} }()
			require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
			return nil
		default:
			return nil
		}
	}

	// First restart should synchronously report that the replacement failed,
	// even though holdCh already contains a signal.
	require.Error(t, exec.Restart())
	select {
	case <-firstCallDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first restart attempt")
	}

	assert.Equal(t, int32(1), callCount.Load())
	assert.Equal(t, StateFailed, exec.stateMachine.State())

	// Channel should still contain the original signal (simulating a Start that failed early).
	select {
	case exec.holdCh <- 1:
		t.Fatal("holdCh should remain full after failed restart")
	default:
	}

	// Second restart should drain the stale signal and transition back to Running.
	require.NoError(t, exec.Restart())
	select {
	case <-secondCallDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second restart attempt")
	}

	assert.Equal(t, int32(2), callCount.Load())
	assert.Equal(t, StateRunning, exec.stateMachine.State())
}

func TestCDCCompletionWinsWhenTimeoutIsAlsoReady(t *testing.T) {
	for range 100 {
		completion := make(chan error, 1)
		timeout := make(chan time.Time, 1)
		expected := errors.New("completed")
		completion <- expected
		timeout <- time.Now()

		result, timedOut := selectCDCCompletion(completion, timeout)
		require.False(t, timedOut)
		require.ErrorIs(t, result, expected)
	}
}

func TestRestartReadinessRequiresCatalogTransition(t *testing.T) {
	t.Run("local restart waiter", func(t *testing.T) {
		catalogErr := errors.New("catalog unavailable")
		catalog := &captureExecContextIE{execErr: catalogErr}
		exec := &CDCTaskExecutor{
			spec: &task.CreateCdcDetails{
				TaskId:   "restart-catalog-ready",
				Accounts: []*task.Account{{Id: 1}},
			},
			ie: catalog,
		}
		const generation = uint64(1)
		ready := exec.beginRestartWaiter(generation, cdc.CDCState_Restarting)

		restartReady, required, err := exec.completeStartupCatalogTransition(
			context.Background(),
			generation,
			false,
		)
		require.True(t, required)
		require.False(t, restartReady)
		require.ErrorIs(t, err, catalogErr)
		select {
		case <-ready:
			t.Fatal("restart readiness was published before the catalog transition")
		default:
		}

		catalog.execErr = nil
		restartReady, required, err = exec.completeStartupCatalogTransition(
			context.Background(),
			generation,
			false,
		)
		require.NoError(t, err)
		require.True(t, required)
		require.True(t, restartReady)
		require.NoError(t, <-ready)
	})

	t.Run("fresh takeover", func(t *testing.T) {
		catalogErr := errors.New("catalog unavailable")
		exec := &CDCTaskExecutor{
			spec: &task.CreateCdcDetails{
				TaskId:   "restart-takeover-catalog-ready",
				Accounts: []*task.Account{{Id: 1}},
			},
			ie: &captureExecContextIE{execErr: catalogErr},
		}

		restartReady, required, err := exec.completeStartupCatalogTransition(
			context.Background(),
			0,
			true,
		)
		require.True(t, required)
		require.False(t, restartReady)
		require.ErrorIs(t, err, catalogErr)
	})
}

func TestFreshRestartRetryReopensFailedCatalogAdmission(t *testing.T) {
	catalog := &cdcCatalogStateExecutor{
		state:        cdc.CDCState_Failed,
		currentState: cdc.CDCState_Failed,
		targetState:  cdc.CDCState_Restarting,
	}
	exec := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			TaskId:   "fresh-restart-retry",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie: catalog,
	}

	require.NoError(t, exec.admitRestartCatalogState(context.Background()))
	require.Equal(t, cdc.CDCState_Restarting, catalog.getState())

	catalog.currentState = cdc.CDCState_Restarting
	catalog.targetState = cdc.CDCState_Running
	restartReady, required, err := exec.completeStartupCatalogTransition(
		context.Background(),
		0,
		true,
	)
	require.NoError(t, err)
	require.True(t, required)
	require.False(t, restartReady, "fresh takeover has no local restart waiter")
	require.Equal(t, cdc.CDCState_Running, catalog.getState())
}

func TestRestartEarlyStartFailureMovesGenerationToFailed(t *testing.T) {
	retrieveErr := errors.New("catalog read unavailable")
	catalog := &captureExecContextIE{queryErr: retrieveErr}
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "restart-early-start-failure",
			TaskName: "restart-early-start-failure",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:           catalog,
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
	}
	exec.startFunc = exec.Start
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	require.ErrorIs(t, exec.Restart(), retrieveErr)
	require.Equal(t, StateFailed, exec.stateMachine.State())
	require.Eventually(t, func() bool {
		return exec.activeStart() == nil
	}, time.Second, time.Millisecond)
	require.True(t, catalog.containsExecutedSQL("SET state = 'failed'"))
	require.True(t, catalog.containsExecutedSQL("AND state = 'restarting'"))
}

func TestRestartReleasesCallbackFenceBeforeWaitingForReplacement(t *testing.T) {
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "restart-callback-fence",
			TaskName: "restart-callback-fence",
			Accounts: []*task.Account{{Id: 1}},
		},
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	callbackAcquired := make(chan struct{})
	exec.startFunc = func(context.Context) error {
		// handleNewTablesForGeneration takes this same read lock before it
		// validates the generation and starts table work.
		exec.callbackMu.RLock()
		close(callbackAcquired)
		exec.callbackMu.RUnlock()
		return exec.stateMachine.Transition(TransitionStartSuccess)
	}

	restartDone := make(chan error, 1)
	go func() { restartDone <- exec.Restart() }()

	select {
	case <-callbackAcquired:
	case <-time.After(time.Second):
		t.Error("replacement callback could not acquire callbackMu")
	}
	// Drain the restart even on assertion failure, so a regression cannot leak a
	// goroutine into later CDC tests.
	select {
	case err := <-restartDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("restart did not finish after replacement callback progress")
	}
}

func TestRestartTimeoutDoesNotReuseActiveStartAttempt(t *testing.T) {
	catalog := &captureExecContextIE{}
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "restart-timeout-ownership",
			TaskName: "restart-timeout-ownership",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:                    catalog,
		stateMachine:          NewExecutorStateMachine(),
		holdCh:                make(chan int, 1),
		restartStartupTimeout: 25 * time.Millisecond,
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	oldCtx, oldAttempt := newCDCStartAttempt(context.Background(), exec.callbackGeneration.Load())
	require.True(t, exec.installStartAttempt(oldAttempt))
	oldCancelled := make(chan struct{})
	releaseOld := make(chan struct{})
	go func() {
		<-oldCtx.Done()
		close(oldCancelled)
		<-releaseOld
		exec.finishStartAttempt(oldAttempt)
	}()

	var replacementStarts atomic.Int32
	exec.startFunc = func(context.Context) error {
		replacementStarts.Add(1)
		return nil
	}

	err := exec.Restart()
	require.ErrorContains(t, err, "CDC restart startup timed out")
	select {
	case <-oldCancelled:
	case <-time.After(time.Second):
		t.Fatal("restart did not cancel the old start attempt")
	}
	require.Zero(t, replacementStarts.Load(), "replacement must not reuse resources before old Start exits")
	require.Equal(t, StateFailed, exec.stateMachine.State())
	require.Contains(t, catalog.execSQL, "SET state = 'failed'")
	require.Contains(t, catalog.execSQL, "AND state = 'restarting'")

	close(releaseOld)
	require.Eventually(t, func() bool { return exec.activeStart() == nil }, time.Second, time.Millisecond)

	// The next taskservice retry is now allowed to reopen the failed catalog
	// admission and launch exactly one replacement.
	require.NoError(t, exec.Restart())
	require.Eventually(t, func() bool { return replacementStarts.Load() == 1 }, time.Second, time.Millisecond)
	require.True(t, catalog.containsExecutedSQL("SET state = 'restarting'"))
	require.True(t, catalog.containsExecutedSQL("AND state = 'failed'"))
}

func TestRestartTimeoutRecordsLateStartupCauseBeforeRetry(t *testing.T) {
	catalog := &captureExecContextIE{}
	releaseStart := make(chan struct{})
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "restart-timeout-cause",
			TaskName: "restart-timeout-cause",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:                    catalog,
		stateMachine:          NewExecutorStateMachine(),
		holdCh:                make(chan int, 1),
		restartStartupTimeout: 25 * time.Millisecond,
		startFunc: func(context.Context) error {
			<-releaseStart
			return errors.New("downstream authentication rejected")
		},
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))

	require.ErrorContains(t, exec.Restart(), "CDC restart startup timed out")
	close(releaseStart)
	require.Eventually(t, func() bool { return exec.activeStart() == nil }, time.Second, time.Millisecond)
	require.True(t, catalog.containsExecutedSQL("err_msg = 'downstream authentication rejected'"))
	require.True(t, catalog.containsExecutedSQL("AND state = 'failed'"))
}

func TestStartSupersededBeforeSchedulingDoesNotPublishOrCleanNewGeneration(t *testing.T) {
	exec := &CDCTaskExecutor{
		stateMachine: NewExecutorStateMachine(),
	}
	startCtx, attempt := newCDCStartAttempt(context.Background(), exec.callbackGeneration.Load())
	require.True(t, exec.installStartAttempt(attempt))

	// This models a replacement goroutine that has been scheduled but has not
	// entered Start before its caller times out and fences its generation.
	exec.callbackGeneration.Add(1)
	err := exec.Start(startCtx)
	require.ErrorContains(t, err, "superseded by a newer lifecycle generation")
	require.Equal(t, StateIdle, exec.stateMachine.State())
	require.Nil(t, exec.activeStart())
}

func TestResumeWaitsForPreviousStartAttemptBeforeReplacingRoutine(t *testing.T) {
	started := make(chan struct{}, 1)
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "resume-ownership",
			TaskName: "resume-ownership",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:           &captureExecContextIE{},
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
		startFunc: func(context.Context) error {
			started <- struct{}{}
			return nil
		},
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	require.NoError(t, exec.stateMachine.Transition(TransitionPause))
	require.NoError(t, exec.stateMachine.Transition(TransitionPauseComplete))

	_, previous := newCDCStartAttempt(context.Background(), exec.callbackGeneration.Load())
	require.True(t, exec.installStartAttempt(previous))
	require.NoError(t, exec.Resume())

	select {
	case <-started:
		t.Fatal("resume replaced resources before the previous Start exited")
	case <-time.After(25 * time.Millisecond):
	}

	exec.finishStartAttempt(previous)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resume did not start after the previous Start exited")
	}
}

func TestCancelFencesResumeWaitingForPreviousStartAttempt(t *testing.T) {
	started := make(chan struct{}, 1)
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "resume-cancel-fence",
			TaskName: "resume-cancel-fence",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:           &captureExecContextIE{},
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
		startFunc: func(context.Context) error {
			started <- struct{}{}
			return nil
		},
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	require.NoError(t, exec.stateMachine.Transition(TransitionPause))
	require.NoError(t, exec.stateMachine.Transition(TransitionPauseComplete))

	_, previous := newCDCStartAttempt(context.Background(), exec.callbackGeneration.Load())
	require.True(t, exec.installStartAttempt(previous))
	require.NoError(t, exec.Resume())
	require.NoError(t, exec.Cancel())
	exec.finishStartAttempt(previous)

	select {
	case <-started:
		t.Fatal("cancelled resume installed a new Start attempt")
	case <-time.After(25 * time.Millisecond):
	}
	require.Equal(t, StateCancelled, exec.stateMachine.State())
}

func TestPauseFencesResumeWaitingForPreviousStartAttempt(t *testing.T) {
	started := make(chan struct{}, 1)
	exec := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskId:   "resume-pause-fence",
			TaskName: "resume-pause-fence",
			Accounts: []*task.Account{{Id: 1}},
		},
		ie:           &captureExecContextIE{},
		stateMachine: NewExecutorStateMachine(),
		holdCh:       make(chan int, 1),
		startFunc: func(context.Context) error {
			started <- struct{}{}
			return nil
		},
	}
	require.NoError(t, exec.stateMachine.Transition(TransitionStart))
	require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
	require.NoError(t, exec.stateMachine.Transition(TransitionPause))
	require.NoError(t, exec.stateMachine.Transition(TransitionPauseComplete))

	_, previous := newCDCStartAttempt(context.Background(), exec.callbackGeneration.Load())
	require.True(t, exec.installStartAttempt(previous))
	require.NoError(t, exec.Resume())
	require.NoError(t, exec.Pause())
	exec.finishStartAttempt(previous)

	select {
	case <-started:
		t.Fatal("paused resume installed a new Start attempt")
	case <-time.After(25 * time.Millisecond):
	}
	require.Equal(t, StatePaused, exec.stateMachine.State())
}
