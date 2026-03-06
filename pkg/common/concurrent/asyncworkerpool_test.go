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

package concurrent

import (
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAsyncTaskResultStore(t *testing.T) {
	store := NewAsyncTaskResultStore()
	assert.NotNil(t, store)
	assert.NotNil(t, store.states)
	assert.Equal(t, uint64(0), store.nextJobID)
}

func TestAsyncTaskResultStore_GetNextJobID(t *testing.T) {
	store := NewAsyncTaskResultStore()
	id1 := store.GetNextJobID()
	id2 := store.GetNextJobID()
	id3 := store.GetNextJobID()

	assert.Equal(t, uint64(1), id1)
	assert.Equal(t, uint64(2), id2)
	assert.Equal(t, uint64(3), id3)
}

func TestAsyncTaskResultStore_StoreAndWait(t *testing.T) {
	store := NewAsyncTaskResultStore()
	jobID := store.GetNextJobID()
	expectedResult := "task completed"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Simulate some work before storing
		store.Store(&AsyncTaskResult{
			ID:     jobID,
			Result: expectedResult,
			Error:  nil,
		})
	}()

	result, err := store.Wait(jobID)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, jobID, result.ID)
	assert.Equal(t, expectedResult, result.Result)
	assert.Nil(t, result.Error)

	wg.Wait()

	// Verify that the result is removed after retrieval
	store.mu.Lock()
	_, ok := store.states[jobID]
	store.mu.Unlock()
	assert.False(t, ok, "Result should be removed from store after Wait")
}

func TestAsyncTaskResultStore_ConcurrentStoreAndWait(t *testing.T) {
	store := NewAsyncTaskResultStore()
	numTasks := 100

	var submitWg sync.WaitGroup
	var waitWg sync.WaitGroup
	submitWg.Add(numTasks)
	waitWg.Add(numTasks)

	results := make(chan *AsyncTaskResult, numTasks)

	// Launch goroutines to wait for results
	for i := 0; i < numTasks; i++ {
		jobID := store.GetNextJobID() // Pre-generate job IDs
		go func(id uint64) {
			defer waitWg.Done()
			result, err := store.Wait(id)
			assert.NoError(t, err)
			results <- result
		}(jobID)
	}

	// Launch goroutines to store results
	for i := 1; i <= numTasks; i++ {
		go func(id uint64) {
			defer submitWg.Done()
			// Simulate random delay
			time.Sleep(time.Duration(id%10) * time.Millisecond)
			store.Store(&AsyncTaskResult{
				ID:     id,
				Result: fmt.Sprintf("result-%d", id),
				Error:  nil,
			})
		}(uint64(i))
	}

	submitWg.Wait()
	waitWg.Wait() // Ensure all waiters have completed
	close(results)

	receivedResults := make(map[uint64]string)
	for r := range results {
		receivedResults[r.ID] = r.Result.(string)
	}

	assert.Len(t, receivedResults, numTasks)
	for i := 1; i <= numTasks; i++ {
		assert.Equal(t, fmt.Sprintf("result-%d", i), receivedResults[uint64(i)])
	}
}

type dummyResource struct {
	closed bool
}

func (m *dummyResource) Close() {
	m.closed = true
}

func testCreateResource() (any, error) {
	return &dummyResource{}, nil
}

func testCleanupResource(res any) {
	if res == nil {
		return
	}
	resource := res.(*dummyResource)
	resource.Close()
}

func TestAsyncWorkerPool_LifecycleAndTaskExecution(t *testing.T) {

	worker := NewAsyncWorkerPool(5, testCreateResource, testCleanupResource)
	require.NotNil(t, worker)

	// Start the worker
	worker.Start(nil, func(_ any) error { return nil }) // Pass nil initFn

	// Submit a task
	expectedTaskResult := "processed by CUDA (mocked)"
	taskID, err := worker.Submit(func(res any) (any, error) {
		// In a real scenario, this would use the real resource
		// For testing, we just return a value.
		// Assert that res is not nil, even if it's a dummy one.
		assert.NotNil(t, res)
		return expectedTaskResult, nil
	})
	require.NoError(t, err)

	// Wait for the result
	result, err := worker.Wait(taskID)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, taskID, result.ID)
	assert.Equal(t, expectedTaskResult, result.Result)
	assert.Nil(t, result.Error)

	// Submit another task
	expectedTaskResult2 := 123
	taskID2, err := worker.Submit(func(res any) (any, error) {
		assert.NotNil(t, res)
		return expectedTaskResult2, nil
	})
	require.NoError(t, err)

	result2, err := worker.Wait(taskID2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, taskID2, result2.ID)
	assert.Equal(t, expectedTaskResult2, result2.Result)
	assert.Nil(t, result2.Error)

	// Test a task that returns an error
	expectedError := fmt.Errorf("cuda operation failed")
	taskID3, err := worker.Submit(func(res any) (any, error) {
		assert.NotNil(t, res)
		return nil, expectedError
	})
	require.NoError(t, err)

	result3, err := worker.Wait(taskID3)
	assert.NoError(t, err) // Error is returned in AsyncTaskResult, not as return value of Wait
	assert.NotNil(t, result3)
	assert.Equal(t, taskID3, result3.ID)
	assert.Nil(t, result3.Result)
	assert.Equal(t, expectedError, result3.Error)

	// Stop the worker
	worker.Stop()

	t.Log("AsyncWorkerPool stopped. Further submissions would block or panic.")
}

func TestAsyncWorkerPool_StopDuringTaskProcessing(t *testing.T) {

	worker := NewAsyncWorkerPool(5, testCreateResource, testCleanupResource)
	worker.Start(nil, func(_ any) error { return nil }) // Pass nil initFn

	// Submit a long-running task
	longTaskSignal := make(chan struct{})
	longTaskID, err := worker.Submit(func(res any) (any, error) {
		assert.NotNil(t, res)
		<-longTaskSignal // Block until signaled
		return "long task done", nil
	})
	require.NoError(t, err)

	// Give the worker a moment to pick up the task
	time.Sleep(50 * time.Millisecond)

	// Stop the worker while the task is running
	doneStopping := make(chan struct{})
	go func() {
		worker.Stop()
		close(doneStopping)
	}()

	// Wait for a short period to see if Stop is blocked by the task
	select {
	case <-doneStopping:
		t.Fatal("Worker stopped too quickly, long task might not have started blocking")
	case <-time.After(100 * time.Millisecond):
		// This means Stop is likely waiting for the `run` goroutine, which is blocked by the task.
		t.Log("Worker.Stop is blocked by the long-running task as expected.")
	}

	// Now unblock the long-running task
	close(longTaskSignal)

	// The worker should now be able to stop
	select {
	case <-doneStopping:
		t.Log("Worker successfully stopped after long task completed.")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Worker did not stop even after long task completed.")
	}

	// Verify that the long task result was stored
	result, err := worker.Wait(longTaskID)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, longTaskID, result.ID)
	assert.Equal(t, "long task done", result.Result)
}

func TestAsyncWorkerPool_MultipleSubmitsBeforeStart(t *testing.T) {

	worker := NewAsyncWorkerPool(5, testCreateResource, testCleanupResource)

	// Start the worker - now takes initFn
	worker.Start(nil, func(_ any) error { return nil }) // Pass nil initFn

	// Submit multiple tasks before starting the worker
	numTasks := 5
	taskIDs := make([]uint64, numTasks) // Still need to collect IDs
	for i := 0; i < numTasks; i++ {
		var err error
		taskIDs[i], err = worker.Submit(func(res any) (any, error) {
			assert.NotNil(t, res)
			return fmt.Sprintf("result-%d", i), nil
		})
		require.NoError(t, err)
	}

	// Start the worker
	// worker.Start() // Already started above, remove duplicate

	// Wait for all results
	for i, id := range taskIDs {
		result, err := worker.Wait(id)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, id, result.ID)
		assert.Equal(t, fmt.Sprintf("result-%d", i), result.Result)
	}

	worker.Stop()
}

func TestAsyncWorkerPool_GracefulShutdown(t *testing.T) {

	worker := NewAsyncWorkerPool(5, testCreateResource, testCleanupResource)
	worker.Start(nil, func(_ any) error { return nil }) // Pass nil initFn

	var wg sync.WaitGroup
	numTasks := 10
	results := make(chan *AsyncTaskResult, numTasks) // Changed type

	// Submit tasks
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		// Capture loop index for the anonymous function
		loopIndex := i

		var submitErr error
		taskID, submitErr := worker.Submit(func(res any) (any, error) {
			assert.NotNil(t, res)
			time.Sleep(10 * time.Millisecond)                     // Simulate work
			return fmt.Sprintf("final-result-%d", loopIndex), nil // Use captured loop index
		})
		require.NoError(t, submitErr)

		go func(id uint64) {
			defer wg.Done()
			r, waitErr := worker.Wait(id)
			assert.NoError(t, waitErr)
			results <- r
		}(taskID)
	}

	// Give some time for tasks to be submitted and processed
	time.Sleep(50 * time.Millisecond)

	// Stop the worker
	worker.Stop()

	// All tasks submitted before Stop should complete and their results should be retrievable
	wg.Wait()
	close(results)

	assert.Len(t, results, numTasks)
	for r := range results {
		assert.Contains(t, r.Result.(string), "final-result-")
	}

	// Ensure new tasks cannot be submitted after stop
	_, err := worker.Submit(func(res any) (any, error) { // Use := for first declaration of err in this scope
		return "should not be processed", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")
}

func TestAsyncWorkerPool_SignalTermination(t *testing.T) {

	worker := NewAsyncWorkerPool(1, testCreateResource, testCleanupResource) // Use 1 thread for easier control and observation
	require.NotNil(t, worker)

	worker.Start(nil, func(_ any) error { return nil })

	// Submit a task that will complete after the signal, to ensure graceful processing
	taskDone := make(chan struct{})
	taskID1, err := worker.Submit(func(res any) (any, error) {
		assert.NotNil(t, res)
		<-taskDone // Wait for signal to complete
		return "task1 processed", nil
	})
	require.NoError(t, err)

	// Submit a second quick task that should complete before or around the signal
	taskID2, err := worker.Submit(func(res any) (any, error) {
		assert.NotNil(t, res)
		return "task2 processed", nil
	})
	require.NoError(t, err)

	// Give the worker a moment to pick up the tasks
	time.Sleep(50 * time.Millisecond)

	// Simulate SIGTERM by sending to the signal channel
	t.Log("Simulating SIGTERM to AsyncWorkerPool")
	worker.sigc <- syscall.SIGTERM

	// Allow some time for the signal handler to process and call worker.Stop()
	time.Sleep(100 * time.Millisecond)

	// Unblock the long-running task to allow it to finish and the worker to fully stop
	close(taskDone)

	// Wait for all worker goroutines to finish
	// The worker.Stop() method, which is called by the signal handler,
	// internally waits for worker.wg.Wait().
	// So, we can verify by checking if new submissions fail and if old tasks results are available.

	// Check if previously submitted tasks completed
	result1, err := worker.Wait(taskID1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)
	assert.Equal(t, taskID1, result1.ID)
	assert.Equal(t, "task1 processed", result1.Result)

	result2, err := worker.Wait(taskID2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, taskID2, result2.ID)
	assert.Equal(t, "task2 processed", result2.Result)

	// Attempt to submit a new task after termination. It should fail.
	_, err = worker.Submit(func(res any) (any, error) {
		return "should not be processed", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")
}

func TestAsyncWorkerPool_GetFirstError(t *testing.T) {

	var err error // Explicitly declare err here

	worker := NewAsyncWorkerPool(1, testCreateResource, testCleanupResource)
	assert.Nil(t, worker.GetFirstError(), "GetFirstError should be nil initially")

	// Trigger an error in initFn, which will be pushed to w.errch
	expectedErr1 := fmt.Errorf("simulated init error 1")
	initFn1 := func(resource any) error {
		return expectedErr1
	}
	stopFn := func(_ any) error { return nil }

	worker.Start(initFn1, stopFn)

	// Give the `run` goroutine and the signal handler a moment to process initFn and store the first error.
	time.Sleep(50 * time.Millisecond)

	// GetFirstError should now return the expected error
	assert.Equal(t, expectedErr1, worker.GetFirstError(), "GetFirstError should return the first recorded error")

	// Submit a task that causes an error (this error won't be saved as firstError via w.errch)
	// This ensures that only errors propagated through w.errch are considered.
	_, err = worker.Submit(func(res any) (any, error) { // Use = for assignment
		assert.NotNil(t, res)
		return nil, fmt.Errorf("task error, should not affect GetFirstError()")
	})
	require.Error(t, err) // Expect an error because the worker should be stopped
	assert.Contains(t, err.Error(), "worker is stopped")

	// Give some time for the task to be processed, if it affects anything
	time.Sleep(50 * time.Millisecond)

	// Ensure GetFirstError remains the same even if other errors (from tasks) occur.
	assert.Equal(t, expectedErr1, worker.GetFirstError(), "GetFirstError should not change after the first error is set")

	worker.Stop()

	// After stop, GetFirstError should still be the same.
	assert.Equal(t, expectedErr1, worker.GetFirstError(), "GetFirstError should retain the first error after stopping")
}

func TestAsyncWorkerPool_MultipleStopCalls(t *testing.T) {

	worker := NewAsyncWorkerPool(1, testCreateResource, testCleanupResource) // Use 1 thread
	require.NotNil(t, worker)

	worker.Start(nil, func(_ any) error { return nil })

	// Call Stop multiple times from the main goroutine
	worker.Stop()
	worker.Stop()
	worker.Stop()

	// Call Stop from another goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.Stop()
	}()
	wg.Wait()

	// Ensure no panics occurred during multiple Stop calls
	// (Go's testing framework will catch panics)

	// Optionally, try submitting a task again to ensure it's truly stopped
	_, err := worker.Submit(func(res any) (any, error) { return nil, nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")

	t.Log("Successfully called Stop multiple times without panic.")
}
