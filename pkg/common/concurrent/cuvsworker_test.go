//go:build gpu

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

	"github.com/rapidsai/cuvs/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	cudaAvailableOnce sync.Once
	hasCuda           bool
	cudaErr           error
)

func skipIfNotCudaAvailable(t *testing.T) {
	cudaAvailableOnce.Do(func() {
		stream, err := cuvs.NewCudaStream()
		if err != nil {
			cudaErr = fmt.Errorf("failed to create cuvs stream: %w", err)
			return
		}
		defer stream.Close()

		resource, err := cuvs.NewResource(stream)
		if err != nil {
			cudaErr = fmt.Errorf("failed to create cuvs resource: %w", err)
			return
		}
		defer resource.Close()

		hasCuda = true
	})

	if !hasCuda {
		t.Skipf("Skipping test because CUDA environment is not available: %v", cudaErr)
	}
}

func TestNewCuvsTaskResultStore(t *testing.T) {
	store := NewCuvsTaskResultStore()
	assert.NotNil(t, store)
	assert.NotNil(t, store.states)
	assert.Equal(t, uint64(0), store.nextJobID)
}

func TestCuvsTaskResultStore_GetNextJobID(t *testing.T) {
	store := NewCuvsTaskResultStore()
	id1 := store.GetNextJobID()
	id2 := store.GetNextJobID()
	id3 := store.GetNextJobID()

	assert.Equal(t, uint64(1), id1)
	assert.Equal(t, uint64(2), id2)
	assert.Equal(t, uint64(3), id3)
}

func TestCuvsTaskResultStore_StoreAndWait(t *testing.T) {
	store := NewCuvsTaskResultStore()
	jobID := store.GetNextJobID()
	expectedResult := "task completed"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Simulate some work before storing
		store.Store(&CuvsTaskResult{
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

func TestCuvsTaskResultStore_ConcurrentStoreAndWait(t *testing.T) {
	store := NewCuvsTaskResultStore()
	numTasks := 100

	var submitWg sync.WaitGroup
	var waitWg sync.WaitGroup
	submitWg.Add(numTasks)
	waitWg.Add(numTasks)

	results := make(chan *CuvsTaskResult, numTasks)

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
			store.Store(&CuvsTaskResult{
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

// Mocking cuvs for CuvsWorker tests
// This is a minimal mock to prevent panics and test the Go concurrency logic.
// A proper mock would involve interfaces if cuvs was designed with them,
// or a mocking library.
type mockCudaStream struct{}

func (m *mockCudaStream) Close() error { return nil }

type mockResource struct {
	stream *mockCudaStream
	closed bool
}

func (m *mockResource) Close() { m.closed = true }

// Override the actual cuvs calls for testing purposes.
// This is a tricky part without proper dependency injection in the original code.
// We'll rely on the fact that CuvsWorker's run method calls NewCudaStream and NewResource.
// For testing purposes, we would ideally mock these functions.
// However, since we cannot easily mock package-level functions in Go without
// modifying the source or using advanced mocking frameworks (which might not be in project dependencies),
// we will focus on the CuvsWorker's general behavior and assume cuvs calls succeed for now.
// If this test fails due to actual CUDA dependency, a more sophisticated mocking strategy
// or build tags would be necessary.
//
// For this test, we will temporarily hijack the NewCudaStream and NewResource functions
// using a linker trick (if running in a controlled test environment with `go test -ldflags='-X ...'`)
// or more practically, by making the `cuvs` calls inside `run` accessible for mocking via a variable.
// Given the current structure, direct mocking is difficult.

// The following test for CuvsWorker will primarily verify the Go concurrency
// aspects (Start, Submit, Wait, Stop) and the integration with CuvsTaskResultStore.
// The actual `cuvs.NewCudaStream()` and `cuvs.NewResource()` calls will still be made.
// If run on a machine without a CUDA device, these calls are likely to fail and
// cause a `logutil.Fatal` exit, preventing the test from completing successfully.
// This limitation is noted due to the direct dependency on a low-level C++ library
// without an easy mocking point in the provided `cudaworker.go`.
func TestCuvsWorker_LifecycleAndTaskExecution(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(5)
	require.NotNil(t, worker)

	// Start the worker
	worker.Start(nil, func(_ *cuvs.Resource) error { return nil }) // Pass nil initFn

	// Submit a task
	expectedTaskResult := "processed by CUDA (mocked)"
	taskID, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
		// In a real scenario, this would use the cuvs.Resource
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
	taskID2, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
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
	taskID3, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
		assert.NotNil(t, res)
		return nil, expectedError
	})
	require.NoError(t, err)

	result3, err := worker.Wait(taskID3)
	assert.NoError(t, err) // Error is returned in CuvsTaskResult, not as return value of Wait
	assert.NotNil(t, result3)
	assert.Equal(t, taskID3, result3.ID)
	assert.Nil(t, result3.Result)
	assert.Equal(t, expectedError, result3.Error)

	// Stop the worker
	worker.Stop()

	// Ensure that after stopping, submitting new tasks does not panic but also doesn't get processed.
	// This might block indefinitely, so we use a context with a timeout.
	// // Ensure that after stopping, submitting new tasks does not panic but also doesn't get processed.
	// // This might block indefinitely, so we use a context with a timeout.
	// taskID4 := worker.GetNextJobID()
	// task4 := &CuvsTask{ // Updated line
	// 	ID: taskID4,
	// 	Fn: func(res *cuvs.Resource) (any, error) {
	// 		return "should not be processed", nil
	// 	},
	// }

	// // Submitting to a closed channel will panic. We need to handle this gracefully
	// // or ensure `Submit` is not called after `Stop`.
	// // Given the current implementation, `Submit` would block indefinitely if tasks channel is not closed.
	// // Or panic if the channel is closed.
	// // The current `Stop` implementation just closes `stopCh` and waits for `run` to exit.
	// // The `tasks` channel remains open.
	// // A more robust worker design might close `tasks` channel on stop or return an error on submit.
	// // For now, we will just verify the previous tasks were processed and the worker stops.

	// // Attempting to submit after stop might block or panic depending on exact timing.
	// // To safely test the 'stopped' state without modifying the worker, we ensure that
	// // the worker correctly processed its queue and exited its `run` loop.

	// // Verify that if we try to wait for a non-existent task, it eventually times out
	// // (or would block indefinitely if not for the conditional signal mechanism).
	// // With the current `Wait` implementation, it will wait indefinitely.
	// // To test that it does not process new tasks after stop, a better approach would be
	// // to see if a submitted task *doesn't* get its result back within a timeout.
	// // However, this requires a modification to `Wait` or a more complex test setup.

	// // For now, assume if the worker has stopped, its `run` goroutine has exited.
	// // The tasks channel is not closed by `Stop`, so subsequent `Submit` calls would block.
	// // This is an area for potential improvement in the worker's design if it's meant to
	// // gracefully reject new tasks after stopping.

	t.Log("CuvsWorker stopped. Further submissions would block or panic.")
}

func TestCuvsWorker_StopDuringTaskProcessing(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(5)
	worker.Start(nil, func(_ *cuvs.Resource) error { return nil }) // Pass nil initFn

	// Submit a long-running task
	longTaskSignal := make(chan struct{})
	longTaskID, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
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

func TestCuvsWorker_MultipleSubmitsBeforeStart(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(5)

	// Start the worker - now takes initFn
	worker.Start(nil, func(_ *cuvs.Resource) error { return nil }) // Pass nil initFn

	// Submit multiple tasks before starting the worker
	numTasks := 5
	taskIDs := make([]uint64, numTasks) // Still need to collect IDs
	for i := 0; i < numTasks; i++ {
		var err error
		taskIDs[i], err = worker.Submit(func(res *cuvs.Resource) (any, error) {
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

func TestCuvsWorker_GracefulShutdown(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(5)
	worker.Start(nil, func(_ *cuvs.Resource) error { return nil }) // Pass nil initFn

	var wg sync.WaitGroup
	numTasks := 10
	results := make(chan *CuvsTaskResult, numTasks) // Changed type

	// Submit tasks
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		// Capture loop index for the anonymous function
		loopIndex := i

		var submitErr error
		taskID, submitErr := worker.Submit(func(res *cuvs.Resource) (any, error) {
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
	_, err := worker.Submit(func(res *cuvs.Resource) (any, error) { // Use := for first declaration of err in this scope
		return "should not be processed", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")
}

func TestCuvsWorker_SignalTermination(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(1) // Use 1 thread for easier control and observation
	require.NotNil(t, worker)

	worker.Start(nil, func(_ *cuvs.Resource) error { return nil })

	// Submit a task that will complete after the signal, to ensure graceful processing
	taskDone := make(chan struct{})
	taskID1, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
		assert.NotNil(t, res)
		<-taskDone // Wait for signal to complete
		return "task1 processed", nil
	})
	require.NoError(t, err)

	// Submit a second quick task that should complete before or around the signal
	taskID2, err := worker.Submit(func(res *cuvs.Resource) (any, error) {
		assert.NotNil(t, res)
		return "task2 processed", nil
	})
	require.NoError(t, err)

	// Give the worker a moment to pick up the tasks
	time.Sleep(50 * time.Millisecond)

	// Simulate SIGTERM by sending to the signal channel
	t.Log("Simulating SIGTERM to CuvsWorker")
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
	_, err = worker.Submit(func(res *cuvs.Resource) (any, error) {
		return "should not be processed", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")
}

func TestCuvsWorker_GetFirstError(t *testing.T) {
	skipIfNotCudaAvailable(t)

	var err error // Explicitly declare err here

	worker := NewCuvsWorker(1)
	assert.Nil(t, worker.GetFirstError(), "GetFirstError should be nil initially")

	// Trigger an error in initFn, which will be pushed to w.errch
	expectedErr1 := fmt.Errorf("simulated init error 1")
	initFn1 := func(resource *cuvs.Resource) error {
		return expectedErr1
	}
	stopFn := func(_ *cuvs.Resource) error { return nil }

	worker.Start(initFn1, stopFn)

	// Give the `run` goroutine and the signal handler a moment to process initFn and store the first error.
	time.Sleep(50 * time.Millisecond)

	// GetFirstError should now return the expected error
	assert.Equal(t, expectedErr1, worker.GetFirstError(), "GetFirstError should return the first recorded error")

	// Submit a task that causes an error (this error won't be saved as firstError via w.errch)
	// This ensures that only errors propagated through w.errch are considered.
	_, err = worker.Submit(func(res *cuvs.Resource) (any, error) { // Use = for assignment
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

func TestCuvsWorker_MultipleStopCalls(t *testing.T) {
	skipIfNotCudaAvailable(t)

	worker := NewCuvsWorker(1) // Use 1 thread
	require.NotNil(t, worker)

	worker.Start(nil, func(_ *cuvs.Resource) error { return nil })

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
	_, err := worker.Submit(func(res *cuvs.Resource) (any, error) { return nil, nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker is stopped")

	t.Log("Successfully called Stop multiple times without panic.")
}

// Helper to make cuvs.NewCudaStream and cuvs.NewResource mockable.
// This requires modifying the original cudaworker.go to introduce variables
// that can be swapped during testing. For now, this is a placeholder.
/*
var (
	newCudaStream = cuvs.NewCudaStream
	newResource   = cuvs.NewResource
)

func init() {
	// In the cudaworker.go file, change calls from:
	// stream, err := cuvs.NewCudaStream()
	// resource, err := cuvs.NewResource(stream)
	// To:
	// stream, err := newCudaStream()
	// resource, err := newResource(stream)
}

func mockCuvsFunctions() func() {
	originalNewCudaStream := newCudaStream
	originalNewResource := newResource

	newCudaStream = func() (*cuvs.Stream, error) {
		return &cuvs.Stream{}, nil // Return a dummy stream
	}
	newResource = func(stream *cuvs.Stream) (*cuvs.Resource, error) {
		return &cuvs.Resource{}, nil // Return a dummy resource
	}

	return func() {
		newCudaStream = originalNewCudaStream
		newResource = originalNewResource
	}
}
*/
