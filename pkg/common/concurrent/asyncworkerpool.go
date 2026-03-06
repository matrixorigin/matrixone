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
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// AsyncTask represents a task to be executed by the AsyncWorkerPool.
type AsyncTask struct {
	ID uint64
	Fn func(res any) (any, error)
}

// AsyncTaskResult holds the result of a AsyncTask execution.
type AsyncTaskResult struct {
	ID     uint64
	Result any
	Error  error
}

// AsyncTaskResultStore manages the storage and retrieval of AsyncTaskResults.
type AsyncTaskResultStore struct {
	states    map[uint64]*taskState
	mu        sync.Mutex
	nextJobID uint64
	stopCh    chan struct{}
	stopped   atomic.Bool
}

type taskState struct {
	done   chan struct{}
	result *AsyncTaskResult
}

// NewAsyncTaskResultStore creates a new AsyncTaskResultStore.
func NewAsyncTaskResultStore() *AsyncTaskResultStore {
	return &AsyncTaskResultStore{
		states:    make(map[uint64]*taskState),
		nextJobID: 0,
		stopCh:    make(chan struct{}),
		stopped:   atomic.Bool{},
	}
}

// Store saves a AsyncTaskResult in the store and signals any waiting goroutines.
func (s *AsyncTaskResultStore) Store(result *AsyncTaskResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[result.ID]
	if !ok {
		state = &taskState{done: make(chan struct{})}
		s.states[result.ID] = state
	}
	state.result = result
	close(state.done)
}

// Wait blocks until the result for the given jobID is available and returns it.
// The result is removed from the internal map after being retrieved.
func (s *AsyncTaskResultStore) Wait(jobID uint64) (*AsyncTaskResult, error) {
	s.mu.Lock()
	state, ok := s.states[jobID]
	if !ok {
		// If task was not submitted yet, create state and wait.
		state = &taskState{done: make(chan struct{})}
		s.states[jobID] = state
		s.mu.Unlock() // Release lock before blocking
	} else if state.result != nil {
		// If result is already available, return it immediately without blocking.
		delete(s.states, jobID) // Remove after retrieval
		s.mu.Unlock()
		return state.result, nil
	} else {
		// Task was submitted, but result not yet available. Release lock and wait.
		s.mu.Unlock() // Release lock before blocking
	}

	select {
	case <-state.done:
		s.mu.Lock()
		delete(s.states, jobID)
		s.mu.Unlock()
		return state.result, nil
	case <-s.stopCh:
		return nil, moerr.NewInternalErrorNoCtx("AsyncTaskResultStore stopped before result was available")
	}
}

// GetNextJobID atomically increments and returns a new unique job ID.
func (s *AsyncTaskResultStore) GetNextJobID() uint64 {
	return atomic.AddUint64(&s.nextJobID, 1)
}

// Stop signals the AsyncTaskResultStore to stop processing new waits.
func (s *AsyncTaskResultStore) Stop() {
	if s.stopped.CompareAndSwap(false, true) {
		close(s.stopCh)
	}
}

// AsyncWorkerPool runs tasks in a dedicated OS thread with a CUDA context.
type AsyncWorkerPool struct {
	tasks                chan *AsyncTask
	stopCh               chan struct{}
	wg                   sync.WaitGroup
	stopped              atomic.Bool // Indicates if the worker has been stopped
	firstError           error
	*AsyncTaskResultStore // Embed the result store
	nthread              uint
	sigc                 chan os.Signal // Add this field
	errch                chan error
	createResource       func() (any, error)
	cleanupResource      func(any)
}

// NewAsyncWorkerPool creates a new AsyncWorkerPool.
func NewAsyncWorkerPool(nthread uint, createResource func() (any, error), cleanupResource func(any)) *AsyncWorkerPool {
	return &AsyncWorkerPool{
		tasks:               make(chan *AsyncTask, nthread),
		stopCh:              make(chan struct{}),
		stopped:             atomic.Bool{}, // Initialize to false
		AsyncTaskResultStore: NewAsyncTaskResultStore(),
		nthread:             nthread,
		sigc:                make(chan os.Signal, 1),   // Initialize sigc
		errch:               make(chan error, nthread), // Initialize errch
		createResource:      createResource,
		cleanupResource:     cleanupResource,
	}
}

// handleAndStoreTask processes a single AsyncTask and stores its result.
func (w *AsyncWorkerPool) handleAndStoreTask(task *AsyncTask, resource any) {
	result, err := task.Fn(resource)
	asyncResult := &AsyncTaskResult{
		ID:     task.ID,
		Result: result,
		Error:  err,
	}
	w.AsyncTaskResultStore.Store(asyncResult)
}

// drainAndProcessTasks drains the w.tasks channel and processes each task.
// It stops when the channel is empty or closed.
func (w *AsyncWorkerPool) drainAndProcessTasks(resource any) {
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return // Channel closed, no more tasks. Exit.
			}
			w.handleAndStoreTask(task, resource)
		default:
			return // All tasks drained, or channel is empty.
		}
	}
}

// Start begins the worker's execution loop.
func (w *AsyncWorkerPool) Start(initFn func(res any) error, stopFn func(resource any) error) {
	w.wg.Add(1) // for w.run
	go w.run(initFn, stopFn)

	signal.Notify(w.sigc, syscall.SIGTERM, syscall.SIGINT) // Notify signals to sigc

	w.wg.Add(1) // for the signal handler goroutine
	go func() {
		defer w.wg.Done() // Ensure wg.Done() is called when this goroutine exits
		select {
		case <-w.sigc: // Wait for a signal
			logutil.Info("AsyncWorkerPool received shutdown signal, stopping...")
			if w.stopped.CompareAndSwap(false, true) {
				close(w.stopCh) // Signal run() to stop.
				close(w.tasks)  // Close tasks channel here.
			}
		case err := <-w.errch: // Listen for errors from worker goroutines
			logutil.Error("AsyncWorkerPool received internal error, stopping...", zap.Error(err))
			if w.firstError == nil {
				w.firstError = err
			}
			if w.stopped.CompareAndSwap(false, true) {
				close(w.stopCh) // Signal run() to stop.
				close(w.tasks)  // Close tasks channel here.
			}
		case <-w.stopCh: // Listen for internal stop signal from w.Stop()
			logutil.Info("AsyncWorkerPool signal handler received internal stop signal, exiting...")
			// Do nothing, just exit. w.Stop() will handle the rest.
		}
	}()
}

// Stop signals the worker to terminate.
func (w *AsyncWorkerPool) Stop() {
	if w.stopped.CompareAndSwap(false, true) {
		close(w.stopCh) // Signal run() to stop.
		close(w.tasks)  // Close tasks channel here.
	}
	w.wg.Wait()
	w.AsyncTaskResultStore.Stop() // Signal the result store to stop
}

// Submit sends a task to the worker.
func (w *AsyncWorkerPool) Submit(fn func(res any) (any, error)) (uint64, error) {
	if w.stopped.Load() {
		return 0, moerr.NewInternalErrorNoCtx("cannot submit task: worker is stopped")
	}
	jobID := w.GetNextJobID()
	task := &AsyncTask{
		ID: jobID,
		Fn: fn,
	}
	w.tasks <- task
	return jobID, nil
}

func (w *AsyncWorkerPool) workerLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var resource any
	var err error
	if w.createResource != nil {
		resource, err = w.createResource()
		if err != nil {
			w.errch <- err
			return
		}
	}
	if w.cleanupResource != nil {
		defer w.cleanupResource(resource)
	}

	for {
		select {
		case task, ok := <-w.tasks:
			if !ok { // tasks channel closed
				return // No more tasks, and channel is closed. Exit.
			}
			w.handleAndStoreTask(task, resource) // Pass resource directly
		case <-w.stopCh:
			// stopCh signaled. Drain remaining tasks from w.tasks then exit.
			w.drainAndProcessTasks(resource) // Pass resource directly
			return
		}
	}
}

func (w *AsyncWorkerPool) run(initFn func(res any) error, stopFn func(resource any) error) {
	defer w.wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var parentResource any
	var err error
	if w.createResource != nil {
		parentResource, err = w.createResource()
		if err != nil {
			w.errch <- err
			return
		}
	}
	if w.cleanupResource != nil {
		defer w.cleanupResource(parentResource)
	}

	// Execute initFn once.
	if initFn != nil {
		if err := initFn(parentResource); err != nil {
			logutil.Error("failed to initialize async resource with provided function", zap.Error(err))
			w.errch <- err

			return
		}
	}

	if stopFn != nil {
		defer func() {
			if err := stopFn(parentResource); err != nil {
				logutil.Error("error during async resource stop function", zap.Error(err))
				w.errch <- err
			}
		}()
	}

	if w.nthread == 1 {
		// Special case: nthread is 1, process tasks directly in this goroutine
		for {
			select {
			case task, ok := <-w.tasks:
				if !ok { // tasks channel closed
					return // Channel closed, no more tasks. Exit.
				}
				w.handleAndStoreTask(task, parentResource)
			case <-w.stopCh:
				// Drain the tasks channel before exiting
				w.drainAndProcessTasks(parentResource)
				return
			}
		}
	} else {
		// General case: nthread > 1, create worker goroutines
		var workerWg sync.WaitGroup
		workerWg.Add(int(w.nthread))
		for i := 0; i < int(w.nthread); i++ {
			go w.workerLoop(&workerWg)
		}

		// Wait for stop signal
		<-w.stopCh

		// Signal workers to stop and wait for them to finish.
		workerWg.Wait()
	}
}

// Wait blocks until the result for the given jobID is available and returns it.
// The result is removed from the internal map after being retrieved.
func (w *AsyncWorkerPool) Wait(jobID uint64) (*AsyncTaskResult, error) {
	return w.AsyncTaskResultStore.Wait(jobID)
}

// GetFirstError returns the first internal error encountered by the worker.
func (w *AsyncWorkerPool) GetFirstError() error {
	return w.firstError
}

