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
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	cuvs "github.com/rapidsai/cuvs/go"
	"go.uber.org/zap"
)

// CuvsTask represents a task to be executed by the CuvsWorker.
type CuvsTask struct {
	ID uint64
	Fn func(res *cuvs.Resource) (any, error)
}

// CuvsTaskResult holds the result of a CuvsTask execution.
type CuvsTaskResult struct {
	ID     uint64
	Result any
	Error  error
}

// CuvsTaskResultStore manages the storage and retrieval of CuvsTaskResults.
type CuvsTaskResultStore struct {
	states    map[uint64]*taskState
	mu        sync.Mutex
	nextJobID uint64
	stopCh    chan struct{}
	stopped   atomic.Bool
}

type taskState struct {
	done   chan struct{}
	result *CuvsTaskResult
}

// NewCuvsTaskResultStore creates a new CuvsTaskResultStore.
func NewCuvsTaskResultStore() *CuvsTaskResultStore {
	return &CuvsTaskResultStore{
		states:    make(map[uint64]*taskState),
		nextJobID: 0,
		stopCh:    make(chan struct{}),
		stopped:   atomic.Bool{},
	}
}

// Store saves a CuvsTaskResult in the store and signals any waiting goroutines.
func (s *CuvsTaskResultStore) Store(result *CuvsTaskResult) {
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
func (s *CuvsTaskResultStore) Wait(jobID uint64) (*CuvsTaskResult, error) {
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
		return nil, moerr.NewInternalErrorNoCtx("CuvsTaskResultStore stopped before result was available")
	}
}

// GetNextJobID atomically increments and returns a new unique job ID.
func (s *CuvsTaskResultStore) GetNextJobID() uint64 {
	return atomic.AddUint64(&s.nextJobID, 1)
}

// Stop signals the CuvsTaskResultStore to stop processing new waits.
func (s *CuvsTaskResultStore) Stop() {
	if s.stopped.CompareAndSwap(false, true) {
		close(s.stopCh)
	}
}

// CuvsWorker runs tasks in a dedicated OS thread with a CUDA context.
type CuvsWorker struct {
	tasks                chan *CuvsTask
	stopCh               chan struct{}
	wg                   sync.WaitGroup
	stopped              atomic.Bool // Indicates if the worker has been stopped
	firstError           error
	*CuvsTaskResultStore // Embed the result store
	nthread              uint
	sigc                 chan os.Signal // Add this field
	errch                chan error
}

// NewCuvsWorker creates a new CuvsWorker.
func NewCuvsWorker(nthread uint) *CuvsWorker {
	return &CuvsWorker{
		tasks:               make(chan *CuvsTask, nthread),
		stopCh:              make(chan struct{}),
		stopped:             atomic.Bool{}, // Initialize to false
		CuvsTaskResultStore: NewCuvsTaskResultStore(),
		nthread:             nthread,
		sigc:                make(chan os.Signal, 1),   // Initialize sigc
		errch:               make(chan error, nthread), // Initialize errch
	}
}

// handleAndStoreTask processes a single CuvsTask and stores its result.
func (w *CuvsWorker) handleAndStoreTask(task *CuvsTask, resource *cuvs.Resource) {
	result, err := task.Fn(resource)
	cuvsResult := &CuvsTaskResult{
		ID:     task.ID,
		Result: result,
		Error:  err,
	}
	w.CuvsTaskResultStore.Store(cuvsResult)
}

// drainAndProcessTasks drains the w.tasks channel and processes each task.
// It stops when the channel is empty or closed.
func (w *CuvsWorker) drainAndProcessTasks(resource *cuvs.Resource) {
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
func (w *CuvsWorker) Start(initFn func(res *cuvs.Resource) error, stopFn func(resource *cuvs.Resource) error) {
	w.wg.Add(1) // for w.run
	go w.run(initFn, stopFn)

	signal.Notify(w.sigc, syscall.SIGTERM, syscall.SIGINT) // Notify signals to sigc

	w.wg.Add(1) // for the signal handler goroutine
	go func() {
		defer w.wg.Done() // Ensure wg.Done() is called when this goroutine exits
		select {
		case <-w.sigc: // Wait for a signal
			logutil.Info("CuvsWorker received shutdown signal, stopping...")
			if w.stopped.CompareAndSwap(false, true) {
				close(w.stopCh) // Signal run() to stop.
				close(w.tasks)  // Close tasks channel here.
			}
		case err := <-w.errch: // Listen for errors from worker goroutines
			logutil.Error("CuvsWorker received internal error, stopping...", zap.Error(err))
			if w.firstError == nil {
				w.firstError = err
			}
			if w.stopped.CompareAndSwap(false, true) {
				close(w.stopCh) // Signal run() to stop.
				close(w.tasks)  // Close tasks channel here.
			}
		case <-w.stopCh: // Listen for internal stop signal from w.Stop()
			logutil.Info("CuvsWorker signal handler received internal stop signal, exiting...")
			// Do nothing, just exit. w.Stop() will handle the rest.
		}
	}()
}

// Stop signals the worker to terminate.
func (w *CuvsWorker) Stop() {
	if w.stopped.CompareAndSwap(false, true) {
		close(w.stopCh) // Signal run() to stop.
		close(w.tasks)  // Close tasks channel here.
	}
	w.wg.Wait()
	w.CuvsTaskResultStore.Stop() // Signal the result store to stop
}

// Submit sends a task to the worker.
func (w *CuvsWorker) Submit(fn func(res *cuvs.Resource) (any, error)) (uint64, error) {
	if w.stopped.Load() {
		return 0, moerr.NewInternalErrorNoCtx("cannot submit task: worker is stopped")
	}
	jobID := w.GetNextJobID()
	task := &CuvsTask{
		ID: jobID,
		Fn: fn,
	}
	w.tasks <- task
	return jobID, nil
}

func (w *CuvsWorker) workerLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	resourcePtr, cleanup, err := w.setupResource()
	if err != nil {
		return
	}
	defer cleanup()
	defer runtime.KeepAlive(resourcePtr) // KeepAlive the pointer

	for {
		select {
		case task, ok := <-w.tasks:
			if !ok { // tasks channel closed
				return // No more tasks, and channel is closed. Exit.
			}
			w.handleAndStoreTask(task, resourcePtr) // Pass resourcePtr directly
		case <-w.stopCh:
			// stopCh signaled. Drain remaining tasks from w.tasks then exit.
			w.drainAndProcessTasks(resourcePtr) // Pass resourcePtr directly
			return
		}
	}
}

func (w *CuvsWorker) run(initFn func(res *cuvs.Resource) error, stopFn func(resource *cuvs.Resource) error) {
	defer w.wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	parentResource, cleanup, err := w.setupResource()
	if err != nil {
		return
	}
	defer cleanup()
	defer runtime.KeepAlive(parentResource)

	// Execute initFn once.
	if initFn != nil {
		if err := initFn(parentResource); err != nil {
			logutil.Error("failed to initialize cuvs resource with provided function", zap.Error(err))
			w.errch <- err

			return
		}
	}

	if stopFn != nil {
		defer func() {
			if err := stopFn(parentResource); err != nil {
				logutil.Error("error during cuvs resource stop function", zap.Error(err))
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
func (w *CuvsWorker) Wait(jobID uint64) (*CuvsTaskResult, error) {
	return w.CuvsTaskResultStore.Wait(jobID)
}

// GetFirstError returns the first internal error encountered by the worker.
func (w *CuvsWorker) GetFirstError() error {
	return w.firstError
}

func (w *CuvsWorker) setupResource() (*cuvs.Resource, func(), error) {
	stream, err := cuvs.NewCudaStream()
	if err != nil {
		logutil.Error("failed to create parent cuda stream", zap.Error(err))
		w.errch <- err
		return nil, nil, err
	}

	resource, err := cuvs.NewResource(stream)
	if err != nil {
		logutil.Error("failed to create parent cuvs resource", zap.Error(err))
		w.errch <- err
		stream.Close() // Close stream if resource creation fails
		return nil, nil, err
	}

	cleanup := func() {
		resource.Close()
		stream.Close()
	}
	return &resource, cleanup, nil
}
