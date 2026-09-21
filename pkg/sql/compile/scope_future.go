// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// scopeFuture is the scheduler boundary for a synchronous compatibility
// operation. The operation itself runs on the bounded blocking lane, while
// every registered callback is re-admitted to the ready queue. This keeps
// Scope state machines non-blocking without requiring the engine/catalog API
// to grow a second asynchronous interface immediately.
//
// A future cannot interrupt an already-running synchronous function. Cancel
// therefore only completes the future with the supplied cause; the adapter
// still drains the underlying operation before the scheduler can retire.
type scopeFuture[T any] struct {
	scheduler    *scopeTaskScheduler
	callbackName string

	mu        sync.Mutex
	completed bool
	result    scopeFutureResult[T]
	callbacks []func(T, error)
	done      chan struct{}
}

type scopeFutureResult[T any] struct {
	value T
	err   error
}

func newScopeFuture[T any](scheduler *scopeTaskScheduler, callbackName string) *scopeFuture[T] {
	return &scopeFuture[T]{
		scheduler:    scheduler,
		callbackName: callbackName,
		done:         make(chan struct{}),
	}
}

// submitBlockingFuture adapts one synchronous operation to a callback-driven
// completion. Panics are converted into operation errors so the Scope state
// machine receives exactly one terminal result.
func submitBlockingFuture[T any](
	s *scopeTaskScheduler,
	name string,
	allowCanceled bool,
	op func() (T, error),
) (*scopeFuture[T], error) {
	if op == nil {
		return nil, errors.New("nil blocking future operation")
	}
	future := newScopeFuture[T](s, name+"-ready")
	err := s.submitBlockingEventWithContext(name, func() {
		var (
			value T
			opErr error
		)
		defer func() {
			if recovered := recover(); recovered != nil {
				opErr = moerr.ConvertPanicError(s.ctx, recovered)
			}
			future.complete(value, opErr)
		}()
		value, opErr = op()
	}, allowCanceled)
	if err != nil {
		var zero T
		future.complete(zero, err)
		return nil, err
	}
	return future, nil
}

func (f *scopeFuture[T]) complete(value T, err error) {
	if f == nil {
		return
	}
	f.mu.Lock()
	if f.completed {
		f.mu.Unlock()
		return
	}
	f.completed = true
	f.result = scopeFutureResult[T]{value: value, err: err}
	callbacks := append([]func(T, error){}, f.callbacks...)
	f.callbacks = nil
	close(f.done)
	f.mu.Unlock()

	for _, callback := range callbacks {
		f.dispatch(callback, value, err)
	}
}

// OnComplete registers a callback that executes as a ready task. If the
// future is already complete, registration still preserves that scheduling
// boundary rather than invoking user code on the caller or blocking worker.
func (f *scopeFuture[T]) OnComplete(callback func(T, error)) error {
	if f == nil {
		return errors.New("nil scope future")
	}
	if callback == nil {
		return errors.New("nil scope future callback")
	}

	f.mu.Lock()
	if !f.completed {
		f.callbacks = append(f.callbacks, callback)
		f.mu.Unlock()
		return nil
	}
	result := f.result
	f.mu.Unlock()
	return f.dispatch(callback, result.value, result.err)
}

func (f *scopeFuture[T]) dispatch(callback func(T, error), value T, err error) error {
	if f.scheduler == nil {
		callback(value, err)
		return nil
	}
	if submitErr := f.scheduler.submitRootWithContext(
		f.callbackName,
		func() { callback(value, err) },
		true,
	); submitErr != nil {
		// The scheduler may close concurrently with the final compatibility
		// operation. Completion must still reach cleanup; invoke the callback as
		// the terminal fallback after admission has failed.
		callback(value, errors.Join(err, submitErr))
		return submitErr
	}
	return nil
}

// Await is intentionally only used at synchronous API boundaries. Scope
// execution itself uses OnComplete and never waits on this channel.
func (f *scopeFuture[T]) Await(ctx context.Context) (T, error) {
	var zero T
	if f == nil {
		return zero, errors.New("nil scope future")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-f.done:
		f.mu.Lock()
		result := f.result
		f.mu.Unlock()
		return result.value, result.err
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	}
}

// Cancel publishes a terminal result for callbacks. It does not attempt to
// interrupt the synchronous operation currently owned by the blocking lane.
func (f *scopeFuture[T]) Cancel(err error) {
	if err == nil {
		err = context.Canceled
	}
	var zero T
	f.complete(zero, err)
}

// newScopeExecutionFuture wraps a Scope's event-driven start function so the
// legacy error-returning methods share exactly the production state machine.
func newScopeExecutionFuture(
	c *Compile,
	name string,
	start func(func(error)) error,
) (*scopeFuture[struct{}], error) {
	if c == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil compile for scope future")
	}
	if start == nil {
		return nil, errors.New("nil scope future starter")
	}
	scheduler := c.ensureScopeTaskScheduler(max(1, len(c.scopes)))
	future := newScopeFuture[struct{}](scheduler, name+"-complete")
	err := start(func(runErr error) {
		future.complete(struct{}{}, runErr)
	})
	if err != nil {
		future.complete(struct{}{}, err)
		return nil, err
	}
	return future, nil
}
