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

package vm

import (
	"context"
	"errors"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Continuation is one resumable execution owner. Step never recursively runs a
// child or waits for another task on this driver. A returned child transfers
// ownership even when Step returns an error. Accept must own any child values
// it retains: the driver closes the child immediately afterwards. Close is
// called exactly once, including partial initialization and admission failure.
// Accepted values remain provisional until RunContinuation returns success.
// Implementations use the same operator kernels/state as their ordinary Call.
type Continuation interface {
	Step(context.Context) (ContinuationStep, error)
	Accept(Continuation) error
	Close(error) error
}

type ContinuationStep struct {
	Child Continuation
	Done  bool
}

// RunContinuation lends the consumer's goroutine to a depth-first trampoline.
// It creates no workers and charges its live traversal capacity to the caller's
// existing statement generation. consume copies/transfers the root's result;
// callers must discard that provisional result if cleanup fails afterwards.
func RunContinuation(ctx context.Context, budget *process.ExecutionResourceGeneration, root Continuation, consume func(Continuation) error) (err error) {
	if root == nil {
		return moerr.NewInternalError(ctx, "nil continuation")
	}
	if budget == nil {
		err = moerr.NewInternalError(ctx, "continuation requires statement budget")
		return continuationError(err, root.Close(err))
	}
	var stack []Continuation
	var charged uint64
	defer func() {
		for len(stack) > 0 {
			task := stack[len(stack)-1]
			stack[len(stack)-1] = nil
			stack = stack[:len(stack)-1]
			err = continuationError(err, task.Close(err))
		}
		budget.ReleaseAllocationCapacity(charged)
	}()
	push := func(task Continuation) error {
		if len(stack) == cap(stack) {
			capacity := max(8, cap(stack)*2)
			if capacity < cap(stack) {
				return moerr.NewOOM(ctx)
			}
			size := uint64(capacity) * uint64(unsafe.Sizeof(task))
			if err := budget.AcquireAllocationCapacity(size); err != nil {
				return err
			}
			next := make([]Continuation, len(stack), capacity)
			copy(next, stack)
			stack = next
			budget.ReleaseAllocationCapacity(charged)
			charged = size
		}
		stack = append(stack, task)
		return nil
	}
	if err = push(root); err != nil {
		return continuationError(err, root.Close(err))
	}
	for len(stack) > 0 {
		if err = context.Cause(ctx); err != nil {
			return err
		}
		task := stack[len(stack)-1]
		var step ContinuationStep
		step, err = task.Step(ctx)
		if err != nil {
			if step.Child != nil {
				err = continuationError(err, step.Child.Close(err))
			}
			return err
		}
		if step.Child != nil {
			if step.Done {
				err = moerr.NewInternalError(ctx, "completed continuation returned child")
				return continuationError(err, step.Child.Close(err))
			}
			if err = push(step.Child); err != nil {
				return continuationError(err, step.Child.Close(err))
			}
			continue
		}
		if !step.Done {
			continue
		}
		if len(stack) > 1 {
			err = stack[len(stack)-2].Accept(task)
		} else if consume != nil {
			err = consume(task)
		}
		// Remove before Close: a failure must not cause a second cleanup in
		// the deferred unwind. Parents retain their own leases until then.
		stack[len(stack)-1] = nil
		stack = stack[:len(stack)-1]
		err = continuationError(err, task.Close(err))
		if err != nil {
			return err
		}
	}
	return nil
}

// Cancellation echoes cannot replace an already observed substantive failure.
// Conversely a cleanup/read failure must not be hidden by cancellation. Keep
// the exact selected error so retry and SQL error classification remain intact.
func continuationError(current, next error) error {
	if next == nil {
		return current
	}
	if current == nil {
		return next
	}
	if !process.IsPipelineFailure(current) &&
		(errors.Is(current, context.Canceled) || errors.Is(current, context.DeadlineExceeded) || current == process.ErrPipelineStopped) &&
		(!errors.Is(next, context.Canceled) && !errors.Is(next, context.DeadlineExceeded) && next != process.ErrPipelineStopped || process.IsPipelineFailure(next)) {
		return next
	}
	return current
}
