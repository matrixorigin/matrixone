// Copyright 2021 - 2022 Matrix Origin
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

package moerr

import (
	"context"
	"errors"
	"sync"
)

// PipelineStop marks cancellation by a downstream pipeline that has finished
// taking input. Its lock covers propagation to the cancelled context's children.
type PipelineStop struct {
	propagation sync.Mutex
}

func (*PipelineStop) Error() string { return "pipeline no longer needs input" }

func StopPipeline(cancel context.CancelCauseFunc) {
	stop := new(PipelineStop)
	stop.propagation.Lock()
	defer stop.propagation.Unlock()
	cancel(stop)
}

// PipelineStopCause recognizes a normal stop carried through an ordinary error
// wrapper. An aggregate with another failure must retain that failure.
func PipelineStopCause(err error) *PipelineStop {
	for err != nil {
		if stop, ok := err.(*PipelineStop); ok {
			return stop
		}
		err = errors.Unwrap(err)
	}
	return nil
}

// WaitForPropagation lets a dependency consumer distinguish a sibling whose
// Done is still being propagated from a live consumer outside the stopped tree.
func (s *PipelineStop) WaitForPropagation() {
	s.propagation.Lock()
	s.propagation.Unlock()
}

type pipelineCancellationOrigin uint8

func (pipelineCancellationOrigin) Error() string { return context.Canceled.Error() }
func (pipelineCancellationOrigin) Unwrap() error { return context.Canceled }

const (
	// PipelineContextCanceled identifies a locally observed cancellation
	// without a more specific cause.
	PipelineContextCanceled pipelineCancellationOrigin = iota
	// PipelineReportedCanceled preserves a bare cancellation independently
	// returned by an execution path when the coordinator propagates it.
	PipelineReportedCanceled
)

// PipelineContextError reads the cause at a local cancellation observation.
// A timeout's diagnostic cause must not replace its deadline classification.
func PipelineContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != context.Canceled {
		return err
	}
	if cause := context.Cause(ctx); cause != context.Canceled {
		return cause
	}
	return PipelineContextCanceled
}

// PipelineIOError resolves a context-shaped error at the call that passed the
// context to an external I/O implementation. Explicit terminal messages must
// bypass this helper. Those messages already carry an execution decision.
func PipelineIOError(ctx context.Context, err error) error {
	if err == nil || ctx == nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil && isPipelineIOCancellation(err, ctxErr) {
		return PipelineContextError(ctx)
	}
	return err
}

func isPipelineIOCancellation(err, ctxErr error) bool {
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		for _, child := range children {
			if !isPipelineIOCancellation(child, ctxErr) {
				return false
			}
		}
		return len(children) != 0
	case interface{ Unwrap() error }:
		return isPipelineIOCancellation(e.Unwrap(), ctxErr)
	default:
		return errors.Is(err, ctxErr) || IsMoErrCode(err, ErrQueryInterrupted)
	}
}

// PipelineEndDeliveryFailed can survive protocol encoding as an internal
// error. Its fixed message identifies this specific cleanup fallback.
var PipelineEndDeliveryFailed = NewInternalError(context.Background(), "pipeline end signal delivery failed")

func IsPipelineEndDeliveryFailure(err error) bool {
	me, ok := err.(*Error)
	return ok && me.code == ErrInternal && me.message == PipelineEndDeliveryFailed.message
}

// PipelineError selects a representative leaf before an execution error tree
// crosses a protocol that carries a single MatrixOne error. Typed execution
// failures survive incidental Go errors and cancellation, in tree order.
func PipelineError(err error) error {
	if err == nil {
		return nil
	}
	var best error
	rank := -1
	var visit func(error)
	visit = func(e error) {
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
			return
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				visit(child)
				return
			}
			return
		}
		if e == nil || PipelineStopCause(e) != nil {
			return
		}
		r := 3
		if code, ok := GetMoErrCode(e); ok && code != ErrInternal {
			r = 4
		}
		switch {
		case IsPipelineEndDeliveryFailure(e):
			r = 0
		case IsPipelineInterruption(e):
			r = 1
		case errors.Is(e, context.DeadlineExceeded):
			r = 2
		}
		if r > rank {
			best, rank = e, r
		}
	}
	visit(err)
	return best
}

// IsPipelineInterruption identifies the local and wire forms of an
// interruption. Callers comparing aggregate results first select a leaf.
func IsPipelineInterruption(err error) bool {
	return errors.Is(err, context.Canceled) || IsMoErrCode(err, ErrQueryInterrupted)
}
