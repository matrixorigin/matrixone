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
	"reflect"
	"sync"
)

// PipelineStop marks cancellation by a downstream pipeline that has finished
// taking input. It signals when the cancellation reaches the context's children.
type PipelineStop struct {
	propagation sync.WaitGroup
}

func (*PipelineStop) Error() string { return "pipeline no longer needs input" }

func StopPipeline(cancel context.CancelCauseFunc) {
	stop := new(PipelineStop)
	stop.propagation.Add(1)
	defer stop.propagation.Done()
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
	s.propagation.Wait()
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

type reportedPipelineError struct{ error }

func (e reportedPipelineError) Unwrap() error { return e.error }

// ReportedPipelineError fixes the origin of a cancellation-shaped error read
// from an explicit failure signal or stored dependency outcome.
func ReportedPipelineError(err error) error {
	if err == context.Canceled {
		return PipelineReportedCanceled
	}
	if isPipelineIOCancellation(err, nil, nil, false) {
		return reportedPipelineError{err}
	}
	return err
}

// PipelineIOError classifies a legacy cancellation at the point it returns
// from execution or external I/O. If the context is live, freeze the error
// before cleanup or forwarding can cancel that context.
func PipelineIOError(ctx context.Context, err error) error {
	return pipelineIOError(ctx, err, false)
}

// PipelineRPCIOError also accepts the error code legacy RPC transport uses
// for cancellation. Explicit peer result frames bypass this path.
func PipelineRPCIOError(ctx context.Context, err error) error {
	return pipelineIOError(ctx, err, true)
}

func pipelineIOError(ctx context.Context, err error, legacyRPC bool) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		ctxErr := ctx.Err()
		var diagnostic error
		if ctxErr == context.DeadlineExceeded && errors.Is(err, ctxErr) {
			cause := context.Cause(ctx)
			// AttachCause joins the exact timeout diagnostic supplied by
			// this context. A different error in the same tree is a failure.
			if cause != nil && cause != ctxErr && reflect.TypeOf(cause).Comparable() {
				diagnostic = cause
			}
		}
		if ctxErr != nil && isPipelineIOCancellation(err, ctxErr, diagnostic, legacyRPC) {
			return PipelineContextError(ctx)
		}
	}
	return ReportedPipelineError(err)
}

func isPipelineIOCancellation(err, ctxErr, diagnostic error, legacyRPC bool) bool {
	switch e := err.(type) {
	case nil, pipelineCancellationOrigin, reportedPipelineError:
		return false
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		for _, child := range children {
			if !isPipelineIOCancellation(child, ctxErr, diagnostic, legacyRPC) {
				return false
			}
		}
		return len(children) != 0
	case interface{ Unwrap() error }:
		return isPipelineIOCancellation(e.Unwrap(), ctxErr, diagnostic, legacyRPC)
	default:
		if ctxErr == nil {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		}
		return diagnostic != nil && err == diagnostic ||
			errors.Is(err, ctxErr) || legacyRPC && IsMoErrCode(err, ErrQueryInterrupted)
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
