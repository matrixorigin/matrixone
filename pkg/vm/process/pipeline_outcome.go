// Copyright 2026 Matrix Origin
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

package process

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ErrPipelineStopped is a successful consumer retirement, not an execution error.
// Only the owner that no longer needs input may use it as a cancellation cause.
var ErrPipelineStopped error = &pipelineStoppedError{message: "pipeline consumer finished"}

// Keep this control cause outside moerr: dependency receivers recover
// substantive moerr causes, while a successful stop has no failure to recover.
type pipelineStoppedError struct{ message string }

func (e *pipelineStoppedError) Error() string { return e.message }

// IsPipelineStopped exposes immutable control shape to dependency snapshots
// without an import cycle. Only the scope's cancel cause certifies success.
func (e *pipelineStoppedError) IsPipelineStopped() bool { return true }

// isPipelineInterruption classifies error shape only for choosing between
// failures. Unlike IsPipelineCancellationError, it ignores failure provenance:
// an interrupted Error still fails, but must not hide a substantive cause.
func isPipelineInterruption(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isPipelineInterruption(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return isPipelineInterruption(child)
		}
	}
	return err == ErrPipelineStopped || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted)
}

type pipelineFailure struct{ err error }

func (e *pipelineFailure) Error() string               { return e.err.Error() }
func (e *pipelineFailure) Unwrap() error               { return e.err }
func (e *pipelineFailure) IsPipelineFailure() bool     { return true }
func (e *pipelineFailure) PipelineFailureCause() error { return e.err }

// IsPipelineFailure retains failure provenance before traversing error wrappers.
// The structural marker also supports immutable dependency errors without cycles.
func IsPipelineFailure(err error) bool {
	if marker, ok := err.(interface{ IsPipelineFailure() bool }); ok && marker.IsPipelineFailure() {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if IsPipelineFailure(child) {
				return true
			}
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return IsPipelineFailure(wrapped.Unwrap())
	}
	return false
}

// MarkPipelineFailure freezes cancellation-shaped failures at their owner or
// declared Error terminal. Ordinary execution/retry errors keep their exact type.
func MarkPipelineFailure(err error) error {
	if err == nil || IsPipelineFailure(err) {
		return err
	}
	if IsPipelineCancellationError(err) || moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted) {
		return &pipelineFailure{err: err}
	}
	return err
}

// UnwrapPipelineFailure removes only our provenance envelope at public/wire
// boundaries. Joined children retain all errors; unrelated wrappers stay intact.
func UnwrapPipelineFailure(err error) error {
	result, _ := unwrapPipelineFailure(err)
	return result
}

func unwrapPipelineFailure(err error) (error, bool) {
	if marked, ok := err.(interface {
		IsPipelineFailure() bool
		PipelineFailureCause() error
	}); ok && marked.IsPipelineFailure() {
		result, _ := unwrapPipelineFailure(marked.PipelineFailureCause())
		return result, true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		var unwrapped []error
		for i, child := range children {
			next, changed := unwrapPipelineFailure(child)
			if changed && unwrapped == nil {
				unwrapped = append([]error(nil), children...)
			}
			if unwrapped != nil {
				unwrapped[i] = next
			}
		}
		if unwrapped != nil {
			return errors.Join(unwrapped...), true
		}
	}
	return err, false
}

// isPipelineCancellationFrom reports whether every leaf in err can be attributed
// to the same canceled context. A joined error is attributable only as a whole;
// one matching cancellation leaf must not hide an independent deadline leaf.
func isPipelineCancellationFrom(err error, contextErr error) bool {
	if err == nil || contextErr == nil || IsPipelineFailure(err) {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isPipelineCancellationFrom(child, contextErr) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return isPipelineCancellationFrom(child, contextErr)
		}
	}
	return errors.Is(err, contextErr) ||
		(errors.Is(contextErr, context.Canceled) && errors.Is(err, ErrPipelineStopped))
}

// NormalizePipelineRunError distinguishes a substantive execution failure from
// cancellation fallout.  A child pipeline canceled by a successfully finished
// consumer is secondary and may be ignored, while a real error carried as the
// pipeline cancel cause must survive.  Query-level cancellation remains owned
// by the query context and is reported by Compile.Run.
func NormalizePipelineRunError(
	err error,
	pipelineCtx context.Context,
	queryCtx context.Context,
) (error, bool) {
	if err == nil {
		return nil, false
	}
	if !IsPipelineCancellationError(err) {
		return MarkPipelineFailure(err), false
	}
	if queryCtx != nil && queryCtx.Err() != nil && isPipelineCancellationFrom(err, queryCtx.Err()) {
		if errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
			return MarkPipelineFailure(queryCtx.Err()), true
		}
		if cause := context.Cause(queryCtx); cause != nil {
			return MarkPipelineFailure(cause), true
		}
		return MarkPipelineFailure(queryCtx.Err()), true
	}
	if pipelineCtx == nil || pipelineCtx.Err() == nil || !isPipelineCancellationFrom(err, pipelineCtx.Err()) {
		return MarkPipelineFailure(err), false
	}
	cause := context.Cause(pipelineCtx)
	if cause == ErrPipelineStopped && queryCtx != nil {
		// A first-wins consumer stop can outlive its query's deadline. It is
		// successful only while that query remains live.
		return MarkPipelineFailure(queryCtx.Err()), true
	}
	if cause != nil {
		return MarkPipelineFailure(cause), true
	}
	return MarkPipelineFailure(err), false
}
