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

// ErrPipelineStopped certifies that a consumer no longer needs input.
// It is a control cause, never an operator failure or a wire error.
var ErrPipelineStopped error = &pipelineStoppedError{message: "pipeline consumer finished"}

// A successful consumer retirement is a control cause, not a moerr failure.
// Keep a single identity distinct from ordinary execution failures.
type pipelineStoppedError struct{ message string }

func (e *pipelineStoppedError) Error() string { return e.message }

type pipelineFailure struct{ error }

func (e *pipelineFailure) Unwrap() error { return e.error }

func IsPipelineFailure(err error) bool {
	var failure *pipelineFailure
	return errors.As(err, &failure)
}

// MarkPipelineFailure preserves declared cancellation-shaped failures across
// cleanup. Ordinary execution/retry errors retain their concrete error type.
func MarkPipelineFailure(err error) error {
	if err != nil && !IsPipelineFailure(err) && IsPipelineCancellationError(err) {
		return &pipelineFailure{err}
	}
	return err
}

func UnwrapPipelineFailure(err error) error {
	if failure, ok := err.(*pipelineFailure); ok {
		return failure.error
	}
	return err
}

// Every leaf must be cancellation-shaped; a joined substantive failure must
// never disappear merely because another leaf is context.Canceled.
func IsPipelineCancellationError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !IsPipelineCancellationError(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return IsPipelineCancellationError(child)
		}
	}
	return err == ErrPipelineStopped || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted)
}
