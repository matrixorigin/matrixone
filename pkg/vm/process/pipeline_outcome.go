// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package process

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ErrPipelineStopped certifies that a consumer no longer needs input.
// It is a control cause, never an operator failure or a wire error.
var ErrPipelineStopped = errors.New("pipeline consumer finished")

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
