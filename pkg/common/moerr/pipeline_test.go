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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineErrorKeepsObservedAndReportedOrigins(t *testing.T) {
	live := context.Background()
	stopped, stop := context.WithCancelCause(live)
	StopPipeline(stop)
	require.Same(t, context.Cause(stopped), PipelineIOError(stopped, context.Canceled))

	reported := PipelineIOError(live, fmt.Errorf("reader: %w", context.Canceled))
	require.ErrorIs(t, reported, context.Canceled)
	require.Equal(t, reported, PipelineIOError(stopped, reported))
	require.Equal(t, PipelineReportedCanceled, ReportedPipelineError(context.Canceled))
	require.Equal(t, PipelineReportedCanceled, PipelineRPCIOError(stopped, PipelineReportedCanceled))
	require.Equal(t, PipelineContextCanceled, PipelineIOError(stopped, PipelineContextCanceled))
	peerFailure := NewQueryInterrupted(live)
	require.Same(t, peerFailure, PipelineIOError(stopped, peerFailure))
	require.Same(t, context.Cause(stopped), PipelineRPCIOError(stopped, peerFailure))

	query, cancelQuery := context.WithTimeoutCause(live, 0, CauseInternalExecutorExec)
	defer cancelQuery()
	attached := AttachCause(query, query.Err())
	require.Equal(t, context.DeadlineExceeded, PipelineIOError(query, attached))
	fk := NewErrFKNoReferencedRow2(live)
	require.ErrorIs(t, PipelineIOError(query, errors.Join(attached, fk)), fk)
	independentDiagnostic := errors.New(context.Cause(query).Error())
	sameShape := errors.Join(context.DeadlineExceeded, independentDiagnostic)
	require.Same(t, sameShape, PipelineIOError(query, sameShape))
	require.Same(t, fk, PipelineError(errors.Join(PipelineReportedCanceled, fk)))
}

type pipelineErrorChildren struct {
	children []error
}

func (*pipelineErrorChildren) Error() string { return "pipeline failure" }

func (e *pipelineErrorChildren) Unwrap() []error { return e.children }

func TestPipelineErrorKeepsFailuresWithoutChildren(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	StopPipeline(stop)
	stopped := context.Cause(ctx)
	emptyChildren := &pipelineErrorChildren{}
	nilChildren := &pipelineErrorChildren{children: []error{nil, nil}}
	typedFailure := NewErrFKNoReferencedRow2(ctx)

	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"empty children", emptyChildren, emptyChildren},
		{"nil children", nilChildren, nilChildren},
		{"failure beside stop", errors.Join(emptyChildren, stopped), emptyChildren},
		{"stop beside failure", errors.Join(stopped, emptyChildren), emptyChildren},
		{"specific error after generic failure", errors.Join(emptyChildren, typedFailure), typedFailure},
		{"generic failure after specific error", errors.Join(typedFailure, emptyChildren), typedFailure},
		{"interruption beside failure", errors.Join(context.Canceled, nilChildren), nilChildren},
		{"wrapped stop only", fmt.Errorf("downstream: %w", stopped), nil},
		{"multi-error containing only stop and nil", &pipelineErrorChildren{children: []error{nil, stopped}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := PipelineError(tc.err)
			if tc.want == nil {
				require.NoError(t, got)
			} else {
				require.Same(t, tc.want, got)
			}
		})
	}
}

type pipelinePublicTestError struct{ cause error }

func (*pipelinePublicTestError) Error() string              { return "local diagnostic" }
func (*pipelinePublicTestError) PublicErrorMessage() string { return "public metadata error" }
func (e *pipelinePublicTestError) Unwrap() error            { return e.cause }
func (e *pipelinePublicTestError) Is(target error) bool     { return target == e.cause }

func (e *pipelinePublicTestError) As(target any) bool {
	if mo, ok := target.(**Error); ok {
		*mo, ok = e.cause.(*Error)
		return ok
	}
	return false
}

func TestPipelinePublicErrorDoesNotUsePrivateControlSignals(t *testing.T) {
	live := context.Background()
	stopped, stop := context.WithCancelCause(live)
	StopPipeline(stop)
	canceled, cancel := context.WithCancel(live)
	cancel()
	timedOut, cancelTimeout := context.WithTimeout(live, 0)
	defer cancelTimeout()

	for _, tc := range []struct {
		name    string
		ctx     context.Context
		private error
	}{
		{"normal stop", stopped, context.Cause(stopped)},
		{"cancellation", canceled, context.Canceled},
		{"deadline", timedOut, context.DeadlineExceeded},
		{"RPC cancellation", canceled, NewQueryInterrupted(live)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := &pipelinePublicTestError{cause: tc.private}
			wrapped := fmt.Errorf("execution: %w", failure)
			require.ErrorIs(t, wrapped, tc.private, "local callers retain the cause")
			require.Nil(t, PipelineStopCause(wrapped))
			require.False(t, IsPipelineInterruption(wrapped))
			require.Same(t, wrapped, PipelineIOError(tc.ctx, wrapped))
			require.Same(t, wrapped, PipelineRPCIOError(tc.ctx, wrapped))
			require.EqualError(t, PipelineError(wrapped), failure.PublicErrorMessage())
		})
	}
}

func TestPipelinePublicErrorKeepsAggregatePriority(t *testing.T) {
	ctx := context.Background()
	fk := NewErrFKNoReferencedRow2(ctx)
	retry := NewTxnNeedRetry(ctx)
	failure := &pipelinePublicTestError{cause: retry}
	var localCause *Error
	require.ErrorAs(t, failure, &localCause)
	require.Same(t, retry, localCause)
	require.EqualError(t, PipelineError(failure), failure.PublicErrorMessage())
	generic := errors.New("another execution failure")

	for _, external := range []*Error{fk, retry} {
		require.Same(t, external, PipelineError(errors.Join(failure, external)))
		require.Same(t, external, PipelineError(errors.Join(external, failure)))
	}

	cause, diagnostic := PipelineErrorForTransfer(errors.Join(generic, failure))
	require.Same(t, generic, cause)
	require.Same(t, generic, diagnostic, "the fallback uses the selected safe diagnostic")
	cause, diagnostic = PipelineErrorForTransfer(errors.Join(failure, generic))
	require.EqualError(t, diagnostic, failure.PublicErrorMessage())
	require.Same(t, cause, diagnostic)
}

type pipelineCustomInterruption struct{ error }

func (e pipelineCustomInterruption) Unwrap() error { return e.error }
func (pipelineCustomInterruption) Is(target error) bool {
	return target == context.Canceled
}

func TestPipelineInterruptionKeepsOrdinaryCustomIs(t *testing.T) {
	failure := pipelineCustomInterruption{errors.New("execution interrupted")}
	require.ErrorIs(t, failure, context.Canceled)
	require.True(t, IsPipelineInterruption(failure))
}
