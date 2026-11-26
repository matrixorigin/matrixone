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
