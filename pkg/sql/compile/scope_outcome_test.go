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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScopeRunResultFrozenBeforeCleanup(t *testing.T) {
	query := context.Background()
	ctx, cancel := context.WithCancelCause(query)
	original := moerr.NewQueryInterrupted(query)
	result := newScopeRunResultForContext(original, ctx, query)
	// Cleanup cancels after execution returned; publication must not reread it.
	cancel(process.ErrPipelineStopped)
	got, _ := result.resolveCancelCause()
	require.Same(t, original, process.UnwrapPipelineFailure(got.err))
	require.True(t, process.IsPipelineFailure(got.err))

	activeCtx, activeCancel := context.WithCancelCause(query)
	defer activeCancel(nil)
	independent := newScopeRunResultForContext(context.Canceled, activeCtx, query)
	activeCancel(process.ErrPipelineStopped)
	got, _ = independent.resolveCancelCause()
	require.ErrorIs(t, got.err, context.Canceled)
	require.True(t, process.IsPipelineFailure(got.err))

	stoppedCtx, stoppedCancel := context.WithCancelCause(query)
	stoppedCancel(process.ErrPipelineStopped)
	stopped := newScopeRunResultForContext(stoppedCtx.Err(), stoppedCtx, query)
	require.NoError(t, stopped.err)
	require.NoError(t, newScopeRunResultForContext(context.Cause(stoppedCtx), stoppedCtx, query).err)
	require.Error(t, newScopeRunResultForContext(process.MarkPipelineFailure(process.ErrPipelineStopped), stoppedCtx, query).err)
	failed := newScopeRunResultForContext(process.MarkPipelineFailure(context.Canceled), stoppedCtx, query)
	require.ErrorIs(t, failed.err, context.Canceled)
	for _, cause := range []error{process.MarkPipelineFailure(process.ErrPipelineStopped), errors.Join(process.ErrPipelineStopped, original)} {
		failedCtx, failedCancel := context.WithCancelCause(query)
		failedCancel(cause)
		got := newScopeRunResultForContext(context.Canceled, failedCtx, query)
		require.Error(t, got.err, "stop leaf cannot hide a declared or joined failure cause")
	}
	// A new execution context does not carry any prior generation outcome.
	nextCtx, nextCancel := context.WithCancelCause(query)
	defer nextCancel(nil)
	require.NoError(t, newScopeRunResultForContext(nil, nextCtx, query).err)
	require.NoError(t, nextCtx.Err())
}

func TestJoinMapFailureSnapshotSurvivesSuccessfulStop(t *testing.T) {
	for _, terminal := range []error{context.Canceled, errors.Join(context.Canceled, context.DeadlineExceeded)} {
		t.Run(terminal.Error(), func(t *testing.T) {
			snapshot := message.NewJoinMapBuildError(process.MarkPipelineFailure(terminal))
			declared := snapshot.AsError()
			require.True(t, process.IsPipelineFailure(declared))
			require.False(t, process.IsPipelineCancellationError(declared))
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(process.ErrPipelineStopped)
			result := newScopeRunResultForContext(declared, ctx, context.Background())
			require.Error(t, result.err, "immutable build failure must survive a first-wins stop cause")
			require.True(t, process.IsPipelineFailure(result.err))
			var me *moerr.Error
			require.ErrorAs(t, result.err, &me)
			code := uint16(moerr.ErrQueryInterrupted)
			if errors.Is(terminal, context.DeadlineExceeded) {
				code = moerr.ErrQueryTimeout
			}
			require.Equal(t, code, me.ErrorCode())
			copyErr := snapshot.AsMoErr()
			copyErr.SetDetail("mutated consumer snapshot")
			require.Empty(t, snapshot.AsMoErr().Detail())
			publicErr := process.UnwrapPipelineFailure(result.err)
			require.ErrorIs(t, publicErr, context.Canceled)
			require.Equal(t, errors.Is(terminal, context.DeadlineExceeded), errors.Is(publicErr, context.DeadlineExceeded))
		})
	}
}
