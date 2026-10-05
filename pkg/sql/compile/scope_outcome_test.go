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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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
	got := result
	require.Same(t, original, process.UnwrapPipelineFailure(got.err))
	require.True(t, process.IsPipelineFailure(got.err))

	activeCtx, activeCancel := context.WithCancelCause(query)
	defer activeCancel(nil)
	independent := newScopeRunResultForContext(context.Canceled, activeCtx, query)
	activeCancel(process.ErrPipelineStopped)
	got = independent
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
	t.Run("message board receive retains declared failure", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(process.ErrPipelineStopped)
		board := message.NewMessageBoard()
		require.True(t, message.FinalizeJoinMapBuildError(board, 91, false, 0, process.MarkPipelineFailure(context.Canceled)))
		_, err := message.ReceiveJoinMap(91, false, 0, board, ctx)
		result := newScopeRunResultForContext(err, ctx, context.Background())
		require.ErrorIs(t, result.err, context.Canceled)
		require.True(t, process.IsPipelineFailure(result.err))
	})
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

func TestCollectMergeRunResultsPreservesRetryPolicy(t *testing.T) {
	for _, isolation := range []txn.TxnIsolation{txn.TxnIsolation_RC, txn.TxnIsolation_SI} {
		for _, retry := range []error{moerr.NewTxnNeedRetry(context.Background()), moerr.NewTxnNeedRetryWithDefChanged(context.Background())} {
			for _, retryFirst := range []bool{false, true} {
				for _, notify := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%v/first=%t/notify=%t", isolation, retry, retryFirst, notify), func(t *testing.T) {
						c := NewMockCompile(t)
						_, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(gomock.NewController(t), isolation)
						query := c.proc.Base.GetContextBase().BuildQueryCtx(c.proc.GetTopContext())
						c.proc.BuildPipelineContext(query)
						edge := process.NewPipelineEdge(1, 1)
						edge.PublishTerminal(process.NewErrorSignal(context.Canceled))
						receiver := process.InitPipelineSignalReceiverFromProcess(c.proc, []*process.WaitRegister{edge})
						_, err := receiver.GetNextBatch(nil)
						require.ErrorIs(t, err, context.Canceled)
						current := newScopeRunResultForProcess(err, c.proc)
						// The producer reports retry after the receiver froze its own error.
						c.proc.Cancel(retry)
						candidate := newScopeRunResultForProcess(retry, c.proc)
						if retryFirst {
							current, candidate = candidate, current
						}
						preScopes := make(chan scopeRunResult, 1)
						notifiers := make(chan notifyMessageResult, 1)
						if notify {
							notifiers <- notifyMessageResult{err: candidate.err}
						} else {
							preScopes <- candidate
						}
						got := c.collectMergeRunResults(c.proc, current, preScopes, notifiers)
						if isolation == txn.TxnIsolation_RC || retryFirst {
							require.Same(t, retry, got, "retry keeps its direct moerr identity")
						} else {
							require.Same(t, current.err, got, "SI retains its first failure")
						}
						require.Empty(t, preScopes)
						require.Empty(t, notifiers)
						require.NoError(t, query.Err())
					})
				}
			}
		}
	}
}
