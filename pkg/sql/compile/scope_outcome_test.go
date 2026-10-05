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
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

// Exercise the production sentinel through the immutable dependency and its
// real broadcast readers. Control shape alone cannot certify a scope's success.
func TestJoinMapStopSnapshotRequiresSuccessfulScope(t *testing.T) {
	t.Run("consumer stops before query deadline", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		synctest.Test(t, func(t *testing.T) {
			query, queryCancel := context.WithTimeoutCause(context.Background(), time.Hour, errors.New("query deadline diagnostic"))
			defer queryCancel()
			query = proc.Base.GetContextBase().BuildQueryCtx(query)
			pipelineCtx := proc.BuildPipelineContext(query)
			proc.Cancel(process.ErrPipelineStopped)
			// Blocking on the actual parent cancellation advances virtual time
			// and joins deadline propagation before inspecting the child outcome.
			<-query.Done()
			require.ErrorIs(t, query.Err(), context.DeadlineExceeded)
			require.Same(t, process.ErrPipelineStopped, context.Cause(pipelineCtx))
			board := message.NewMessageBoard()
			require.True(t, message.FinalizeJoinMapBuildError(board, 93, false, 0, process.ErrPipelineStopped))
			_, err := message.ReceiveJoinMap(93, false, 0, board, context.Background())
			require.ErrorIs(t, newScopeRunResultForContext(err, pipelineCtx, query).err, context.DeadlineExceeded)
			scope := &Scope{RootOp: colexec.NewMockOperator(), Proc: proc}
			require.ErrorIs(t, scope.Run(&Compile{proc: proc}), context.DeadlineExceeded)
		})
	})
	stop := process.ErrPipelineStopped
	retry := moerr.NewTxnNeedRetry(context.Background())
	retry.SetDetail("retry diagnostic")
	cases := []struct {
		name     string
		source   error
		benign   bool
		deadline bool
		marked   bool
		code     uint16
	}{
		{name: "canceled control", source: context.Canceled, benign: true},
		{name: "stop", source: stop, benign: true},
		{name: "wrapped stop", source: fmt.Errorf("consumer: %w", stop), benign: true},
		{name: "joined stop canceled", source: errors.Join(stop, context.Canceled), benign: true},
		{name: "resnapshot stop", source: message.NewJoinMapBuildError(stop).AsError(), benign: true},
		{name: "declared stop", source: process.MarkPipelineFailure(stop), marked: true},
		{name: "wrapped declared stop", source: fmt.Errorf("producer: %w", process.MarkPipelineFailure(stop)), marked: true},
		{name: "joined declared stop", source: errors.Join(context.Canceled, process.MarkPipelineFailure(stop)), marked: true},
		{name: "resnapshot declared stop", source: message.NewJoinMapBuildError(process.MarkPipelineFailure(stop)).AsError(), marked: true},
		{name: "stop deadline", source: errors.Join(stop, context.DeadlineExceeded), deadline: true},
		{name: "deadline stop", source: errors.Join(context.DeadlineExceeded, stop), deadline: true},
		{name: "stop query timeout", source: errors.Join(stop, moerr.NewQueryTimeout(context.Background())), deadline: true},
		{name: "stop interrupted", source: errors.Join(stop, moerr.NewQueryInterrupted(context.Background())), marked: true},
		{name: "stop retry", source: errors.Join(stop, retry), code: retry.ErrorCode()},
		{name: "retry stop", source: errors.Join(retry, stop), code: retry.ErrorCode()},
		{name: "stop generic failure", source: errors.Join(stop, errors.New("storage failed")), code: moerr.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			board := message.NewMessageBoard()
			require.True(t, message.FinalizeJoinMapBuildError(board, 92, false, 0, tc.source))
			query := context.Background()
			pipelineCtx, cancel := context.WithCancelCause(query)
			cancel(stop)
			for consumer := 0; consumer < 2; consumer++ {
				jm, received := message.ReceiveJoinMap(92, false, 0, board, query)
				require.Nil(t, jm)
				require.Error(t, received)
				require.Equal(t, tc.marked, process.IsPipelineFailure(received))
				result := newScopeRunResultForContext(received, pipelineCtx, query)
				if tc.benign {
					require.NoError(t, result.err)
					active, activeCancel := context.WithCancelCause(query)
					defer activeCancel(nil)
					require.Error(t, newScopeRunResultForContext(received, active, query).err)
					require.Error(t, newScopeRunResultForContext(received, pipelineCtx, nil).err)
					canceledQuery, queryCancel := context.WithCancel(query)
					queryCancel()
					require.ErrorIs(t, newScopeRunResultForContext(received, pipelineCtx, canceledQuery).err, context.Canceled)
					timeoutQuery, timeoutCancel := context.WithDeadlineCause(query, time.Now().Add(-time.Second), errors.New("query deadline"))
					defer timeoutCancel()
					require.ErrorIs(t, newScopeRunResultForContext(received, pipelineCtx, timeoutQuery).err, context.DeadlineExceeded)
				} else {
					require.Error(t, result.err)
					if tc.deadline {
						require.ErrorIs(t, result.err, context.DeadlineExceeded)
					}
					if tc.code != 0 {
						require.True(t, moerr.IsMoErrCode(result.err, tc.code), result.err)
					}
				}
				var snapshot *message.JoinMapBuildError
				if errors.As(received, &snapshot) {
					// Every consumer receives independent mutable compatibility views.
					snapshot.AsMoErr().SetDetail("changed by first consumer")
					require.NotEqual(t, "changed by first consumer", snapshot.Detail())
					if tree, ok := snapshot.Unwrap().(interface{ Unwrap() []error }); ok {
						tree.Unwrap()[0] = errors.New("changed by first consumer")
						require.ErrorIs(t, snapshot.Unwrap(), context.DeadlineExceeded)
						require.ErrorIs(t, snapshot.Unwrap(), context.Canceled)
					}
				}
				if tc.code == retry.ErrorCode() {
					require.Equal(t, "retry diagnostic", received.(*moerr.Error).Detail())
				}
			}
		})
	}
}
