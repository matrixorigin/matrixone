// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retiringBuildInput struct {
	*colexec.MockOperator
	retire  func()
	failure error
	initial *batch.Batch
}

func (r *retiringBuildInput) Call(*process.Process) (vm.CallResult, error) {
	if r.initial != nil {
		input := r.initial
		r.initial = nil
		return vm.CallResult{Status: vm.ExecNext, Batch: input}, nil
	}
	r.retire()
	return vm.CancelResult, r.failure
}

func TestHashBuildRetirementPublishesBeforeConsumerCancellation(t *testing.T) {
	fatal := moerr.NewInternalErrorNoCtx("independent build failure")
	fatal.SetDetail("fatal build diagnostics")
	cause := moerr.NewTxnNeedRetryNoCtx()
	cause.SetDetail("causal build diagnostics")
	for _, tc := range []struct {
		name          string
		failure       error
		queryCancel   bool
		success       bool
		stopAfterCall bool
		cause         error
		want          error
	}{
		{name: "normal stop", failure: context.Canceled, success: true},
		{name: "query cancel", failure: context.Canceled, queryCancel: true},
		{name: "independent marked cancellation", failure: process.MarkPipelineFailure(context.Canceled)},
		{name: "independent deadline", failure: context.DeadlineExceeded},
		{name: "mixed deadline", failure: errors.Join(context.Canceled, context.DeadlineExceeded)},
		{name: "source error", failure: errors.New("source failed")},
		{name: "raw cancellation before normal stop", failure: context.Canceled, stopAfterCall: true},
		{name: "producer failure cause", failure: context.Canceled, cause: cause, want: cause},
		{name: "query failure cause", failure: context.Canceled, queryCancel: true, cause: cause, want: cause},
		{name: "mixed fatal after normal stop", failure: errors.Join(context.Canceled, fatal), want: fatal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := newTestCase(t, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
			query, cancelQuery := context.WithCancelCause(context.Background())
			defer cancelQuery(nil)
			test.proc.ReplaceTopCtx(query)
			producer, cancelProducer := context.WithCancelCause(test.proc.Base.GetContextBase().BuildQueryCtx(query))
			defer cancelProducer(nil)
			test.proc.Ctx = producer
			test.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 99}
			input := newBatch(test.types, test.proc, Rows)
			child := &retiringBuildInput{initial: input, MockOperator: colexec.NewMockOperator(), failure: tc.failure, retire: func() {
				if tc.stopAfterCall {
					return
				}
				if tc.queryCancel {
					cancelProducer(process.ErrPipelineStopped)
					cancelQuery(tc.cause)
				} else if tc.cause != nil {
					cancelProducer(tc.cause)
				} else {
					cancelProducer(process.ErrPipelineStopped)
				}
			}}
			test.arg.SetChildren([]vm.Operator{child})
			t.Cleanup(func() {
				test.arg.Free(test.proc, false, nil)
				input.Clean(test.proc.Mp())
				test.proc.Free()
				require.Zero(t, test.proc.Mp().CurrNB())
			})
			require.NoError(t, test.arg.Prepare(test.proc))
			result, err := test.arg.Call(test.proc)
			if tc.cause != nil {
				require.Same(t, tc.want, err, "return the selected cause with its original type")
			}
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, vm.ExecStop, result.Status)
			} else {
				require.Error(t, err)
			}
			// The consumer is deliberately still live. This deterministic propagation
			// skew must not depend on which context child Go cancels first.
			consumer := context.Background()
			terminal, receiveErr := message.ReceiveJoinMapResult(test.arg.JoinMapTag, false, 0, test.proc.GetMessageBoard(), consumer)
			if tc.stopAfterCall {
				require.NoError(t, producer.Err())
				require.NoError(t, query.Err())
				require.NoError(t, receiveErr)
				require.True(t, terminal.IsBuildError())
				cancelProducer(process.ErrPipelineStopped)
				for _, frozen := range []error{err, terminal.Err()} {
					outcome, _ := process.NormalizePipelineRunError(frozen, producer, query)
					assert.ErrorIs(t, outcome, context.Canceled, "a later stop cannot erase the earlier failure")
					assert.True(t, process.IsPipelineFailure(outcome))
				}
			}
			if tc.want != nil {
				require.NoError(t, receiveErr)
				require.True(t, terminal.IsBuildError())
				var expected *moerr.Error
				require.ErrorAs(t, tc.want, &expected)
				snapshot := terminal.BuildError().AsMoErr()
				require.Equal(t, expected.ErrorCode(), snapshot.ErrorCode())
				require.Equal(t, expected.Detail(), snapshot.Detail())
			}
			if tc.success {
				require.NoError(t, receiveErr)
				require.True(t, terminal.IsSuccess())
				require.Nil(t, terminal.JoinMap())
			} else if receiveErr != nil {
				// Cancellation/deadline may be exposed through the receiver's
				// error result rather than its immutable build-error terminal.
				if errors.Is(tc.failure, context.DeadlineExceeded) {
					require.ErrorIs(t, receiveErr, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, receiveErr, context.Canceled)
				}
			} else {
				require.True(t, terminal.IsBuildError())
				want := tc.failure
				if tc.want != nil {
					want = tc.want
				}
				require.Contains(t, terminal.Err().Error(), want.Error())
			}
			require.NotEqual(t, SendSucceed, test.arg.ctr.state, "no ownership transfer on retirement")
			receiver := message.NewMessageReceiver([]int32{99}, message.AddrBroadCastOnCurrentCN(), test.proc.GetMessageBoard())
			filters, _, receiveErr := receiver.ReceiveMessage(false, consumer)
			require.NoError(t, receiveErr)
			require.Len(t, filters, 1)
			require.Equal(t, int32(message.RuntimeFilter_PASS), filters[0].(message.RuntimeFilterMessage).Typ)
			test.arg.Reset(test.proc, !tc.success, err)
			test.arg.Reset(test.proc, !tc.success, err)
			filters, _, receiveErr = receiver.ReceiveMessage(false, consumer)
			require.NoError(t, receiveErr)
			require.Empty(t, filters, "Reset must not publish a duplicate runtime filter")
			terminals, _, receiveErr := message.NewMessageReceiver([]int32{test.arg.JoinMapTag}, message.AddrBroadCastOnCurrentCN(), test.proc.GetMessageBoard()).ReceiveMessage(false, consumer)
			require.NoError(t, receiveErr)
			require.Len(t, terminals, 1, "Call and Reset share one terminal gate")
			if tc.stopAfterCall {
				// Reset owns the partial build. Reopen only after its terminal and
				// resource cleanup, with a fresh context and MessageBoard generation.
				test.proc.GetMessageBoard().Reset()
				test.proc.Ctx = test.proc.Base.GetContextBase().BuildQueryCtx(query)
				test.arg.SetChildren([]vm.Operator{colexec.NewMockOperator().WithBatchs(nil)})
				require.NoError(t, test.arg.Prepare(test.proc))
				result, err = test.arg.Call(test.proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecStop, result.Status)
				terminal, receiveErr = message.ReceiveJoinMapResult(test.arg.JoinMapTag, false, 0, test.proc.GetMessageBoard(), consumer)
				require.NoError(t, receiveErr)
				require.True(t, terminal.IsSuccess())
				require.Nil(t, terminal.JoinMap())
				test.arg.Reset(test.proc, false, nil)
			}
		})
	}
}
