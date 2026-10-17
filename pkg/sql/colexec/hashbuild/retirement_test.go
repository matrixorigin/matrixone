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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	for _, tc := range []struct {
		name        string
		failure     error
		queryCancel bool
		success     bool
	}{
		{"normal stop", context.Canceled, false, true},
		{"query cancel", context.Canceled, true, false},
		{"independent marked cancellation", process.MarkPipelineFailure(context.Canceled), false, false},
		{"independent deadline", context.DeadlineExceeded, false, false},
		{"mixed deadline", errors.Join(context.Canceled, context.DeadlineExceeded), false, false},
		{"source error", errors.New("source failed"), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := newTestCase(t, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
			query, cancelQuery := context.WithCancel(context.Background())
			defer cancelQuery()
			test.proc.ReplaceTopCtx(query)
			producer, cancelProducer := context.WithCancelCause(test.proc.Base.GetContextBase().BuildQueryCtx(query))
			defer cancelProducer(nil)
			test.proc.Ctx = producer
			test.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 99}
			input := newBatch(test.types, test.proc, Rows)
			child := &retiringBuildInput{initial: input, MockOperator: colexec.NewMockOperator(), failure: tc.failure, retire: func() {
				cancelProducer(process.ErrPipelineStopped)
				if tc.queryCancel {
					cancelQuery()
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
				require.Contains(t, terminal.Err().Error(), tc.failure.Error())
			}
			require.NotEqual(t, SendSucceed, test.arg.ctr.state, "no ownership transfer on retirement")
			receiver := message.NewMessageReceiver([]int32{99}, message.AddrBroadCastOnCurrentCN(), test.proc.GetMessageBoard())
			filters, _, receiveErr := receiver.ReceiveMessage(false, consumer)
			require.NoError(t, receiveErr)
			require.Len(t, filters, 1)
			require.Equal(t, int32(message.RuntimeFilter_PASS), filters[0].(message.RuntimeFilterMessage).Typ)
			test.arg.Reset(test.proc, !tc.success, err)
		})
	}
}
