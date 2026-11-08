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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestPipelineContextError(t *testing.T) {
	require.NoError(t, PipelineContextError(nil))
	require.NoError(t, PipelineContextError(context.Background()))
	for _, cause := range []error{ErrPipelineStopped, errors.New("producer failed")} {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		require.ErrorIs(t, PipelineContextError(ctx), cause)
	}
	diagnostic := errors.New("deadline diagnostic")
	ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), diagnostic)
	defer cancel()
	require.ErrorIs(t, PipelineContextError(ctx), context.DeadlineExceeded)
	require.NotErrorIs(t, PipelineContextError(ctx), diagnostic)
	normalized, _ := NormalizePipelineRunError(context.DeadlineExceeded, ctx, context.Background())
	require.ErrorIs(t, normalized, context.DeadlineExceeded, "a pipeline deadline keeps its class even if the query context is still live")
	require.NotErrorIs(t, normalized, diagnostic)
}

func TestPipelineFailureProvenance(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, moerr.NewQueryInterrupted(context.Background())} {
		t.Run(err.Error(), func(t *testing.T) {
			failure := MarkPipelineFailure(err)
			require.True(t, IsPipelineFailure(failure))
			require.False(t, IsPipelineCancellationError(failure))
			require.Same(t, failure, MarkPipelineFailure(failure))
			require.Equal(t, err, UnwrapPipelineFailure(failure))
			require.True(t, IsPipelineFailure(fmt.Errorf("dependency: %w", failure)))
			joined := errors.Join(failure, errors.New("other failure"))
			require.True(t, IsPipelineFailure(joined))
			require.ErrorIs(t, UnwrapPipelineFailure(joined), err)
			for _, signal := range []PipelineSignal{NewErrorSignal(err), NewAbortSignal(err)} {
				_, terminalErr := signal.Action()
				require.True(t, IsPipelineFailure(terminalErr))
			}
		})
	}
	retry := moerr.NewTxnNeedRetry(context.Background())
	require.Same(t, retry, MarkPipelineFailure(retry), "retry dispatch uses the original concrete moerr type")
	require.True(t, moerr.IsMoErrCode(MarkPipelineFailure(retry), moerr.ErrTxnNeedRetry))
	require.False(t, IsPipelineCancellationError(moerr.NewQueryInterrupted(context.Background())))
}

func TestPipelineReceiverStopCannotEraseFailure(t *testing.T) {
	for _, cause := range []error{nil, ErrPipelineStopped} {
		for _, terminal := range []error{context.Canceled, moerr.NewQueryInterrupted(context.Background())} {
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			edge := NewPipelineEdge(1, 1)
			edge.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)
			require.False(t, edge.SendError(terminal))
			receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{edge})
			err := receiver.contextDoneError()
			require.True(t, IsPipelineFailure(err))
			require.ErrorIs(t, err, terminal)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(nil)
	receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{NewPipelineEdge(1, 1)})
	require.ErrorIs(t, receiver.contextDoneError(), context.Canceled, "unspecified cancellation is not successful stop")
}
