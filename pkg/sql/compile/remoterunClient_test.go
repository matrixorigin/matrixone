// Copyright 2021 Matrix Origin
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
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ cnclient.PipelineClient = new(testPipelineClient)

type testPipelineClient struct {
	genStream func(context.Context, string) (morpc.Stream, error)
}

func (tPCli *testPipelineClient) NewStream(ctx context.Context, backend string) (morpc.Stream, error) {
	return tPCli.genStream(ctx, backend)
}

func (tPCli *testPipelineClient) Raw() morpc.RPCClient {
	//TODO implement me
	panic("implement me")
}

func (tPCli *testPipelineClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func TestNewMessageSenderOnClientCleansUpStreamOnReceiveError(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, s string) (morpc.Stream, error) {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, moerr.NewInternalErrorNoCtx("return error")).AnyTimes()
			stream.EXPECT().Close(true).Return(nil)
			return stream, nil
		},
	}

	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestNewMessageSenderOnClientSetsDeadlineBeforeNewStream(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			_, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, "addr", backend)

			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(make(chan morpc.Message), nil)
			stream.EXPECT().Close(true).Return(nil)
			return stream, nil
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.True(t, client.useInternalTimeout)
	require.NotNil(t, client.ctxCancel)

	client.close(context.Background())
}

// TestNewMessageSenderOnClientPropagatesBackendCreateTimeout verifies the
// statement boundary used by RemoteRun. A stale fixed endpoint must surface
// the typed MORPC terminal error without canceling the caller's longer query
// context or constructing a sender that would require stream cleanup.
func TestNewMessageSenderOnClientPropagatesBackendCreateTimeout(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	var calls atomic.Int32
	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			calls.Add(1)
			require.Equal(t, "stale-cn:6002", backend)
			return nil, morpc.ErrBackendCreateTimeout
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	queryCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sender, err := newMessageSenderOnClient(
		queryCtx,
		sid,
		"stale-cn:6002",
		mpool.MustNewZero(),
		nil,
	)
	require.Nil(t, sender)
	require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, context.Cause(queryCtx),
		"RemoteRun stream creation canceled the owning statement context")
}

func TestNewMessageSenderOnClientReturnsErrorWithoutPipelineClient(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "pipeline client is not initialized")
}

func TestNewMessageSenderOnClientReturnsErrorWithoutServiceRuntime(t *testing.T) {
	client, err := newMessageSenderOnClient(
		context.Background(),
		t.Name(),
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "service runtime is not initialized")
}

func TestPipelineStreamReuseRuntimeGate(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	require.True(t, pipelineStreamReuseEnabled(sid))
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.EnablePipelineStreamReuse, false)
	require.False(t, pipelineStreamReuseEnabled(sid))
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.EnablePipelineStreamReuse, true)
	require.True(t, pipelineStreamReuseEnabled(sid))
}

func TestMessageSenderBatchCreditProtocol(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	stream.EXPECT().ID().Return(uint64(17))
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request morpc.Message) error {
			message := request.(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineBatchAck, message.GetCmd())
			require.Equal(t, uint64(9), message.GetBatchAckSequence())
			return nil
		})

	sender := &messageSenderOnClient{
		ctx:              context.Background(),
		streamSender:     stream,
		requestFinishAck: true,
		pendingBatchAck:  9,
	}
	request := &pipeline.Message{}
	sender.requestStreamProtocols(request)
	require.Equal(t, pipeline.StreamTeardownMode_FinishAck, request.GetRequestedTeardownMode())
	require.Equal(t, pipelineBatchCreditCount, request.GetRequestedBatchCreditCount())
	require.Equal(t, pipelineBatchCreditBytes, request.GetRequestedBatchCreditBytes())
	require.NoError(t, sender.acknowledgeRemoteBatch())
	require.Zero(t, sender.pendingBatchAck)
}

func TestNewMessageSenderOnClientReturnsErrorOnNilStream(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			return nil, nil
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "pipeline stream is not initialized")
}

func TestMessageSenderOnClientNegotiatedStreamTeardown(t *testing.T) {

	t.Run("accepted FIN ACK reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{
			Id:                   7,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(7))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request morpc.Message) error {
				message := request.(*pipeline.Message)
				require.Equal(t, pipeline.Method_PipelineStreamFinish, message.GetCmd())
				require.Equal(t, pipeline.Status_Last, message.GetSid())
				return nil
			})
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close(context.Background())
		sender.close(context.Background())
	})

	t.Run("legacy End closes backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:          context.Background(),
			streamSender: stream,
			safeToClose:  true,
		}
		sender.close(context.Background())
	})

	for _, tt := range []struct {
		name         string
		closeChannel bool
	}{
		{name: "closed receive channel releases stream ownership", closeChannel: true},
		{name: "nil receive message releases stream ownership"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			responses := make(chan morpc.Message, 1)
			if tt.closeChannel {
				close(responses)
			} else {
				responses <- nil
			}
			stream.EXPECT().Close(true).Return(nil).Times(1)
			sender := &messageSenderOnClient{
				ctx:           context.Background(),
				streamSender:  stream,
				receiveCh:     responses,
				reuseEligible: true,
			}

			message, err := sender.receiveMessage()
			require.Nil(t, message)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
			require.True(t, sender.receiveClosed)
			require.False(t, sender.reuseEligible)

			sender.close(context.Background())
			sender.close(context.Background())
		})
	}

	t.Run("peer close while waiting for FIN ACK poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message)
		close(responses)
		stream.EXPECT().ID().Return(uint64(13))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}

		sender.close(context.Background())
		require.True(t, sender.receiveClosed)
		require.False(t, sender.reuseEligible)
	})

	t.Run("malformed FIN ACK poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{Id: 9, Cmd: pipeline.Method_PipelineStreamFinishAck, Sid: pipeline.Status_MessageEnd}
		stream.EXPECT().ID().Return(uint64(9))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close(context.Background())
	})

	t.Run("query cancellation after End poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           ctx,
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close(context.Background())
	})

	t.Run("certified local cleanup cancellation still reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		ctx, cancel := context.WithCancelCause(context.Background())
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{
			Id:                   11,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(11))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           ctx,
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		cancel(process.ErrPipelineStopped)
		sender.close(context.Background())
	})

	t.Run("cancellation before cleanup completion poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		pipelineCtx, cancelPipeline := context.WithCancel(context.Background())
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           pipelineCtx,
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		cancelPipeline()
		sender.close(context.Background())
	})

	t.Run("FIN ACK timeout poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		stream.EXPECT().ID().Return(uint64(10))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		oldTimeout := pipelineStreamFinishClientTimeout
		pipelineStreamFinishClientTimeout = 10 * time.Millisecond
		t.Cleanup(func() { pipelineStreamFinishClientTimeout = oldTimeout })
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		cleanupCtx, cancel := newRemoteCleanupContext(nil)
		defer cancel()
		sender.close(cleanupCtx)
	})

	t.Run("clean StopSending End reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 2)
		responses <- &pipeline.Message{
			Id:                   12,
			Cmd:                  pipeline.Method_PipelineMessage,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		responses <- &pipeline.Message{
			Id:                   12,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(12)).Times(2)
		gomock.InOrder(
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					require.Equal(t, pipeline.Method_StopSending, request.(*pipeline.Message).GetCmd())
					return nil
				}),
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					require.Equal(t, pipeline.Method_PipelineStreamFinish, request.(*pipeline.Message).GetCmd())
					return nil
				}),
		)
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:          context.Background(),
			streamSender: stream,
			receiveCh:    responses,
			safeToClose:  false,
			expectedEnd:  pipeline.Method_PipelineMessage,
		}
		sender.close(context.Background())
	})
}

func TestRemoteRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	catalog.SetupDefines("")

	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, s string) (morpc.Stream, error) {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, nil).AnyTimes()
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("send error")).AnyTimes()
			return stream, nil
		},
	}

	runtime.ServiceRuntime("").SetGlobalVariables(runtime.PipelineClient, tPCli)

	fault.Enable()
	fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "echo", 0, "test_tbl", false)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	sql := "insert into test_tbl values (1,1)"
	c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, nil, false, nil, time.Now())
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	// if the root operator is connector.
	s1 := &Scope{
		Proc:          proc,
		RootOp:        connector.NewArgument(),
		ScopeAnalyzer: &ScopeAnalyzer{isStoped: true},
	}
	s1.RootOp.(*connector.Connector).Reg = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 1),
	}
	// ch, err1 := sender.streamSender.Receive()
	// require.Nil(t, err1)
	// sender.receiveCh = ch

	_, err := s1.remoteRun(c)
	assert.Error(t, err)
}

func TestRemoteRunNormalizesPipelineCancellationCause(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	testRuntime := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", testRuntime)
	t.Cleanup(func() {
		runtime.SetupServiceBasedRuntime("", oldRuntime)
	})
	catalog.SetupDefines("")

	duplicateErr := moerr.NewDuplicateEntryNoCtx("1", "primary")
	tests := []struct {
		name                       string
		cancelCause                error
		cancelQuery                bool
		deadlineQuery              bool
		remoteErr                  error
		receiveRemoteTerminal      bool
		terminalAnalysis           []byte
		wantErrorContains          string
		stopResponseErr            error
		stopSendErr                error
		closeStopResponse          bool
		timeoutStopResponse        bool
		assertTerminalBeforeCancel bool
		stopWaitsForLocalEnd       bool
		wantErr                    error
		wantErrCode                uint16
		wantStopSendingCount       int
	}{
		{
			name:                 "substantive cancellation cause survives",
			cancelCause:          duplicateErr,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending send failure",
			cancelCause:          duplicateErr,
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending response closure",
			cancelCause:          duplicateErr,
			closeStopResponse:    true,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending timeout",
			cancelCause:          duplicateErr,
			timeoutStopResponse:  true,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "remote terminal waits for local cleanup",
			stopWaitsForLocalEnd: true,
			wantStopSendingCount: 1,
		},
		{
			name:                 "late failure after local cleanup remains terminal",
			stopWaitsForLocalEnd: true,
			stopResponseErr:      moerr.NewQueryInterrupted(context.Background()),
			wantErrCode:          moerr.ErrQueryInterrupted,
			wantStopSendingCount: 1,
		},
		{
			name:                 "malformed terminal cannot complete parent successfully",
			stopWaitsForLocalEnd: true, terminalAnalysis: []byte("{"),
			wantErrorContains: "unexpected end of JSON input", wantStopSendingCount: 1,
		},
		{
			name:                  "malformed normal terminal fails parent",
			receiveRemoteTerminal: true, terminalAnalysis: []byte("{"),
			wantErrorContains: "unexpected end of JSON input",
		},
		{
			name:                  "normal terminal retry survives malformed analysis",
			receiveRemoteTerminal: true, terminalAnalysis: []byte("{"),
			remoteErr: moerr.NewTxnNeedRetry(context.Background()), wantErrCode: moerr.ErrTxnNeedRetry,
		},
		{
			name:                 "stop terminal retry survives malformed analysis",
			stopWaitsForLocalEnd: true, terminalAnalysis: []byte("{"),
			stopResponseErr: moerr.NewTxnNeedRetry(context.Background()),
			wantErrCode:     moerr.ErrTxnNeedRetry, wantStopSendingCount: 1,
		},
		{
			name:                 "normal internal cancellation remains secondary",
			wantStopSendingCount: 1,
		},
		{
			name:        "query cancellation skips optional StopSending",
			cancelQuery: true,
			wantErr:     context.Canceled,
		},
		{
			name:          "query deadline skips optional StopSending",
			deadlineQuery: true,
			wantErr:       context.DeadlineExceeded,
		},
		{
			name:                       "remote failure reaches receiver before scope cancellation",
			remoteErr:                  duplicateErr,
			assertTerminalBeforeCancel: true,
			wantErr:                    duplicateErr,
			wantErrCode:                moerr.ErrDuplicateEntry,
		},
		{
			name:                 "remote failure returned after internal cancellation survives",
			stopResponseErr:      duplicateErr,
			wantErr:              duplicateErr,
			wantErrCode:          moerr.ErrDuplicateEntry,
			wantStopSendingCount: 1,
		},
		{
			name:                 "remote interrupted Error survives successful stop",
			stopResponseErr:      moerr.NewQueryInterrupted(context.Background()),
			wantErrCode:          moerr.ErrQueryInterrupted,
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending send failure is terminal",
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErrCode:          moerr.ErrBackendClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "canceled StopSending send is a closed stream",
			stopSendErr:          context.Canceled,
			wantErrCode:          moerr.ErrStreamClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending response channel closure is terminal",
			closeStopResponse:    true,
			wantErrCode:          moerr.ErrStreamClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending timeout is terminal and attempted once",
			timeoutStopResponse:  true,
			wantErrCode:          moerr.ErrRPCTimeout,
			wantStopSendingCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.timeoutStopResponse || tt.stopWaitsForLocalEnd {
				oldTimeout := pipelineStopSendingClientTimeout
				pipelineStopSendingClientTimeout = 10 * time.Millisecond
				if tt.stopWaitsForLocalEnd {
					pipelineStopSendingClientTimeout = time.Second
				}
				defer func() { pipelineStopSendingClientTimeout = oldTimeout }()
			}
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			queryParent := proc.GetTopContext()
			if tt.deadlineQuery {
				var cancelDeadline context.CancelFunc
				queryParent, cancelDeadline = context.WithDeadline(queryParent, time.Now().Add(-time.Second))
				t.Cleanup(cancelDeadline)
			}
			queryCtx := proc.Base.GetContextBase().BuildQueryCtx(queryParent)
			_, cancelQuery := process.GetQueryCtxFromProc(proc)
			t.Cleanup(cancelQuery)
			proc.BuildPipelineContext(queryCtx)
			txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp

			reg := process.NewPipelineEdge(1, 0)
			responses := make(chan morpc.Message, 1)
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(responses, nil)
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stopSendingCount := 0
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					message := request.(*pipeline.Message)
					switch message.GetCmd() {
					case pipeline.Method_PipelineMessage:
						if tt.remoteErr != nil || tt.receiveRemoteTerminal {
							response := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
							response.SetMessageType(pipeline.Method_PipelineMessage)
							response.Analyse = tt.terminalAnalysis
							response.SetMoError(context.Background(), tt.remoteErr)
							responses <- response
						} else if tt.cancelQuery {
							cancelQuery()
						} else {
							cause := tt.cancelCause
							if cause == nil {
								cause = process.ErrPipelineStopped
							}
							proc.Cancel(cause)
						}
					case pipeline.Method_StopSending:
						stopSendingCount++
						if tt.stopSendErr != nil {
							return tt.stopSendErr
						}
						if tt.closeStopResponse {
							close(responses)
							return nil
						}
						if tt.timeoutStopResponse {
							return nil
						}
						response := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
						response.SetMessageType(pipeline.Method_PipelineMessage)
						response.Analyse = tt.terminalAnalysis
						if tt.stopResponseErr != nil {
							response.SetMoError(context.Background(), tt.stopResponseErr)
						}
						if tt.stopWaitsForLocalEnd {
							// The remote Merge cannot finish until this retained local
							// sender releases its input. Model that dependency explicitly.
							go func() {
								<-reg.Done()
								responses <- response
							}()
						} else {
							responses <- response
						}
					}
					return nil
				}).AnyTimes()
			stream.EXPECT().Close(true).Return(nil)
			testRuntime.SetGlobalVariables(runtime.PipelineClient, &testPipelineClient{
				genStream: func(context.Context, string) (morpc.Stream, error) {
					return stream, nil
				},
			})

			c := NewCompile(
				"local-cn:6002",
				"test",
				"insert into test_tbl values (1, 1)",
				"",
				"",
				newStubEngine(),
				proc,
				nil,
				false,
				nil,
				time.Now(),
			)
			c.anal = &AnalyzeModule{qry: &plan.Query{}}

			root := connector.NewArgument().WithReg(reg)
			defer root.Release()
			if tt.assertTerminalBeforeCancel {
				originalCancel := proc.Cancel
				proc.Cancel = func(cause error) {
					require.True(t, moerr.IsMoErrCode(reg.Err(), moerr.ErrDuplicateEntry),
						"remote root terminal must be published before its scope is canceled")
					originalCancel(cause)
				}
			}
			s := &Scope{
				Magic:         Remote,
				Proc:          proc,
				RootOp:        root,
				ScopeAnalyzer: &ScopeAnalyzer{},
				NodeInfo:      engine.Node{Addr: "remote-cn:6002", Mcpu: 1},
			}

			err := s.RemoteRun(c)
			if tt.wantErrorContains != "" {
				require.ErrorContains(t, err, tt.wantErrorContains)
			} else if tt.wantErrCode != 0 {
				require.True(t, moerr.IsMoErrCode(process.UnwrapPipelineFailure(err), tt.wantErrCode), err)
			} else if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Equal(t, tt.wantStopSendingCount, stopSendingCount)
			if tt.terminalAnalysis != nil {
				results := make(chan scopeRunResult, 1)
				results <- newScopeRunResult(err, s)
				parentErr := c.collectMergeRunResults(proc, scopeRunResult{}, results, nil, context.Background())
				require.Error(t, parentErr, "parent must not consume malformed completion as success")
				require.NoError(t, queryCtx.Err(), "fault is independent of user cancellation")
				if tt.wantErrCode == moerr.ErrTxnNeedRetry {
					require.True(t, c.canRetry(parentErr), "parent must preserve actual RC retry policy")
				} else {
					require.ErrorContains(t, parentErr, tt.wantErrorContains)
				}
			}

			select {
			case signal := <-reg.Ch2:
				_, terminalErr := signal.Action()
				if tt.remoteErr == nil && !tt.receiveRemoteTerminal && tt.cancelCause == nil && !tt.cancelQuery && !tt.deadlineQuery {
					// Late handshake failures are retained by RemoteRun, after the
					// local terminal has released consumers needed for remote cleanup.
					require.Equal(t, process.EventEnd, signal.EventType)
					require.NoError(t, terminalErr)
				} else {
					require.Equal(t, process.EventError, signal.EventType)
					if tt.wantErrorContains != "" {
						require.ErrorContains(t, terminalErr, tt.wantErrorContains)
					} else if tt.wantErrCode != 0 {
						require.True(t, moerr.IsMoErrCode(process.UnwrapPipelineFailure(terminalErr), tt.wantErrCode), terminalErr)
					} else {
						require.ErrorIs(t, terminalErr, tt.wantErr)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("remote cleanup did not terminate its receiver")
			}
		})
	}
}

func TestRemoteRunFailureReleasesPendingRetainedDispatchAttach(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	testRuntime := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", testRuntime)
	_ = colexec.NewServer("")
	t.Cleanup(func() {
		runtime.SetupServiceBasedRuntime("", oldRuntime)
	})

	runErr := moerr.NewInternalErrorNoCtx("injected new stream failure")
	var newStreamCalled atomic.Bool
	testRuntime.SetGlobalVariables(runtime.PipelineClient, &testPipelineClient{
		genStream: func(context.Context, string) (morpc.Stream, error) {
			newStreamCalled.Store(true)
			return nil, runErr
		},
	})

	ctrl := gomock.NewController(t)
	catalog.SetupDefines("")
	proc := testutil.NewProcess(t)
	accountCtx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(accountCtx)
	queryCtx := proc.Base.GetContextBase().BuildQueryCtx(accountCtx)
	proc.BuildPipelineContext(queryCtx)
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	c := NewCompile("local-cn:6002", "test", "select 1", "", "", newStubEngine(), proc, nil, false, nil, time.Now())
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	uid := uuid.Must(uuid.NewV7())
	child := value_scan.NewArgument()
	defer child.Release()
	root := dispatch.NewArgument()
	defer root.Release()
	root.FuncId = dispatch.SendToAllFunc
	root.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	root.AppendChild(child)
	s := &Scope{
		Magic:    Remote,
		Proc:     proc,
		RootOp:   root,
		NodeInfo: engine.Node{Addr: "remote-cn:6002", Mcpu: 1},
	}

	registrations, err := registerLocalDispatchReceivers([]*Scope{s}, c.addr)
	require.NoError(t, err)
	defer registrations.cleanup()
	registeredProc, notifyCh, _, err := (&messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}).getRemoteDispatchReceiver(uid, nil)
	require.NoError(t, err)
	require.Same(t, proc, registeredProc)

	pendingDone := make(chan string, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		select {
		case notifyCh <- &process.WrapCs{Uid: uid}:
			pendingDone <- "attached"
		case <-proc.Ctx.Done():
			pendingDone <- "canceled"
		}
	}()
	<-started
	select {
	case result := <-pendingDone:
		t.Fatalf("pending remote notify completed before RemoteRun failed: %s", result)
	default:
	}

	start := time.Now()
	err = s.RemoteRun(c)
	require.Less(t, time.Since(start), time.Second)
	require.True(t, newStreamCalled.Load(), "test must reach the injected NewStream failure")
	require.ErrorIs(t, err, runErr)
	require.ErrorIs(t, context.Cause(proc.Ctx), runErr)
	select {
	case result := <-pendingDone:
		require.Equal(t, "canceled", result)
	case <-time.After(time.Second):
		t.Fatal("RemoteRun failure did not release the pending retained-root attach")
	}
	registrations.cleanup()
	registeredProc, notifyCh, attachState, lookupWaiter, _ := colexec.GetServer("").AttachProcByUuidOrWait(uid)
	lookupWaiter.Close()
	require.Equal(t, colexec.RemoteReceiverMissing, attachState)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

// Exercise the two real terminal consumers with the same protocol faults.
func TestRemoteTerminalValidationAndReuse(t *testing.T) {
	for _, stop := range []bool{false, true} {
		for _, outcome := range []string{"valid", "bad analysis", "retry", "bad analysis plus retry", "bad error bytes", "closed transport"} {
			name := "receive/" + outcome
			if stop {
				name = "stop/" + outcome
			}
			t.Run(name, func(t *testing.T) {
				stream := mock_morpc.NewMockStream(gomock.NewController(t))
				responses := make(chan morpc.Message, 1)
				terminal := &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineMessage,
					Sid: pipeline.Status_MessageEnd, Analyse: []byte("{}"),
					AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
				malformed := outcome == "bad analysis" || outcome == "bad analysis plus retry" || outcome == "bad error bytes"
				if outcome == "bad analysis" || outcome == "bad analysis plus retry" {
					terminal.Analyse = []byte("{")
				}
				if outcome == "retry" || outcome == "bad analysis plus retry" {
					terminal.SetMoError(context.Background(), moerr.NewTxnNeedRetry(context.Background()))
				}
				if outcome == "bad error bytes" {
					terminal.Err = []byte{255}
				}
				if outcome == "closed transport" {
					close(responses)
				} else {
					responses <- terminal
				}
				stream.EXPECT().ID().Return(uint64(7)).AnyTimes()
				if stop {
					stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
				}
				sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream,
					receiveCh: responses, expectedEnd: pipeline.Method_PipelineMessage}
				var err error
				if stop {
					err = finalizeRemoteResult(nil, sender)
				} else {
					_, _, err = sender.receiveBatch()
				}
				if outcome == "valid" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				if outcome == "retry" || outcome == "bad analysis plus retry" {
					require.True(t, moerr.IsMoErrCode(process.UnwrapPipelineFailure(err), moerr.ErrTxnNeedRetry))
				} else if outcome == "bad analysis" {
					var syntax *json.SyntaxError
					require.ErrorAs(t, err, &syntax)
				}
				if malformed || outcome != "valid" {
					require.False(t, sender.reuseEligible)
					stream.EXPECT().Close(true).Return(nil).Times(1)
					sender.close(context.Background())
					sender.close(context.Background())
				} else {
					require.True(t, sender.reuseEligible, "valid negotiated completion retains reuse")
				}
			})
		}
	}
}

// Exercise the existing parent collector and sender cleanup with production
// budgets; virtual time controls absent ACKs without scheduler-sized deadlines.
func TestRemoteCleanupSharedBudget(t *testing.T) {
	for _, mode := range []string{"query canceled", "live missing FIN", "cancel during FIN", "Stop fallback", "Stop then FIN", "normal then deferred", "certified stop reuse"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			synctest.Test(t, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				queryCtx := proc.Base.GetContextBase().BuildQueryCtx(context.Background())
				_, cancelQuery := process.GetQueryCtxFromProc(proc)
				defer cancelQuery()
				proc.BuildPipelineContext(queryCtx)
				proc.Cancel(process.ErrPipelineStopped)
				cleanupCtx, cancel := newRemoteCleanupContext(proc)
				defer cancel()
				if mode == "query canceled" {
					cancelQuery()
				}
				original := moerr.NewInternalErrorNoCtx("producer failure")
				results := make(chan notifyMessageResult, 3)
				for i := 0; i < 3; i++ {
					stream := mock_morpc.NewMockStream(ctrl)
					responses := make(chan morpc.Message, 1)
					stream.EXPECT().ID().Return(uint64(i + 1)).AnyTimes()
					sends := 0
					if mode == "certified stop reuse" || (i == 0 && mode != "query canceled") {
						sends = 1
					}
					if i == 0 && mode == "Stop then FIN" {
						sends = 2
					}
					sendCall := stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, msg morpc.Message) error {
						m := msg.(*pipeline.Message)
						if m.GetCmd() != pipeline.Method_StopSending {
							require.Equal(t, cleanupCtx, ctx, "FIN receives the parent context")
						}
						if mode == "Stop fallback" || m.GetCmd() == pipeline.Method_StopSending {
							// Stop uses a child cap of the same absolute parent deadline.
							parentDeadline, _ := cleanupCtx.Deadline()
							deadline, _ := ctx.Deadline()
							require.Equal(t, parentDeadline, deadline)
							if mode == "Stop then FIN" {
								go func() {
									time.Sleep(29 * time.Second)
									responses <- &pipeline.Message{Id: m.GetID(), Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_MessageEnd, AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
								}()
							}
						}
						if mode == "cancel during FIN" {
							cancelQuery()
						}
						if mode == "certified stop reuse" {
							responses <- &pipeline.Message{Id: m.GetID(), Cmd: pipeline.Method_PipelineStreamFinishAck, Sid: pipeline.Status_MessageEnd, AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
						}
						return nil
					})
					if mode == "Stop fallback" && i > 0 {
						// Equal-deadline child and parent timer callbacks may run in
						// either order; a claimed Stop still inherits the expired budget.
						sendCall.MaxTimes(1)
					} else {
						sendCall.Times(sends)
					}
					stream.EXPECT().Close(mode != "certified stop reuse").Return(nil).Times(1)
					sender := &messageSenderOnClient{ctx: proc.Ctx, streamSender: stream, receiveCh: responses, safeToClose: true, reuseEligible: true, expectedEnd: pipeline.Method_PipelineMessage}
					result := notifyMessageResult{sender: sender}
					if mode == "Stop fallback" {
						sender.safeToClose = false
						sender.reuseEligible = false
						result.err = original
					}
					if mode == "Stop then FIN" {
						sender.safeToClose = false
					}
					results <- result
				}
				started := time.Now()
				if mode == "normal then deferred" {
					result := <-results
					result.clean(proc, cleanupCtx)
				}
				got := (&Compile{proc: proc}).collectMergeRunResults(proc, scopeRunResult{}, nil, results, cleanupCtx)
				if mode == "Stop fallback" {
					require.Same(t, original, got)
				} else {
					require.NoError(t, got)
				}
				expected := time.Duration(0)
				if mode == "live missing FIN" || mode == "Stop fallback" || mode == "Stop then FIN" || mode == "normal then deferred" {
					expected = 30 * time.Second
				}
				require.Equal(t, expected, time.Since(started))
				require.Empty(t, results)
				require.Zero(t, proc.Mp().CurrNB())
			})
		})
	}
}

func TestRemoteCleanupCancellationAuthority(t *testing.T) {
	for _, mode := range []string{"certified during FIN", "generic during FIN", "wrapped stop", "marked stop", "query canceled after certified stop", "query deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			queryCtx, cancelQuery := context.WithCancel(context.Background())
			defer cancelQuery()
			if mode == "query deadline" {
				var cancel context.CancelFunc
				queryCtx, cancel = context.WithDeadline(queryCtx, time.Now().Add(-time.Second))
				defer cancel()
			}
			senderCtx, cancelSender := context.WithCancelCause(queryCtx)
			defer cancelSender(nil)
			switch mode {
			case "wrapped stop":
				cancelSender(fmt.Errorf("wrapped: %w", process.ErrPipelineStopped))
			case "marked stop":
				cancelSender(process.MarkPipelineFailure(process.ErrPipelineStopped))
			case "query canceled after certified stop":
				cancelSender(process.ErrPipelineStopped)
			}
			stream := mock_morpc.NewMockStream(ctrl)
			responses := make(chan morpc.Message, 1)
			responses <- &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineStreamFinishAck, Sid: pipeline.Status_MessageEnd, AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
			sends := 0
			if mode == "certified during FIN" || mode == "generic during FIN" || mode == "query canceled after certified stop" {
				sends = 1
			}
			stream.EXPECT().ID().Return(uint64(7)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, morpc.Message) error {
				switch mode {
				case "certified during FIN":
					cancelSender(process.ErrPipelineStopped)
				case "generic during FIN":
					cancelSender(nil)
				case "query canceled after certified stop":
					cancelQuery()
				}
				return nil
			}).Times(sends)
			stream.EXPECT().Close(mode != "certified during FIN").Return(nil).Times(1)
			sender := &messageSenderOnClient{ctx: senderCtx, streamSender: stream, receiveCh: responses, safeToClose: true, reuseEligible: true}
			sender.close(queryCtx)
			sender.close(queryCtx)
		})
	}
}

func TestRemoteSettlementSurvivesExpiredCleanup(t *testing.T) {
	for _, terminalErr := range []error{moerr.NewInternalErrorNoCtx("late remote failure"), moerr.NewTxnNeedRetry(context.Background())} {
		t.Run(terminalErr.Error(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			responses := make(chan morpc.Message, 1)
			cleanupCtx, cancel := context.WithCancel(context.Background())
			cancel()
			stream.EXPECT().ID().Return(uint64(9)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request morpc.Message) error {
				require.NoError(t, ctx.Err(), "required settlement has independent authority")
				require.Equal(t, pipeline.Method_StopSending, request.(*pipeline.Message).GetCmd())
				response := &pipeline.Message{Id: 9, Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_MessageEnd, AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
				response.SetMoError(context.Background(), terminalErr)
				responses <- response
				return nil
			}).Times(1)
			stream.EXPECT().Close(true).Return(nil).Times(1)
			sender := &messageSenderOnClient{ctx: cleanupCtx, streamSender: stream, receiveCh: responses, expectedEnd: pipeline.Method_PipelineMessage}
			got := finalizeRemoteResult(nil, sender)
			require.Equal(t, terminalErr.Error(), process.UnwrapPipelineFailure(got).Error())
			sender.close(cleanupCtx)
			sender.close(cleanupCtx)
		})
	}
}
