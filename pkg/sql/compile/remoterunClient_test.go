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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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

	client.close()
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
	e, _, _ := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
	c := NewCompile("test", "test", sql, "", "", e, proc, nil, false, nil, time.Now())
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
