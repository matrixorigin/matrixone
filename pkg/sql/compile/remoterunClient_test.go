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
	"sync/atomic"
	"testing"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
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

func TestRemoteRunFailureReleasesPendingRetainedDispatchAttach(t *testing.T) {
	_ = colexec.NewServer(nil)

	oldRuntime := runtime.ServiceRuntime("")
	testRuntime := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", testRuntime)
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

	e, _, _ := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
	c := NewCompile("local-cn:6002", "test", "select 1", "", "", e, proc, nil, false, nil, time.Now())
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
	registeredProc, notifyCh, err := (&messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}).TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, proc, registeredProc)

	pendingDone := make(chan string, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		select {
		case notifyCh <- &process.WrapCs{Uid: uid, Err: make(chan error, 1)}:
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
	registeredProc, notifyCh, ok := colexec.Get().GetProcByUuid(uid, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}
