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
	genStream func(string) morpc.Stream
}

func (tPCli *testPipelineClient) NewStream(backend string) (morpc.Stream, error) {
	return tPCli.genStream(backend), nil
}

func (tPCli *testPipelineClient) Raw() morpc.RPCClient {
	//TODO implement me
	panic("implement me")
}

func (tPCli *testPipelineClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func Test_newMessageSenderOnClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tPCli := &testPipelineClient{
		genStream: func(s string) morpc.Stream {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, moerr.NewInternalErrorNoCtx("return error")).AnyTimes()
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("send error")).AnyTimes()
			return stream
		},
	}

	runtime.ServiceRuntime("").SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		ctx,
		"",
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	assert.Error(t, err)
	assert.NotNil(t, client)

	client.safeToClose = false
	client.alreadyClose = false

	client.waitingTheStopResponse()
}

func TestRemoteRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	proc := testutil.NewProcess()
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))

	tPCli := &testPipelineClient{
		genStream: func(s string) morpc.Stream {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, nil).AnyTimes()
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("send error")).AnyTimes()
			return stream
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
