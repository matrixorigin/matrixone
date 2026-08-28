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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// issue27261BlockingScan models a remote table reader that is still in flight
// when its downstream LIMIT stops consuming and sends StopSending.
type issue27261BlockingScan struct {
	*colexec.MockOperator
	started chan struct{}
	once    sync.Once
}

func (op *issue27261BlockingScan) Call(proc *process.Process) (vm.CallResult, error) {
	op.once.Do(func() { close(op.started) })
	<-proc.Ctx.Done()
	return vm.CancelResult, proc.Ctx.Err()
}

// This is a deterministic reduction of the production failure. A normal
// downstream early stop is delivered as StopSending while the remote reader is
// still in flight. It must stop that pipeline tree without canceling the query
// or leaking context.Canceled through Scope.Run.
func TestIssue27261StopSendingCancelsOnlyRemotePipeline(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	t.Cleanup(func() { runtime.SetupServiceBasedRuntime("", oldRuntime) })

	server := colexec.NewServer("")
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()

	rootProc := testutil.NewProcess(t)
	remoteCtx := context.WithValue(rootProc.GetTopContext(), defines.RemoteRunContext{}, true)
	queryCtx := rootProc.Base.GetContextBase().BuildQueryCtx(remoteCtx)
	rootProc.BuildPipelineContext(queryCtx)
	t.Cleanup(func() { rootProc.Cancel(nil) })
	readerProc := rootProc.NewContextChildProc(0)

	const streamID = uint64(27261)
	server.RecordBuiltPipeline(session, streamID, rootProc)
	t.Cleanup(func() { server.RemoveRelatedPipeline(session, streamID) })

	op := &issue27261BlockingScan{
		MockOperator: colexec.NewMockOperator(),
		started:      make(chan struct{}),
	}
	scope := &Scope{Proc: readerProc, RootOp: op}
	result := make(chan error, 1)
	go func() { result <- scope.Run(&Compile{proc: rootProc}) }()

	select {
	case <-op.started:
	case <-time.After(time.Second):
		t.Fatal("remote scan did not start")
	}

	// This is the server-side action for a normal StopSending request.
	server.CancelPipelineSending(session, streamID)

	select {
	case err := <-result:
		require.NoError(t, err)
		require.ErrorIs(t, rootProc.Ctx.Err(), context.Canceled,
			"StopSending should cancel the remote pipeline root")
		require.ErrorIs(t, readerProc.Ctx.Err(), context.Canceled,
			"the root cancellation should reach an in-flight reader pipeline")
		require.NoError(t, rootProc.GetTopContext().Err(),
			"the client/frontend context is still active")
		require.NoError(t, rootProc.GetQueryContextError(),
			"StopSending must not cancel the query context")
	case <-time.After(time.Second):
		t.Fatal("remote scan did not stop")
	}
}
