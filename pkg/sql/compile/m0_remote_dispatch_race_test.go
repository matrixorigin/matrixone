//go:build mo_m0

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

package compile

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type m0AtomicDispatchWorkspace struct {
	*Ws
	revision atomic.Uint64
}

func (w *m0AtomicDispatchWorkspace) Readonly() bool { return true }
func (w *m0AtomicDispatchWorkspace) CatalogVisibility() (uint64, bool) {
	return w.revision.Load(), true
}

func TestM0SingleRemoteDispatchConcurrentRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := &m0AtomicDispatchWorkspace{Ws: &Ws{}}
	_, op := newTestTxnClientAndOp(ctrl, workspace)
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
	c.addr = "ingress:6001"
	ctx, cancel := context.WithTimeout(c.proc.Ctx, 15*time.Second)
	defer cancel()
	c.proc.Ctx = ctx
	stream := mock_morpc.NewMockStream(ctrl)
	stream.EXPECT().Receive().Return(nil, nil)
	stream.EXPECT().Close(true).Return(nil)
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).Times(0)
	opened, release := make(chan struct{}), make(chan struct{})
	finished, result := make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	client := &testPipelineClient{genStream: func(ctx context.Context, _ string) (morpc.Stream, error) {
		close(opened)
		select {
		case <-release:
			return stream, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	previous, existed := rt.GetGlobalVariables(runtime.PipelineClient)
	rt.SetGlobalVariables(runtime.PipelineClient, client)
	defer func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		select {
		case <-finished:
		case <-time.After(15 * time.Second):
			t.Error("dispatch goroutine did not exit")
		}
		if existed {
			rt.SetGlobalVariables(runtime.PipelineClient, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.PipelineClient, client)
		}
	}()
	opRoot := connector.NewArgument()
	opRoot.Reg = &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
	def := &plan.TableDef{TblId: 42, Name: "customer"}
	scope := &Scope{
		Magic: Remote, Proc: c.proc, RootOp: opRoot, ScopeAnalyzer: &ScopeAnalyzer{isStoped: true},
		NodeInfo: engine.Node{Addr: "remote:6001", CNCNT: 1, Mcpu: 1},
		DataSource: &Source{SchemaName: "db", RelationName: "customer", TableDef: def,
			node: &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def}},
	}
	admitM0RemoteDispatchFixture(t, c, scope)
	go func() {
		defer close(finished)
		sender, err := scope.remoteRun(c)
		if sender != nil {
			sender.close(context.Background())
		}
		result <- err
	}()
	select {
	case <-opened:
	case <-ctx.Done():
		t.Fatal("dispatch did not open a stream before the deadline")
	}
	workspace.revision.Add(1)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
	case <-ctx.Done():
		t.Fatal("dispatch did not reject the changed revision before the deadline")
	}
}
