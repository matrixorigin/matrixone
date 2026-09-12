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
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// issue27261BlockingScan models a remote table reader that is still in flight
// when its downstream LIMIT stops consuming and sends StopSending.
type issue27261BlockingScan struct {
	*colexec.MockOperator
	started chan struct{}
	once    sync.Once
}

// issue27261BlockingRelation holds ParallelRun in reader construction. This is
// the production window in which StopSending can arrive before a scan's
// parallel scope exists and cleanup has to classify the cancellation itself.
type issue27261BlockingRelation struct {
	engine.Relation
	started chan struct{}
	once    sync.Once
}

func (r *issue27261BlockingRelation) BuildReaders(
	ctx context.Context,
	_ any,
	_ *plan.Expr,
	_ engine.RelData,
	_ int,
	_ int,
	_ bool,
	_ engine.TombstoneApplyPolicy,
	_ engine.FilterHint,
) ([]engine.Reader, error) {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	return nil, ctx.Err()
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

// A remote SAMPLE scan can still be constructing its parallel readers when a
// downstream LIMIT sends StopSending. ParallelRun owns cleanup until the
// parallel scope has been built, so it must normalize that internal
// cancellation before Connector.Reset chooses End versus Error. The other
// producer on the shared edge models a genuine fan-in sibling that has already
// ended normally.
func TestIssue27261StopSendingDuringParallelReaderBuildStaysGraceful(t *testing.T) {
	cleanupCounter := metricv2.PipelineCleanupEventCounter.WithLabelValues(
		parallelScopeBuildInternalCancel,
	)
	cleanupCountBefore := promtestutil.ToFloat64(cleanupCounter)

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

	const streamID = uint64(2)
	server.RecordBuiltPipeline(session, streamID, rootProc)
	t.Cleanup(func() { server.RemoveRelatedPipeline(session, streamID) })

	reg := process.NewPipelineEdge(2, 2)
	completedConnector := connector.NewArgument().WithReg(reg)
	completedConnector.AppendChild(colexec.NewMockOperator())
	completedScope := &Scope{
		Proc:   rootProc.NewContextChildProc(0),
		RootOp: completedConnector,
	}
	compile := &Compile{proc: rootProc}
	require.NoError(t, completedScope.Run(compile))

	relation := &issue27261BlockingRelation{started: make(chan struct{})}
	scan := table_scan.NewArgument()
	blockedConnector := connector.NewArgument().WithReg(reg)
	blockedConnector.AppendChild(scan)
	blockedScope := &Scope{
		Proc:     rootProc.NewContextChildProc(0),
		RootOp:   blockedConnector,
		NodeInfo: engine.Node{Mcpu: 2},
		DataSource: &Source{
			Rel:        relation,
			FilterList: []*plan.Expr{plan2.MakeFalseExpr()},
		},
	}

	result := make(chan error, 1)
	go func() { result <- blockedScope.ParallelRun(compile) }()

	select {
	case <-relation.started:
	case <-time.After(time.Second):
		t.Fatal("parallel reader construction did not start")
	}

	server.CancelPipelineSending(session, streamID)

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("parallel reader construction did not stop")
	}

	receiver := process.InitPipelineSignalReceiver(
		context.Background(),
		[]*process.WaitRegister{reg},
	)
	batch, err := receiver.GetNextBatch(nil)
	require.NoError(t, err)
	require.Nil(t, batch)
	require.NoError(t, reg.Err(), "StopSending cancellation must publish End, not Error")
	require.NoError(t, rootProc.GetQueryContextError(),
		"StopSending must leave the remote query context active")
	require.Equal(t, cleanupCountBefore+1, promtestutil.ToFloat64(cleanupCounter),
		"the cleanup decision must leave a durable internal-cancellation signal")
}

func TestParallelReaderBuildPreservesQueryCancellation(t *testing.T) {
	cleanupCounter := metricv2.PipelineCleanupEventCounter.WithLabelValues(
		parallelScopeBuildQueryCancel,
	)
	cleanupCountBefore := promtestutil.ToFloat64(cleanupCounter)

	rootProc := testutil.NewProcess(t)
	queryCtx := rootProc.Base.GetContextBase().BuildQueryCtx(rootProc.GetTopContext())
	_, cancelQuery := process.GetQueryCtxFromProc(rootProc)
	rootProc.BuildPipelineContext(queryCtx)
	t.Cleanup(func() { rootProc.Cancel(nil) })

	reg := process.NewPipelineEdge(1, 1)
	relation := &issue27261BlockingRelation{started: make(chan struct{})}
	scan := table_scan.NewArgument()
	conn := connector.NewArgument().WithReg(reg)
	conn.AppendChild(scan)
	scope := &Scope{
		Proc:     rootProc.NewContextChildProc(0),
		RootOp:   conn,
		NodeInfo: engine.Node{Mcpu: 2},
		DataSource: &Source{
			Rel:        relation,
			FilterList: []*plan.Expr{plan2.MakeFalseExpr()},
		},
	}

	result := make(chan error, 1)
	go func() { result <- scope.ParallelRun(&Compile{proc: rootProc}) }()

	select {
	case <-relation.started:
	case <-time.After(time.Second):
		t.Fatal("parallel reader construction did not start")
	}
	cancelQuery()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("parallel reader construction did not stop after query cancellation")
	}

	receiver := process.InitPipelineSignalReceiver(
		context.Background(),
		[]*process.WaitRegister{reg},
	)
	batch, err := receiver.GetNextBatch(nil)
	require.Nil(t, batch)
	require.ErrorIs(t, err, context.Canceled,
		"query cancellation must remain a terminal pipeline error")
	require.Equal(t, cleanupCountBefore+1, promtestutil.ToFloat64(cleanupCounter),
		"the cleanup decision must leave a durable query-cancellation signal")
}
