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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

func TestReaderContextFollowsSourceScopeBeforeCompileFallback(t *testing.T) {
	source := testutil.NewProcess(t)
	defer source.Free()
	fallback := testutil.NewProcess(t)
	defer fallback.Free()

	type contextKey struct{}
	sourceCtx := context.WithValue(source.Ctx, contextKey{}, "source")
	fallbackCtx := context.WithValue(fallback.Ctx, contextKey{}, "fallback")
	source.Ctx = sourceCtx
	fallback.Ctx = fallbackCtx
	compile := &Compile{proc: fallback}

	readerScope := &Scope{Proc: source}
	require.Same(t, sourceCtx, readerScope.readerContext(compile))
	require.Equal(t, "source", readerScope.readerContext(compile).Value(contextKey{}))

	noSourceScope := &Scope{}
	require.Same(t, fallbackCtx, noSourceScope.readerContext(compile))
	require.Equal(t, "fallback", noSourceScope.readerContext(compile).Value(contextKey{}))
	require.Nil(t, noSourceScope.readerContext(nil))
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
	rootProc := newIssue27261Process(t)
	server := colexec.NewServer("")
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()

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
	result := runIssue27261Scope(t, rootProc, func() error { return scope.Run(&Compile{proc: rootProc}) })

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

	rootProc := newIssue27261Process(t)
	server := colexec.NewServer("")
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()

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

	result := runIssue27261Scope(t, rootProc, func() error { return blockedScope.ParallelRun(compile) })

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

	result := runIssue27261Scope(t, rootProc, func() error { return scope.ParallelRun(&Compile{proc: rootProc}) })

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

// Capture the context at the production relation boundary, not only the selector.
type issue32389ReaderRelation struct {
	engine.Relation
	read func(context.Context) error
}

func (r *issue32389ReaderRelation) BuildReaders(ctx context.Context, _ any, _ *plan.Expr, _ engine.RelData, _ int, _ int, _ bool, _ engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
	return nil, r.read(ctx)
}
func TestBuildReadersUsesSourceScopeContext(t *testing.T) {
	root := testutil.NewProcess(t)
	t.Cleanup(root.Free)
	source := root.NewContextChildProc(0)
	t.Cleanup(func() { source.Cancel(nil) })
	type key struct{}
	source.Ctx = context.WithValue(source.Ctx, key{}, "source")
	expected := errors.New("reader construction sentinel")
	called := false
	rel := &issue32389ReaderRelation{read: func(ctx context.Context) error {
		called = true
		require.Equal(t, "source", ctx.Value(key{}))
		require.Equal(t, source.Ctx.Done(), ctx.Done())
		source.Cancel(nil)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.NoError(t, root.Ctx.Err())
		return expected
	}}
	scope := &Scope{Proc: source, DataSource: &Source{Rel: rel, FilterList: []*plan.Expr{plan2.MakeFalseExpr()}}}
	_, err := scope.buildReaders(&Compile{proc: root})
	require.True(t, called)
	require.ErrorIs(t, err, expected)
}
func TestParallelReaderBuildPreservesPrimaryErrors(t *testing.T) {
	primary := errors.New("storage read failed")
	for _, tc := range []struct {
		name  string
		err   error
		cause error
	}{
		{"storage", primary, nil},
		{"joined", errors.Join(primary, context.Canceled), nil},
		{"cancel cause", context.Canceled, primary},
		{"independent deadline", context.DeadlineExceeded, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := testutil.NewProcess(t)
			t.Cleanup(root.Free)
			queryCtx := root.Base.GetContextBase().BuildQueryCtx(root.GetTopContext())
			root.BuildPipelineContext(queryCtx)
			t.Cleanup(func() { root.Cancel(nil) })
			source := root.NewContextChildProc(0)
			t.Cleanup(func() { source.Cancel(nil) })
			reg := process.NewPipelineEdge(1, 1)
			conn := connector.NewArgument().WithReg(reg)
			conn.AppendChild(table_scan.NewArgument())
			rel := &issue32389ReaderRelation{read: func(context.Context) error { source.Cancel(tc.cause); return tc.err }}
			scope := &Scope{Proc: source, RootOp: conn, NodeInfo: engine.Node{Mcpu: 2}, DataSource: &Source{Rel: rel, FilterList: []*plan.Expr{plan2.MakeFalseExpr()}}}
			err := scope.ParallelRun(&Compile{proc: root})
			want := tc.err
			if tc.cause != nil {
				want = tc.cause
			}
			require.ErrorIs(t, err, want)
			require.ErrorIs(t, reg.Err(), want)
			require.NoError(t, root.GetQueryContextError())
		})
	}
}

type issue32389ReaderEngine struct {
	engine.Engine
	read func(context.Context) error
}

func (e *issue32389ReaderEngine) BuildBlockReaders(ctx context.Context, _ any, _ timestamp.Timestamp, _ *plan.Expr, _ *plan.TableDef, _ engine.RelData, _ int, _ ...engine.FilterHint) ([]engine.Reader, error) {
	return nil, e.read(ctx)
}
func TestBuildBlockReadersUsesSourceScopeContext(t *testing.T) {
	root := testutil.NewProcess(t)
	t.Cleanup(root.Free)
	source := root.NewContextChildProc(0)
	t.Cleanup(func() { source.Cancel(nil) })
	type key struct{}
	source.Ctx = context.WithValue(source.Ctx, key{}, "remote-source")
	expected := errors.New("remote reader sentinel")
	called := false
	e := &issue32389ReaderEngine{read: func(ctx context.Context) error {
		called = true
		require.Equal(t, "remote-source", ctx.Value(key{}))
		require.Equal(t, source.Ctx.Done(), ctx.Done())
		return expected
	}}
	scope := &Scope{Proc: source, IsRemote: true, DataSource: &Source{TableDef: &plan.TableDef{}, FilterList: []*plan.Expr{plan2.MakeFalseExpr()}}}
	_, err := scope.buildReaders(&Compile{proc: root, e: e})
	require.True(t, called)
	require.ErrorIs(t, err, expected)
}

func TestParallelReaderBuildPreservesQueryDeadline(t *testing.T) {
	root := testutil.NewProcess(t)
	t.Cleanup(root.Free)
	expired, cancel := context.WithDeadline(root.GetTopContext(), time.Unix(1, 0))
	t.Cleanup(cancel)
	queryCtx := root.Base.GetContextBase().BuildQueryCtx(expired)
	root.BuildPipelineContext(queryCtx)
	t.Cleanup(func() { root.Cancel(nil) })
	source := root.NewContextChildProc(0)
	t.Cleanup(func() { source.Cancel(nil) })
	reg := process.NewPipelineEdge(1, 1)
	conn := connector.NewArgument().WithReg(reg)
	conn.AppendChild(table_scan.NewArgument())
	rel := &issue32389ReaderRelation{read: func(ctx context.Context) error { return ctx.Err() }}
	scope := &Scope{Proc: source, RootOp: conn, NodeInfo: engine.Node{Mcpu: 2}, DataSource: &Source{Rel: rel, FilterList: []*plan.Expr{plan2.MakeFalseExpr()}}}
	err := scope.ParallelRun(&Compile{proc: root})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, reg.Err(), context.DeadlineExceeded)
}

// The private runtime owns its increment service. Never close the service on
// the runtime that was installed by another test.
func newIssue27261Process(t *testing.T) *process.Process {
	t.Helper()
	oldRuntime := runtime.ServiceRuntime("")
	rt := runtime.DefaultRuntime()
	t.Cleanup(func() { runtime.SetupServiceBasedRuntime("", oldRuntime) })
	runtime.SetupServiceBasedRuntime("", rt)
	// NewProcess installs the service; capture that instance for cleanup.
	proc := testutil.NewProcess(t)
	service := incrservice.GetAutoIncrementService("")
	t.Cleanup(func() {
		service.Close()
		rt.CompareAndDeleteGlobalVariables(runtime.AutoIncrementService, service)
	})
	t.Cleanup(proc.Free)
	proc.BuildPipelineContext(proc.GetTopContext())
	t.Cleanup(func() { proc.Cancel(nil) })
	return proc
}

// Cleanup must join the worker even when an assertion returns before the test
// consumes its result. The separate done channel is not consumed by assertions.
func runIssue27261Scope(t *testing.T, proc *process.Process, run func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		proc.Cancel(nil)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("scope worker did not exit during cleanup")
		}
	})
	go func() {
		defer close(done)
		result <- run()
	}()
	return result
}

func TestIssue27261FixtureClosesOwnedServiceAndWorker(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	var ownedRuntime runtime.Runtime
	var ownedService incrservice.AutoIncrementService
	exited := make(chan struct{})
	if !t.Run("return before consuming worker result", func(t *testing.T) {
		proc := newIssue27261Process(t)
		ownedRuntime = runtime.ServiceRuntime("")
		ownedService = incrservice.GetAutoIncrementService("")
		require.NoError(t, ownedService.Reload(context.Background(), 1))
		started := make(chan struct{})
		runIssue27261Scope(t, proc, func() error {
			defer close(exited)
			close(started)
			<-proc.Ctx.Done()
			return proc.Ctx.Err()
		})
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("scope worker did not start")
		}
	}) {
		return
	}
	require.Same(t, oldRuntime, runtime.ServiceRuntime(""))
	select {
	case <-exited:
	default:
		t.Fatal("fixture returned before worker exited")
	}
	require.Error(t, ownedService.Reload(context.Background(), 1), "owned service must be closed")
	_, installed := ownedRuntime.GetGlobalVariables(runtime.AutoIncrementService)
	require.False(t, installed, "private runtime must release its service")
}
