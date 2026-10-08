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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type productBuildReader struct {
	*colexec.MockOperator
	started, release, stopped  chan struct{}
	first                      bool
	panicAfterBatch, delivered bool
	terminal                   error
	initialBatch               *batch.Batch
	ackStopped                 <-chan struct{}
}

func (r *productBuildReader) Call(proc *process.Process) (vm.CallResult, error) {
	if r.initialBatch != nil {
		b := r.initialBatch
		r.initialBatch = nil
		return vm.CallResult{Status: vm.ExecNext, Batch: b}, nil
	}
	if !r.first {
		r.first = true
		close(r.started)
		select {
		case <-r.release:
		case <-proc.Ctx.Done():
		}
		if err := proc.Ctx.Err(); err != nil {
			close(r.stopped)
			if r.ackStopped != nil {
				<-r.ackStopped
			}
			return vm.CancelResult, err
		}
	}
	if r.panicAfterBatch {
		if r.delivered {
			panic(r.terminal)
		}
		r.delivered = true
		return r.MockOperator.Call(proc)
	}
	if r.terminal != nil {
		return vm.CancelResult, r.terminal
	}
	return r.MockOperator.Call(proc)
}

type broadcastProductCase struct {
	name                                                                           string
	emptyPeer, emptyBuild, cancel, stop, outer, reverse, panicAfterBatch, deadline bool
	hashJoin                                                                       bool
	loopJoin                                                                       bool
	allSkipped                                                                     bool
	reuse                                                                          bool
	remoteSource                                                                   bool
	terminal                                                                       error
}

func TestBroadcastProductSharedProducerOwnership(t *testing.T) {
	for _, tc := range []broadcastProductCase{
		{name: "nonempty_peer"},
		{name: "outer_join_skips_product", outer: true},
		{name: "outer_join_skips_last_product", outer: true, reverse: true},
		{name: "all_probes_empty", emptyPeer: true},
		{name: "empty_build", emptyBuild: true},
		{name: "build_failure", terminal: moerr.NewInternalErrorNoCtx("build reader failed")},
		{name: "query_cancel", cancel: true},
		{name: "query_deadline", deadline: true},
		{name: "partial_source_then_panic", panicAfterBatch: true, terminal: moerr.NewInternalErrorNoCtx("source panic after output")},
		{name: "ancestor_stop", stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testBroadcastProductOwner(t, tc)
		})
	}
}

// A colocated broadcast HashJoin is a supported compiler domain: independent
// probe scopes consume the same build/map, even when an outer join never calls
// one of them. Skipping that probe must not cancel the producer needed by its
// peer. This internal scope contract does not claim a particular SQL plan uses
// this placement.
func TestBroadcastHashJoinSharedProducerOwnership(t *testing.T) {
	testBroadcastNonProductOwners(t, false, false)
}

func TestBroadcastLoopJoinSharedProducerOwnership(t *testing.T) {
	testBroadcastNonProductOwners(t, true, false)
}

func TestBroadcastHashJoinRemoteSourceOwnership(t *testing.T) {
	testBroadcastNonProductOwners(t, false, true)
}

func TestBroadcastLoopJoinRemoteSourceOwnership(t *testing.T) {
	testBroadcastNonProductOwners(t, true, true)
}

func testBroadcastNonProductOwners(t *testing.T, loopJoin, remoteSource bool) {
	for _, tc := range []broadcastProductCase{
		{name: "first_consumer_skipped"},
		{name: "last_consumer_skipped", reverse: true},
		{name: "empty_peer", emptyPeer: true},
		{name: "all_consumers_skipped", allSkipped: true},
		{name: "empty_build", emptyBuild: true},
		{name: "build_failure", terminal: moerr.NewInternalErrorNoCtx("build reader failed")},
		{name: "query_cancel", cancel: true},
		{name: "query_deadline", deadline: true},
		{name: "partial_source_then_panic", panicAfterBatch: true, terminal: moerr.NewInternalErrorNoCtx("source panic after output")},
		{name: "ancestor_stop", stop: true},
		{name: "normal_stop_then_reuse", reuse: true},
		{name: "query_cancel_then_reuse", cancel: true, reuse: true},
	} {
		tc.hashJoin, tc.loopJoin, tc.outer = !loopJoin, loopJoin, true
		tc.remoteSource = remoteSource
		t.Run(tc.name, func(t *testing.T) { testBroadcastProductOwner(t, tc) })
	}
}

func testBroadcastProductOwner(t *testing.T, tc broadcastProductCase) {
	nonProduct := tc.hashJoin || tc.loopJoin
	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	// Retain the operator templates solely to exercise Scope.Reset and fresh
	// pipeline contexts. This is not a claim that AP prepared SQL is supported.
	c.isPrepare = tc.reuse
	c.counterSet = &perfcounter.CounterSet{}
	c.MessageBoard = message.NewMessageBoard()
	c.proc.SetMessageBoard(c.MessageBoard)
	// Register the remote process's automatic cleanup before the scope cleanup,
	// so workers and board/account users quiesce before its pool is released.
	var remoteProc *process.Process
	if tc.remoteSource {
		remoteProc = testutil.NewProcess(t)
		remoteProc.SetMessageBoard(c.MessageBoard)
	}
	c.addr = "cn1:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 1}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{qry: &planpb.Query{}}
	makeScope := func(op vm.Operator) *Scope {
		s := newScope(Remote)
		s.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
		if tc.remoteSource {
			s.NodeInfo.Addr = "cn2:6001"
		}
		s.Proc = c.proc.NewNoContextChildProc(0)
		s.RootOp = op
		return s
	}
	started, retired := make(chan struct{}), make(chan struct{})
	emptyProbe := colexec.NewMockOperator().WithEndOfDataCallback(func() { <-started; close(retired) })
	peer := colexec.NewMockOperator()
	if !tc.emptyPeer {
		peer.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
	}
	probes := []*Scope{makeScope(emptyProbe), makeScope(peer)}
	emptyScope := probes[0]
	if tc.reverse {
		probes[0], probes[1] = probes[1], probes[0]
	}
	node := &planpb.Node{JoinType: planpb.Node_INNER, Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
		ProjectList: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}},
		SendMsgList: []planpb.MsgHeader{{MsgType: int32(message.MsgJoinMap), MsgTag: 42}}}
	if nonProduct {
		col := func(rel int32) *planpb.Expr {
			return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: 0}}}
		}
		condition := "="
		if tc.loopJoin {
			condition = "<"
		}
		equal, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, condition, []*planpb.Expr{col(0), col(1)})
		require.NoError(t, err)
		node.OnList = []*planpb.Expr{equal}
	}
	input := &planpb.Node{ProjectList: node.ProjectList}
	probes = c.compileProbeSideForBroadcastJoin(node, input, input, probes)
	reader := &productBuildReader{MockOperator: colexec.NewMockOperator(), started: started, release: make(chan struct{}), stopped: make(chan struct{}), terminal: tc.terminal, panicAfterBatch: tc.panicAfterBatch}
	if !tc.emptyBuild {
		reader.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
	}
	source := makeScope(reader)
	source.Magic = Normal
	owners := c.compileBuildSideForBroadcastJoin(node, probes, []*Scope{source})
	c.scopes = owners
	var done chan struct{}
	var configured []executionAllocationAccountOwner
	var encodedBuild *Scope
	var registry *mpool.AllocationAccountRegistry
	var account *mpool.AllocationAccount
	t.Cleanup(func() {
		c.proc.Cancel(context.Canceled)
		if done != nil {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("scheduler cleanup did not terminate")
			}
		}
		for _, s := range c.scopes {
			s.FreeOperator(c)
		}
		if encodedBuild != nil {
			encodedBuild.FreeOperator(c)
			encodedBuild.release()
		}
		c.MessageBoard.Reset()
		for i := len(configured) - 1; i >= 0; i-- {
			require.NoError(t, configured[i].ClearAllocationAccount(account))
		}
		if account != nil {
			require.Zero(t, account.Snapshot().Used, "quiescent spool/account cleanup must release allocations")
			_, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
		}
		if remoteProc != nil {
			require.Zero(t, remoteProc.Mp().CurrNB(), "decoded producer pool must drain before process cleanup")
			require.Zero(t, c.proc.Mp().CurrNB(), "coordinator pool must drain before process cleanup")
		}
		for _, s := range c.scopes {
			s.release()
		}
	})
	owners = c.finishProductBuilds(owners, true)
	c.scopes = owners
	owner := owners[0]
	var buildScope *Scope
	if nonProduct && len(owners) == 2 {
		// Exercise the current first-probe owner rather than failing a structural
		// expectation before runtime. Its second child is the colocated build.
		buildScope = emptyScope.PreScopes[len(emptyScope.PreScopes)-1]
	} else {
		require.Len(t, owners, 1)
		require.Len(t, owner.PreScopes, 3)
		require.Empty(t, emptyScope.PreScopes)
		job := owner.PreScopes[2]
		buildScope = job.PreScopes[0]
	}
	if tc.remoteSource {
		// Round-trip the exact remote producer fragment: only its top connector
		// stays on the coordinator. The nested Source Dispatch and its local
		// receiver must survive together. This is codec/runtime evidence, not a
		// full MORPC cluster; the reader is a deterministic admission barrier.
		require.Equal(t, "cn2:6001", buildScope.NodeInfo.Addr)
		require.Equal(t, []*Scope{source}, buildScope.PreScopes)
		require.Nil(t, findPipelineExternalLocalReceiver(buildScope))
		data, err := func() ([]byte, error) {
			wireReader := value_scan.NewArgument()
			root := source.RootOp.GetOperatorBase()
			children := root.Children
			root.SetChildren([]vm.Operator{wireReader})
			defer func() {
				root.SetChildren(children)
				wireReader.Free(source.Proc, false, nil)
				wireReader.Release()
			}()
			wireScope, withoutOutput := getScopeForRemoteRunEncoding(buildScope)
			require.False(t, withoutOutput)
			require.Equal(t, vm.HashBuild, wireScope.RootOp.OpType())
			return encodeScope(wireScope)
		}()
		require.NoError(t, err)
		decoded, err := decodeScope(data, remoteProc, true, nil)
		detachedDecoded := decoded
		t.Cleanup(func() {
			if detachedDecoded != nil {
				detachedDecoded.FreeOperator(c)
				detachedDecoded.release()
			}
		})
		require.NoError(t, err)
		require.Len(t, decoded.PreScopes, 1)
		originalSource := source
		source = decoded.PreScopes[0]
		d := source.RootOp.(*dispatch.Dispatch)
		require.Len(t, d.LocalRegs, 1)
		require.Empty(t, d.RemoteRegs, "colocated payload must not travel through the coordinator")
		require.Same(t, decoded.Proc.Reg.MergeReceivers[0], d.LocalRegs[0])
		// Replace only the serialized ValueScan leaf, not the compiled data edge.
		d.GetChildren(0).Free(source.Proc, false, nil)
		d.GetChildren(0).Release()
		d.SetChildren([]vm.Operator{reader})
		conn := connector.NewArgument().WithReg(buildScope.RootOp.(*connector.Connector).Reg)
		conn.SetAnalyzeControl(c.anal.curNodeIdx, false)
		decoded.setRootOperator(conn)
		originalSource.RootOp.GetOperatorBase().SetChildren([]vm.Operator{value_scan.NewArgument()})
		owner.PreScopes[2].PreScopes[0] = decoded
		encodedBuild = buildScope // only now is the original tree detached
		detachedDecoded = nil     // ownership transferred to c.scopes
		buildScope = decoded
	}
	build, ok := buildScope.RootOp.(*hashbuild.HashBuild)
	if !ok {
		build = buildScope.RootOp.GetOperatorBase().GetChildren(0).(*hashbuild.HashBuild)
	}
	require.Equal(t, int32(2), build.JoinMapRefCnt)
	wrapSkippedProbe := func(scope *Scope, ended chan struct{}) {
		outer := hashjoin.NewArgument()
		outer.JoinType, outer.JoinMapTag = planpb.Node_INNER, 43
		col := func(rel int32) *planpb.Expr {
			return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: 0}}}
		}
		outer.EqConds = [][]*planpb.Expr{{col(0)}, {col(1)}}
		outer.LeftTypes, outer.RightTypes = []types.Type{types.T_int8.ToType()}, []types.Type{types.T_int8.ToType()}
		outer.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}}
		root := scope.RootOp
		if root.OpType() == vm.Connector {
			outer.AppendChild(root.GetOperatorBase().Children[0])
		} else {
			outer.AppendChild(root)
		}
		gate := &productOuterJoinGate{MockOperator: colexec.NewMockOperator(), started: started, ended: ended}
		gate.AppendChild(outer)
		if root.OpType() == vm.Connector {
			root.GetOperatorBase().Children[0] = gate
		} else {
			scope.RootOp = gate
		}
	}
	if tc.outer {
		wrapSkippedProbe(emptyScope, retired)
		if tc.allSkipped {
			for _, probe := range probes {
				if probe != emptyScope {
					wrapSkippedProbe(probe, make(chan struct{}))
				}
			}
		}
		require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(nil), 43, false, 0, c.MessageBoard))
	}
	var values []int8
	root := c.newMergeScope(owners)
	root = c.newMergeScope([]*Scope{root})
	c.markProductProducerRegions([]*Scope{root})
	root.setRootOperator(output.NewArgument().WithFunc(func(b *batch.Batch, _ *perfcounter.CounterSet) error {
		if b != nil {
			values = append(values, vector.MustFixedColNoTypeCheck[int8](b.Vecs[0])...)
		}
		return nil
	}))
	c.scopes = []*Scope{root}
	for _, s := range probes {
		s.Magic = Merge
	}
	buildScope.Magic = Merge
	owner.Magic = Merge
	budget, err := c.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err = mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	accountLimit := uint64(1 << 20)
	if nonProduct {
		accountLimit = 64 << 20
	}
	account, err = registry.OpenWithController(accountLimit, budget)
	require.NoError(t, err)
	accountOwners, err := collectAllocationAccountOwners(c.scopes)
	require.NoError(t, err)
	configured, err = configureAllocationAccountOwners(accountOwners, account)
	require.NoError(t, err)
	if tc.deadline {
		ctx, cancel := context.WithTimeout(c.proc.GetTopContext(), time.Second)
		t.Cleanup(cancel)
		c.proc.ReplaceTopCtx(ctx)
	}
	c.InitPipelineContextToExecuteQuery()
	query, queryCancel := process.GetQueryCtxFromProc(c.proc)

	done = make(chan struct{})
	result := make(chan error, 1)
	go func() { defer close(done); result <- root.MergeRun(c) }()
	select {
	case <-retired:
	case err := <-result:
		t.Fatalf("scheduler stopped before the empty consumer retired: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("empty consumer did not retire")
	}
	select {
	case <-emptyScope.Proc.Ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("consumer cleanup did not finish independently of shared build")
	}
	if nonProduct {
		require.ErrorIs(t, context.Cause(emptyScope.Proc.Ctx), process.ErrPipelineStopped,
			"the skipped consumer must finish normally, not fail from fixture configuration")
		require.NoError(t, query.Err(), "the parent query must still be live when the probe ends")
		if source.Proc.Ctx.Err() != nil {
			require.ErrorIs(t, context.Cause(source.Proc.Ctx), process.ErrPipelineStopped,
				"source cancellation must be inherited from normal probe cleanup")
		}
	}
	if tc.cancel {
		queryCancel()
	} else if tc.stop {
		root.Proc.Cancel(process.ErrPipelineStopped)
	} else if (!tc.emptyPeer || nonProduct) && !tc.allSkipped && !tc.deadline {
		require.NoError(t, source.Proc.Ctx.Err(), "one consumer must not retire shared producer")
		close(reader.release)
	}
	select {
	case err := <-result:
		if tc.deadline {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		} else if tc.cancel {
			require.ErrorIs(t, err, context.Canceled)
		} else if tc.terminal != nil {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "dependency error must survive: %v", err)
			require.Contains(t, err.Error(), tc.terminal.Error())
		} else {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scheduler did not terminate after build release")
	}
	if (tc.emptyPeer && !nonProduct) || tc.allSkipped || tc.cancel || tc.stop || tc.deadline {
		select {
		case <-reader.stopped:
		default:
			t.Fatal("blocked source was not interrupted")
		}
	}
	if tc.deadline {
		require.ErrorIs(t, query.Err(), context.DeadlineExceeded)
	} else if tc.cancel {
		require.ErrorIs(t, query.Err(), context.Canceled)
	} else {
		require.NoError(t, query.Err())
	}
	if !tc.emptyPeer && !tc.allSkipped && !tc.emptyBuild && tc.terminal == nil && !tc.cancel && !tc.stop && !tc.deadline {
		if tc.hashJoin {
			require.ElementsMatch(t, []int8{1, 2, 3}, values, "peer must receive every matching build row")
		} else if tc.loopJoin {
			require.ElementsMatch(t, []int8{1, 1, 2}, values, "peer must receive every non-equi match")
		} else {
			require.ElementsMatch(t, []int8{1, 1, 1, 2, 2, 2, 3, 3, 3}, values, "peer must receive complete cross product")
		}
	} else {
		require.Empty(t, values)
	}
	if tc.reuse {
		// First-generation cancellation and terminal edges must not leak into a
		// second execution of the same shared-owner tree.
		c.MessageBoard.Reset()
		for i := len(configured) - 1; i >= 0; i-- {
			require.NoError(t, configured[i].ClearAllocationAccount(account))
		}
		require.Zero(t, account.Snapshot().Used, "first-generation allocations must retire before rebinding")
		_, _, err = registry.CompleteTerminal(account)
		require.NoError(t, err)
		configured, account = nil, nil
		require.NoError(t, root.Reset(c))
		c.proc.ResetQueryContext()
		reader.Free(c.proc, false, nil)
		peer.Free(c.proc, false, nil)
		started, retired = make(chan struct{}), make(chan struct{})
		reader.started, reader.release, reader.stopped = started, make(chan struct{}), make(chan struct{})
		reader.first, reader.delivered, reader.terminal = false, false, nil
		reader.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
		peer.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
		emptyProbe.WithEndOfDataCallback(func() { <-started; close(retired) })
		require.NoError(t, vm.HandleAllOp(emptyScope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if gate, ok := op.(*productOuterJoinGate); ok {
				gate.started, gate.ended, gate.once = started, retired, sync.Once{}
			}
			return nil
		}))
		require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(nil), 43, false, 0, c.MessageBoard))
		values = nil
		root.RootOp.(*output.Output).Func = func(b *batch.Batch, _ *perfcounter.CounterSet) error {
			if b != nil {
				values = append(values, vector.MustFixedColNoTypeCheck[int8](b.Vecs[0])...)
			}
			return nil
		}
		account, err = registry.OpenWithController(accountLimit, budget)
		require.NoError(t, err)
		configured, err = configureAllocationAccountOwners(accountOwners, account)
		require.NoError(t, err)
		c.InitPipelineContextToExecuteQuery()
		secondQuery, _ := process.GetQueryCtxFromProc(c.proc)
		done, result = make(chan struct{}), make(chan error, 1)
		go func() { defer close(done); result <- root.MergeRun(c) }()
		select {
		case <-retired:
		case err := <-result:
			t.Fatalf("reused scheduler stopped before consumer retirement: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("reused consumer did not retire")
		}
		require.NoError(t, source.Proc.Ctx.Err(), "previous consumer cancellation must not reach the reused producer")
		close(reader.release)
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("reused scheduler did not terminate")
		}
		require.NoError(t, secondQuery.Err())
		if tc.hashJoin {
			require.ElementsMatch(t, []int8{1, 2, 3}, values)
		} else {
			require.ElementsMatch(t, []int8{1, 1, 2}, values)
		}
	}
}

// Gate a real outer HashJoin on source admission; the child join must never
// be called when that outer join receives an empty map.
type productOuterJoinGate struct {
	*colexec.MockOperator
	started, ended chan struct{}
	once           sync.Once
}

func (g *productOuterJoinGate) Call(proc *process.Process) (vm.CallResult, error) {
	select {
	case <-g.started:
	case <-proc.Ctx.Done():
		return vm.CancelResult, proc.Ctx.Err()
	}
	result, err := vm.ChildrenCall(g.GetChildren(0), proc, g.OpAnalyzer)
	g.once.Do(func() { close(g.ended) })
	return result, err
}

// This composes the real scope codec, Product cleanup, notify handler and
// MergeRun owner. The transport is a mocked session, not a full MORPC cluster.
func TestBroadcastProductRemoteRetirementCancelsBeforeJoin(t *testing.T) {
	c := NewMockCompile(t)
	c.counterSet = &perfcounter.CounterSet{}
	c.anal = &AnalyzeModule{qry: &planpb.Query{}}
	makeScope := func(op vm.Operator) *Scope {
		s := newScope(Normal)
		s.Proc = c.proc.NewNoContextChildProc(0)
		s.RootOp = op
		return s
	}
	ack, eof := make(chan struct{}), make(chan struct{})
	var ackOnce, eofOnce sync.Once
	unblock := func() { eofOnce.Do(func() { close(eof) }); ackOnce.Do(func() { close(ack) }) }
	t.Cleanup(unblock)
	firstBatch := newLazyUnionAllInt8Batch(c, 1)
	reader := &productBuildReader{MockOperator: colexec.NewMockOperator(), started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{}), ackStopped: ack,
		initialBatch: firstBatch}
	d := dispatch.NewArgument()
	d.FuncId = dispatch.SendToAllFunc
	uid := uuid.New()
	d.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	d.AppendChild(reader)
	source := makeScope(d)
	server := colexec.NewServer(c.proc.GetService())
	session := mock_morpc.NewMockClientSession(gomock.NewController(t))
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
	wire := make(chan morpc.Message, 1)
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg any) error {
		m := msg.(*pb.Message)
		if m.GetId() == 10 {
			wire <- m
		}
		return nil
	}).AnyTimes()
	receiver := &messageReceiverOnServer{messageCtx: context.Background(), connectionCtx: context.Background(), messageId: 9,
		messageTyp: pb.Method_PrepareDoneNotifyMessage, messageUuid: uid, clientSession: session, colexecServer: server}
	notifyDone := make(chan struct{})
	notifier := makeScope(&productRunGate{MockOperator: colexec.NewMockOperator(), run: func() error { defer close(notifyDone); return handlePipelineMessage(receiver) }})
	remoteProc := testutil.NewProcess(t)
	remoteProc.BuildPipelineContext(remoteProc.Base.GetContextBase().BuildQueryCtx(context.Background()))
	arg := product.NewArgument()
	arg.JoinMapTag = 42
	arg.AppendChild(value_scan.NewArgument())
	encodedScope := makeScope(arg)
	encodedScope.Magic = Merge
	data, err := encodeScope(encodedScope)
	require.NoError(t, err)
	decoded, err := decodeScope(data, remoteProc, true, nil)
	require.NoError(t, err)
	productDone := make(chan struct{})
	relay := makeScope(&productRunGate{MockOperator: colexec.NewMockOperator(), run: func() error {
		// The first dispatch batch already consumed the attachment.
		select {
		case <-reader.started:
		case <-source.Proc.Ctx.Done():
			return context.Cause(source.Proc.Ctx)
		}
		p := pipeline.NewMerge(decoded.RootOp)
		_, runErr := p.Run(remoteProc)
		p.Cleanup(remoteProc, runErr != nil, false, runErr)
		close(productDone)
		if runErr != nil {
			return runErr
		}
		<-eof
		terminal := &messageReceiverOnServer{messageCtx: context.Background(), messageId: 10, messageTyp: pb.Method_PipelineMessage,
			clientSession: session, messageAcquirer: func() morpc.Message { return &pb.Message{} }}
		if err := terminal.sendEndMessage(); err != nil {
			return err
		}
		sender := &messageSenderOnClient{ctx: context.Background(), mp: c.proc.Mp(), proc: c.proc, receiveCh: wire}
		return receiveMessageFromCnServerIfOnlyRun(source, sender)
	}})
	owner := c.newMergeScope([]*Scope{relay, notifier})
	owner.RootOp.(*merge.Merge).WithPartial(0, 1)
	owner.PreScopes = append(owner.PreScopes, source)
	owner.ConcurrentPreScopes = true
	c.scopes = []*Scope{owner}
	budget, err := c.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	accountOwners, err := collectAllocationAccountOwners([]*Scope{owner, decoded})
	require.NoError(t, err)
	configured, err := configureAllocationAccountOwners(accountOwners, account)
	require.NoError(t, err)
	c.InitPipelineContextToExecuteQuery()
	query, _ := process.GetQueryCtxFromProc(c.proc)
	registration, err := d.RegisterRemoteReceiversWithHandle(source.Proc)
	require.NoError(t, err)
	t.Cleanup(registration.Cleanup)
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		c.proc.Cancel(context.Canceled)
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("scheduler survived cleanup")
			}
		}
		registration.Cleanup()
		server.RemoveRelatedPipeline(session, 9)
		firstBatch.Clean(c.proc.Mp())
		owner.FreeOperator(c)
		decoded.FreeOperator(c)
		encodedScope.FreeOperator(c)
		for _, op := range configured {
			require.NoError(t, op.ClearAllocationAccount(account))
		}
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		owner.release()
		decoded.release()
		encodedScope.release()
		c.proc.Free()
	})
	go func() { done <- owner.MergeRun(c) }()
	select {
	case <-productDone:
	case <-time.After(10 * time.Second):
		t.Fatal("remote Product did not retire")
	}
	require.NoError(t, handlePipelineMessage(&messageReceiverOnServer{messageTyp: pb.Method_StopSending, messageId: 9, clientSession: session, colexecServer: server}))
	require.NoError(t, source.Proc.Ctx.Err(), "retiring one receiver must not cancel the producer")
	require.NoError(t, query.Err())
	select {
	case <-notifyDone:
		t.Fatal("attached notify completed before producer terminal")
	default:
	}
	eofOnce.Do(func() { close(eof) })
	select {
	case <-reader.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("result EOF did not cancel blocked source")
	}
	require.ErrorIs(t, context.Cause(source.Proc.Ctx), process.ErrPipelineStopped)
	require.NoError(t, query.Err())
	select {
	case err := <-done:
		joined = true
		t.Fatalf("scheduler joined before source returned: %v", err)
	default:
	}
	ackOnce.Do(func() { close(ack) })
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("owner did not join terminal cleanup")
	}
}

type productRunGate struct {
	*colexec.MockOperator
	run func() error
}

func (g *productRunGate) Call(*process.Process) (vm.CallResult, error) {
	return vm.CancelResult, g.run()
}

func TestBroadcastProductPreservesDownstreamPlacement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		local   vm.OpType
		compile func(*testing.T, *Compile, []*Scope) []*Scope
	}{
		{"independent_roots", vm.Product, func(t *testing.T, c *Compile, probes []*Scope) []*Scope { return probes }},
		{"group", vm.Group, func(t *testing.T, c *Compile, probes []*Scope) []*Scope {
			node, nodes := newShuffleGroupTestNodes(2)
			return c.compileGroupWithoutShuffle(node, probes, nodes, false)
		}},
		{"limit", vm.Limit, func(t *testing.T, c *Compile, probes []*Scope) []*Scope {
			return c.compileLimit(&planpb.Node{Limit: plan2.MakePlan2Uint64ConstExprWithType(1)}, probes)
		}},
		{"order", vm.Order, func(t *testing.T, c *Compile, probes []*Scope) []*Scope {
			node := newMergeTopFallbackTestNode(plan2.MakePlan2Uint64ConstExprWithType(1))
			return c.compileOrder(node, probes)
		}},
		{"ordered_top", vm.Top, func(t *testing.T, c *Compile, probes []*Scope) []*Scope {
			enableDistributedOrderedTopForTest(t, c.proc)
			node := newMergeTopFallbackTestNode(plan2.MakePlan2Uint64ConstExprWithType(1))
			node.NodeType = planpb.Node_SORT
			return c.compileTop(node, node.Limit, probes)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}})
			c.execType = plan2.ExecTypeAP_MULTICN
			probes := make([]*Scope, len(c.cnList))
			for i, cn := range c.cnList {
				probes[i] = generateScopeWithRootOperator(c.proc, []vm.OpType{vm.Product})
				probes[i].NodeInfo = scopeNodeWithMcpu(cn, 1)
			}
			source := generateScopeWithRootOperator(c.proc, []vm.OpType{vm.TableScan})
			source.NodeInfo = scopeNodeWithMcpu(c.cnList[0], 1)
			node := &planpb.Node{Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}}}
			data := c.compileBuildSideForBroadcastJoin(node, probes, []*Scope{source})
			require.Equal(t, probes, data, "producer lifetime must not collapse the data plane")
			outputs := tc.compile(t, c, data)
			if tc.name == "independent_roots" {
				result := c.finishProductBuilds(outputs, false)
				require.Len(t, result, 3)
				for i, probe := range probes {
					require.Same(t, probe, result[i], "independent writer roots must retain placement and accounting")
					require.Empty(t, probe.PreScopes)
				}
				job := result[2]
				require.Contains(t, c.auxiliaryProductScopes, job)
				require.Contains(t, job.PreScopes, source)
				ReleaseScopes(result)
				c.proc.Free()
				return
			}
			require.Len(t, outputs, 1)
			resultReceivers := append([]*process.WaitRegister(nil), outputs[0].Proc.Reg.MergeReceivers...)
			for _, probe := range probes {
				require.True(t, scopesContainOperator([]*Scope{probe}, tc.local), "each probe must reduce/order before exchange")
			}
			result := c.finishProductBuilds(outputs, true)
			require.Same(t, outputs[0], result[0], "reuse the existing completed result owner")
			require.Equal(t, resultReceivers, result[0].Proc.Reg.MergeReceivers, "job terminals must not alter ordered or result receiver indexing")
			job := result[0].PreScopes[len(result[0].PreScopes)-1]
			require.Contains(t, job.PreScopes, source)
			require.Len(t, job.Proc.Reg.MergeReceivers, 2)
			ReleaseScopes(result)
			c.proc.Free()
		})
	}
}

// Preserve existing writer-root accounting while the ordinary root scheduler
// owns shared producers that cannot belong to either independent writer.
type productRootWriter struct {
	*productBuildReader
	affected uint64
}

func (w *productRootWriter) GetAffectedRows() uint64 { return w.affected }

func TestBroadcastProductIndependentRootsRetireProducer(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "last_primary", true: "primary_failure"}[failure], func(t *testing.T) {
			c := NewMockCompile(t)
			c.counterSet = &perfcounter.CounterSet{}
			c.addr = "cn1:6001"
			c.execType = plan2.ExecTypeAP_ONECN
			c.anal = &AnalyzeModule{}
			c.pn = &planpb.Plan{}
			c.affectRows = &atomic.Uint64{}
			makeReader := func() *productBuildReader {
				return &productBuildReader{MockOperator: colexec.NewMockOperator(), started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
			}
			makeScope := func(op vm.Operator) *Scope {
				scope := newScope(Normal)
				scope.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
				scope.Proc = c.proc.NewNoContextChildProc(0)
				scope.RootOp = op
				return scope
			}
			first := &productRootWriter{productBuildReader: makeReader(), affected: 5}
			second := &productRootWriter{productBuildReader: makeReader(), affected: 7}
			source := makeReader()
			ack := make(chan struct{})
			source.ackStopped = ack
			wantErr := moerr.NewInternalErrorNoCtx("independent writer failed")
			if failure {
				first.terminal = wantErr
			}
			roots := make([]*Scope, 0, 3)
			roots = append(roots, makeScope(first), makeScope(second))
			job := c.newMergeScope([]*Scope{makeScope(source)})
			job.ConcurrentPreScopes = true
			c.auxiliaryProductScopes = map[*Scope]bool{job: true}
			c.scopes = append(roots, job)
			c.InitPipelineContextToExecuteQuery()
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() { defer close(finished); done <- c.runOnce() }()
			acknowledged := false
			t.Cleanup(func() {
				c.proc.Cancel(context.Canceled)
				for _, scope := range c.scopes {
					scope.Proc.Cancel(context.Canceled)
				}
				if !acknowledged {
					close(ack)
				}
				select {
				case <-finished:
				case <-time.After(10 * time.Second):
					t.Error("root scheduler cleanup did not finish")
					return
				}
				ReleaseScopes(c.scopes)
				c.proc.Free()
			})
			for _, ready := range []<-chan struct{}{first.started, second.started, source.started} {
				select {
				case <-ready:
				case <-time.After(10 * time.Second):
					t.Fatal("root did not start")
				}
			}
			close(first.release)
			select {
			case <-roots[0].Proc.Ctx.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("first writer did not finish")
			}
			if !failure {
				// Give the completion collector an observation window while the second
				// writer remains blocked. Retiring on the first result must be observable.
				select {
				case <-source.stopped:
					t.Fatal("first writer retired the shared producer")
				case <-time.After(25 * time.Millisecond):
				}
				require.NoError(t, job.Proc.Ctx.Err())
				close(second.release)
			}
			select {
			case <-source.stopped:
			case <-time.After(10 * time.Second):
				t.Fatal("producer was not canceled")
			}
			select {
			case err := <-done:
				t.Fatalf("root scheduler joined before producer cleanup acknowledgement: %v", err)
			default:
			}
			close(ack)
			acknowledged = true
			select {
			case err := <-done:
				if failure {
					require.ErrorContains(t, err, wantErr.Error())
					require.Zero(t, c.getAffectedRows())
				} else {
					require.NoError(t, err)
					require.EqualValues(t, 12, c.getAffectedRows())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("root scheduler did not join")
			}
		})
	}
}
