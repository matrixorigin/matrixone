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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
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
func testBroadcastProductOwner(t *testing.T, tc broadcastProductCase) {
	c := NewMockCompile(t)
	c.counterSet = &perfcounter.CounterSet{}
	c.MessageBoard = message.NewMessageBoard()
	c.proc.SetMessageBoard(c.MessageBoard)
	c.addr = "cn1:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 1}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{qry: &planpb.Query{}}
	makeScope := func(op vm.Operator) *Scope {
		s := newScope(Remote)
		s.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
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
	input := &planpb.Node{ProjectList: node.ProjectList}
	probes = c.compileProbeSideForBroadcastJoin(node, input, input, probes)
	reader := &productBuildReader{MockOperator: colexec.NewMockOperator(), started: started, release: make(chan struct{}), stopped: make(chan struct{}), terminal: tc.terminal, panicAfterBatch: tc.panicAfterBatch}
	if !tc.emptyBuild {
		reader.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
	}
	source := makeScope(reader)
	source.Magic = Normal
	owners := c.compileBuildSideForBroadcastJoin(node, probes, []*Scope{source})
	require.Len(t, owners, 1)
	owner := owners[0]
	require.Len(t, owner.PreScopes, 4)
	require.Empty(t, emptyScope.PreScopes)
	buildScope := owner.PreScopes[2]
	require.Equal(t, int32(2), buildScope.RootOp.(*hashbuild.HashBuild).JoinMapRefCnt)
	if tc.outer {
		outer := hashjoin.NewArgument()
		outer.JoinType, outer.JoinMapTag = planpb.Node_INNER, 43
		col := func(rel int32) *planpb.Expr {
			return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: 0}}}
		}
		outer.EqConds = [][]*planpb.Expr{{col(0)}, {col(1)}}
		outer.LeftTypes, outer.RightTypes = []types.Type{types.T_int8.ToType()}, []types.Type{types.T_int8.ToType()}
		outer.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}}
		conn := emptyScope.RootOp
		outer.AppendChild(conn.GetOperatorBase().Children[0])
		gate := &productOuterJoinGate{MockOperator: colexec.NewMockOperator(), started: started, ended: retired}
		gate.AppendChild(outer)
		conn.GetOperatorBase().Children[0] = gate
		require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(nil), 43, false, 0, c.MessageBoard))
	}
	var values []int8
	root := c.newMergeScope(owners)
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
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	accountOwners, err := collectAllocationAccountOwners(c.scopes)
	require.NoError(t, err)
	configured, err := configureAllocationAccountOwners(accountOwners, account)
	require.NoError(t, err)
	var done chan struct{}
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
		// As in Compile.clear, destroy the board-owned unclaimed map before
		// completing the statement's allocation account.
		c.MessageBoard.Reset()
		for i := len(configured) - 1; i >= 0; i-- {
			require.NoError(t, configured[i].ClearAllocationAccount(account))
		}
		require.Zero(t, account.Snapshot().Used, "quiescent spool/account cleanup must release allocations")
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		for _, s := range c.scopes {
			s.release()
		}
		c.proc.Free()
	})
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
	case <-time.After(10 * time.Second):
		t.Fatal("empty consumer did not retire")
	}
	select {
	case <-emptyScope.Proc.Ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("consumer cleanup did not finish independently of shared build")
	}
	if tc.cancel {
		queryCancel()
	} else if tc.stop {
		root.Proc.Cancel(process.ErrPipelineStopped)
	} else if !tc.emptyPeer && !tc.deadline {
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
	if tc.emptyPeer || tc.cancel || tc.stop || tc.deadline {
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
	if !tc.emptyPeer && !tc.emptyBuild && tc.terminal == nil && !tc.cancel && !tc.stop && !tc.deadline {
		require.ElementsMatch(t, []int8{1, 1, 1, 2, 2, 2, 3, 3, 3}, values, "peer must receive complete cross product")
	} else {
		require.Empty(t, values)
	}
}

// Gate a real outer HashJoin on source admission; the child Product must never
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
