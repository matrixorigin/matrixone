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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The context observer is armed only after the empty probe returns EOF.
// Its next Done access observes the dependency receive, not a timing sleep.
type productDependencyWaitContext struct {
	context.Context
	armed   bool
	once    sync.Once
	waiting chan struct{}
}

func (ctx *productDependencyWaitContext) Done() <-chan struct{} {
	if ctx.armed {
		ctx.once.Do(func() { close(ctx.waiting) })
	}
	return ctx.Context.Done()
}

type productBuildReader struct {
	*colexec.MockOperator
	started, release chan struct{}
	first            bool
	terminal         error
}

func (r *productBuildReader) Call(proc *process.Process) (vm.CallResult, error) {
	if !r.first {
		r.first = true
		close(r.started)
		select {
		case <-r.release:
		case <-proc.Ctx.Done():
		}
		if err := proc.Ctx.Err(); err != nil {
			return vm.CancelResult, err
		}
	}
	if r.terminal != nil {
		return vm.CancelResult, r.terminal
	}
	return r.MockOperator.Call(proc)
}

func TestBroadcastProductEmptyOwnerWaitsForSharedBuild(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		emptyPeer, emptyBuild, cancel, stop bool
		terminal                            error
	}{
		{name: "nonempty_peer"},
		{name: "all_probes_empty", emptyPeer: true},
		{name: "empty_build", emptyBuild: true},
		{name: "build_failure", terminal: moerr.NewInternalErrorNoCtx("build reader failed")},
		{name: "query_cancel", cancel: true},
		{name: "ancestor_stop", stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testBroadcastProductEmptyOwner(t, tc.emptyPeer, tc.emptyBuild, tc.cancel, tc.stop, tc.terminal)
		})
	}
}
func testBroadcastProductEmptyOwner(t *testing.T, emptyPeer, emptyBuild, cancelQuery, stopAncestor bool, terminal error) {
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
	wait := &productDependencyWaitContext{waiting: make(chan struct{})}
	emptyProbe := colexec.NewMockOperator().WithEndOfDataCallback(func() { wait.armed = true })
	peer := colexec.NewMockOperator()
	if !emptyPeer {
		peer.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
	}
	probes := []*Scope{makeScope(emptyProbe), makeScope(peer)}
	node := &planpb.Node{JoinType: planpb.Node_INNER, Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
		ProjectList: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}},
		SendMsgList: []planpb.MsgHeader{{MsgType: int32(message.MsgJoinMap), MsgTag: 42}}}
	input := &planpb.Node{ProjectList: node.ProjectList}
	probes = c.compileProbeSideForBroadcastJoin(node, input, input, probes)
	reader := &productBuildReader{MockOperator: colexec.NewMockOperator(), started: make(chan struct{}), release: make(chan struct{}), terminal: terminal}
	if !emptyBuild {
		reader.WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1, 2, 3)})
	}
	source := makeScope(reader)
	source.Magic = Normal
	c.compileBuildSideForBroadcastJoin(node, probes, []*Scope{source})
	require.Len(t, probes[0].PreScopes, 2)
	require.Empty(t, probes[1].PreScopes)
	buildScope := probes[0].PreScopes[1]
	require.Equal(t, int32(2), buildScope.RootOp.(*hashbuild.HashBuild).JoinMapRefCnt)
	var values []int8
	root := c.newMergeScope(probes)
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
	budget, err := c.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	owners, err := collectAllocationAccountOwners(c.scopes)
	require.NoError(t, err)
	configured, err := configureAllocationAccountOwners(owners, account)
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
	c.InitPipelineContextToExecuteQuery()
	wait.Context = probes[0].Proc.Ctx
	probes[0].Proc.Ctx = wait
	query, queryCancel := process.GetQueryCtxFromProc(c.proc)

	done = make(chan struct{})
	result := make(chan error, 1)
	go func() { defer close(done); result <- root.MergeRun(c) }()
	select {
	case <-wait.waiting:
		require.NoError(t, probes[0].Proc.Ctx.Err(), "empty owner retired before build dependency")
	case err := <-result:
		t.Fatalf("scheduler retired empty owner before build: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("empty Product did not wait for build dependency")
	}
	select {
	case <-reader.started:
	case <-time.After(10 * time.Second):
		t.Fatal("source did not start")
	}
	if cancelQuery {
		queryCancel()
	} else if stopAncestor {
		root.Proc.Cancel(process.ErrPipelineStopped)
	} else {
		close(reader.release)
	}
	select {
	case err := <-result:
		if cancelQuery {
			require.ErrorIs(t, err, context.Canceled)
		} else if terminal != nil {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "dependency error must survive: %v", err)
			require.Contains(t, err.Error(), terminal.Error())
		} else {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scheduler did not terminate after build release")
	}
	if cancelQuery {
		require.ErrorIs(t, query.Err(), context.Canceled)
	} else {
		require.NoError(t, query.Err())
	}
	if !emptyPeer && !emptyBuild && terminal == nil && !cancelQuery && !stopAncestor {
		require.ElementsMatch(t, []int8{1, 1, 1, 2, 2, 2, 3, 3, 3}, values, "peer must receive complete cross product")
	} else {
		require.Empty(t, values)
	}
}
