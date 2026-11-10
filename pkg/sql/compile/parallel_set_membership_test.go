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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSetMembershipIdentityKeys(t *testing.T) {
	visible := []*plan.Expr{
		plan2.GetColExpr(types.T_varchar.ToType().PlanType(), 0, 0),
		plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 1),
	}
	node := &plan.Node{ProjectList: visible, PhysicalEqualityKeyList: plan2.DeepCopyExprList(visible)}
	// A computed comparison key must not disqualify an independent identity
	// column, and must never itself be used to route raw text.
	node.PhysicalEqualityKeyList[0] = &plan.Expr{Typ: visible[0].Typ, Expr: &plan.Expr_F{F: &plan.Function{}}}
	require.Equal(t, []int32{1}, setMembershipIdentityKeys(node))
	node.PhysicalEqualityKeyList[1].Typ.Id = int32(types.T_uint64)
	require.Empty(t, setMembershipIdentityKeys(node))
	node.PhysicalEqualityKeyList = node.PhysicalEqualityKeyList[:1]
	require.Empty(t, setMembershipIdentityKeys(node))
}

func TestOwnedShuffleRuntimeAccountAttachmentRollback(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 32)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)
	router := shuffle.NewArgument()
	defer router.Release()
	router.OwnedOutput, router.OwnedColumnCount = true, 1
	router.SetShufflePool(shuffle.NewOwnedShufflePool(2, 2, 1))
	failing := &allocationLifecycleOwnerOperator{MockOperator: colexec.NewMockOperator(), failSet: true}
	_, err = configureAllocationAccountOwners([]executionAllocationAccountOwner{router, failing}, first)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountMismatch)
	require.NotErrorIs(t, err, mpool.ErrAllocationAccountLive, "an uninitialized descriptor owns no allocations")
	require.NoError(t, router.SetAllocationAccount(second), "rollback must clear the first account")
	require.NoError(t, router.ClearAllocationAccount(second))
	for _, account := range []*mpool.AllocationAccount{first, second} {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}
}

type ownedMembershipFailingPrepare struct {
	*colexec.MockOperator
	err error
}

func (op *ownedMembershipFailingPrepare) Prepare(*process.Process) error { return op.err }

// Map a manually triggered cancellation onto deadline semantics while retaining
// the underlying context's diagnostic cause.
type manuallyExpiredDeadline struct{ context.Context }

func (c manuallyExpiredDeadline) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

// Hold the first context error snapshot while a controlled deadline expires. This
// makes the original Err-then-Cause implementation observe both sides of the
// transition, while a single cause-first observation may linearize before it.
type deadlineCauseBarrierContext struct {
	context.Context
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *deadlineCauseBarrierContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.entered); <-c.resume })
	return err
}

func TestOwnedSetMembershipDeadlineDuringCauseLookup(t *testing.T) {
	for _, sender := range []bool{false, true} {
		name := "compile"
		if sender {
			name = "sender scope"
		}
		t.Run(name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			defer c.proc.Free()
			c.proc.BuildPipelineContext(c.proc.Ctx)
			diagnostic := errors.New("deadline diagnostic")
			causeCtx, expire := context.WithCancelCause(context.Background())
			defer expire(nil)
			ctx := manuallyExpiredDeadline{causeCtx}
			barrier := &deadlineCauseBarrierContext{Context: ctx, entered: make(chan struct{}), resume: make(chan struct{})}
			c.proc.Ctx = barrier
			run := func() error {
				_, err := c.tryCompileOwnedSetMembership(0, nil, nil, nil)
				return err
			}
			var reg *process.WaitRegister
			if sender {
				budget, err := c.proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				registry, err := mpool.NewAllocationAccountRegistry(1, 128)
				require.NoError(t, err)
				account, err := registry.OpenWithController(1<<20, budget)
				require.NoError(t, err)
				router := shuffle.NewArgument()
				defer router.Release()
				router.BucketNum, router.ShuffleType = 2, int32(plan.ShuffleType_Hash)
				router.ShuffleKeyColumns, router.DrainAllBuckets, router.OwnedOutput = []int32{0}, true, true
				router.OwnedColumnCount = 1
				router.SetShufflePool(shuffle.NewOwnedShufflePool(2, 1, 1))
				require.NoError(t, router.SetAllocationAccount(account))
				router.AppendChild(colexec.NewMockOperator())
				dispatcher := dispatch.NewArgument()
				defer dispatcher.Release()
				dispatcher.FuncId = dispatch.ShuffleToAllFunc
				reg = process.NewPipelineEdge(1, 1)
				dispatcher.LocalRegs = []*process.WaitRegister{reg}
				dispatcher.AppendChild(router)
				scope := &Scope{Proc: c.proc, RootOp: dispatcher}
				run = func() error { return scope.Run(c) }
				defer func() {
					require.NoError(t, router.ClearAllocationAccount(account))
					require.Zero(t, budget.Used())
					account.Seal()
					_, err := registry.Finalize(account)
					require.NoError(t, err)
				}()
			}
			done, finished := make(chan error, 1), make(chan struct{})
			var release sync.Once
			defer func() { release.Do(func() { close(barrier.resume) }); <-finished }()
			go func() { defer close(finished); done <- run() }()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cause lookup was not entered")
			}
			expire(diagnostic)
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			require.ErrorIs(t, context.Cause(ctx), diagnostic)
			release.Do(func() { close(barrier.resume) })
			var got error
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("execution did not finish")
			}
			require.NotErrorIs(t, got, diagnostic)
			if reg != nil {
				require.ErrorIs(t, got, context.DeadlineExceeded)
				require.ErrorIs(t, reg.Err(), context.DeadlineExceeded)
			} else if got == nil {
				// A cause-first read can finish logically before the deadline.
				// The next compilation observes the already expired context.
				_, err := c.tryCompileOwnedSetMembership(0, nil, nil, nil)
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, got, context.DeadlineExceeded)
			}
		})
	}
}

func TestOwnedShuffleFirstErrorReachesSenderAndScope(t *testing.T) {
	for _, tc := range []struct {
		name        string
		started     bool
		cancelQuery bool
	}{
		{"unstarted", false, false},
		{"running", true, false},
		{"query cancellation", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			defer c.proc.Free()
			c.proc.BuildPipelineContext(c.proc.Ctx)
			c.proc.Base.Lim.Size = 64 << 10
			budget, err := c.proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			router := shuffle.NewArgument()
			defer router.Release()
			router.BucketNum, router.ShuffleType = 2, int32(plan.ShuffleType_Hash)
			router.ShuffleKeyColumns, router.DrainAllBuckets, router.OwnedOutput = []int32{0}, true, true
			router.OwnedColumnCount = 1
			router.SetShufflePool(shuffle.NewOwnedShufflePool(2, 1, 1))
			require.NoError(t, router.SetAllocationAccount(account))
			first := router.Prepare(c.proc)
			require.ErrorIs(t, first, process.ErrExecutionResourceAdmission)
			later := errors.New("later producer failure")
			var want error = process.ErrExecutionResourceAdmission
			if tc.cancelQuery {
				queryCtx, cancel := context.WithCancel(context.Background())
				cancel()
				c.proc.ReplaceTopCtx(queryCtx)
				later, want = process.ErrPipelineStopped, context.Canceled
			}
			router.AppendChild(&ownedMembershipFailingPrepare{MockOperator: colexec.NewMockOperator(), err: later})
			sender := dispatch.NewArgument()
			defer sender.Release()
			reg := process.NewPipelineEdge(1, 1)
			sender.LocalRegs = []*process.WaitRegister{reg}
			sender.AppendChild(router)
			scope := &Scope{Proc: c.proc, RootOp: sender}
			var got error
			if tc.started {
				got = scope.Run(c)
			} else {
				got = cleanScopeTreeWithStartFail(scope, later, false)
			}
			require.ErrorIs(t, got, want)
			require.ErrorIs(t, reg.Err(), want)
			require.NoError(t, router.ClearAllocationAccount(account))
			require.Zero(t, budget.Used())
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
		})
	}
}

func TestOwnedSetMembershipTopology(t *testing.T) {
	for _, tc := range []struct {
		typ  plan.Node_NodeType
		want any
	}{
		{plan.Node_INTERSECT, &intersect.Intersect{}},
		{plan.Node_INTERSECT_ALL, &intersectall.IntersectAll{}},
		{plan.Node_MINUS, &minus.Minus{}},
		{plan.Node_MINUS_ALL, &minusall.MinusAll{}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			defer c.proc.Free()
			c.addr = "127.0.0.1:18000"
			local := engine.Node{Addr: c.addr, Mcpu: 4}
			left, right := newDistinctSetTestScopes(c, 2), newDistinctSetTestScopes(c, 1)
			for _, s := range append(left, right...) {
				s.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
			}
			node := &plan.Node{NodeType: tc.typ, ProjectList: []*plan.Expr{plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)}}
			result, err := c.compileOwnedSetMembership(node, left, right, []int32{0}, 4, false, local)
			require.NoError(t, err)
			defer func() { result[0].FreeOperator(c); result[0].release() }()
			require.Len(t, result[0].PreScopes, 4)
			for i, owner := range result[0].PreScopes {
				op := owner.RootOp.GetOperatorBase().GetChildren(0)
				require.IsType(t, tc.want, op)
				require.Len(t, owner.Proc.Reg.MergeReceivers, 3)
				for _, reg := range owner.Proc.Reg.MergeReceivers {
					require.Equal(t, 1, reg.NilBatchCnt)
				}
				leftMerge := op.GetOperatorBase().GetChildren(0).(*merge.Merge)
				rightMerge := op.GetOperatorBase().GetChildren(1).(*merge.Merge)
				require.Equal(t, []int32{0, 2, 2, 3}, []int32{leftMerge.StartIDX, leftMerge.EndIDX, rightMerge.StartIDX, rightMerge.EndIDX})
				if i == 0 {
					require.Len(t, owner.PreScopes, 3)
					for _, feeder := range owner.PreScopes {
						router := feeder.RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
						require.True(t, router.OwnedOutput)
						require.Equal(t, 1, router.OwnedColumnCount)
					}
				} else {
					require.Empty(t, owner.PreScopes)
				}
			}
		})
	}
}

func TestSetMembershipProducerCount(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	defer c.proc.Free()
	local := engine.Node{Addr: "127.0.0.1:18000", Mcpu: 16}
	scan := newDistinctSetTestScope(c)
	scan.RootOp.Release()
	scan.RootOp = table_scan.NewArgument()
	scan.Magic, scan.NodeInfo = Remote, local
	defer scan.release()
	count, ok := setMembershipProducerCount(local, local.Addr, []*Scope{scan})
	require.True(t, ok)
	require.Equal(t, 16, count)
	_, ok = setMembershipProducerCount(local, local.Addr, []*Scope{scan, scan})
	require.False(t, ok, "shared scopes cannot be consumed twice")
	t.Run("shared operator", func(t *testing.T) {
		shared := top.NewArgument()
		scan.RootOp.AppendChild(shared)
		scan.RootOp.AppendChild(shared)
		defer func() {
			scan.RootOp.GetOperatorBase().ResetChildren()
			shared.Release()
		}()
		_, ok := setMembershipProducerCount(local, local.Addr, []*Scope{scan})
		require.False(t, ok, "shared operators cannot be consumed twice")
	})
	t.Run("cyclic operator", func(t *testing.T) {
		scan.RootOp.AppendChild(scan.RootOp)
		defer scan.RootOp.GetOperatorBase().ResetChildren()
		_, ok := setMembershipProducerCount(local, local.Addr, []*Scope{scan})
		require.False(t, ok, "cyclic operator graphs cannot be routed")
	})
	scan.Magic = Normal
	_, ok = setMembershipProducerCount(local, local.Addr, []*Scope{scan})
	require.False(t, ok, "Normal runs only one pipeline regardless of Mcpu")
	scan.Magic = Remote
	ordered := top.NewArgument().WithOrderedOutput()
	ordered.AppendChild(scan.RootOp)
	scan.RootOp = ordered
	_, ok = setMembershipProducerCount(local, local.Addr, []*Scope{scan})
	require.False(t, ok, "a router must not hide an ordered-Top gather")
}

func TestSetMembershipRowWidth(t *testing.T) {
	intType := types.T_int64.ToType().PlanType()
	textType := types.New(types.T_varchar, 128, 0).PlanType()
	cols := []*plan.Expr{plan2.GetColExpr(intType, 0, 0), plan2.GetColExpr(textType, 0, 1)}
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{Rowsize: 32}, ProjectList: plan2.DeepCopyExprList(cols)},
		{NodeType: plan.Node_PROJECT, Stats: &plan.Stats{Rowsize: 100}, Children: []int32{0}, ProjectList: plan2.DeepCopyExprList(cols)},
		{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{Rowsize: 64}, ProjectList: plan2.DeepCopyExprList(cols)},
		{NodeType: plan.Node_PROJECT, Stats: &plan.Stats{Rowsize: 100}, Children: []int32{2}, ProjectList: plan2.DeepCopyExprList(cols)},
	}}
	set := &plan.Node{Children: []int32{1, 3}, ProjectList: cols}
	width, ok := setMembershipRowWidth(query, set)
	require.True(t, ok, "scan estimates must not be replaced by synthetic projection statistics")
	require.Greater(t, width, float64(128), "the budget heuristic uses the declared capacity")
	query.Nodes[2].Stats.Rowsize = 96
	_, ok = setMembershipRowWidth(query, set)
	require.False(t, ok, "copy-heavy wide rows stay serial")
	query.Nodes[2].Stats.Rowsize = 32
	query.Nodes[3].ProjectList[1] = plan2.MakePlan2Int64ConstExprWithType(1)
	_, ok = setMembershipRowWidth(query, set)
	require.False(t, ok, "unknown projection expansion uses the declared bound")
	set.ProjectList[1].Typ.Width = 8
	_, ok = setMembershipRowWidth(query, set)
	require.True(t, ok, "a small declared bound can admit an otherwise unproved projection")
}

func TestOwnedSetMembershipAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*Compile, *plan.Node)
		limit  int64
		want   bool
		shrink bool
	}{
		{"default", nil, 512 << 20, true, false},
		{"small input", func(_ *Compile, n *plan.Node) { n.Stats.Cost = 100 }, 512 << 20, false, false},
		{"unknown cost", func(_ *Compile, n *plan.Node) { n.Stats.Cost = math.NaN() }, 512 << 20, false, false},
		{"low memory", nil, 1 << 20, false, false},
		{"prepared", func(c *Compile, _ *plan.Node) { c.isPrepare = true }, 512 << 20, false, false},
		{"draining coordinator", func(c *Compile, _ *plan.Node) { c.cnList[0].WorkState = metadata.WorkState_Draining }, 512 << 20, false, false},
		{"shrunk shared cap", nil, 512 << 20, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			c.isPrepare, c.addr, c.ncpu = false, "127.0.0.1:18000", 4
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 4}}
			if tc.shrink {
				budget := process.MustNewExecutionResourceBudget(1<<30, 1<<30)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				other, err := budget.OpenGeneration(2)
				require.NoError(t, err)
				lease, err := other.ReserveTransientMemory(128 << 20)
				require.NoError(t, err)
				require.NoError(t, budget.UpdateAggregateCap(64<<20))
				require.NoError(t, c.proc.SetExecutionResourceBudgetForTesting(generation))
				t.Cleanup(func() { lease.Release(); other.Close(); budget.Close() })
			} else {
				setOwnedSetMembershipTestBudget(t, c, uint64(tc.limit))
			}
			typ := types.T_int64.ToType().PlanType()
			scan := func() *plan.Node {
				return &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}}},
					ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}, Stats: &plan.Stats{Rowsize: 8}}
			}
			node := &plan.Node{NodeType: plan.Node_INTERSECT_ALL, Children: []int32{0, 1}, Stats: &plan.Stats{Cost: 400000}, ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}}
			query := &plan.Query{Steps: []int32{2}, Nodes: []*plan.Node{scan(), scan(), node}}
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
			inputs := newDistinctSetTestScopes(c, 2)
			for _, s := range inputs {
				s.NodeInfo.Addr = c.addr
			}
			if tc.edit != nil {
				tc.edit(c, node)
			}
			result, err := c.tryCompileOwnedSetMembership(2, node, inputs[:1], inputs[1:])
			t.Cleanup(func() {
				if result != nil {
					result[0].FreeOperator(c)
					result[0].release()
				} else {
					ReleaseScopes(inputs)
				}
				c.proc.Free()
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, result != nil)
		})
	}
	t.Run("cancelled entry", func(t *testing.T) {
		c := newDistinctSetTestCompile(t)
		defer c.proc.Free()
		ctx, cancel := context.WithCancel(c.proc.Ctx)
		cancel()
		c.proc.Ctx = ctx
		_, err := c.tryCompileOwnedSetMembership(0, nil, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("cancelled with cause", func(t *testing.T) {
		c := newDistinctSetTestCompile(t)
		defer c.proc.Free()
		ctx, cancel := context.WithCancelCause(c.proc.Ctx)
		cause := errors.New("upstream failure")
		cancel(cause)
		c.proc.Ctx = ctx
		_, err := c.tryCompileOwnedSetMembership(0, nil, nil, nil)
		require.ErrorIs(t, err, cause)
	})
	t.Run("deadline with diagnostic cause", func(t *testing.T) {
		c := newDistinctSetTestCompile(t)
		defer c.proc.Free()
		diagnostic := errors.New("deadline diagnostic")
		ctx, cancel := context.WithDeadlineCause(c.proc.Ctx, time.Now().Add(-time.Second), diagnostic)
		defer cancel()
		c.proc.Ctx = ctx
		_, err := c.tryCompileOwnedSetMembership(0, nil, nil, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, diagnostic)
	})
}

func TestOwnedSetMembershipConsolidatesCompleteInputs(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	defer c.proc.Free()
	c.isPrepare, c.addr, c.ncpu = false, "127.0.0.1:18000", 16
	setOwnedSetMembershipTestBudget(t, c, 1<<30)
	local := engine.Node{Addr: c.addr, Mcpu: 16}
	c.cnList = engine.Nodes{local}
	left, right := newDistinctSetTestScopes(c, 2), newDistinctSetTestScopes(c, 1)
	for _, s := range append(left, right...) {
		s.RootOp.Release()
		s.RootOp = table_scan.NewArgument()
		s.Magic, s.NodeInfo = Remote, engine.Node{Addr: c.addr, Mcpu: 8}
	}
	count, ok := setMembershipProducerCount(local, c.addr, left, right)
	require.True(t, ok)
	require.Equal(t, 24, count)
	typ := types.T_int64.ToType().PlanType()
	scan := func() *plan.Node {
		return &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}}},
			ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}, Stats: &plan.Stats{Rowsize: 8}}
	}
	node := &plan.Node{NodeType: plan.Node_MINUS_ALL, Children: []int32{0, 1}, Stats: &plan.Stats{Cost: 400000}, ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Steps: []int32{2}, Nodes: []*plan.Node{scan(), scan(), node}}}}
	result, err := c.tryCompileOwnedSetMembership(2, node, left, right)
	require.NoError(t, err)
	require.Len(t, result, 1)
	defer func() { result[0].FreeOperator(c); result[0].release() }()
	require.Len(t, result[0].PreScopes, 8, "consolidating input routers must not reduce membership owners")
	feeders := result[0].PreScopes[0].PreScopes
	require.Len(t, feeders, 2)
	require.Len(t, feeders[0].PreScopes, 2)
	require.Len(t, feeders[1].PreScopes, 1)
	for _, feeder := range feeders {
		for _, reg := range feeder.Proc.Reg.MergeReceivers {
			require.Equal(t, 8, reg.NilBatchCnt)
		}
	}
	for _, owner := range result[0].PreScopes {
		for _, reg := range owner.Proc.Reg.MergeReceivers {
			require.Equal(t, 1, reg.NilBatchCnt, "each consolidated feeder has one router")
		}
	}
}

func TestOwnedSetMembershipOwnerBoundaries(t *testing.T) {
	// Eight direct routers and one nullable int64 column give U = 8 MiB +
	// 2*8*8192*9 bytes. The observed query headroom must reach 16*U.
	const eightOwnerHeadroom = 16 * ((8 << 20) + 2*8*8192*9)
	for _, tc := range []struct {
		name         string
		cpus, maxDop int
		left, right  int
		headroom     int64
		want         int
	}{
		{"eight at boundary", 16, 16, 4, 4, eightOwnerHeadroom, 8},
		{"one byte below boundary", 16, 16, 4, 4, eightOwnerHeadroom - 1, 4},
		{"five workers", 16, 16, 3, 2, 1 << 30, 4},
		{"seven workers", 16, 16, 4, 3, 1 << 30, 4},
		{"three workers", 16, 16, 2, 1, 1 << 30, 3},
		{"seven cpus", 7, 16, 4, 4, 1 << 30, 4},
		{"max dop seven", 16, 7, 4, 4, 1 << 30, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			c.isPrepare, c.addr, c.ncpu = false, "127.0.0.1:18000", tc.cpus
			setOwnedSetMembershipTestBudget(t, c, uint64(tc.headroom))
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: tc.cpus}}
			typ := types.T_int64.ToType().PlanType()
			scan := func() *plan.Node {
				return &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}}},
					ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}, Stats: &plan.Stats{Rowsize: 8}}
			}
			node := &plan.Node{NodeType: plan.Node_INTERSECT_ALL, Children: []int32{0, 1}, Stats: &plan.Stats{Cost: 400000}, ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}}
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: int64(tc.maxDop), Steps: []int32{2}, Nodes: []*plan.Node{scan(), scan(), node}}}}
			inputs := newDistinctSetTestScopes(c, 2)
			for i, workers := range []int{tc.left, tc.right} {
				inputs[i].RootOp.Release()
				inputs[i].RootOp = table_scan.NewArgument()
				inputs[i].Magic, inputs[i].NodeInfo = Remote, engine.Node{Addr: c.addr, Mcpu: workers}
			}
			result, err := c.tryCompileOwnedSetMembership(2, node, inputs[:1], inputs[1:])
			t.Cleanup(func() {
				if result != nil {
					result[0].FreeOperator(c)
					result[0].release()
				} else {
					ReleaseScopes(inputs)
				}
				c.proc.Free()
			})
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, tc.want)
		})
	}
}

func setOwnedSetMembershipTestBudget(t *testing.T, c *Compile, headroom uint64) {
	t.Helper()
	budget := process.MustNewExecutionResourceBudget(headroom, headroom)
	t.Cleanup(func() { budget.Close() })
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(func() { generation.Close() })
	require.NoError(t, c.proc.SetExecutionResourceBudgetForTesting(generation))
}
