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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func mixedAsofTestPlan(t *testing.T, legalKey bool) (*plan.Node, *plan.Node, *plan.Node) {
	t.Helper()
	col := func(rel, pos int32, typ types.T) *plan.Expr {
		return &plan.Expr{Typ: typ.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
	}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		e, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return e
	}
	left := &plan.Node{Stats: &plan.Stats{Outcnt: 1, Rowsize: 10}, ProjectList: []*plan.Expr{col(0, 0, types.T_json), col(0, 1, types.T_timestamp), col(0, 2, types.T_int64)}}
	right := &plan.Node{Stats: &plan.Stats{Outcnt: 1e9, Rowsize: 1000}, ProjectList: []*plan.Expr{col(1, 0, types.T_int64), col(1, 1, types.T_timestamp), col(1, 2, types.T_int64)}}
	n := newShuffleJoinTestNode(4)
	n.JoinType = plan.Node_ASOF_LEFT
	n.AsofRightCol = 1
	n.ProjectList = []*plan.Expr{right.ProjectList[1]}
	n.OnList = []*plan.Expr{bind("=", left.ProjectList[0], right.ProjectList[0]), bind(">=", left.ProjectList[1], right.ProjectList[1])}
	if legalKey {
		n.OnList = append(n.OnList, bind("=", left.ProjectList[2], right.ProjectList[2]))
	}
	n.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: 123}}
	n.Stats.HashmapStats.HashOnPK = true
	return n, left, right
}
func TestCompileMixedAsofPhysicalPlan(t *testing.T) {
	for _, key := range []bool{false, true} {
		t.Run(map[bool]string{false: "constant", true: "real-key"}[key], func(t *testing.T) {
			n, left, right := mixedAsofTestPlan(t, key)
			c := newCompileForShuffleJoinTest(t, engine.Nodes{{Id: "local", Addr: "cn1:6001", Mcpu: 4}, {Id: "remote", Addr: "cn2:6001", Mcpu: 4}})
			require.False(t, shouldBuildLeftForAsof(n, left, right), "mixed class cannot use temporal-only build-left even if stats favor it")
			require.True(t, c.broadcastJoinUsesHash(n))
			// A single remote input on each side must still gather locally.
			probes := []*Scope{newShuffleJoinTestScope(t, c.cnList[1], 4)}
			builds := []*Scope{newShuffleJoinTestScope(t, c.cnList[1], 4)}
			out := c.compileJoin(n, left, right, probes, builds)
			t.Cleanup(func() { ReleaseScopes(out) })
			require.Len(t, out, 1)
			require.True(t, c.scopesRunOnCoordinator(out))
			op := out[0].RootOp.(*hashjoin.HashJoin)
			require.False(t, op.AsofBuildLeft)
			require.False(t, op.IsShuffle)
			require.False(t, op.HashOnPK)
			require.Empty(t, op.RuntimeFilterSpecs)
			require.False(t, n.Stats.HashmapStats.Shuffle)
			require.Equal(t, int32(-1), n.Stats.HashmapStats.ShuffleColIdx)
			require.Empty(t, n.RuntimeFilterBuildList)
			require.Len(t, op.EqConds[0], 1)
			require.Len(t, op.EqConds[1], 1)
			require.True(t, colexec.HasMixedJSONNumericEquality(op.NonEqCond))
			buildScope := out[0].PreScopes[len(out[0].PreScopes)-1]
			require.Equal(t, c.addr, buildScope.NodeInfo.Addr)
			require.Equal(t, 1, buildScope.NodeInfo.Mcpu)
			build := buildScope.RootOp.(*hashbuild.HashBuild)
			require.True(t, build.NeedBatches)
			require.True(t, build.NeedAllocateSels)
			require.True(t, build.NeedHashMap)
			require.False(t, build.IsShuffle)
			require.False(t, build.HashOnPK)
			require.Nil(t, build.RuntimeFilterSpec)
			require.Len(t, build.Conditions, 1)
			if !key {
				require.NotSame(t, op.EqConds[0][0], op.EqConds[1][0])
				require.NotSame(t, op.EqConds[1][0], build.Conditions[0])
				require.True(t, build.Conditions[0].Typ.NotNullable)
				require.Equal(t, int64(0), build.Conditions[0].GetLit().GetI64Val())
			} else {
				require.NotNil(t, op.EqConds[0][0].GetCol())
			}
		})
	}
}
func TestMixedAsofPlacementAndRoundTrip(t *testing.T) {
	n, left, right := mixedAsofTestPlan(t, false)
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion127
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(&plan.Query{Nodes: []*plan.Node{n}, Steps: []int32{0}}))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	require.Len(t, c.cnList, 1)
	require.Equal(t, c.addr, c.cnList[0].Addr)
	op := constructHashJoin(n, left, []types.Type{types.T_json.ToType(), types.T_timestamp.ToType(), types.T_int64.ToType()}, []types.Type{types.T_int64.ToType(), types.T_timestamp.ToType(), types.T_int64.ToType()}, c.proc)
	defer op.Release()
	copied := dupOperator(op, 0, 1).(*hashjoin.HashJoin)
	defer copied.Release()
	_, instruction, err := convertToPipelineInstruction(copied, c.proc, nil, 0)
	require.NoError(t, err)
	bytes, err := instruction.Marshal()
	require.NoError(t, err)
	var decoded pipeline.Instruction
	require.NoError(t, decoded.Unmarshal(bytes))
	decodedOp, err := convertToVmOperator(&decoded, nil, nil)
	require.NoError(t, err)
	roundtrip := decodedOp.(*hashjoin.HashJoin)
	defer roundtrip.Release()
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	for _, arg := range []*hashjoin.HashJoin{copied, roundtrip} {
		require.NoError(t, arg.SetAllocationAccount(account))
		require.NoError(t, arg.Prepare(c.proc))
		require.True(t, colexec.HasMixedJSONNumericEquality(arg.NonEqCond))
		arg.Reset(c.proc, false, nil)
		require.NoError(t, arg.Prepare(c.proc))
		arg.Free(c.proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
		require.NoError(t, arg.ClearAllocationAccount(account))
	}
	_ = right
}
func TestDependentMixedAsofFactoryExecution(t *testing.T) {
	n, left, right := mixedAsofTestPlan(t, false)
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	defer proc.Free()
	factory, err := (&Compile{proc: proc}).compileDependentHashJoin(n, []*plan.Node{left, right})
	require.NoError(t, err)
	op, err := factory(proc, []*dependent.DemandInput{{}, {}})
	require.NoError(t, err)
	root := op.(*dependent.JoinKernel)
	join := root.Join.(*hashjoin.HashJoin)
	require.False(t, join.AsofBuildLeft)
	require.False(t, join.IsShuffle)
	require.Len(t, join.EqConds[0], 1)
	require.True(t, root.Build.DirectInput)
	require.True(t, root.Build.NeedBatches)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.Open(1 << 24)
	require.NoError(t, err)
	require.NoError(t, join.SetAllocationAccount(account))
	require.NoError(t, root.Build.SetAllocationAccount(account))
	l := batch.NewWithSize(3)
	r := batch.NewWithSize(3)
	for _, b := range []*batch.Batch{l, r} {
		b.Vecs[1] = vector.NewVec(types.T_timestamp.ToType())
		b.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	}
	l.Vecs[0] = vector.NewVec(types.T_json.ToType())
	j, err := types.ParseStringToByteJson("1")
	require.NoError(t, err)
	encoded, err := types.EncodeJson(j)
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(l.Vecs[0], encoded, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(l.Vecs[1], types.Timestamp(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(l.Vecs[2], int64(1), false, proc.Mp()))
	l.SetRowCount(1)
	r.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
	require.NoError(t, vector.AppendFixedList(r.Vecs[1], []types.Timestamp{8, 9}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(r.Vecs[2], []int64{1, 1}, nil, proc.Mp()))
	r.SetRowCount(2)
	// Replace demand ports with finite input operators, executing the real factory
	// root's build/probe sequence and DirectInput path without a service fixture.
	lm, rm := colexec.NewMockOperator().WithBatchs([]*batch.Batch{l}), colexec.NewMockOperator().WithBatchs([]*batch.Batch{r})
	join.SetChildren([]vm.Operator{lm})
	root.Build.SetChildren([]vm.Operator{rm})
	defer func() {
		root.Reset(proc, false, nil)
		join.Reset(proc, false, nil)
		root.Build.Reset(proc, false, nil)
		root.Free(proc, false, nil)
		join.Free(proc, false, nil)
		root.Build.Free(proc, false, nil)
		lm.Free(proc, false, nil)
		rm.Free(proc, false, nil)
		join.Release()
		root.Build.Release()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, proc.Mp().CurrNB())
	}()
	require.NoError(t, root.Prepare(proc))
	require.NoError(t, join.Prepare(proc))
	require.NoError(t, root.Build.Prepare(proc))
	out, err := vm.Exec(root, proc)
	require.NoError(t, err)
	require.NotNil(t, out.Batch)
	require.Equal(t, 1, out.Batch.RowCount())
	require.False(t, out.Batch.Vecs[0].GetNulls().Contains(0))
	require.Equal(t, []types.Timestamp{8}, vector.MustFixedColWithTypeCheck[types.Timestamp](out.Batch.Vecs[0]))
}
