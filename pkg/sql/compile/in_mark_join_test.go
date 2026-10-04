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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestCanUseHashMarkJoin(t *testing.T) {
	tests := []struct {
		name       string
		conditions []*plan.Expr
		want       bool
	}{
		{
			name:       "single nullable equality",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)},
			want:       true,
		},
		{
			name: "composite non-null equality",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, true),
			},
			want: true,
		},
		{
			name: "composite nullable equality needs row-aware null evaluation",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, false),
			},
			want: false,
		},
		{
			name:       "non-equality condition",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "<", 0, true)},
			want:       false,
		},
		{
			name: "hash key plus residual non-equality retains loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "<", 1, true),
			},
			want: false,
		},
		{
			name:       "same-side equality is not a hash join key",
			conditions: []*plan.Expr{makeMarkJoinTestSameSideCondition(t)},
			want:       false,
		},
		{
			name: "hash key plus mixed-side correlated equality retains loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestMixedSideCondition(t),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_MARK,
				OnList:   tt.conditions,
			}
			require.Equal(t, tt.want, canUseHashMarkJoin(node))
		})
	}
}

func TestCanUseShuffleHashMarkJoin(t *testing.T) {
	tests := []struct {
		name       string
		conditions []*plan.Expr
		want       bool
	}{
		{
			name:       "single non-null equality",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, true)},
			want:       true,
		},
		{
			name:       "single nullable equality needs global build facts",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)},
		},
		{
			name: "composite non-null equality",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, true),
			},
			want: true,
		},
		{
			name:       "non-equality condition",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "<", 0, true)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_MARK,
				OnList:   tt.conditions,
			}
			require.Equal(t, tt.want, canUseShuffleHashMarkJoin(node))
		})
	}
}

func TestCompileJoinFallsBackForUnsafeShuffleMark(t *testing.T) {
	node := newShuffleJoinTestNode(1)
	node.JoinType = plan.Node_MARK
	node.Stats.HashmapStats.Shuffle = true
	// Keep stale bind-time NOT NULL metadata on the condition while the
	// materialized probe output is nullable, as happens after outer-join
	// null extension.
	node.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, true)}
	left := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, false)}}
	right := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, true)}}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	probe := newShuffleJoinTestScope(t, c.cnList[0], 1)
	build := newShuffleJoinTestScope(t, c.cnList[0], 1)

	var result []*Scope
	require.NotPanics(t, func() {
		result = c.compileJoin(node, left, right, []*Scope{probe}, []*Scope{build})
	})

	require.False(t, node.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
	require.Len(t, result, 1)
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.False(t, op.IsShuffle)
}

func TestCompileJoinFallsBackForUnprovenMaterializedMarkKey(t *testing.T) {
	node := newShuffleJoinTestNode(1)
	node.JoinType = plan.Node_MARK
	node.Stats.HashmapStats.Shuffle = true
	// The materialized child still carries optimistic bind-time metadata, but
	// planner remap records that the original key expression was not proven
	// non-NULL (for example, json_extract on non-NULL arguments).
	node.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)}
	left := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, true)}}
	right := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, true)}}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	probe := newShuffleJoinTestScope(t, c.cnList[0], 1)
	build := newShuffleJoinTestScope(t, c.cnList[0], 1)

	result := c.compileJoin(node, left, right, []*Scope{probe}, []*Scope{build})

	require.False(t, node.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
	require.Len(t, result, 1)
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.False(t, op.IsShuffle)
}

func TestCompileBroadcastCompositeMarkExpressionsUseHashJoin(t *testing.T) {
	compilerCtx := plan2.NewMockCompilerContext(true)
	statements, err := mysql.Parse(
		compilerCtx.GetContext(),
		`select (n.n_nationkey + 0, n.n_regionkey + 0) in (
			select l.l_orderkey + 0, l.l_partkey + 0
			from tpch.lineitem l
		)
		from tpch.nation n`,
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)

	logicPlan, err := plan2.BuildPlan(compilerCtx, statements[0], false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var mark *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
			mark = node
			break
		}
	}
	require.NotNil(t, mark)
	require.Len(t, mark.Children, 2)
	require.Len(t, mark.OnList, 2)
	left := query.Nodes[mark.Children[0]]
	right := query.Nodes[mark.Children[1]]

	// The shuffle-only proof is deliberately encoded in the remapped operand
	// types. Ordinary arithmetic is not accepted by that proof, but it remains
	// non-null for broadcast hash MARK when its materialized inputs are non-null.
	for _, condition := range mark.OnList {
		fn := condition.GetF()
		require.NotNil(t, fn)
		require.Len(t, fn.Args, 2)
		require.False(t, fn.Args[0].Typ.NotNullable)
		require.False(t, fn.Args[1].Typ.NotNullable)
	}
	require.True(t, canUseHashMarkJoinWithInputs(mark, left, right))
	require.False(t, canUseShuffleHashMarkJoinWithInputs(mark, left, right))

	nullableLeft := plan2.DeepCopyNode(left)
	nullableLeft.ProjectList[0].Typ.NotNullable = false
	require.False(t, canUseHashMarkJoinWithInputs(mark, nullableLeft, right),
		"a nullable component must keep composite broadcast MARK on LoopJoin")

	// Even if stale planner state requests shuffle, the compiler must retain
	// the strong bucket-local guard, fall back to broadcast, and still select
	// HashJoin using the broadcast nullability contract.
	mark.Stats.HashmapStats.Shuffle = true
	mark.SendMsgList = []plan.MsgHeader{{
		MsgType: int32(message.MsgJoinMap),
		MsgTag:  1,
	}}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	probe := newShuffleJoinTestScope(t, c.cnList[0], 1)
	build := newShuffleJoinTestScope(t, c.cnList[0], 1)

	result := c.compileJoin(mark, left, right, []*Scope{probe}, []*Scope{build})

	require.Len(t, result, 1)
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.False(t, op.IsShuffle)
	require.Len(t, op.EqConds[0], 2)
	require.Len(t, op.EqConds[1], 2)
	for side := range op.EqConds {
		for _, key := range op.EqConds[side] {
			require.True(t, key.Typ.NotNullable)
		}
	}
	for _, condition := range mark.OnList {
		for _, key := range condition.GetF().Args {
			require.False(t, key.Typ.NotNullable,
				"operator construction must not mutate the reusable plan")
		}
	}
}

func TestCompileNullableNotExistsAntiJoinUsesHashJoin(t *testing.T) {
	compilerCtx := plan2.NewMockCompilerContext(true)
	statements, err := mysql.Parse(
		compilerCtx.GetContext(),
		`select n.n_nationkey
		from tpch.nation n
		where not exists (
			select 1 from tpch.region r where r.r_comment = n.n_comment
		)`,
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)

	logicPlan, err := plan2.BuildPlan(compilerCtx, statements[0], false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var anti *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_ANTI {
			anti = node
			break
		}
	}
	require.NotNil(t, anti)
	require.Len(t, anti.Children, 2)
	require.NotNil(t, anti.Stats)
	anti.Stats.HashmapStats.Shuffle = false
	anti.SendMsgList = []plan.MsgHeader{{
		MsgType: int32(message.MsgJoinMap),
		MsgTag:  1,
	}}

	left := query.Nodes[anti.Children[0]]
	right := query.Nodes[anti.Children[1]]
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	probe := newShuffleJoinTestScope(t, c.cnList[0], 1)
	build := newShuffleJoinTestScope(t, c.cnList[0], 1)

	result := c.compileJoin(anti, left, right, []*Scope{probe}, []*Scope{build})

	require.Len(t, result, 1)
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok, "compiled %T, want HashJoin", result[0].RootOp)
	require.Equal(t, plan.Node_ANTI, op.JoinType)
	require.Len(t, op.EqConds[0], 1)
	require.Len(t, op.EqConds[1], 1)
}

func TestCompileBroadcastMarkJoinSelectsPhysicalOperator(t *testing.T) {
	tests := []struct {
		name       string
		conditions []*plan.Expr
		left       *plan.Node
		right      *plan.Node
		wantHash   bool
	}{
		{
			name:       "single nullable equality uses hash join",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)},
			left:       &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, false)}},
			right:      &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, false)}},
			wantHash:   true,
		},
		{
			name: "partially nullable composite equality uses loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, false),
			},
			left: &plan.Node{ProjectList: []*plan.Expr{
				makeMarkJoinTestColumn(0, 0, true),
				makeMarkJoinTestColumn(0, 1, false),
			}},
			right: &plan.Node{ProjectList: []*plan.Expr{
				makeMarkJoinTestColumn(1, 0, true),
				makeMarkJoinTestColumn(1, 1, false),
			}},
			wantHash: false,
		},
		{
			name: "equality plus residual uses loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "<", 1, true),
			},
			left: &plan.Node{ProjectList: []*plan.Expr{
				makeMarkJoinTestColumn(0, 0, true),
				makeMarkJoinTestColumn(0, 1, true),
			}},
			right: &plan.Node{ProjectList: []*plan.Expr{
				makeMarkJoinTestColumn(1, 0, true),
				makeMarkJoinTestColumn(1, 1, true),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newShuffleJoinTestNode(1)
			node.JoinType = plan.Node_MARK
			node.Stats.HashmapStats.Shuffle = false
			node.OnList = tt.conditions
			c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
			probe := newShuffleJoinTestScope(t, c.cnList[0], 1)
			build := newShuffleJoinTestScope(t, c.cnList[0], 1)

			result := c.compileJoin(node, tt.left, tt.right, []*Scope{probe}, []*Scope{build})
			require.Len(t, result, 1)
			if tt.wantHash {
				op, ok := result[0].RootOp.(*hashjoin.HashJoin)
				require.True(t, ok, "compiled %T, want HashJoin", result[0].RootOp)
				require.Equal(t, plan.Node_MARK, op.JoinType)
				return
			}
			op, ok := result[0].RootOp.(*loopjoin.LoopJoin)
			require.True(t, ok, "compiled %T, want LoopJoin", result[0].RootOp)
			require.Equal(t, plan.Node_MARK, op.JoinType)
		})
	}
}

func TestConstructShuffleJoinOperatorForMark(t *testing.T) {
	node := newShuffleJoinTestNode(1)
	node.JoinType = plan.Node_MARK
	node.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, true)}
	left := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, true)}}
	right := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, true)}}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	scope := newShuffleJoinTestScope(t, c.cnList[0], 1)

	require.NotPanics(t, func() {
		constructShuffleJoinOP(c, []*Scope{scope}, node, left, right, false)
	})

	op, ok := scope.RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.Equal(t, plan.Node_MARK, op.JoinType)
	require.True(t, op.IsShuffle)
	require.Equal(t, int32(0), op.ShuffleIdx)
}

func TestCompileShuffleMarkJoinTopologies(t *testing.T) {
	tests := []struct {
		name       string
		stageNodes engine.Nodes
		wantScopes int
	}{
		{
			name:       "local shared pool",
			stageNodes: engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}},
			wantScopes: 1,
		},
		{
			name: "distributed",
			stageNodes: engine.Nodes{
				{Addr: "cn1:6001", Mcpu: 1},
				{Addr: "cn2:6001", Mcpu: 1},
			},
			wantScopes: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newShuffleJoinTestNode(1)
			node.JoinType = plan.Node_MARK
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			node.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, true)}
			left := &plan.Node{
				Stats:       &plan.Stats{Dop: 1},
				ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, true)},
			}
			right := &plan.Node{
				Stats:       &plan.Stats{Dop: 1},
				ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, true)},
			}
			c := newCompileForShuffleJoinTest(t, tt.stageNodes)
			probe := newShuffleJoinTestScope(t, tt.stageNodes[0], 1)
			build := newShuffleJoinTestScope(t, tt.stageNodes[0], 1)

			var result []*Scope
			require.NotPanics(t, func() {
				result = c.compileShuffleJoin(node, left, right, []*Scope{probe}, []*Scope{build})
			})

			require.Len(t, result, tt.wantScopes)
			for _, scope := range result {
				op, ok := scope.RootOp.(*hashjoin.HashJoin)
				require.True(t, ok)
				require.Equal(t, plan.Node_MARK, op.JoinType)
				require.True(t, op.IsShuffle)
			}
		})
	}
}

func makeMarkJoinTestSameSideCondition(t *testing.T) *plan.Expr {
	t.Helper()

	condition := makeMarkJoinTestCondition(t, "=", 0, true)
	condition.GetF().Args[1].GetCol().RelPos = 0
	return condition
}

// makeMarkJoinTestMixedSideCondition models a pulled-up correlated predicate
// such as t2.z + t1.c = t1.d. The left operand cannot be a hash key because it
// references both the build and probe relations.
func makeMarkJoinTestMixedSideCondition(t *testing.T) *plan.Expr {
	t.Helper()

	typ := types.T_int64.ToType()
	plus, err := function.GetFunctionByName(context.Background(), "+", []types.Type{typ, typ})
	require.NoError(t, err)
	equal, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
	require.NoError(t, err)

	mixedOperand := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: plus.GetEncodedOverloadID(), ObjName: "+"},
			Args: []*plan.Expr{
				makeMarkJoinTestColumn(1, 1, true),
				makeMarkJoinTestColumn(0, 1, true),
			},
		}},
	}

	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: []*plan.Expr{
				mixedOperand,
				makeMarkJoinTestColumn(0, 2, true),
			},
		}},
	}
}

func TestConstructBroadcastHashBuildForMark(t *testing.T) {
	op := hashjoin.NewArgument()
	op.JoinType = plan.Node_MARK
	op.JoinMapTag = 1
	op.EqConds = [][]*plan.Expr{
		{makeMarkJoinTestColumn(0, 0, false)},
		{makeMarkJoinTestColumn(1, 0, false)},
	}
	defer op.Release()

	build := constructBroadcastHashBuild(op, nil, 1)
	defer build.Release()

	require.True(t, build.NeedHashMap)
	require.False(t, build.NeedAllocateSels)
	require.False(t, build.NeedBatches)
	require.True(t, build.TrackNullKeys)
}

func TestConstructShuffleHashBuildForMarkPreservesSemanticFlags(t *testing.T) {
	op := hashjoin.NewArgument()
	op.JoinType = plan.Node_MARK
	op.JoinMapTag = 1
	op.ShuffleIdx = 0
	op.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{Tag: 2}}
	op.EqConds = [][]*plan.Expr{
		{makeMarkJoinTestColumn(0, 0, false)},
		{makeMarkJoinTestColumn(1, 0, false)},
	}
	defer op.Release()

	build := constructShuffleHashBuild(&plan.Node{}, op, nil)
	defer build.Release()

	require.True(t, build.TrackNullKeys)
	require.False(t, build.NeedAllocateSels)
}

func makeMarkJoinTestCondition(t *testing.T, name string, colPos int32, notNullable bool) *plan.Expr {
	t.Helper()

	typ := types.T_int64.ToType()
	overload, err := function.GetFunctionByName(context.Background(), name, []types.Type{typ, typ})
	require.NoError(t, err)

	args := make([]*plan.Expr, 2)
	for i := range args {
		args[i] = makeMarkJoinTestColumn(int32(i), colPos, notNullable)
	}

	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: notNullable,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     overload.GetEncodedOverloadID(),
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func makeMarkJoinTestColumn(relPos, colPos int32, notNullable bool) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: notNullable,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}
