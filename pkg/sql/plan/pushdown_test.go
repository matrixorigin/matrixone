// Copyright 2024 Matrix Origin
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

package plan

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func makeVolatileJoinFilter(t *testing.T, ctx *MockCompilerContext, tag *int32) *plan.Expr {
	t.Helper()
	randFn, err := function.GetFunctionByName(context.Background(), "rand", nil)
	require.NoError(t, err)
	value := &plan.Expr{
		Typ: Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: randFn.GetEncodedOverloadID(), ObjName: "rand"},
		}},
	}
	if tag != nil {
		value, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "+", []*plan.Expr{
			{Typ: Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: *tag, ColPos: 0},
			}},
			value,
		})
		require.NoError(t, err)
	}
	filter, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "<", []*plan.Expr{
		value, makePlan2Float64ConstExprWithType(0.5),
	})
	require.NoError(t, err)
	return filter
}

func newVolatileJoinPushdownBuilder(ctx *MockCompilerContext, joinType plan.Node_JoinType) (*QueryBuilder, int32, int32) {
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	leftTag := builder.GenNewBindTag()
	rightTag := builder.GenNewBindTag()
	builder.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{leftTag}, Stats: &plan.Stats{Outcnt: 1}},
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{rightTag}, Stats: &plan.Stats{Outcnt: 1}},
		{NodeType: plan.Node_JOIN, JoinType: joinType, Children: []int32{0, 1}},
	}
	return builder, leftTag, rightTag
}

func TestJoinDoesNotPushDownVolatileFilter(t *testing.T) {
	for _, side := range []string{"none", "left", "right"} {
		t.Run("inner/"+side, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			builder, leftTag, rightTag := newVolatileJoinPushdownBuilder(ctx, plan.Node_INNER)
			var tag *int32
			switch side {
			case "left":
				tag = &leftTag
			case "right":
				tag = &rightTag
			}
			filter := makeVolatileJoinFilter(t, ctx, tag)

			nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filter}, false)
			require.Equal(t, int32(2), nodeID)
			require.Equal(t, []*plan.Expr{filter}, cantPushdown)
			require.Empty(t, builder.qry.Nodes[0].FilterList)
			require.Empty(t, builder.qry.Nodes[1].FilterList)
		})
	}

	t.Run("inner on-list", func(t *testing.T) {
		ctx := NewMockCompilerContext(true)
		builder, leftTag, _ := newVolatileJoinPushdownBuilder(ctx, plan.Node_INNER)
		filter := makeVolatileJoinFilter(t, ctx, &leftTag)
		builder.qry.Nodes[2].OnList = []*plan.Expr{filter}

		_, cantPushdown := builder.pushdownFilters(2, nil, false)
		require.Equal(t, []*plan.Expr{filter}, cantPushdown)
		require.Empty(t, builder.qry.Nodes[0].FilterList)
		require.Empty(t, builder.qry.Nodes[1].FilterList)
	})

	t.Run("left on-list", func(t *testing.T) {
		ctx := NewMockCompilerContext(true)
		builder, _, rightTag := newVolatileJoinPushdownBuilder(ctx, plan.Node_LEFT)
		filter := makeVolatileJoinFilter(t, ctx, &rightTag)
		builder.qry.Nodes[2].OnList = []*plan.Expr{filter}

		_, cantPushdown := builder.pushdownFilters(2, nil, false)
		require.Empty(t, cantPushdown)
		require.Equal(t, []*plan.Expr{filter}, builder.qry.Nodes[2].OnList)
		require.Empty(t, builder.qry.Nodes[0].FilterList)
		require.Empty(t, builder.qry.Nodes[1].FilterList)
	})

	t.Run("function scan bypass", func(t *testing.T) {
		ctx := NewMockCompilerContext(true)
		builder, leftTag, _ := newVolatileJoinPushdownBuilder(ctx, plan.Node_INNER)
		builder.qry.Nodes[1].NodeType = plan.Node_FUNCTION_SCAN
		filter := makeVolatileJoinFilter(t, ctx, &leftTag)

		_, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filter}, false)
		require.Equal(t, []*plan.Expr{filter}, cantPushdown)
		require.Empty(t, builder.qry.Nodes[0].FilterList)
		require.Empty(t, builder.qry.Nodes[1].FilterList)
	})
}

func TestVolatileFilterStopsAtPlanBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		node       *plan.Node
		storedHere bool
	}{
		{name: "aggregate", node: &plan.Node{NodeType: plan.Node_AGG, BindingTags: []int32{1, 2}, Children: []int32{0}}, storedHere: true},
		{name: "sample", node: &plan.Node{NodeType: plan.Node_SAMPLE, BindingTags: []int32{1, 2}, Children: []int32{0}}, storedHere: true},
		{name: "window", node: &plan.Node{NodeType: plan.Node_WINDOW, BindingTags: []int32{1}, Children: []int32{0}}, storedHere: true},
		{name: "time window", node: &plan.Node{NodeType: plan.Node_TIME_WINDOW, BindingTags: []int32{1}, Children: []int32{0}}, storedHere: true},
		{name: "set operation", node: &plan.Node{NodeType: plan.Node_UNION_ALL, Children: []int32{0, 1}}},
		{name: "apply", node: &plan.Node{NodeType: plan.Node_APPLY, Children: []int32{0}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
			builder.qry.Nodes = []*plan.Node{
				{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, Stats: &plan.Stats{Outcnt: 1}},
				{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{2}, Stats: &plan.Stats{Outcnt: 1}},
			}
			rootID := int32(len(builder.qry.Nodes))
			if test.node.NodeType == plan.Node_TABLE_SCAN {
				rootID = 0
				builder.qry.Nodes[0] = DeepCopyNode(test.node)
			} else {
				builder.qry.Nodes = append(builder.qry.Nodes, DeepCopyNode(test.node))
			}
			filter := makeVolatileJoinFilter(t, ctx, nil)

			nodeID, cantPushdown := builder.pushdownFilters(rootID, []*plan.Expr{filter}, false)
			require.Equal(t, rootID, nodeID)
			if test.storedHere {
				require.Empty(t, cantPushdown)
				require.Equal(t, []*plan.Expr{filter}, builder.qry.Nodes[rootID].FilterList)
			} else {
				require.Equal(t, []*plan.Expr{filter}, cantPushdown)
			}
			require.Empty(t, builder.qry.Nodes[0].FilterList)
			require.Empty(t, builder.qry.Nodes[1].FilterList)
		})
	}
}

func TestProjectDoesNotPushDownFilterRewrittenWithVolatileExpression(t *testing.T) {
	t.Run("volatile projection", func(t *testing.T) {
		ctx := NewMockCompilerContext(true)
		builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
		childTag := builder.GenNewBindTag()
		projectTag := builder.GenNewBindTag()
		projectExpr := makeVolatileJoinFilter(t, ctx, nil)
		filter := &plan.Expr{
			Typ:  Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: 0}},
		}
		builder.qry.Nodes = []*plan.Node{
			{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{childTag}, Stats: &plan.Stats{Outcnt: 1}},
			{
				NodeType:    plan.Node_PROJECT,
				BindingTags: []int32{projectTag},
				Children:    []int32{0},
				ProjectList: []*plan.Expr{projectExpr},
			},
		}

		rewritten := replaceColRefs(DeepCopyExpr(filter), projectTag, builder.qry.Nodes[1].ProjectList)
		require.False(t, ContainsVolatileFunction(filter))
		require.True(t, ContainsVolatileFunction(rewritten))

		nodeID, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{filter}, false)
		require.Equal(t, int32(1), nodeID)
		require.Equal(t, []*plan.Expr{filter}, cantPushdown)
		require.Empty(t, builder.qry.Nodes[0].FilterList)
	})

	t.Run("deterministic projection", func(t *testing.T) {
		ctx := NewMockCompilerContext(true)
		builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
		childTag := builder.GenNewBindTag()
		projectTag := builder.GenNewBindTag()
		projectExpr := &plan.Expr{
			Typ:  Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: childTag, ColPos: 0}},
		}
		filter := &plan.Expr{
			Typ:  Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: 0}},
		}
		builder.qry.Nodes = []*plan.Node{
			{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{childTag}, Stats: &plan.Stats{Outcnt: 1}},
			{
				NodeType:    plan.Node_PROJECT,
				BindingTags: []int32{projectTag},
				Children:    []int32{0},
				ProjectList: []*plan.Expr{projectExpr},
			},
		}

		nodeID, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{filter}, false)
		require.Equal(t, int32(1), nodeID)
		require.Empty(t, cantPushdown)
		require.Len(t, builder.qry.Nodes[0].FilterList, 1)
		require.Equal(t, childTag, builder.qry.Nodes[0].FilterList[0].GetCol().RelPos)
	})
}

func TestAssertIsFilterPushdownBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, false, false)
	tag := builder.GenNewBindTag()
	boolType := Type{Id: int32(types.T_bool)}
	assertExpr := &plan.Expr{
		Typ:  boolType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: 0}},
	}
	parentFilter := DeepCopyExpr(assertExpr)
	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{tag},
			ProjectList: []*plan.Expr{DeepCopyExpr(assertExpr)},
		},
		{
			NodeType:   plan.Node_ASSERT,
			Children:   []int32{0},
			FilterList: []*plan.Expr{assertExpr},
			Limit:      MakePlan2Uint64ConstExprWithType(1),
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{parentFilter}, false)
	require.Equal(t, int32(1), nodeID)
	require.Equal(t, []*plan.Expr{parentFilter}, cantPushdown)
	require.Equal(t, []*plan.Expr{assertExpr}, builder.qry.Nodes[1].FilterList)
	require.Empty(t, builder.qry.Nodes[0].FilterList)
}

func TestBarrierFilterIsFilterPushdownBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, false, false)
	tag := builder.GenNewBindTag()
	boolType := Type{Id: int32(types.T_bool)}
	barrierExpr := &plan.Expr{
		Typ:  boolType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: 0}},
	}
	parentFilter := DeepCopyExpr(barrierExpr)
	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_PRE_INSERT,
			BindingTags: []int32{tag},
			ProjectList: []*plan.Expr{DeepCopyExpr(barrierExpr)},
		},
		{
			NodeType:        plan.Node_FILTER,
			Children:        []int32{0},
			FilterList:      []*plan.Expr{barrierExpr},
			FilterIsBarrier: true,
			Limit:           MakePlan2Uint64ConstExprWithType(1),
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{parentFilter}, false)
	require.Equal(t, int32(1), nodeID)
	require.Equal(t, []*plan.Expr{parentFilter}, cantPushdown)
	require.Equal(t, []*plan.Expr{barrierExpr}, builder.qry.Nodes[1].FilterList)
	require.Empty(t, builder.qry.Nodes[0].FilterList)
}

func TestDedupUpdateIsFilterPushdownBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, false, false)
	leftTag := builder.GenNewBindTag()
	rightTag := builder.GenNewBindTag()
	boolType := Type{Id: int32(types.T_bool)}
	leftFilter := &plan.Expr{
		Typ:  boolType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: leftTag, ColPos: 0}},
	}
	rightFilter := &plan.Expr{
		Typ:  boolType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rightTag, ColPos: 0}},
	}
	bothFilter, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*plan.Expr{
		DeepCopyExpr(leftFilter), DeepCopyExpr(rightFilter),
	})
	require.NoError(t, err)
	constantFilter := MakePlan2BoolConstExprWithType(false)
	externalFilters := []*plan.Expr{leftFilter, rightFilter, bothFilter, constantFilter}
	builder.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{leftTag}},
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{rightTag}},
		{
			NodeType:          plan.Node_JOIN,
			JoinType:          plan.Node_DEDUP,
			OnDuplicateAction: plan.Node_UPDATE,
			Children:          []int32{0, 1},
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(2, externalFilters, false)
	require.Equal(t, int32(2), nodeID)
	require.Equal(t, externalFilters, cantPushdown)
	require.Empty(t, builder.qry.Nodes[0].FilterList)
	require.Empty(t, builder.qry.Nodes[1].FilterList)

	control := NewQueryBuilder(plan.Query_UPDATE, ctx, false, false)
	control.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{leftTag}},
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{rightTag}},
		{
			NodeType:          plan.Node_JOIN,
			JoinType:          plan.Node_DEDUP,
			OnDuplicateAction: plan.Node_IGNORE,
			Children:          []int32{0, 1},
		},
	}
	_, cantPushdown = control.pushdownFilters(2, []*plan.Expr{DeepCopyExpr(rightFilter)}, false)
	require.Empty(t, cantPushdown)
	require.Len(t, control.qry.Nodes[1].FilterList, 1,
		"non-mutating DEDUP actions must retain their existing one-side pushdown")
}

func TestAsofConstantFilterPushesOnlyToProbeSide(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	leftTag := builder.GenNewBindTag()
	rightTag := builder.GenNewBindTag()
	filter := MakePlan2BoolConstExprWithType(false)
	builder.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{leftTag}},
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{rightTag}},
		{NodeType: plan.Node_JOIN, JoinType: plan.Node_ASOF_LEFT, Children: []int32{0, 1}},
	}

	_, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filter}, false)
	require.Empty(t, cantPushdown)
	require.Len(t, builder.qry.Nodes[0].FilterList, 1)
	require.Empty(t, builder.qry.Nodes[1].FilterList)
}

func setupLeftJoinBase(t *testing.T) (*MockCompilerContext, *QueryBuilder, *plan.Expr, *plan.Expr, *plan.Expr) {
	t.Helper()

	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	leftTag := builder.GenNewBindTag()
	rightTag := builder.GenNewBindTag()

	intType := Type{Id: int32(types.T_int64)}

	leftIDCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: leftTag,
				ColPos: 0,
			},
		},
	}
	rightIDCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: rightTag,
				ColPos: 0,
			},
		},
	}
	leftSpaceCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: leftTag,
				ColPos: 1,
			},
		},
	}

	onExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftIDCol), DeepCopyExpr(rightIDCol),
	})
	require.NoError(t, err)

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{leftTag},
			ProjectList: []*plan.Expr{
				DeepCopyExpr(leftIDCol),
				DeepCopyExpr(leftSpaceCol),
			},
		},
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{rightTag},
			ProjectList: []*plan.Expr{
				DeepCopyExpr(rightIDCol),
			},
		},
		{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_LEFT,
			Children: []int32{0, 1},
			OnList:   []*plan.Expr{onExpr},
			ProjectList: []*plan.Expr{
				DeepCopyExpr(leftIDCol),
				DeepCopyExpr(leftSpaceCol),
				DeepCopyExpr(rightIDCol),
			},
		},
	}

	return ctx, builder, leftIDCol, rightIDCol, leftSpaceCol
}

func TestLeftJoinOrFilterKeepsLeftJoin(t *testing.T) {
	ctx, builder, leftIDCol, rightIDCol, leftSpaceCol := setupLeftJoinBase(t)

	isNotNullExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnotnull", []*plan.Expr{
		DeepCopyExpr(rightIDCol),
	})
	require.NoError(t, err)

	constExpr := &plan.Expr{
		Typ: leftIDCol.Typ,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 11},
			},
		},
	}
	eqExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftSpaceCol),
		constExpr,
	})
	require.NoError(t, err)

	filterExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*plan.Expr{
		isNotNullExpr,
		eqExpr,
	})
	require.NoError(t, err)

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterExpr}, false)
	require.Equal(t, plan.Node_LEFT, builder.qry.Nodes[nodeID].JoinType, "left join should not be rewritten to inner join")
	require.Len(t, cantPushdown, 1)

	require.Equal(t, int32(types.T_bool), filterExpr.Typ.Id)
}

func TestLeftJoinOrFilterWithConstKeepsLeftJoin(t *testing.T) {
	ctx, builder, leftIDCol, rightIDCol, leftSpaceCol := setupLeftJoinBase(t)

	rightConst := &plan.Expr{
		Typ: rightIDCol.Typ,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 5},
			},
		},
	}
	rightEqConst, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(rightIDCol),
		rightConst,
	})
	require.NoError(t, err)

	leftConst := &plan.Expr{
		Typ: leftIDCol.Typ,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 11},
			},
		},
	}
	leftEqConst, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftSpaceCol),
		leftConst,
	})
	require.NoError(t, err)

	filterExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*plan.Expr{
		rightEqConst,
		leftEqConst,
	})
	require.NoError(t, err)

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterExpr}, false)
	require.Equal(t, plan.Node_LEFT, builder.qry.Nodes[nodeID].JoinType)
	require.Len(t, cantPushdown, 1)
}

func TestLeftJoinOrFilterWithAndKeepsLeftJoin(t *testing.T) {
	ctx, builder, leftIDCol, rightIDCol, leftSpaceCol := setupLeftJoinBase(t)

	isNotNullExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnotnull", []*plan.Expr{
		DeepCopyExpr(rightIDCol),
	})
	require.NoError(t, err)

	leftEquals11, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftSpaceCol),
		{
			Typ: leftIDCol.Typ,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: 11},
				},
			},
		},
	})
	require.NoError(t, err)

	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*plan.Expr{
		isNotNullExpr,
		leftEquals11,
	})
	require.NoError(t, err)

	leftEquals12, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftSpaceCol),
		{
			Typ: leftIDCol.Typ,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: 12},
				},
			},
		},
	})
	require.NoError(t, err)

	filterExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*plan.Expr{
		orExpr,
		leftEquals12,
	})
	require.NoError(t, err)

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterExpr}, false)
	require.Equal(t, plan.Node_LEFT, builder.qry.Nodes[nodeID].JoinType)
	require.Len(t, cantPushdown, 1)
}

func TestJoinOrderPushdownKeepsOuterScopeFilterAtJoin(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	outerTag := builder.genNewBindTag()
	intType := Type{Id: int32(types.T_int64)}

	leftCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: leftTag, ColPos: 0},
		},
	}
	rightCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: rightTag, ColPos: 0},
		},
	}
	outerCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: outerTag, ColPos: 0},
		},
	}
	filterExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftCol),
		DeepCopyExpr(outerCol),
	})
	require.NoError(t, err)

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{leftTag},
			ProjectList: []*plan.Expr{DeepCopyExpr(leftCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{rightTag},
			ProjectList: []*plan.Expr{DeepCopyExpr(rightCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
		{
			NodeType:    plan.Node_JOIN,
			JoinType:    plan.Node_INNER,
			Children:    []int32{0, 1},
			ProjectList: []*plan.Expr{DeepCopyExpr(leftCol), DeepCopyExpr(rightCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterExpr}, true)
	require.Equal(t, int32(2), nodeID)
	require.Empty(t, cantPushdown)
	require.Empty(t, builder.qry.Nodes[0].FilterList)
	require.Empty(t, builder.qry.Nodes[1].FilterList)
	require.Len(t, builder.qry.Nodes[2].OnList, 1)
	require.Same(t, filterExpr, builder.qry.Nodes[2].OnList[0])
}

func TestJoinOrderPushdownKeepsOuterScopeFilterAtJoinWithExistingOnList(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	outerTag := builder.genNewBindTag()
	intType := Type{Id: int32(types.T_int64)}

	leftCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: leftTag, ColPos: 0},
		},
	}
	rightCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: rightTag, ColPos: 0},
		},
	}
	outerCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: outerTag, ColPos: 0},
		},
	}
	onExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftCol),
		DeepCopyExpr(rightCol),
	})
	require.NoError(t, err)
	filterExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(leftCol),
		DeepCopyExpr(outerCol),
	})
	require.NoError(t, err)

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{leftTag},
			ProjectList: []*plan.Expr{DeepCopyExpr(leftCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{rightTag},
			ProjectList: []*plan.Expr{DeepCopyExpr(rightCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
		{
			NodeType:    plan.Node_JOIN,
			JoinType:    plan.Node_INNER,
			Children:    []int32{0, 1},
			OnList:      []*plan.Expr{onExpr},
			ProjectList: []*plan.Expr{DeepCopyExpr(leftCol), DeepCopyExpr(rightCol)},
			Stats:       &plan.Stats{Outcnt: 10},
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterExpr}, true)
	require.Equal(t, int32(2), nodeID)
	require.Len(t, cantPushdown, 1)
	require.Same(t, filterExpr, cantPushdown[0])
	require.Empty(t, builder.qry.Nodes[0].FilterList)
	require.Empty(t, builder.qry.Nodes[1].FilterList)
	require.Len(t, builder.qry.Nodes[2].OnList, 1)
	require.Same(t, onExpr, builder.qry.Nodes[2].OnList[0])
}

func TestJoinOrderPushdownDetectsOuterScopeTagInsideExprList(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	outerTag := builder.genNewBindTag()
	intType := Type{Id: int32(types.T_int64)}

	leftCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: leftTag, ColPos: 0},
		},
	}
	outerCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: outerTag, ColPos: 0},
		},
	}
	filterExpr := &plan.Expr{
		Typ: Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				DeepCopyExpr(leftCol),
				{
					Typ: intType,
					Expr: &plan.Expr_List{List: &plan.ExprList{
						List: []*plan.Expr{DeepCopyExpr(outerCol)},
					}},
				},
			},
		}},
	}

	leftTags := map[int32]bool{leftTag: true}
	rightTags := map[int32]bool{rightTag: true}

	require.Equal(t, int8(JoinSideLeft|JoinSideOuter), getJoinSideWithOuterScope(filterExpr, leftTags, rightTags, 0))
	require.False(t, containsOnlyTags(filterExpr, leftTags))
}

func TestWindowFilterPushesDownToOwningWindowNode(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	baseTag := builder.GenNewBindTag()
	windowTag := builder.GenNewBindTag()
	intType := Type{Id: int32(types.T_int64)}

	baseCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: baseTag,
				ColPos: 0,
			},
		},
	}
	prevWindowCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: windowTag,
				ColPos: 0,
			},
		},
	}
	currentWindowCol := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: windowTag,
				ColPos: 1,
			},
		},
	}

	filterOnPrevWindow, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(prevWindowCol),
		{
			Typ: intType,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: 1},
				},
			},
		},
	})
	require.NoError(t, err)

	filterOnCurrentWindow, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(currentWindowCol),
		{
			Typ: intType,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: 1},
				},
			},
		},
	})
	require.NoError(t, err)

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{baseTag},
			ProjectList: []*plan.Expr{DeepCopyExpr(baseCol)},
		},
		{
			NodeType:    plan.Node_WINDOW,
			Children:    []int32{0},
			WindowIdx:   0,
			BindingTags: []int32{windowTag},
			WinSpecList: []*plan.Expr{DeepCopyExpr(prevWindowCol)},
		},
		{
			NodeType:    plan.Node_WINDOW,
			Children:    []int32{1},
			WindowIdx:   1,
			BindingTags: []int32{windowTag},
			WinSpecList: []*plan.Expr{DeepCopyExpr(currentWindowCol)},
		},
	}

	nodeID, cantPushdown := builder.pushdownFilters(2, []*plan.Expr{filterOnPrevWindow, filterOnCurrentWindow}, false)
	require.Equal(t, int32(2), nodeID)
	require.Empty(t, cantPushdown)
	require.Len(t, builder.qry.Nodes[2].FilterList, 2)
	require.Empty(t, builder.qry.Nodes[1].FilterList)
	require.Same(t, filterOnPrevWindow, builder.qry.Nodes[2].FilterList[0])
	require.Same(t, filterOnCurrentWindow, builder.qry.Nodes[2].FilterList[1])
}

// TestWindowNonPartitionFilterNotPushedDown verifies that a filter on a
// non-partition-by column is NOT pushed below the WINDOW node (issue #24020).
func TestWindowNonPartitionFilterNotPushedDown(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	baseTag := builder.GenNewBindTag()
	windowTag := builder.GenNewBindTag()
	intType := Type{Id: int32(types.T_int64)}

	// col-a: partition-by column
	colA := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: baseTag, ColPos: 0},
		},
	}
	// col-b: non-partition-by column
	colB := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: baseTag, ColPos: 1},
		},
	}

	winExpr := &plan.Expr{
		Typ: intType,
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				WindowFunc:  &plan.Expr{Typ: intType},
				PartitionBy: []*plan.Expr{DeepCopyExpr(colA)},
			},
		},
	}

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{baseTag},
		},
		{
			NodeType:    plan.Node_WINDOW,
			Children:    []int32{0},
			BindingTags: []int32{windowTag},
			WinSpecList: []*plan.Expr{winExpr},
		},
	}

	// Filter on non-partition-by column — must NOT be pushed down.
	filterOnB, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(colB),
		{Typ: intType, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
	})
	require.NoError(t, err)

	// Filter on partition-by column — safe to push down.
	filterOnA, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(colA),
		{Typ: intType, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 42}}}},
	})
	require.NoError(t, err)

	nodeID, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{filterOnB, filterOnA}, false)
	require.Equal(t, int32(1), nodeID)
	// filterOnB must come back as cantPushdown (not pushed below window).
	require.Len(t, cantPushdown, 1)
	require.Same(t, filterOnB, cantPushdown[0])
	// filterOnA (partition-by col) should have been pushed to the child.
	require.Empty(t, builder.qry.Nodes[1].FilterList)
}

func TestFunctionScanDoesNotDropMixedTagFilter(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	childTag := builder.GenNewBindTag()
	functionTag := builder.GenNewBindTag()
	intType := Type{Id: int32(types.T_int64)}
	childCol := GetColExpr(intType, childTag, 0)
	functionCol := GetColExpr(intType, functionTag, 0)
	filter, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*plan.Expr{childCol, functionCol})
	require.NoError(t, err)

	builder.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{childTag}},
		{NodeType: plan.Node_FUNCTION_SCAN, BindingTags: []int32{functionTag}, Children: []int32{0}},
	}

	_, cantPushdown := builder.pushdownFilters(1, []*plan.Expr{filter}, false)
	require.Len(t, cantPushdown, 1)
	require.True(t, exprStructuralEqual(filter, cantPushdown[0]))
	require.Empty(t, builder.qry.Nodes[0].FilterList)
	require.Empty(t, builder.qry.Nodes[1].FilterList)
}

func makeVectorTopPushdownBuilder(limit uint64) (*QueryBuilder, *plan.Node, *plan.Node) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	scanTag := builder.GenNewBindTag()

	vectorCol := &plan.Expr{
		Typ: Type{Id: int32(types.T_array_float32), Width: 2},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: scanTag,
				ColPos: 1,
			},
		},
	}
	orderExpr := &plan.Expr{
		Typ: Type{Id: int32(types.T_float64), NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{ObjName: metric.DistFn_L2Distance},
				Args: []*plan.Expr{
					DeepCopyExpr(vectorCol),
					MakePlan2Vecf32ConstExprWithType("[0,0]", 2),
				},
			},
		},
	}

	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{scanTag},
			TableDef:    &plan.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
			Stats:       &plan.Stats{BlockNum: 2},
		},
		{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{0},
			ProjectList: []*plan.Expr{orderExpr},
		},
		{
			NodeType: plan.Node_SORT,
			Children: []int32{1},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ: Type{Id: int32(types.T_float64), NotNullable: true},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 0},
						},
					},
				},
			},
			Limit: MakePlan2Uint64ConstExprWithType(limit),
		},
	}

	return builder, builder.qry.Nodes[0], builder.qry.Nodes[1]
}

func TestPushdownVectorIndexTopToTableScanSkipsOverflowLimit(t *testing.T) {
	builder, scanNode, projNode := makeVectorTopPushdownBuilder(maxVectorIndexTopPushdownLimit + 1)

	builder.pushdownVectorIndexTopToTableScan(2)

	require.Nil(t, scanNode.IndexReaderParam)
	require.Nil(t, projNode.ProjectList[0].GetCol())
	require.NotNil(t, projNode.ProjectList[0].GetF())
}

func TestPushdownVectorIndexTopToTableScanKeepsSupportedLimit(t *testing.T) {
	builder, scanNode, projNode := makeVectorTopPushdownBuilder(8)

	builder.pushdownVectorIndexTopToTableScan(2)

	require.NotNil(t, scanNode.IndexReaderParam)
	require.Equal(t, uint64(8), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	require.NotNil(t, projNode.ProjectList[0].GetCol())
}

func TestPushdownVectorIndexTopToTableScanSkipsDynamicLimit(t *testing.T) {
	builder, scanNode, projNode := makeVectorTopPushdownBuilder(8)
	builder.qry.Nodes[2].Limit = &plan.Expr{
		Typ:  Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}

	require.NotPanics(t, func() { builder.pushdownVectorIndexTopToTableScan(2) })
	require.Nil(t, scanNode.IndexReaderParam)
	require.NotNil(t, projNode.ProjectList[0].GetF())
}

func TestPushdownTopThroughLeftJoinSkipsOverflowingCandidateLimit(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	leftTag := builder.GenNewBindTag()
	left := &plan.Node{NodeType: plan.Node_TABLE_SCAN, NodeId: 0, BindingTags: []int32{leftTag}}
	right := &plan.Node{NodeType: plan.Node_TABLE_SCAN, NodeId: 1}
	join := &plan.Node{NodeType: plan.Node_JOIN, NodeId: 2, JoinType: plan.Node_LEFT, Children: []int32{0, 1}}
	sort := &plan.Node{
		NodeType: plan.Node_SORT,
		NodeId:   3,
		Children: []int32{2},
		OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: leftTag, ColPos: 0}},
		}}},
		Limit:  MakePlan2Uint64ConstExprWithType(math.MaxUint64),
		Offset: MakePlan2Uint64ConstExprWithType(1),
	}
	builder.qry.Nodes = []*plan.Node{left, right, join, sort}

	require.NotPanics(t, func() { builder.pushdownTopThroughLeftJoin(3) })
	require.Equal(t, []int32{0, 1}, join.Children)
	require.Len(t, builder.qry.Nodes, 4)
}
