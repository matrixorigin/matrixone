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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func setupLeftJoinBase(t *testing.T) (*MockCompilerContext, *QueryBuilder, *plan.Expr, *plan.Expr, *plan.Expr) {
	t.Helper()

	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()

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

func TestWindowFilterPushesDownToOwningWindowNode(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	baseTag := builder.genNewBindTag()
	windowTag := builder.genNewBindTag()
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
	require.Len(t, builder.qry.Nodes[2].FilterList, 1)
	require.Len(t, builder.qry.Nodes[1].FilterList, 1)
	require.Same(t, filterOnCurrentWindow, builder.qry.Nodes[2].FilterList[0])
	require.Same(t, filterOnPrevWindow, builder.qry.Nodes[1].FilterList[0])
}

func makeVectorTopPushdownBuilder(limit uint64) (*QueryBuilder, *plan.Node, *plan.Node) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	scanTag := builder.genNewBindTag()

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
