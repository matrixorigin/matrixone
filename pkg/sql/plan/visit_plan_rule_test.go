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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestResetPreparePlanCollectsHiddenIndexSchemas(t *testing.T) {
	const hiddenTable = "__mo_index_hidden"
	mock := NewMockCompilerContext(false)
	mock.objects[hiddenTable] = &planpb.ObjectRef{
		Db:         10,
		Obj:        20,
		SchemaName: "db",
		ObjName:    hiddenTable,
	}
	mock.tables[hiddenTable] = &planpb.TableDef{Name: hiddenTable, DbId: 10, TblId: 20, Version: 30}

	queryPlan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef: &planpb.ObjectRef{
					Db:         1,
					Obj:        2,
					SchemaName: "db",
					ObjName:    "src",
				},
				TableDef: &planpb.TableDef{
					Name:    "src",
					DbId:    1,
					TblId:   2,
					Version: 3,
					Indexes: []*planpb.IndexDef{{
						IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
						IndexTableName: hiddenTable,
					}},
				},
			}},
		}},
	}

	schemas, _, err := ResetPreparePlan(mock, queryPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "src", schemas[0].ObjName)
	require.Equal(t, hiddenTable, schemas[1].ObjName)
	require.Equal(t, int64(30), schemas[1].Server)
	require.Equal(t, int64(10), schemas[1].Db)
	require.Equal(t, int64(20), schemas[1].Obj)
}

func TestResetPreparePlanResetsWindowParameterOrder(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	window := &planpb.WindowSpec{
		WindowFunc: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{paramExpr(9)}}}},
		PartitionBy: []*planpb.Expr{
			paramExpr(3),
		},
		OrderBy: []*planpb.OrderBySpec{{Expr: paramExpr(7)}},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: paramExpr(1)},
			End:   &planpb.FrameBound{Val: paramExpr(5)},
		},
	}
	queryPlan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeId:      0,
				NodeType:    planpb.Node_WINDOW,
				WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}},
			}},
		}},
	}

	_, paramTypes, err := ResetPreparePlan(NewMockCompilerContext(false), queryPlan)
	require.NoError(t, err)
	require.Len(t, paramTypes, 5)
	require.Equal(t, int32(4), window.WindowFunc.GetF().Args[0].GetP().Pos)
	require.Equal(t, int32(1), window.PartitionBy[0].GetP().Pos)
	require.Equal(t, int32(3), window.OrderBy[0].Expr.GetP().Pos)
	require.Equal(t, int32(0), window.Frame.Start.Val.GetP().Pos)
	require.Equal(t, int32(2), window.Frame.End.Val.GetP().Pos)
}

func TestResetParamRefRuleReplacesWindowParameters(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	windowFunc, err := BindFuncExprImplByPlanExpr(context.Background(), "abs", []*planpb.Expr{paramExpr(0)})
	require.NoError(t, err)
	window := &planpb.WindowSpec{
		WindowFunc:  windowFunc,
		PartitionBy: []*planpb.Expr{paramExpr(1)},
		OrderBy:     []*planpb.OrderBySpec{{Expr: paramExpr(2)}},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: paramExpr(3)},
			End:   &planpb.FrameBound{Val: paramExpr(4)},
		},
	}
	node := &planpb.Node{
		NodeId:      0,
		NodeType:    planpb.Node_WINDOW,
		WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}},
	}
	query := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	params := []*planpb.Expr{
		makePlan2Int64ConstExprWithType(10),
		makePlan2Int64ConstExprWithType(11),
		makePlan2Int64ConstExprWithType(12),
		makePlan2Int64ConstExprWithType(13),
		makePlan2Int64ConstExprWithType(14),
	}
	visitor := NewVisitPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{NewResetParamRefRule(context.Background(), params)})

	require.NoError(t, visitor.Visit(context.Background()))
	require.Equal(t, int64(10), window.WindowFunc.GetF().Args[0].GetLit().GetI64Val())
	require.Equal(t, int64(11), window.PartitionBy[0].GetLit().GetI64Val())
	require.Equal(t, int64(12), window.OrderBy[0].Expr.GetLit().GetI64Val())
	require.Equal(t, int64(13), window.Frame.Start.Val.GetLit().GetI64Val())
	require.Equal(t, int64(14), window.Frame.End.Val.GetLit().GetI64Val())
}

func TestVisitPlanDeduplicatesAliasedWindowPartitionExpr(t *testing.T) {
	newPlan := func(t *testing.T) (*planpb.Plan, *planpb.WindowSpec, *planpb.Node) {
		t.Helper()
		paramExpr := func(pos int32) *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.GenNewBindTag()
		inputID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, bindCtx)
		window := &planpb.WindowSpec{
			WindowFunc:  paramExpr(1),
			PartitionBy: []*planpb.Expr{paramExpr(3)},
		}
		bindCtx.windows = []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}}
		windowID, err := builder.appendWindowNode(bindCtx, inputID, nil)
		require.NoError(t, err)
		windowNode := builder.qry.Nodes[windowID]
		partitionNode := builder.qry.Nodes[windowNode.Children[0]]
		require.Equal(t, planpb.Node_PARTITION, partitionNode.NodeType)
		require.Same(t, window.PartitionBy[0], partitionNode.OrderBy[0].Expr)
		return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
			Steps: []int32{windowID},
			Nodes: builder.qry.Nodes,
		}}}, window, partitionNode
	}

	t.Run("collects once", func(t *testing.T) {
		queryPlan, _, _ := newPlan(t)
		rule := NewGetParamRule()
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		rule.SetParamOrder()
		require.Equal(t, map[int]int{1: 0, 3: 1}, rule.params)
	})

	t.Run("resets the shared partition expression once", func(t *testing.T) {
		queryPlan, window, partitionNode := newPlan(t)
		rule := NewResetParamOrderRule(map[int]int{1: 0, 3: 1})
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		require.Equal(t, int32(0), window.WindowFunc.GetP().Pos)
		require.Equal(t, int32(1), window.PartitionBy[0].GetP().Pos)
		require.Equal(t, int32(1), partitionNode.OrderBy[0].Expr.GetP().Pos)
	})

	t.Run("replaces the shared partition expression once", func(t *testing.T) {
		queryPlan, window, partitionNode := newPlan(t)
		rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
			makePlan2Int64ConstExprWithType(7),
			makePlan2Int64ConstExprWithType(11),
			nil,
			makePlan2Int64ConstExprWithType(13),
		})
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		require.Equal(t, int64(11), window.WindowFunc.GetLit().GetI64Val())
		require.Equal(t, int64(13), window.PartitionBy[0].GetLit().GetI64Val())
		require.Same(t, partitionNode.OrderBy[0].Expr, window.PartitionBy[0])
	})
}
