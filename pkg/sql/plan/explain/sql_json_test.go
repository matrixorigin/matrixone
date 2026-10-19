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

package explain

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	planpkg "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

func sqlJSONTestColumn(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:       name,
		OriginName: name,
		Typ:        plan.Type{Id: int32(types.T_int32)},
	}
}

func TestBuildSQLJSONPlanHasCoreShapeAndFiniteStats(t *testing.T) {
	query := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   10,
				NodeType: plan.Node_FILTER,
				Children: []int32{20},
				Stats:    &plan.Stats{Cost: math.NaN(), Outcnt: math.Inf(1), Rowsize: 8},
			},
			{
				NodeId:   20,
				NodeType: plan.Node_TABLE_SCAN,
				TableDef: &plan.TableDef{DbName: "db", Name: "t", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}},
			},
		},
	}
	statsBefore := *query.Nodes[0].Stats

	data, err := BuildSQLJSONPlan(context.Background(), query)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, float64(1), decoded["query_block"].(map[string]any)["select_id"])

	matrixone := decoded["matrixone"].(map[string]any)
	require.Equal(t, float64(1), matrixone["schema_version"])
	require.Len(t, matrixone["steps"], 1)
	require.Len(t, matrixone["nodes"], 2)
	require.Len(t, matrixone["edges"], 1)

	table := decoded["query_block"].(map[string]any)["table"].(map[string]any)
	require.Equal(t, "db.t", table["table_name"])
	require.NotContains(t, string(data), "NaN")
	require.NotContains(t, string(data), "Inf")
	statsAfter := query.Nodes[0].Stats
	require.Equal(t, math.Float64bits(statsBefore.Cost), math.Float64bits(statsAfter.Cost))
	require.Equal(t, math.Float64bits(statsBefore.Outcnt), math.Float64bits(statsAfter.Outcnt))
	require.Equal(t, statsBefore.Rowsize, statsAfter.Rowsize)
	require.True(t, math.IsNaN(statsAfter.Cost))
	require.True(t, math.IsInf(statsAfter.Outcnt, 1))
}

func TestExplainPlanJSONWritesOneDocumentLine(t *testing.T) {
	query := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*plan.Node{{NodeId: 1, NodeType: plan.Node_VALUE_SCAN}},
	}
	buffer := NewExplainDataBuffer()
	err := NewExplainQueryImpl(query).ExplainPlan(context.Background(), buffer, &ExplainOptions{
		Format: EXPLAIN_FORMAT_JSON,
	})
	require.NoError(t, err)
	require.Len(t, buffer.Lines, 1)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(buffer.Lines[0]), &decoded))
	require.Contains(t, decoded, "query_block")
	matrixone := decoded["matrixone"].(map[string]any)
	require.Equal(t, []any{}, matrixone["edges"], "leaf plans must encode an empty edge array")
}

func TestBuildSQLJSONPlanDeduplicatesSharedNodes(t *testing.T) {
	query := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 1, NodeType: plan.Node_JOIN, Children: []int32{2, 2}},
			{NodeId: 2, NodeType: plan.Node_TABLE_SCAN,
				TableDef: &plan.TableDef{Name: "t", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}}},
		},
	}

	data, err := BuildSQLJSONPlan(context.Background(), query)
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
			Edges []sqlJSONEdge `json:"edges"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes, 2)
	require.Len(t, decoded.MatrixOne.Edges, 2)
	require.Equal(t, []string{"2", "2"}, decoded.MatrixOne.Nodes[0].Inputs)
}

func TestBuildSQLJSONPlanUsesBoundTableAlias(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   7,
			NodeType: plan.Node_TABLE_SCAN,
			TableDef: &plan.TableDef{
				DbName: "db",
				Name:   "source",
				Cols:   []*plan.ColDef{sqlJSONTestColumn("id")},
			},
			ProjectList: []*plan.Expr{{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					ColPos:  0,
					Name:    "alias.id",
					TblName: "source",
				}},
			}},
		}},
	})
	require.NoError(t, err)
	var decoded struct {
		QueryBlock struct {
			Table *sqlJSONTable `json:"table"`
		} `json:"query_block"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "alias", decoded.QueryBlock.Table.TableName)
}

func TestBuildSQLJSONPlanDoesNotTreatQualifiedPhysicalNameAsAlias(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   7,
			NodeType: plan.Node_TABLE_SCAN,
			TableDef: &plan.TableDef{DbName: "db", Name: "source", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}},
			ProjectList: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "db.source.id"}},
			}},
		}},
	})
	require.NoError(t, err)
	var decoded struct {
		QueryBlock struct {
			Table *sqlJSONTable `json:"table"`
		} `json:"query_block"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "db.source", decoded.QueryBlock.Table.TableName)
}

func TestBuildSQLJSONPlanPreservesCommonNodeSemantics(t *testing.T) {
	query := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   1,
			NodeType: plan.Node_TABLE_SCAN,
			TableDef: &plan.TableDef{DbName: "db", Name: "t", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}},
			ProjectList: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "t.id"}},
			}},
			FilterList:      []*plan.Expr{sqlJSONTestInt32(1)},
			BlockFilterList: []*plan.Expr{sqlJSONTestInt32(2)},
			Limit:           sqlJSONTestInt32(3),
			Offset:          sqlJSONTestInt32(4),
		}},
	}

	data, err := BuildSQLJSONPlan(context.Background(), query)
	require.NoError(t, err)
	var decoded struct {
		QueryBlock struct {
			Table *sqlJSONTable `json:"table"`
		} `json:"query_block"`
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "1", decoded.MatrixOne.Nodes[0].Filter)
	require.Equal(t, "2", decoded.MatrixOne.Nodes[0].BlockFilter)
	require.Equal(t, "3", decoded.MatrixOne.Nodes[0].Limit)
	require.Equal(t, "4", decoded.MatrixOne.Nodes[0].Offset)
	require.Equal(t, "1", decoded.QueryBlock.Table.AttachedCondition)
}

func TestBuildSQLJSONPlanMapsAggFilterToHaving(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:     1,
			NodeType:   plan.Node_AGG,
			FilterList: []*plan.Expr{sqlJSONTestInt32(5)},
		}},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "5", decoded.MatrixOne.Nodes[0].Having)
	require.Empty(t, decoded.MatrixOne.Nodes[0].Filter)
}

func TestBuildSQLJSONPlanPreservesWindowOrderAndFrame(t *testing.T) {
	registered, err := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	require.NoError(t, err)
	windowExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			WindowFunc: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: "sum"},
				Args: []*plan.Expr{{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "id"}}}},
			}}},
			PartitionBy: []*plan.Expr{{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "grp"}}}},
			OrderBy: []*plan.OrderBySpec{{
				Expr:      &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "id"}}},
				Collation: "utf8mb4_bin",
				Flag:      plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST,
			}},
			Frame: &plan.FrameClause{
				Type:  plan.FrameClause_RANGE,
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
				End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			},
		}},
	}
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*plan.Node{{NodeId: 1, NodeType: plan.Node_WINDOW, WinSpecList: []*plan.Expr{windowExpr}}},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes[0].Windows, 1)
	window := decoded.MatrixOne.Nodes[0].Windows[0]
	require.Equal(t, "DESC", window.OrderBy[0].Direction)
	require.Equal(t, "LAST", window.OrderBy[0].Nulls)
	require.Equal(t, "utf8mb4_bin", window.OrderBy[0].Collation)
	require.Equal(t, "RANGE", window.Frame.Unit)
	require.Equal(t, "UNBOUNDED PRECEDING", window.Frame.Start)
	require.Equal(t, "CURRENT ROW", window.Frame.End)
}

func TestBuildSQLJSONPlanRejectsAmbiguousOrderFlags(t *testing.T) {
	_, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   1,
			NodeType: plan.Node_SORT,
			OrderBy: []*plan.OrderBySpec{{
				Expr: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "id"}}},
				Flag: plan.OrderBySpec_ASC | plan.OrderBySpec_DESC,
			}},
		}},
	})
	require.Error(t, err)
}

func TestBuildSQLJSONPlanPreservesUpdateAssignments(t *testing.T) {
	table := &plan.TableDef{
		DbName: "db",
		Name:   "t",
		Cols:   []*plan.ColDef{sqlJSONTestColumn("id"), sqlJSONTestColumn("v")},
	}
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_UPDATE,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   1,
				NodeType: plan.Node_MULTI_UPDATE,
				Children: []int32{2},
				UpdateCtxList: []*plan.UpdateCtx{{
					ObjRef:     &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
					TableDef:   table,
					InsertCols: []plan.ColRef{{RelPos: 42, ColPos: 0}, {RelPos: 42, ColPos: 1}},
				}},
			},
			{
				NodeId:      2,
				NodeType:    plan.Node_PROJECT,
				BindingTags: []int32{42},
				ProjectList: []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(2)},
			},
		},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes[0].Assignments, 2)
	require.Equal(t, sqlJSONAssignment{Target: "db.t.v", Value: "2"}, decoded.MatrixOne.Nodes[0].Assignments[1])
}

func TestBuildSQLJSONPlanResolvesPrunedUpdateAssignments(t *testing.T) {
	table := &plan.TableDef{
		DbName: "db",
		Name:   "t",
		Cols: []*plan.ColDef{
			sqlJSONTestColumn("id"),
			sqlJSONTestColumn("v"),
			sqlJSONTestColumn(catalog.Row_ID),
		},
	}
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_UPDATE,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   1,
				NodeType: plan.Node_MULTI_UPDATE,
				Children: []int32{2},
				UpdateCtxList: []*plan.UpdateCtx{{
					ObjRef:     &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
					TableDef:   table,
					InsertCols: []plan.ColRef{{RelPos: 0, ColPos: 0}, {RelPos: 0, ColPos: 1}},
				}},
			},
			{
				NodeId:   2,
				NodeType: plan.Node_LOCK_OP,
				Children: []int32{3},
				ProjectList: []*plan.Expr{
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2}}},
				},
			},
			{
				NodeId:      3,
				NodeType:    plan.Node_PROJECT,
				ProjectList: []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(2), sqlJSONTestInt32(3)},
			},
		},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes[0].Assignments, 2)
	require.Equal(t, sqlJSONAssignment{Target: "db.t.id", Value: "1"}, decoded.MatrixOne.Nodes[0].Assignments[0])
	require.Equal(t, sqlJSONAssignment{Target: "db.t.v", Value: "2"}, decoded.MatrixOne.Nodes[0].Assignments[1])
}

func TestBuildSQLJSONPlanResolvesUpdateExpressionInputs(t *testing.T) {
	ref := func(rel, col int32, name string) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: col, Name: name}}}
	}
	// Both arguments read the same right input: reuse is not a reference cycle.
	sum, err := planpkg.BindFuncExprImplByPlanExpr(t.Context(), "+", []*plan.Expr{ref(1, 0, ""), ref(1, 0, "")})
	require.NoError(t, err)
	query := &plan.Query{StmtType: plan.Query_UPDATE, Steps: []int32{0}, Nodes: []*plan.Node{
		{NodeId: 1, NodeType: plan.Node_MULTI_UPDATE, Children: []int32{2}, UpdateCtxList: []*plan.UpdateCtx{{
			TableDef:   &plan.TableDef{DbName: "db", Name: "t", Cols: []*plan.ColDef{sqlJSONTestColumn("id"), sqlJSONTestColumn("v"), sqlJSONTestColumn("w")}},
			InsertCols: []plan.ColRef{{RelPos: 0, ColPos: 0, Name: "target.id"}, {RelPos: 0, ColPos: 1}, {RelPos: 0, ColPos: 2}},
		}}},
		{NodeId: 2, NodeType: plan.Node_LOCK_OP, Children: []int32{3}, ProjectList: []*plan.Expr{ref(0, 1, ""), ref(0, 0, ""), ref(0, 2, "")}},
		{NodeId: 3, NodeType: plan.Node_JOIN, Children: []int32{4, 5}, ProjectList: []*plan.Expr{sum, ref(0, 0, ""), ref(0, 1, "left.v")}},
		{NodeId: 4, NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(99)}},
		{NodeId: 5, NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{sqlJSONTestInt32(2)}},
	}}
	before, err := json.Marshal(query)
	require.NoError(t, err)
	data, err := BuildSQLJSONPlan(t.Context(), query)
	require.NoError(t, err)
	var decoded sqlJSONPlan
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, []sqlJSONAssignment{
		{Target: "db.t.id", Value: "1"},
		{Target: "db.t.v", Value: "(2 + 2)"},
		{Target: "db.t.w", Value: "left.v"},
	}, decoded.MatrixOne.Nodes[0].Assignments)
	after, err := json.Marshal(query)
	require.NoError(t, err)
	require.Equal(t, before, after, "rendering must not mutate execution expressions")
	second, err := BuildSQLJSONPlan(t.Context(), query)
	require.NoError(t, err)
	require.Equal(t, data, second)
}

func TestResolveUpdateExpressionBoundaries(t *testing.T) {
	ref := func(rel, col int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: col}}}
	}
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want string
	}{
		{"logical_chain", ref(42, 0), "7"},
		{"null", &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}, "(null)"},
		{"list", &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{ref(42, 0), sqlJSONTestInt32(8)}}}}, "7, 8"},
		{"missing_binding", ref(43, 0), ""},
		{"negative_column", ref(42, -1), ""},
		{"out_of_range", ref(42, 1), ""},
		{"nil_column", &plan.Expr{Expr: &plan.Expr_Col{}}, ""},
		{"nil_function", &plan.Expr{Expr: &plan.Expr_F{}}, ""},
		{"nil_list", &plan.Expr{Expr: &plan.Expr_List{}}, ""},
		{"nil_expression", nil, ""},
		{"logical_cycle", ref(41, 0), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			producer := &plan.Node{ProjectList: []*plan.Expr{tc.expr}}
			value := &plan.Node{ProjectList: []*plan.Expr{sqlJSONTestInt32(7)}}
			builder := &sqlJSONPlanBuilder{ctx: t.Context(), bindings: map[int32][]*plan.Node{41: {producer}, 42: {value}}}
			expr, ok := builder.resolveUpdateColumnExpr(nil, &plan.ColRef{RelPos: 41, ColPos: 0})
			if tc.want == "" {
				require.False(t, ok)
				return
			}
			require.True(t, ok)
			text, err := sqlJSONExpr(t.Context(), expr, &ExplainOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.want, text)
		})
	}
	// A malformed local slot must not fall back to an unrelated binding tag 0.
	builder := &sqlJSONPlanBuilder{ctx: t.Context(), byID: map[int32]*plan.Node{2: {}}, bindings: map[int32][]*plan.Node{0: {{ProjectList: []*plan.Expr{sqlJSONTestInt32(7)}}}}}
	_, ok := builder.resolveUpdateColumnExpr(&plan.Node{Children: []int32{2}}, &plan.ColRef{})
	require.False(t, ok)
}

func TestResolveUpdateProducerOutputs(t *testing.T) {
	ref := func(rel, col int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: col}}}
	}
	for _, tc := range []struct {
		name     string
		kind     plan.Node_NodeType
		rel, col int32
		want     string
		change   func(*plan.Node, *plan.Query)
		cancel   bool
	}{
		{name: "source_projection_reordering", kind: plan.Node_SINK_SCAN, want: "7"},
		{name: "missing_source", kind: plan.Node_SINK_SCAN, change: func(n *plan.Node, _ *plan.Query) { n.SourceStep = nil }},
		{name: "multiple_sources", kind: plan.Node_SINK_SCAN, change: func(n *plan.Node, _ *plan.Query) { n.SourceStep = []int32{0, 1} }},
		{name: "negative_source", kind: plan.Node_SINK_SCAN, change: func(n *plan.Node, _ *plan.Query) { n.SourceStep = []int32{-1} }},
		{name: "source_out_of_range", kind: plan.Node_SINK_SCAN, change: func(n *plan.Node, _ *plan.Query) { n.SourceStep = []int32{2} }},
		{name: "missing_root", kind: plan.Node_SINK_SCAN, change: func(_ *plan.Node, q *plan.Query) { q.Steps[1] = 99 }},
		{name: "wrong_source_type", kind: plan.Node_SINK_SCAN, change: func(_ *plan.Node, q *plan.Query) { q.Nodes[1].NodeType = plan.Node_PROJECT }},
		{name: "source_slot_out_of_range", kind: plan.Node_SINK_SCAN, col: 2},
		{name: "source_cycle", kind: plan.Node_SINK_SCAN, change: func(n *plan.Node, q *plan.Query) { q.Nodes[1].Children = []int32{n.NodeId} }},
		{name: "group_slot", kind: plan.Node_AGG, rel: -1, want: "8"},
		{name: "positional_group_slot", kind: plan.Node_AGG, rel: -2, want: "8"},
		{name: "aggregate_slot", kind: plan.Node_AGG, rel: -2, col: 1, want: "any_value(7)"},
		{name: "aggregate_out_of_range", kind: plan.Node_AGG, rel: -2, col: 2},
		{name: "window_slot", kind: plan.Node_WINDOW, rel: -1, col: 2, want: "row_number(); Partition By: 7; Order By: 8"},
		{name: "window_input_slot", kind: plan.Node_WINDOW, rel: -1, col: 1},
		{name: "window_out_of_range", kind: plan.Node_WINDOW, rel: -1, col: 3},
		{name: "missing_window", kind: plan.Node_WINDOW, rel: -1, col: 2, change: func(n *plan.Node, _ *plan.Query) { n.WinSpecList = nil }},
		{name: "union_both_branches", kind: plan.Node_UNION_ALL, want: "UNION_ALL[branch 1: 7; branch 2: 9]"},
		{name: "union_null_branch", kind: plan.Node_UNION, col: 1, want: "UNION[branch 1: 8; branch 2: (null)]"},
		{name: "union_out_of_range", kind: plan.Node_UNION_ALL, col: 2},
		{name: "missing_union_input", kind: plan.Node_UNION_ALL, change: func(n *plan.Node, _ *plan.Query) { n.Children[1] = 99 }},
		{name: "union_cycle", kind: plan.Node_UNION_ALL, change: func(n *plan.Node, _ *plan.Query) { n.Children[1] = n.NodeId }},
		{name: "mark_predicate_inputs", kind: plan.Node_JOIN, rel: -1, want: "MARK[on: (7 = 9)]"},
		{name: "mark_multiple_predicates", kind: plan.Node_JOIN, rel: -1, want: "MARK[on: (7 = 9) AND (8 = (null))]", change: func(n *plan.Node, _ *plan.Query) {
			n.OnList = append(n.OnList, &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: n.OnList[0].GetF().Func, Args: []*plan.Expr{ref(0, 1), ref(1, 1)}}}})
		}},
		{name: "mark_no_predicates", kind: plan.Node_JOIN, rel: -1, want: "MARK[on: TRUE]", change: func(n *plan.Node, _ *plan.Query) { n.OnList = nil }},
		{name: "mark_invalid_slot", kind: plan.Node_JOIN, rel: -1, col: 1},
		{name: "non_mark_join", kind: plan.Node_JOIN, rel: -1, change: func(n *plan.Node, _ *plan.Query) { n.JoinType = plan.Node_INNER }},
		{name: "mark_missing_input", kind: plan.Node_JOIN, rel: -1, change: func(n *plan.Node, _ *plan.Query) { n.Children[1] = 99 }},
		{name: "mark_predicate_cycle", kind: plan.Node_JOIN, rel: -1, change: func(n *plan.Node, _ *plan.Query) { n.OnList = n.ProjectList }},
		{name: "mark_canceled", kind: plan.Node_JOIN, rel: -1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaf := &plan.Node{NodeId: 10, NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{sqlJSONTestInt32(7), sqlJSONTestInt32(8)}}
			sink := &plan.Node{NodeId: 20, NodeType: plan.Node_SINK, Children: []int32{10}, ProjectList: []*plan.Expr{ref(0, 1), ref(0, 0)}}
			node := &plan.Node{NodeId: 30, NodeType: tc.kind, Children: []int32{10}, ProjectList: []*plan.Expr{ref(tc.rel, tc.col)}}
			right := &plan.Node{NodeId: 40, NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{sqlJSONTestInt32(9), {Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}}}
			query := &plan.Query{Nodes: []*plan.Node{leaf, sink, node, right}, Steps: []int32{2, 1}}
			switch tc.kind {
			case plan.Node_SINK_SCAN:
				node.Children, node.SourceStep = nil, []int32{1}
				node.ProjectList = []*plan.Expr{ref(0, tc.col+1)}
			case plan.Node_AGG:
				node.GroupBy = []*plan.Expr{ref(0, 1)}
				value, err := planpkg.BindFuncExprImplByPlanExpr(t.Context(), "any_value", []*plan.Expr{ref(0, 0)})
				require.NoError(t, err)
				node.AggList = []*plan.Expr{value}
			case plan.Node_WINDOW:
				function, err := planpkg.BindFuncExprImplByPlanExpr(t.Context(), "row_number", nil)
				require.NoError(t, err)
				node.WinSpecList = []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{WindowFunc: function, PartitionBy: []*plan.Expr{ref(0, 0)}, OrderBy: []*plan.OrderBySpec{{Expr: ref(0, 1)}}}}}}
			case plan.Node_UNION, plan.Node_UNION_ALL:
				node.Children = []int32{10, 40}
			case plan.Node_JOIN:
				node.JoinType, node.Children = plan.Node_MARK, []int32{10, 40}
				predicate, err := planpkg.BindFuncExprImplByPlanExpr(t.Context(), "=", []*plan.Expr{ref(0, 0), ref(1, 0)})
				require.NoError(t, err)
				node.OnList = []*plan.Expr{predicate}
			}
			if tc.change != nil {
				tc.change(node, query)
			}
			before, err := json.Marshal(query)
			require.NoError(t, err)
			ctx := t.Context()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			builder := &sqlJSONPlanBuilder{ctx: ctx, query: query, byID: map[int32]*plan.Node{10: leaf, 20: sink, 30: node, 40: right}}
			expr, ok := builder.resolveUpdateExpr(node, node.ProjectList[0], make(map[sqlJSONUpdateExprKey]bool))
			if tc.want == "" {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				value, err := sqlJSONExpr(t.Context(), expr, &ExplainOptions{})
				require.NoError(t, err)
				require.Equal(t, tc.want, value)
			}
			after, err := json.Marshal(query)
			require.NoError(t, err)
			require.Equal(t, before, after, "producer expansion must not mutate the plan")
		})
	}
}

func TestBuildSQLJSONPlanUsesBoundAliasesForSelfJoin(t *testing.T) {
	newScan := func(id int32, alias string) *plan.Node {
		return &plan.Node{
			NodeId:   id,
			NodeType: plan.Node_TABLE_SCAN,
			TableDef: &plan.TableDef{Name: "source", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}},
			ProjectList: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: alias + ".id"}},
			}},
		}
	}
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 1, NodeType: plan.Node_JOIN, Children: []int32{2, 3}},
			newScan(2, "a"),
			newScan(3, "b"),
		},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	var names []string
	for _, node := range decoded.MatrixOne.Nodes {
		if strings.Contains(strings.ToLower(node.Operator), "table scan") || strings.Contains(strings.ToLower(node.Operator), "table_scan") {
			names = append(names, node.TableName)
		}
	}
	require.ElementsMatch(t, []string{"a", "b"}, names)
}

func sqlJSONTestInt32(value int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: value},
		}},
	}
}

func TestBuildSQLJSONPlanRejectsInvalidReachability(t *testing.T) {
	_, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   1,
			NodeType: plan.Node_PROJECT,
			Children: []int32{99},
		}},
	})
	require.Error(t, err)
}

func TestBuildSQLJSONPlanRejectsCycles(t *testing.T) {
	_, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 1, NodeType: plan.Node_PROJECT, Children: []int32{2}},
			{NodeId: 2, NodeType: plan.Node_FILTER, Children: []int32{1}},
		},
	})
	require.Error(t, err)
}

func TestBuildSQLJSONPlanIgnoresUnreachableNodes(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 1, NodeType: plan.Node_PROJECT},
			{NodeId: 99, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 99, NodeType: plan.Node_FILTER},
		},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes, 1)
	require.Equal(t, "1", decoded.MatrixOne.Nodes[0].ID)
}

func TestBuildSQLJSONPlanCoversOperatorDetails(t *testing.T) {
	value := func(n int32) *plan.Expr { return sqlJSONTestInt32(n) }
	orderBy := func(flag plan.OrderBySpec_OrderByFlag) *plan.OrderBySpec {
		return &plan.OrderBySpec{
			Expr:      value(1),
			Flag:      flag,
			Collation: "utf8mb4_bin",
		}
	}
	window := &plan.Expr{Expr: &plan.Expr_W{W: &plan.WindowSpec{
		WindowFunc:  value(11),
		PartitionBy: []*plan.Expr{value(12)},
		OrderBy:     []*plan.OrderBySpec{orderBy(plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST)},
		Frame: &plan.FrameClause{
			Type:  plan.FrameClause_ROWS,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: value(2)},
			End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: value(3)},
		},
	}}}
	table := &plan.TableDef{
		DbName: "db",
		Name:   "target",
		Cols:   []*plan.ColDef{sqlJSONTestColumn("id"), sqlJSONTestColumn("v")},
	}
	functionTable := &plan.TableDef{TblFunc: &plan.TableFunction{Name: "generate_series"}}
	query := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 1, NodeType: plan.Node_JOIN, Children: []int32{2, 20}, JoinType: plan.Node_LEFT, OnList: []*plan.Expr{value(99)}},
			{NodeId: 2, NodeType: plan.Node_AGG, Children: []int32{3}, FilterList: []*plan.Expr{value(5)}, GroupBy: []*plan.Expr{value(6)}, AggList: []*plan.Expr{value(7)}},
			{NodeId: 3, NodeType: plan.Node_SORT, Children: []int32{4}, OrderBy: []*plan.OrderBySpec{orderBy(plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_UNIQUE)}, Limit: value(8), Offset: value(9)},
			{NodeId: 4, NodeType: plan.Node_WINDOW, Children: []int32{5}, WinSpecList: []*plan.Expr{window}},
			{NodeId: 5, NodeType: plan.Node_TIME_WINDOW, Children: []int32{6}, TimeWindowPartitionBy: []*plan.Expr{value(10)}, Interval: value(11), Sliding: value(12), Timestamp: value(13), WEnd: value(14), GapFillStart: value(15), GapFillEnd: value(16)},
			{NodeId: 6, NodeType: plan.Node_FILL, FillType: plan.Node_VALUE, Children: []int32{7}, FillVal: []*plan.Expr{value(17)}},
			{NodeId: 7, NodeType: plan.Node_PARTITION, Children: []int32{8}, OrderBy: []*plan.OrderBySpec{orderBy(plan.OrderBySpec_DESC)}, Limit: value(18), Offset: value(19)},
			{NodeId: 8, NodeType: plan.Node_PROJECT, Children: []int32{9}, ProjectList: []*plan.Expr{value(20)}},
			{NodeId: 9, NodeType: plan.Node_VALUE_SCAN, Children: []int32{10}, ProjectList: []*plan.Expr{value(21)}},
			{NodeId: 10, NodeType: plan.Node_UNION, Children: []int32{11}, ProjectList: []*plan.Expr{value(22)}},
			{NodeId: 11, NodeType: plan.Node_UNION_ALL, Children: []int32{12}, ProjectList: []*plan.Expr{value(23)}},
			{NodeId: 12, NodeType: plan.Node_INTERSECT, Children: []int32{13}, ProjectList: []*plan.Expr{value(24)}},
			{NodeId: 13, NodeType: plan.Node_INTERSECT_ALL, Children: []int32{14}, ProjectList: []*plan.Expr{value(25)}},
			{NodeId: 14, NodeType: plan.Node_MINUS, Children: []int32{15}, ProjectList: []*plan.Expr{value(26)}},
			{NodeId: 15, NodeType: plan.Node_MINUS_ALL, Children: []int32{16}, ProjectList: []*plan.Expr{value(27)}},
			{NodeId: 16, NodeType: plan.Node_FUNCTION_SCAN, Children: []int32{17}, TableDef: functionTable, TblFuncExprList: []*plan.Expr{value(28)}},
			{NodeId: 17, NodeType: plan.Node_EXTERNAL_FUNCTION, Children: []int32{18}, TableDef: functionTable, TblFuncExprList: []*plan.Expr{value(29)}},
			{NodeId: 18, NodeType: plan.Node_TABLE_SCAN, TableDef: table},
			{NodeId: 20, NodeType: plan.Node_INSERT, Children: []int32{21}, InsertCtx: &plan.InsertCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "target"}, TableDef: table}},
			{NodeId: 21, NodeType: plan.Node_DELETE, Children: []int32{22}, DeleteCtx: &plan.DeleteCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "target"}, TableDef: table}},
			{NodeId: 22, NodeType: plan.Node_PRE_INSERT, Children: []int32{23}, PreInsertCtx: &plan.PreInsertCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "target"}, TableDef: table, CompPkeyExpr: value(30), ClusterByExpr: value(31)}},
			{NodeId: 23, NodeType: plan.Node_MULTI_UPDATE, Children: []int32{24}, UpdateCtxList: []*plan.UpdateCtx{{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "target"}, TableDef: table, InsertCols: []plan.ColRef{{RelPos: 24, ColPos: 0}, {RelPos: 24, ColPos: 1}}}}},
			{NodeId: 24, NodeType: plan.Node_POSTDML, BindingTags: []int32{24}, ProjectList: []*plan.Expr{value(32), value(33)}, PostDmlCtx: &plan.PostDmlCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "target"}}},
		},
	}

	data, err := BuildSQLJSONPlan(context.Background(), query)
	require.NoError(t, err)
	var decoded struct {
		QueryBlock struct {
			Table *sqlJSONTable `json:"table"`
		} `json:"query_block"`
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	byID := make(map[string]sqlJSONNode, len(decoded.MatrixOne.Nodes))
	for _, node := range decoded.MatrixOne.Nodes {
		byID[node.ID] = node
	}
	require.Len(t, byID, 23)
	require.Equal(t, "LEFT", byID["1"].JoinType)
	require.Equal(t, "99", byID["1"].Join)
	require.Equal(t, "5", byID["2"].Having)
	require.Equal(t, "6", byID["2"].GroupBy)
	require.Equal(t, "7", byID["2"].Aggregate)
	require.Equal(t, "8", byID["3"].Limit)
	require.Equal(t, "9", byID["3"].Offset)
	require.Len(t, byID["3"].OrderBySpecs, 1)
	require.Len(t, byID["4"].Windows, 1)
	require.Len(t, byID["5"].Expressions, 7)
	require.Equal(t, []string{"fill_type=VALUE", "fill_value[0]=17"}, byID["6"].Expressions)
	require.Equal(t, "18", byID["7"].Limit)
	require.Equal(t, []string{"28"}, byID["16"].Expressions)
	require.Equal(t, []string{"29"}, byID["17"].Expressions)
	require.Equal(t, "generate_series", byID["16"].TableName)
	require.Equal(t, "generate_series", byID["17"].TableName)
	require.Equal(t, "db.target", byID["20"].TableName)
	require.Equal(t, "db.target", byID["21"].TableName)
	require.Equal(t, []string{"30", "31"}, byID["22"].Expressions)
	require.Len(t, byID["23"].Assignments, 2)
	require.Equal(t, "db.target", byID["24"].TableName)
	require.Equal(t, "db.target", decoded.QueryBlock.Table.TableName)
}

func TestBuildSQLJSONPlanPreservesVectorIndexPayload(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   0,
			NodeType: plan.Node_INDEX_SEARCH_SCAN,
			ObjRef:   &plan.ObjectRef{SchemaName: "db", ObjName: "items"},
			TableDef: &plan.TableDef{Name: "items"},
			IndexSearchScan: &plan.IndexSearchScan{
				Index:            &plan.IndexDef{IndexName: "idx_embedding"},
				DistanceFunction: "l2_distance",
				QueryPayload:     sqlJSONTestInt32(1),
				CandidateLimit:   sqlJSONTestInt32(10),
				AlgoExprNames:    []string{"first_round_limit"},
				AlgoExprs:        []*plan.Expr{sqlJSONTestInt32(4)},
				PreFilters:       []*plan.Expr{sqlJSONTestInt32(2)},
			},
		}},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes, 1)
	require.Equal(t, "db.items", decoded.MatrixOne.Nodes[0].TableName)
	require.Equal(t, []string{
		"index=idx_embedding",
		"metric=l2_distance",
		"direction=INTERNAL",
		"post_filter_over_fetch=false",
		"query_vector=1",
		"candidate_limit=10",
		"first_round_limit=4",
		"pre_filter[0]=2",
	}, decoded.MatrixOne.Nodes[0].Expressions)
}

func TestBuildSQLJSONPlanPreservesLiteralVectorQuery(t *testing.T) {
	data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   0,
			NodeType: plan.Node_INDEX_SEARCH_SCAN,
			TableDef: &plan.TableDef{Name: "items"},
			IndexSearchScan: &plan.IndexSearchScan{
				Index:        &plan.IndexDef{IndexName: "idx_embedding"},
				QueryPayload: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,2]"}}}},
			},
		}},
	})
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, []string{
		"index=idx_embedding",
		"direction=INTERNAL",
		"post_filter_over_fetch=false",
		"query_vector=[1,2]",
	}, decoded.MatrixOne.Nodes[0].Expressions)
}

func TestBuildSQLJSONPlanPreservesSamplePolicy(t *testing.T) {
	buildExpressions := func(spec *plan.SampleFuncSpec) []string {
		data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*plan.Node{{
				NodeId:     0,
				NodeType:   plan.Node_SAMPLE,
				SampleFunc: spec,
			}},
		})
		require.NoError(t, err)
		var decoded struct {
			MatrixOne struct {
				Nodes []sqlJSONNode `json:"nodes"`
			} `json:"matrixone"`
		}
		require.NoError(t, json.Unmarshal(data, &decoded))
		return decoded.MatrixOne.Nodes[0].Expressions
	}

	rows := buildExpressions(&plan.SampleFuncSpec{Rows: 1, Percent: -1, UsingRow: false})
	moreRows := buildExpressions(&plan.SampleFuncSpec{Rows: 100, Percent: -1, UsingRow: true})
	percent := buildExpressions(&plan.SampleFuncSpec{Rows: -1, Percent: 12.5, UsingRow: true})
	require.NotEqual(t, rows, moreRows)
	require.NotEqual(t, rows, percent)
	require.Equal(t, []string{"sample_rows=1", "sample_using_row=false"}, rows)
	require.Equal(t, []string{"sample_rows=100", "sample_using_row=true"}, moreRows)
	require.Equal(t, []string{"sample_percent=12.5", "sample_using_row=true"}, percent)
}

func TestBuildSQLJSONPlanPreservesPlannerAggregateDistinct(t *testing.T) {
	nodes := buildPlannerSQLJSONNodes(t,
		"select n_nationkey, sum(distinct n_regionkey), sum(n_nationkey) from nation group by n_nationkey")
	var aggregate string
	for _, node := range nodes {
		if strings.Contains(strings.ToLower(node.Operator), "agg") {
			aggregate = node.Aggregate
			break
		}
	}
	require.NotEmpty(t, aggregate)
	require.Contains(t, aggregate, "DISTINCT")
	require.Contains(t, aggregate, "sum(")
	require.NotEqual(t, "sum(DISTINCT n_regionkey)", aggregate,
		"the non-DISTINCT control must remain visible in the same aggregate node")
}

func TestBuildSQLJSONPlanPreservesPlannerSampleRoles(t *testing.T) {
	nodes := buildPlannerSQLJSONNodes(t,
		"select n_regionkey, sample(n_nationkey + 1, 2 rows) from nation group by n_regionkey")
	var sample sqlJSONNode
	for _, node := range nodes {
		if strings.Contains(strings.ToLower(node.Operator), "sample") {
			sample = node
			break
		}
	}
	require.NotEmpty(t, sample.ID)
	require.Contains(t, sample.GroupBy, "n_regionkey")
	require.NotEmpty(t, sample.Aggregate)
	require.Contains(t, sample.Aggregate, "+")
}

func buildPlannerSQLJSONNodes(t *testing.T, sql string) []sqlJSONNode {
	t.Helper()
	stmt, err := mysql.ParseOne(t.Context(), sql, 1)
	require.NoError(t, err)
	query, err := planpkg.NewBaseOptimizer(planpkg.NewMockCompilerContext(true, newPlanTestProcess(t))).Optimize(stmt, false)
	require.NoError(t, err)
	data, err := BuildSQLJSONPlan(t.Context(), query)
	require.NoError(t, err)
	var decoded struct {
		MatrixOne struct {
			Nodes []sqlJSONNode `json:"nodes"`
		} `json:"matrixone"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	return decoded.MatrixOne.Nodes
}

func TestBuildSQLJSONPlanResolvesPlannerUpdateAssignments(t *testing.T) {
	for _, tc := range []struct{ name, value, want string }{
		{"constant", "2", "2"},
		{"expression", "n_regionkey + 1", "+ 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := buildPlannerSQLJSONNodes(t, "update nation set n_regionkey = "+tc.value+" where n_nationkey = 1")
			values := make(map[string]string)
			for _, node := range nodes {
				for _, assignment := range node.Assignments {
					require.NotContains(t, assignment.Value, "#[")
					values[assignment.Target] = assignment.Value
				}
			}
			var foundValue, foundKey bool
			for target, value := range values {
				if strings.HasSuffix(target, ".n_regionkey") {
					foundValue = true
					if tc.name == "expression" {
						require.Contains(t, value, "n_regionkey")
						require.Contains(t, value, tc.want)
					} else {
						require.Equal(t, tc.want, value)
					}
				}
				if strings.HasSuffix(target, ".n_nationkey") {
					foundKey = true
					require.Equal(t, "nation.n_nationkey", value)
				}
			}
			require.True(t, foundValue, "assigned column must appear in row image")
			require.True(t, foundKey, "unchanged column must appear in row image")
		})
	}
}

func TestBuildSQLJSONPlanResolvesMultiTargetUpdateAssignments(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      map[string]string
	}{
		{
			name: "different_tables",
			sql:  "update nation n join region r on n.n_regionkey = r.r_regionkey set n.n_regionkey = 2, r.r_name = 'changed'",
			want: map[string]string{".n_regionkey": "2", ".r_name": "'changed'", ".n_nationkey": "n.n_nationkey"},
		},
		{
			name: "same_table_aliases",
			sql:  "update nation n join nation m on n.n_nationkey = m.n_nationkey set n.n_regionkey = 2, m.n_name = 'changed'",
			want: map[string]string{".n_regionkey": "2", ".n_name": "'changed'", ".n_nationkey": "n.n_nationkey"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := buildPlannerSQLJSONNodes(t, tc.sql)
			found := make(map[string]bool)
			for _, node := range nodes {
				for _, assignment := range node.Assignments {
					require.NotContains(t, assignment.Value, "#[")
					for suffix, want := range tc.want {
						if strings.HasSuffix(assignment.Target, suffix) && strings.Contains(assignment.Value, want) {
							found[suffix] = true
						}
					}
				}
			}
			for suffix := range tc.want {
				require.True(t, found[suffix], "missing assigned value for %s", suffix)
			}
		})
	}
}

func TestBuildSQLJSONPlanResolvesMarkUpdateAssignments(t *testing.T) {
	for _, tc := range []struct{ name, condition, wrapper string }{
		{"exists", "exists (select 1 from region r where r.r_regionkey = n.n_regionkey)", "IS TRUE"},
		{"in", "n.n_regionkey in (select r_regionkey from region)", ""},
		{"not_exists", "not exists (select 1 from region r where r.r_regionkey = n.n_regionkey)", "not"},
		{"not_in", "n.n_regionkey not in (select r_regionkey from region)", "not"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := buildPlannerSQLJSONNodes(t, "update nation n set n_regionkey = case when "+tc.condition+" then 1 else 0 end")
			found := false
			for _, node := range nodes {
				for _, assignment := range node.Assignments {
					require.NotContains(t, assignment.Value, "#[")
					if strings.HasSuffix(assignment.Target, ".n_regionkey") {
						found = true
						require.Contains(t, assignment.Value, "MARK")
						require.Contains(t, assignment.Value, "n.n_regionkey")
						require.Contains(t, assignment.Value, "r_regionkey")
						if tc.wrapper != "" {
							require.Contains(t, assignment.Value, tc.wrapper)
						}
					}
				}
			}
			require.True(t, found)
		})
	}
}

func TestBuildSQLJSONPlanPreservesVectorExecutionSettings(t *testing.T) {
	buildExpressions := func(opts ivfflatplan.ScanOptions, direction plan.OrderBySpec_OrderByFlag, overfetch bool) []string {
		settings, err := ivfflatplan.EncodeScanOptions(opts)
		require.NoError(t, err)
		data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*plan.Node{{
				NodeId:   0,
				NodeType: plan.Node_INDEX_SEARCH_SCAN,
				IndexSearchScan: &plan.IndexSearchScan{
					Index:               &plan.IndexDef{IndexName: "idx_embedding", IndexAlgo: "ivfflat"},
					Direction:           direction,
					AlgoOptions:         settings,
					PostFilterOverFetch: overfetch,
				},
			}},
		})
		require.NoError(t, err)
		var decoded struct {
			MatrixOne struct {
				Nodes []sqlJSONNode `json:"nodes"`
			} `json:"matrixone"`
		}
		require.NoError(t, json.Unmarshal(data, &decoded))
		return decoded.MatrixOne.Nodes[0].Expressions
	}

	low := buildExpressions(ivfflatplan.ScanOptions{InitialProbeCount: 1}, plan.OrderBySpec_ASC, false)
	high := buildExpressions(ivfflatplan.ScanOptions{InitialProbeCount: 16}, plan.OrderBySpec_DESC, true)
	require.NotEqual(t, low, high)
	require.Contains(t, low, "NProbe: 1")
	require.Contains(t, low, "direction=ASC")
	require.Contains(t, high, "NProbe: 16")
	require.Contains(t, high, "direction=DESC")
	options, err := ivfflatplan.EncodeScanOptions(ivfflatplan.ScanOptions{InitialProbeCount: 16})
	require.NoError(t, err)
	require.Contains(t, high, fmt.Sprintf("algo_options=0x%X", options))
	require.Contains(t, high, "post_filter_over_fetch=true")
	base := buildExpressions(ivfflatplan.ScanOptions{InitialProbeCount: 1}, plan.OrderBySpec_ASC, false)
	expanded := buildExpressions(ivfflatplan.ScanOptions{InitialProbeCount: 1, BucketExpandStep: 8}, plan.OrderBySpec_ASC, false)
	threaded := buildExpressions(ivfflatplan.ScanOptions{InitialProbeCount: 1, ThreadsSearch: 8}, plan.OrderBySpec_ASC, false)
	require.Contains(t, base, "BucketExpandStep: 0")
	require.Contains(t, base, "ThreadsSearch: 0")
	require.Contains(t, expanded, "BucketExpandStep: 8")
	require.Contains(t, threaded, "ThreadsSearch: 8")
	require.NotEqual(t, base, expanded)
	require.NotEqual(t, base, threaded)

}

func TestBuildSQLJSONPlanPreservesVectorIndexDistanceRange(t *testing.T) {
	buildExpressions := func(upper int32) []string {
		data, err := BuildSQLJSONPlan(context.Background(), &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*plan.Node{{
				NodeId:   0,
				NodeType: plan.Node_INDEX_SEARCH_SCAN,
				IndexSearchScan: &plan.IndexSearchScan{
					Index: &plan.IndexDef{IndexName: "idx_embedding"},
					DistanceRange: &plan.DistRange{
						UpperBoundType: plan.BoundType_INCLUSIVE,
						UpperBound:     sqlJSONTestInt32(upper),
					},
				},
			}},
		})
		require.NoError(t, err)
		var decoded struct {
			MatrixOne struct {
				Nodes []sqlJSONNode `json:"nodes"`
			} `json:"matrixone"`
		}
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Len(t, decoded.MatrixOne.Nodes, 1)
		return decoded.MatrixOne.Nodes[0].Expressions
	}

	lower := buildExpressions(5)
	upper := buildExpressions(50)
	require.NotEqual(t, lower, upper)
	require.Equal(t, []string{
		"index=idx_embedding",
		"direction=INTERNAL",
		"post_filter_over_fetch=false",
		"distance_upper_bound_type=INCLUSIVE",
		"distance_upper_bound=5",
	}, lower)
	require.Equal(t, []string{
		"index=idx_embedding",
		"direction=INTERNAL",
		"post_filter_over_fetch=false",
		"distance_upper_bound_type=INCLUSIVE",
		"distance_upper_bound=50",
	}, upper)
}

func TestSQLJSONIndexSearchRejectsInvalidMetadata(t *testing.T) {
	index := &plan.IndexDef{IndexName: "idx_embedding", IndexAlgo: "ivfflat"}
	for _, tc := range []struct {
		name string
		spec *plan.IndexSearchScan
	}{
		{"nil", nil},
		{"missing_index", &plan.IndexSearchScan{}},
		{"invalid_direction", &plan.IndexSearchScan{Index: index, Direction: plan.OrderBySpec_OrderByFlag(1 << 10)}},
		{"invalid_options", &plan.IndexSearchScan{Index: index, AlgoOptions: []byte(`{`)}},
		{"missing_named_value", &plan.IndexSearchScan{Index: index, AlgoExprNames: []string{"first_round_limit"}}},
		{"empty_name", &plan.IndexSearchScan{Index: index, AlgoExprNames: []string{""}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(1)}}},
		{"duplicate_name", &plan.IndexSearchScan{Index: index, AlgoExprNames: []string{"first_round_limit", "first_round_limit"}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(2)}}},
		{"nil_value", &plan.IndexSearchScan{Index: index, AlgoExprNames: []string{"first_round_limit"}, AlgoExprs: []*plan.Expr{nil}}},
		{"invalid_value", &plan.IndexSearchScan{Index: index, AlgoExprNames: []string{"first_round_limit"}, AlgoExprs: []*plan.Expr{{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlJSONIndexSearchExpressions(t.Context(), tc.spec, &ExplainOptions{CompleteLiteralVectors: true})
			require.Error(t, err)
		})
	}
	values, err := sqlJSONIndexSearchExpressions(t.Context(), &plan.IndexSearchScan{
		Index:           &plan.IndexDef{IndexName: "custom", IndexAlgo: "unregistered"},
		AlgoOptions:     []byte{0x00, 0xff},
		AlgoExprNames:   []string{"custom_budget"},
		AlgoExprs:       []*plan.Expr{sqlJSONTestInt32(7)},
		IncludedColumns: []string{"tag"},
	}, &ExplainOptions{CompleteLiteralVectors: true})
	require.NoError(t, err)
	require.Contains(t, values, "algo_options=0x00FF")
	require.Contains(t, values, "custom_budget=7")
	require.Contains(t, values, "included_column[0]=tag")
}

func TestSQLJSONHelpersRejectMalformedInputsAndPreserveStatistics(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BuildSQLJSONPlan(canceled, &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{NodeId: 1}}})
	require.Error(t, err)
	_, err = BuildSQLJSONPlan(context.Background(), nil)
	require.Error(t, err)
	_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{1}, Nodes: []*plan.Node{{NodeId: 1}}})
	require.Error(t, err)
	_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{NodeId: 1, NodeType: plan.Node_JOIN, Children: []int32{2}}, {NodeId: 2}, {NodeId: 2}}})
	require.Error(t, err)

	for _, spec := range []*plan.OrderBySpec{
		nil,
		{Expr: sqlJSONTestInt32(1), Flag: plan.OrderBySpec_OrderByFlag(1 << 10)},
		{Expr: sqlJSONTestInt32(1), Flag: plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_NULLS_LAST},
		{Flag: plan.OrderBySpec_ASC},
	} {
		_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Nodes:    []*plan.Node{{NodeId: 1, NodeType: plan.Node_SORT, OrderBy: []*plan.OrderBySpec{spec}}},
		})
		require.Error(t, err)
	}

	badFrames := []*plan.FrameClause{
		{Type: plan.FrameClause_FrameType(99)},
		{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW, Val: sqlJSONTestInt32(1)}},
		{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW, UnBounded: true}},
		{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING}},
		{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_BoundType(99)}},
	}
	for _, frame := range badFrames {
		_, err = sqlJSONFrameValue(context.Background(), frame, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
		require.Error(t, err)
	}
	frame, err := sqlJSONFrameValue(context.Background(), &plan.FrameClause{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: sqlJSONTestInt32(2)}}, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
	require.NoError(t, err)
	require.Equal(t, "2 FOLLOWING", frame.Start)

	stats := finiteNodeStatistics(&plan.Node{Stats: &plan.Stats{Cost: math.NaN(), Outcnt: math.Inf(1), Rowsize: math.Inf(-1)}})
	require.Nil(t, stats)
	require.Nil(t, finiteNodeStatistics(nil))
	require.False(t, sqlJSONIsTableScan(nil))
	require.True(t, sqlJSONIsTableScan(&plan.Node{NodeType: plan.Node_EXTERNAL_SCAN}))
	require.True(t, sqlJSONIsTableScan(&plan.Node{NodeType: plan.Node_MATERIAL_SCAN}))
	require.False(t, sqlJSONIsTableScan(&plan.Node{NodeType: plan.Node_PROJECT}))

	_, err = sqlJSONExpr(context.Background(), &plan.Expr{}, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
	require.Error(t, err)
	_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{NodeId: 1, NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{{}}}}})
	require.Error(t, err)
	_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   1,
			NodeType: plan.Node_INDEX_SEARCH_SCAN,
			IndexSearchScan: &plan.IndexSearchScan{
				Index:        &plan.IndexDef{IndexName: "idx_embedding"},
				QueryPayload: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Jsonval{Jsonval: "{}"}}}},
			},
		}},
	})
	require.Error(t, err)
	for _, spec := range []*plan.IndexSearchScan{
		{Index: &plan.IndexDef{IndexName: "idx_embedding"}, AlgoExprNames: []string{"first_round_limit"}},
		{Index: &plan.IndexDef{IndexName: "idx_embedding"}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(4)}},
		{Index: &plan.IndexDef{IndexName: "idx_embedding"}, AlgoExprNames: []string{"first_round_limit"}, AlgoExprs: []*plan.Expr{{}}},
	} {
		_, err = sqlJSONIndexSearchExpressions(context.Background(), spec, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
		require.Error(t, err)
	}
	_, err = BuildSQLJSONPlan(context.Background(), &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*plan.Node{{NodeId: 1, NodeType: plan.Node_SAMPLE}},
	})
	require.Error(t, err)
}

func TestSQLJSONTableAndTargetNameFallbacks(t *testing.T) {
	require.Empty(t, sqlJSONTableName(nil))
	require.Equal(t, "db.t", sqlJSONTableName(&plan.Node{TableDef: &plan.TableDef{DbName: "db", Name: "t"}}))
	require.Equal(t, "t", sqlJSONTableName(&plan.Node{TableDef: &plan.TableDef{Name: "t"}}))
	require.Equal(t, "db.ref", sqlJSONTableName(&plan.Node{ObjRef: &plan.ObjectRef{DbName: "db", ObjName: "ref"}}))
	require.Equal(t, "db.t", sqlJSONTableName(&plan.Node{DeleteCtx: &plan.DeleteCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}}}))
	require.Equal(t, "db.t", sqlJSONTableName(&plan.Node{InsertCtx: &plan.InsertCtx{Ref: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}}}))
	require.Equal(t, "db.t", sqlJSONTargetName(nil, &plan.TableDef{DbName: "db", Name: "t"}))
	require.Equal(t, "db.t", sqlJSONTargetName(&plan.ObjectRef{SchemaName: "db", ObjName: "t"}, nil))
	require.Empty(t, sqlJSONTargetName(nil, nil))
	require.Empty(t, sqlJSONObjectRefName(nil))
	require.Equal(t, "db.t", sqlJSONObjectRefName(&plan.ObjectRef{DbName: "db", ObjName: "t"}))
	require.Equal(t, "t", sqlJSONObjectRefName(&plan.ObjectRef{ObjName: "t"}))
	require.Empty(t, sqlJSONBoundTableAlias(&plan.Node{TableDef: &plan.TableDef{}}))
	require.Empty(t, sqlJSONBoundTableAlias(&plan.Node{TableDef: &plan.TableDef{Name: "t", Cols: []*plan.ColDef{sqlJSONTestColumn("id")}}, ProjectList: []*plan.Expr{{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "a.id"}}}, {Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "b.id"}}}}}))
	require.Equal(t, []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(2)}, sqlJSONLimitExpressions(&plan.Node{Limit: sqlJSONTestInt32(1), Offset: sqlJSONTestInt32(2)}))
	require.Empty(t, sqlJSONLimitExpressions(nil))
}

func TestBuildSQLJSONPlanPreservesFulltextIndexPayload(t *testing.T) {
	node := fulltextSearchScanNode(t)
	node.NodeId = 0
	opts, err := ft2plan.EncodeScanOptions(ft2plan.ScanOptions{
		Config: `{"include_columns":["status"]}`, Mode: 3,
		IncludePreds: `[{"col":0,"op":"=","val":1}]`, ScoreRange: `{"has_min":true,"min":0}`,
	})
	require.NoError(t, err)
	node.IndexSearchScan.AlgoOptions = opts
	node.IndexSearchScan.QueryPayload = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "search terms"}}}}
	node.IndexSearchScan.AlgoExprNames = []string{"zero_relevance_guard"}
	node.IndexSearchScan.AlgoExprs = []*plan.Expr{{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}}
	before, err := json.Marshal(node.IndexSearchScan)
	require.NoError(t, err)
	data, err := BuildSQLJSONPlan(t.Context(), &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{node}})
	require.NoError(t, err)
	var decoded sqlJSONPlan
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes, 1)
	scan := decoded.MatrixOne.Nodes[0]
	require.Equal(t, "Fulltext Index Scan", scan.Operator)
	require.Contains(t, scan.Expressions, "index=ft_body")
	require.Contains(t, scan.Expressions, "zero_relevance_guard=true")
	require.Contains(t, scan.Expressions, "Match Mode: 3")
	require.Contains(t, scan.Expressions, "Include Column[0]: status")
	require.Contains(t, scan.Expressions, `Include Predicates: [{"col":0,"op":"=","val":1}]`)
	require.Contains(t, scan.Expressions, `Score Range: {"has_min":true,"min":0}`)
	require.Contains(t, strings.Join(scan.Expressions, " "), "query_payload=")
	require.Contains(t, strings.Join(scan.Expressions, " "), "search terms")
	require.NotContains(t, strings.Join(scan.Expressions, " "), "query_vector=")
	require.NotContains(t, strings.Join(scan.Expressions, " "), "metric=")
	after, err := json.Marshal(node.IndexSearchScan)
	require.NoError(t, err)
	require.Equal(t, before, after, "EXPLAIN must not mutate the reusable scan")
}

func TestBuildSQLJSONPlanRejectsMalformedIndexSearchMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		scan *plan.IndexSearchScan
	}{
		{name: "missing scan"},
		{name: "missing index", scan: &plan.IndexSearchScan{}},
		{name: "missing index name", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{}}},
		{name: "invalid algorithm options", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix", IndexAlgo: "ivfflat"}, AlgoOptions: []byte("{")}},
		{name: "invalid fulltext options", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ft", IndexAlgo: "fulltext"}, AlgoOptions: []byte("{")}},
		{name: "missing fulltext2 options", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ft", IndexAlgo: "fulltext2"}}},
		{name: "invalid fulltext2 options", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ft", IndexAlgo: "fulltext2"}, AlgoOptions: []byte("{")}},
		{name: "invalid fulltext2 config", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ft", IndexAlgo: "fulltext2"}, AlgoOptions: []byte(`{"config":"{"}`)}},
		{name: "expression without name", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(1)}}},
		{name: "name without expression", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, AlgoExprNames: []string{"budget"}}},
		{name: "empty expression name", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, AlgoExprNames: []string{""}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(1)}}},
		{name: "nil expression", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, AlgoExprNames: []string{"budget"}, AlgoExprs: []*plan.Expr{nil}}},
		{name: "invalid direction", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, Direction: plan.OrderBySpec_OrderByFlag(99)}},
		{name: "invalid bound type", scan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "ix"}, DistanceRange: &plan.DistRange{UpperBoundType: plan.BoundType(99), UpperBound: sqlJSONTestInt32(1)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := BuildSQLJSONPlan(t.Context(), &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{NodeId: 0, NodeType: plan.Node_INDEX_SEARCH_SCAN, IndexSearchScan: tc.scan}}})
			require.Error(t, err)
			require.Empty(t, data, "malformed metadata must not publish partial JSON")
		})
	}
}
