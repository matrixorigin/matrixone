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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
					ColPos: 0,
					Name:   "alias.id",
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
