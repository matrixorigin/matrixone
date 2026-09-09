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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
				NodeId:      2,
				NodeType:    plan.Node_LOCK_OP,
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
			{NodeId: 6, NodeType: plan.Node_FILL, Children: []int32{7}, FillVal: []*plan.Expr{value(17)}},
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
	require.Equal(t, []string{"17"}, byID["6"].Expressions)
	require.Equal(t, "18", byID["7"].Limit)
	require.Equal(t, []string{"28"}, byID["16"].Expressions)
	require.Equal(t, []string{"29"}, byID["17"].Expressions)
	require.Equal(t, "db.target", byID["20"].TableName)
	require.Equal(t, "db.target", byID["21"].TableName)
	require.Equal(t, []string{"30", "31"}, byID["22"].Expressions)
	require.Len(t, byID["23"].Assignments, 2)
	require.Equal(t, "db.target", byID["24"].TableName)
	require.Equal(t, "db.target", decoded.QueryBlock.Table.TableName)
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
