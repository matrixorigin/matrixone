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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func sqlJSONTestColumn(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:          name,
		OriginName:    name,
		TblName:       "t",
		OriginTblName: "t",
		Typ:           plan.Type{Id: int32(types.T_int32)},
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
	require.Equal(t, statsBefore, *query.Nodes[0].Stats)
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
				Cols: []*plan.ColDef{{
					TblName:       "alias",
					OriginTblName: "source",
				}},
			},
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
