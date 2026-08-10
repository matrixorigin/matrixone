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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestGetResultColumnsFromPlanCarriesConstraintMetadata(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
		Cols: []*planpb.ColDef{
			{
				Name:    "id",
				Primary: true,
				Typ: planpb.Type{
					Id:          int32(types.T_int32),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{Name: "uk", Typ: planpb.Type{Id: int32(types.T_int32)}, NotNull: true},
			{Name: "nullable", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Indexes: []*planpb.IndexDef{{IndexName: "uk_t", Parts: []string{"uk"}, Unique: true}},
	}

	scanProjects := make([]*planpb.Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		scanProjects[i] = &planpb.Expr{
			Typ: col.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				ColPos: int32(i),
				Name:   "t." + col.Name,
			}},
		}
	}
	resultProjects := make([]*planpb.Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		resultProjects[i] = &planpb.Expr{
			Typ: col.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				ColPos:  int32(i),
				Name:    "t." + col.Name,
				TblName: "t",
			}},
		}
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, TableDef: tableDef, ProjectList: scanProjects},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: resultProjects},
		},
		Headings: []string{"id", "uk", "nullable"},
	}}})

	require.Len(t, got, 3)
	require.True(t, got[0].Primary)
	require.False(t, got[0].Unique)
	require.True(t, got[0].NotNull)
	require.True(t, got[0].Typ.NotNullable)
	require.True(t, got[0].Typ.AutoIncr)
	require.False(t, got[1].Primary)
	require.True(t, got[1].Unique)
	require.True(t, got[1].NotNull)
	require.False(t, got[2].Primary)
	require.False(t, got[2].Unique)
	require.False(t, got[2].NotNull)

	wire, err := got[1].Marshal()
	require.NoError(t, err)
	var roundTrip planpb.ColDef
	require.NoError(t, roundTrip.Unmarshal(wire))
	require.True(t, roundTrip.Unique)
}

func TestGetResultColumnsFromPlanPreservesReorderedConstraintMetadata(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
		Cols: []*planpb.ColDef{
			{
				Name:    "id",
				Primary: true,
				Typ: planpb.Type{
					Id:          int32(types.T_int32),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{Name: "unique_value", Typ: planpb.Type{Id: int32(types.T_int32)}, NotNull: true},
		},
		Indexes: []*planpb.IndexDef{{IndexName: "uk_t", Parts: []string{"unique_value"}, Unique: true}},
	}

	const scanRelPos = int32(7)
	scanProjects := []*planpb.Expr{
		{Typ: tableDef.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 0, Name: "t.id", TblName: "t",
		}}},
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 1, Name: "t.unique_value", TblName: "t",
		}}},
	}
	// The output order differs from the source order. In particular, ColPos=1
	// is the first output expression, not the second entry in ProjectList.
	resultProjects := []*planpb.Expr{
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 1, Name: "t.unique_value", TblName: "t",
		}}},
		{Typ: tableDef.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 0, Name: "t.id", TblName: "t",
		}}},
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, TableDef: tableDef, ProjectList: scanProjects},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: resultProjects},
		},
		Headings: []string{"unique_value", "id"},
	}}})

	require.Len(t, got, 2)
	require.False(t, got[0].Primary)
	require.True(t, got[0].Unique)
	require.True(t, got[0].NotNull)
	require.False(t, got[0].Typ.AutoIncr)
	require.True(t, got[1].Primary)
	require.False(t, got[1].Unique)
	require.True(t, got[1].NotNull)
	require.True(t, got[1].Typ.AutoIncr)
}

func TestGetResultColumnsFromPlanCarriesJoinedColumnMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey from nation n join region r on n.n_regionkey = r.r_regionkey")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.True(t, columns[0].Primary)
	require.True(t, columns[0].NotNull)
	require.True(t, columns[0].Typ.NotNullable)
}

func TestGetResultColumnsFromPlanClearsNotNullForOuterJoinedColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicalPlan, err := runOneStmt(mock, t,
		"select r.r_regionkey from nation n left join region r on n.n_regionkey = r.r_regionkey")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.True(t, columns[0].Primary)
	require.False(t, columns[0].NotNull)
	require.False(t, columns[0].Typ.NotNullable)
}
