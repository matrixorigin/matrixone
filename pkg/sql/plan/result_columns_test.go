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

func TestGetResultColumnsFromPlanCarriesOriginMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey as key_alias from nation as n")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.Equal(t, "key_alias", columns[0].Name)
	require.Equal(t, "n_nationkey", columns[0].OriginName)
	require.Equal(t, "n", columns[0].TblName)
	require.Equal(t, "nation", columns[0].OriginTblName)
	require.Equal(t, "tpch", columns[0].DbName)

	wire, err := columns[0].Marshal()
	require.NoError(t, err)
	var roundTrip planpb.ColDef
	require.NoError(t, roundTrip.Unmarshal(wire))
	require.Equal(t, columns[0].OriginTblName, roundTrip.OriginTblName)
}

func TestGetResultColumnsFromPlanLeavesComputedOriginEmpty(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey + 1 as derived_key from nation as n")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.Empty(t, columns[0].OriginName)
	require.Empty(t, columns[0].TblName)
	require.Empty(t, columns[0].OriginTblName)
	require.Empty(t, columns[0].DbName)
}

func TestGetResultColumnsFromPlanMapsVectorIndexSourceSlots(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name:         "source_alias",
		OriginalName: "source_table",
		DbName:       "source_db",
		Pkey:         &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Cols: []*planpb.ColDef{
			{Name: "embedding", Typ: planpb.Type{Id: int32(types.T_array_float32)}},
			{Name: "payload", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64}},
			{
				Name:    "id",
				Primary: true,
				Typ: planpb.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{Name: "category", NotNull: true, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
			{Name: "unused", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{
			"embedding": 0,
			"payload":   1,
			"id":        2,
			"category":  3,
			"unused":    4,
		},
		Indexes: []*planpb.IndexDef{{Parts: []string{"category"}, Unique: true}},
	}
	vectorScan := &planpb.VectorIndexScan{
		SourceTable:    &planpb.ObjectRef{SchemaName: "source_db", ObjName: "source_table"},
		SourceTableDef: tableDef,
		IncludedColumns: []string{
			"category",
			"payload",
		},
	}
	scoreType := planpb.Type{Id: int32(types.T_float64), Width: 8}
	vectorNodeProjectList := []*planpb.Expr{
		// The scan projection is deliberately reordered. ColPos remains the
		// synthetic vector output slot, so lineage must follow the reference
		// instead of the projection-list index.
		{Typ: scoreType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 1, Name: "score"}}},
		{Typ: tableDef.Cols[3].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 2, Name: "category"}}},
		{Typ: tableDef.Cols[2].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 0, Name: "pkid"}}},
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 3, Name: "payload"}}},
	}
	projectList := []*planpb.Expr{
		{Typ: tableDef.Cols[2].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 0, Name: "pkid"}}},
		{Typ: tableDef.Cols[3].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 2, Name: "category"}}},
		{Typ: scoreType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 1, Name: "score"}}},
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 3, Name: "payload"}}},
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{
				NodeId:      0,
				NodeType:    planpb.Node_VECTOR_INDEX_SCAN,
				BindingTags: []int32{10},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "pkid", Typ: tableDef.Cols[2].Typ},
					{Name: "score", Typ: scoreType},
					{Name: "__mo_index_include_category", Typ: tableDef.Cols[3].Typ},
					{Name: "__mo_index_include_payload", Typ: tableDef.Cols[1].Typ},
				}},
				ProjectList:     vectorNodeProjectList,
				VectorIndexScan: vectorScan,
			},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: projectList},
		},
		Headings: []string{"id", "category", "score", "payload"},
	}}})

	require.Len(t, got, 4)
	require.Equal(t, "source_db", got[0].DbName)
	require.Equal(t, "source_table", got[0].TblName)
	require.Equal(t, "source_table", got[0].OriginTblName)
	require.Equal(t, "id", got[0].OriginName)
	require.True(t, got[0].Primary)
	require.True(t, got[0].NotNull)
	require.True(t, got[0].Typ.NotNullable)
	require.True(t, got[0].Typ.AutoIncr)

	// The distance score is synthetic and must not inherit source metadata.
	require.Empty(t, got[2].DbName)
	require.Empty(t, got[2].TblName)
	require.Empty(t, got[2].OriginTblName)
	require.Empty(t, got[2].OriginName)
	require.False(t, got[2].Primary)
	require.False(t, got[2].Unique)

	require.Equal(t, "category", got[1].OriginName)
	require.Equal(t, "source_table", got[1].OriginTblName)
	require.True(t, got[1].Unique)
	require.True(t, got[1].NotNull)
	require.False(t, got[3].Unique)
	require.False(t, got[3].NotNull)
	require.Equal(t, "payload", got[3].OriginName)
}

func TestGetResultColumnsFromPlanMapsPrunedVectorIndexSlotsByName(t *testing.T) {
	sourceTable := &planpb.TableDef{
		Name:   "source_table",
		DbName: "source_db",
		Pkey:   &planpb.PrimaryKeyDef{PkeyColName: "id"},
		Cols: []*planpb.ColDef{
			{Name: "embedding", Typ: planpb.Type{Id: int32(types.T_array_float32)}},
			{Name: "payload", Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}},
			{Name: "category", NotNull: true, Typ: planpb.Type{Id: int32(types.T_varchar)}},
		},
		Name2ColIndex: map[string]int32{"embedding": 0, "payload": 1, "id": 2, "category": 3},
		Indexes:       []*planpb.IndexDef{{Parts: []string{"category"}, Unique: true}},
	}
	vectorSpec := &planpb.VectorIndexScan{
		SourceTable:    &planpb.ObjectRef{SchemaName: "source_db", ObjName: "source_table"},
		SourceTableDef: sourceTable,
		IncludedColumns: []string{
			"category",
			"payload",
		},
	}
	makePlan := func(vectorCols, resultCols []*planpb.ColDef, headings []string) *planpb.Plan {
		projectList := make([]*planpb.Expr, len(vectorCols))
		for i, col := range vectorCols {
			projectList[i] = &planpb.Expr{
				Typ: col.Typ,
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 0,
					ColPos: int32(i),
					Name:   col.Name,
				}},
			}
		}
		resultProjectList := make([]*planpb.Expr, len(resultCols))
		for i, col := range resultCols {
			resultProjectList[i] = &planpb.Expr{
				Typ: col.Typ,
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 0,
					ColPos: int32(i),
					Name:   col.Name,
				}},
			}
		}
		return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{1},
			Nodes: []*planpb.Node{
				{
					NodeId:      0,
					NodeType:    planpb.Node_VECTOR_INDEX_SCAN,
					TableDef:    &planpb.TableDef{Cols: vectorCols},
					ProjectList: projectList,
					VectorIndexScan: &planpb.VectorIndexScan{
						SourceTable:     vectorSpec.SourceTable,
						SourceTableDef:  vectorSpec.SourceTableDef,
						IncludedColumns: vectorSpec.IncludedColumns,
					},
				},
				{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: resultProjectList},
			},
			Headings: headings,
		}}}
	}

	// Column pruning can leave score and an included column at local positions
	// zero and one even though pkid was slot zero in the original schema.
	prunedScoreAndInclude := makePlan(
		[]*planpb.ColDef{
			{Name: "score", Typ: planpb.Type{Id: int32(types.T_float64)}},
			{Name: "__mo_index_include_category", Typ: sourceTable.Cols[3].Typ},
		},
		[]*planpb.ColDef{
			{Name: "score", Typ: planpb.Type{Id: int32(types.T_float64)}},
			{Name: "category", Typ: sourceTable.Cols[3].Typ},
		},
		[]string{"score", "category"},
	)
	got := GetResultColumnsFromPlan(prunedScoreAndInclude)
	require.Len(t, got, 2)
	require.Empty(t, got[0].OriginName)
	require.Equal(t, "category", got[1].OriginName)
	require.True(t, got[1].Unique)
	require.True(t, got[1].NotNull)

	// A different pruning pass can keep pkid and an included column while
	// removing score; pkid must still resolve to the nonzero source position.
	prunedPkAndInclude := makePlan(
		[]*planpb.ColDef{
			{Name: "pkid", Typ: sourceTable.Cols[2].Typ},
			{Name: "__mo_index_include_category", Typ: sourceTable.Cols[3].Typ},
		},
		[]*planpb.ColDef{
			{Name: "pkid", Typ: sourceTable.Cols[2].Typ},
			{Name: "category", Typ: sourceTable.Cols[3].Typ},
		},
		[]string{"id", "category"},
	)
	got = GetResultColumnsFromPlan(prunedPkAndInclude)
	require.Len(t, got, 2)
	require.Equal(t, "id", got[0].OriginName)
	require.True(t, got[0].Primary)
	require.True(t, got[0].NotNull)
	require.Equal(t, "category", got[1].OriginName)

	// Sort nodes can expose a compact local output list while retaining the
	// child scan's original source positions. The lineage walker must bridge
	// that local position before resolving the vector slot by name. Keeping two
	// included columns catches collisions where a local slot equals another
	// child's source position.
	sortPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{2},
		Nodes: []*planpb.Node{
			{
				NodeId: 0, NodeType: planpb.Node_VECTOR_INDEX_SCAN,
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "pkid", Typ: sourceTable.Cols[2].Typ},
					{Name: "score", Typ: planpb.Type{Id: int32(types.T_float64)}},
					{Name: "__mo_index_include_category", Typ: sourceTable.Cols[3].Typ},
					{Name: "__mo_index_include_payload", Typ: sourceTable.Cols[1].Typ},
				}},
				ProjectList: []*planpb.Expr{
					{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "pkid"}}},
					{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "score"}}},
					{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2, Name: "__mo_index_include_category"}}},
					{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 3, Name: "__mo_index_include_payload"}}},
				},
				VectorIndexScan: vectorSpec,
			},
			{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, ProjectList: []*planpb.Expr{
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 3}}},
			}},
			{NodeId: 2, NodeType: planpb.Node_PROJECT, Children: []int32{1}, ProjectList: []*planpb.Expr{
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2}}},
			}},
		},
		Headings: []string{"id", "category", "payload"},
	}}}
	got = GetResultColumnsFromPlan(sortPlan)
	require.Len(t, got, 3)
	require.Equal(t, "id", got[0].OriginName)
	require.Equal(t, "category", got[1].OriginName)
	require.Equal(t, "payload", got[2].OriginName)

	// JOIN resolves its selected child slot before descending. That reference
	// is already a source position, so a compact SORT child must not interpret
	// it as a second local slot (SORT outputs [0, 2, 3] here).
	joinTable := &planpb.TableDef{
		Name:   "join_table",
		DbName: "source_db",
		Pkey:   &planpb.PrimaryKeyDef{PkeyColName: "id"},
		Cols: []*planpb.ColDef{
			{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}},
			{Name: "payload", Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: "category", NotNull: true, Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: "extra", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "payload": 1, "category": 2, "extra": 3},
		Indexes:       []*planpb.IndexDef{{Parts: []string{"category"}, Unique: true}},
	}
	leftScanProjects := make([]*planpb.Expr, len(joinTable.Cols))
	for i, col := range joinTable.Cols {
		leftScanProjects[i] = &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 7, ColPos: int32(i), Name: col.Name,
		}}}
	}
	rightScanProjects := []*planpb.Expr{{Typ: joinTable.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
		RelPos: 8, ColPos: 0, Name: "id",
	}}}}
	joinPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{4},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{7}, TableDef: joinTable, ProjectList: leftScanProjects},
			{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, ProjectList: []*planpb.Expr{
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 0}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 2}}},
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 3}}},
			}},
			{NodeId: 2, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{8}, TableDef: joinTable, ProjectList: rightScanProjects},
			{NodeId: 3, NodeType: planpb.Node_JOIN, Children: []int32{1, 2}, ProjectList: []*planpb.Expr{
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}}},
			}},
			{NodeId: 4, NodeType: planpb.Node_PROJECT, Children: []int32{3}, ProjectList: []*planpb.Expr{
				{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}}},
			}},
		},
		Headings: []string{"category"},
	}}}
	got = GetResultColumnsFromPlan(joinPlan)
	require.Len(t, got, 1)
	require.Equal(t, "category", got[0].OriginName)

	// When a transparent node is itself the result step, its expressions are
	// already source references. The entry point must not reinterpret ColPos as
	// a local output index.
	sortResultPlan := *sortPlan
	sortResultQuery := *sortPlan.GetQuery()
	sortResultQuery.Steps = []int32{1}
	sortResultQuery.Headings = []string{"id", "category", "payload"}
	sortResultPlan.Plan = &planpb.Plan_Query{Query: &sortResultQuery}
	got = GetResultColumnsFromPlan(&sortResultPlan)
	require.Len(t, got, 3)
	require.Equal(t, "id", got[0].OriginName)
	require.Equal(t, "category", got[1].OriginName)
	require.Equal(t, "payload", got[2].OriginName)
}

func TestGetResultColumnsFromPlanFailsClosedForInvalidVectorIndexSource(t *testing.T) {
	tableDef := &planpb.TableDef{
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id"},
		Cols: []*planpb.ColDef{{Name: "id", Primary: true}, {Name: "category"}},
	}
	vectorScan := &planpb.VectorIndexScan{
		SourceTableDef:  tableDef,
		IncludedColumns: []string{"missing"},
	}
	projectList := []*planpb.Expr{
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}},
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 4}}},
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_VECTOR_INDEX_SCAN, VectorIndexScan: vectorScan},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: projectList},
		},
		Headings: []string{"id", "score", "missing", "invalid"},
	}}})

	require.Len(t, got, 4)
	require.Equal(t, "id", got[0].OriginName)
	require.Empty(t, got[1].OriginName)
	require.Empty(t, got[2].OriginName)
	require.Empty(t, got[3].OriginName)

	// A vector spec without its source primary-key definition cannot safely
	// claim key metadata from the synthetic pkid slot.
	vectorScan.SourceTableDef.Pkey = nil
	got = GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_VECTOR_INDEX_SCAN, VectorIndexScan: vectorScan},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: projectList},
		},
		Headings: []string{"id", "score", "missing", "invalid"},
	}}})
	require.Len(t, got, 4)
	require.Empty(t, got[0].OriginName)
}
