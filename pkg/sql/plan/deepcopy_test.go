// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestCloneTableDefForPlan(t *testing.T) {
	require.Nil(t, CloneTableDefForPlan(nil, true))

	colA := &planpb.ColDef{Name: "a"}
	colB := &planpb.ColDef{Name: "b"}
	index := &planpb.IndexDef{IndexName: "idx_a"}
	pkey := &planpb.PrimaryKeyDef{PkeyColName: "a"}
	source := &planpb.TableDef{
		Name:          "source",
		Cols:          []*planpb.ColDef{colA, colB},
		Indexes:       []*planpb.IndexDef{index},
		Pkey:          pkey,
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
	}

	cloned := CloneTableDefForPlan(source, true)
	require.NotSame(t, source, cloned)
	require.Equal(t, source, cloned)
	require.Same(t, colA, cloned.Cols[0])
	require.Same(t, index, cloned.Indexes[0])
	require.Same(t, pkey, cloned.Pkey)

	cloned.Name = "clone"
	cloned.Cols[0] = &planpb.ColDef{Name: "replacement"}
	cloned.Cols = append(cloned.Cols, &planpb.ColDef{Name: "c"})
	require.Equal(t, "source", source.Name)
	require.Same(t, colA, source.Cols[0])
	require.Len(t, source.Cols, 2)

	withoutCols := CloneTableDefForPlan(source, false)
	require.Nil(t, withoutCols.Cols)
	require.Same(t, index, withoutCols.Indexes[0])
	require.Same(t, pkey, withoutCols.Pkey)
}

func TestDeepCopyColDefPreservesOriginTable(t *testing.T) {
	source := &planpb.ColDef{
		Name:          "display_name",
		OriginName:    "source_name",
		TblName:       "table_alias",
		OriginTblName: "source_table",
		DbName:        "source_db",
	}

	cloned := DeepCopyColDef(source)
	require.NotSame(t, source, cloned)
	require.Equal(t, source, cloned)
}

func TestDeepCopyVectorIndexScanOwnsNestedMetadata(t *testing.T) {
	source := &planpb.VectorIndexScan{
		SourceTable:         &planpb.ObjectRef{SchemaName: "db", ObjName: "t"},
		SourceTableDef:      &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{{Name: "v"}}},
		Index:               &planpb.IndexDef{IndexName: "idx", IndexAlgo: "ivfflat"},
		HiddenTables:        []*planpb.VectorIndexTableRef{{Role: "entries", Object: &planpb.ObjectRef{ObjName: "e"}}},
		QueryVector:         &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		CandidateLimit:      MakePlan2Uint64ConstExprWithType(4),
		IncludedColumns:     []string{"payload"},
		InitialProbeCount:   2,
		ScanSnapshot:        &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 9}, Tenant: &planpb.SnapshotTenant{TenantID: 7}},
		PostFilterOverFetch: true,
	}

	cloned := DeepCopyVectorIndexScan(source)
	require.NotSame(t, source, cloned)
	require.Equal(t, source.SourceTable, cloned.SourceTable)
	require.Equal(t, source.Index, cloned.Index)
	require.Equal(t, source.QueryVector, cloned.QueryVector)
	require.Equal(t, source.CandidateLimit, cloned.CandidateLimit)
	require.Equal(t, source.IncludedColumns, cloned.IncludedColumns)
	require.Equal(t, source.ScanSnapshot, cloned.ScanSnapshot)
	require.True(t, cloned.PostFilterOverFetch)
	require.NotSame(t, source.SourceTable, cloned.SourceTable)
	require.NotSame(t, source.SourceTableDef, cloned.SourceTableDef)
	require.NotSame(t, source.Index, cloned.Index)
	require.NotSame(t, source.HiddenTables[0], cloned.HiddenTables[0])
	require.NotSame(t, source.QueryVector, cloned.QueryVector)
	require.NotSame(t, source.CandidateLimit, cloned.CandidateLimit)
	require.NotSame(t, source.ScanSnapshot, cloned.ScanSnapshot)
}

func TestDeepCopyExprClonesAggregateConfig(t *testing.T) {
	source := &planpb.Expr{
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func:          &planpb.ObjectRef{ObjName: NameGroupConcat},
			AggConfig:     []byte{1, 2, 3},
			AggConfigType: planpb.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		}},
	}

	cloned := DeepCopyExpr(source)
	require.NotSame(t, source.GetF(), cloned.GetF())
	require.Equal(t, source.GetF().Func, cloned.GetF().Func)
	require.Equal(t, source.GetF().AggConfig, cloned.GetF().AggConfig)
	require.Equal(t, source.GetF().AggConfigType, cloned.GetF().AggConfigType)

	cloned.GetF().AggConfig[0] = 9
	require.Equal(t, byte(1), source.GetF().AggConfig[0])
}

func TestDeepCopyPreInsertCtxPreservesTargetSelector(t *testing.T) {
	source := &planpb.PreInsertCtx{
		HasTargetSelector:  true,
		TargetRowNumberCol: 7,
		TargetActiveCol:    8,
		TargetRowIdCol:     9,
	}

	cloned := DeepCopyPreInsertCtx(source)
	require.NotSame(t, source, cloned)
	require.True(t, cloned.HasTargetSelector)
	require.Equal(t, int32(7), cloned.TargetRowNumberCol)
	require.Equal(t, int32(8), cloned.TargetActiveCol)
	require.Equal(t, int32(9), cloned.TargetRowIdCol)
}

func TestDeepCopyRuntimeFilterSpecPreservesPayloadContract(t *testing.T) {
	source := &planpb.RuntimeFilterSpec{
		Tag:                 7,
		MatchPrefix:         true,
		UpperLimit:          11,
		BuildExpr:           MakePlan2Int64ConstExprWithType(1),
		NotOnPk:             true,
		UseMembershipFilter: true,
		KeyEncoding:         planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		ProbeType: &planpb.Type{
			Id:         4,
			Width:      18,
			Scale:      3,
			Table:      "probe_table",
			Enumvalues: "a,b",
		},
		KeyComponentProbeTypes: []planpb.Type{
			{Id: 4, Width: 18, Scale: 2},
		},
	}

	cloned := DeepCopyRuntimeFilterSpec(source)

	require.Equal(t, source.Tag, cloned.Tag)
	require.Equal(t, source.MatchPrefix, cloned.MatchPrefix)
	require.Equal(t, source.UpperLimit, cloned.UpperLimit)
	require.Equal(t, source.NotOnPk, cloned.NotOnPk)
	require.Equal(t, source.UseMembershipFilter, cloned.UseMembershipFilter)
	require.Equal(t, source.KeyEncoding, cloned.KeyEncoding)
	require.Equal(t, source.ProbeType, cloned.ProbeType)
	require.NotSame(t, source.ProbeType, cloned.ProbeType)
	require.Nil(t, cloned.Expr)
	require.NotSame(t, source.BuildExpr, cloned.BuildExpr)
	require.Equal(t, source.BuildExpr, cloned.BuildExpr)
	require.Equal(t,
		source.KeyComponentProbeTypes,
		cloned.KeyComponentProbeTypes)

	cloned.ProbeType.Scale = 9
	cloned.BuildExpr.Typ.Scale = 9
	cloned.KeyComponentProbeTypes[0].Scale = 9
	require.Equal(t, int32(3), source.ProbeType.Scale)
	require.NotEqual(t, cloned.BuildExpr.Typ.Scale, source.BuildExpr.Typ.Scale)
	require.Equal(t, int32(2), source.KeyComponentProbeTypes[0].Scale)
}

func TestDeepCopyRuntimeFilterSpecPreservesProbeLayout(t *testing.T) {
	source := &planpb.RuntimeFilterSpec{
		Tag:  17,
		Expr: MakePlan2Int64ConstExprWithType(1),
	}

	cloned := DeepCopyRuntimeFilterSpec(source)
	require.Equal(t, source.Expr, cloned.Expr)
	require.NotSame(t, source.Expr, cloned.Expr)
	require.Nil(t, cloned.BuildExpr)

	cloned.Expr.Typ.Scale = 9
	require.NotEqual(t, cloned.Expr.Typ.Scale, source.Expr.Typ.Scale)
}

func TestDeepCopyNodePreservesFuzzyRuntimeFilterDecision(t *testing.T) {
	buildSpec := &planpb.RuntimeFilterSpec{
		Tag:         8,
		BuildExpr:   MakePlan2Int64ConstExprWithType(1),
		KeyEncoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
	}
	probeSpec := &planpb.RuntimeFilterSpec{
		Tag:  8,
		Expr: MakePlan2Int64ConstExprWithType(1),
	}
	source := &planpb.Node{
		NodeType:               planpb.Node_FUZZY_FILTER,
		FuzzyBuildSide:         planpb.Node_FUZZY_BUILD_SIDE_SINK,
		IfInsertFromUnique:     true,
		SpillMem:               64 << 10,
		RuntimeFilterProbeList: []*planpb.RuntimeFilterSpec{probeSpec},
		RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{buildSpec},
		Fuzzymessage: &planpb.OriginTableMessageForFuzzy{
			ParentTableName:  "parent",
			ParentUniqueCols: []*planpb.ColDef{{Name: "uk"}},
		},
	}

	cloned := DeepCopyNode(source)

	require.Equal(t, source.FuzzyBuildSide, cloned.FuzzyBuildSide)
	require.Equal(t, source.IfInsertFromUnique, cloned.IfInsertFromUnique)
	require.Equal(t, source.SpillMem, cloned.SpillMem)
	require.Equal(t, source.RuntimeFilterProbeList,
		cloned.RuntimeFilterProbeList)
	require.Equal(t, source.RuntimeFilterBuildList,
		cloned.RuntimeFilterBuildList)
	require.Equal(t, source.Fuzzymessage, cloned.Fuzzymessage)
	require.NotSame(t, source.RuntimeFilterProbeList[0],
		cloned.RuntimeFilterProbeList[0])
	require.NotSame(t, source.RuntimeFilterBuildList[0],
		cloned.RuntimeFilterBuildList[0])
	require.NotSame(t, source.Fuzzymessage, cloned.Fuzzymessage)
	require.NotSame(t, source.Fuzzymessage.ParentUniqueCols[0],
		cloned.Fuzzymessage.ParentUniqueCols[0])

	cloned.FuzzyBuildSide = planpb.Node_FUZZY_BUILD_SIDE_TABLE
	cloned.SpillMem = 1
	cloned.RuntimeFilterBuildList[0].BuildExpr.Typ.Scale = 9
	cloned.Fuzzymessage.ParentUniqueCols[0].Name = "changed"
	require.Equal(t, planpb.Node_FUZZY_BUILD_SIDE_SINK,
		source.FuzzyBuildSide)
	require.Equal(t, int64(64<<10), source.SpillMem)
	require.NotEqual(t,
		cloned.RuntimeFilterBuildList[0].BuildExpr.Typ.Scale,
		source.RuntimeFilterBuildList[0].BuildExpr.Typ.Scale)
	require.Equal(t, "uk", source.Fuzzymessage.ParentUniqueCols[0].Name)
}

func TestFilterBarrierSurvivesCopiesAndSerialization(t *testing.T) {
	source := &planpb.Node{
		NodeType:             planpb.Node_FILTER,
		FilterIsBarrier:      true,
		DedupInputKeysUnique: true,
	}

	cloned := DeepCopyNode(source)
	require.True(t, cloned.FilterIsBarrier)
	require.True(t, cloned.DedupInputKeysUnique)

	payload, err := source.Marshal()
	require.NoError(t, err)
	roundTrip := new(planpb.Node)
	require.NoError(t, roundTrip.Unmarshal(payload))
	require.True(t, roundTrip.FilterIsBarrier)
	require.True(t, roundTrip.DedupInputKeysUnique)
}

var clonedTableDef *planpb.TableDef

func BenchmarkCloneTableDefForPlan(b *testing.B) {
	cols := make([]*planpb.ColDef, 64)
	name2ColIndex := make(map[string]int32, len(cols))
	for i := range cols {
		name := fmt.Sprintf("col_%d", i)
		cols[i] = &planpb.ColDef{
			Name:    name,
			Default: &planpb.Default{OriginString: "0"},
		}
		name2ColIndex[name] = int32(i)
	}
	indexes := make([]*planpb.IndexDef, 8)
	for i := range indexes {
		indexes[i] = &planpb.IndexDef{
			IndexName: fmt.Sprintf("idx_%d", i),
			Parts:     []string{cols[i].Name, cols[i+1].Name},
		}
	}
	tableDef := &planpb.TableDef{
		Cols:          cols,
		Indexes:       indexes,
		Name2ColIndex: name2ColIndex,
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: cols[0].Name,
			Names:       []string{cols[0].Name},
		},
	}

	b.Run("deep", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			clonedTableDef = DeepCopyTableDef(tableDef, true)
		}
	})
	b.Run("planner", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			clonedTableDef = CloneTableDefForPlan(tableDef, true)
		}
	})
}
