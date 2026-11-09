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

package plan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestPointUpdateRuntimeFiltersWithMissingStats(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keyType   planpb.Type
		predicate string
		want      bool
	}{
		{name: "integer", keyType: planpb.Type{Id: int32(types.T_int32), NotNullable: true}, predicate: "id = 42", want: true},
		{name: "date", keyType: planpb.Type{Id: int32(types.T_date), NotNullable: true}, predicate: "id = '2024-01-02'", want: true},
		// The binder casts the binary column for this comparison, so it is not a direct key predicate.
		{name: "binary column cast", keyType: planpb.Type{Id: int32(types.T_varbinary), Width: 32, NotNullable: true}, predicate: "id = x'616263'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPointUpdateRuntimeFiltersWithMissingStats(t, tc.keyType, tc.predicate, tc.want)
		})
	}
}

func testPointUpdateRuntimeFiltersWithMissingStats(t *testing.T, keyType planpb.Type, predicate string, want bool) {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Indexes = table.Indexes[:2]
	table.Cols[0].Typ = keyType
	ctx := &updateIndexBuildTestContext{
		MockCompilerContext: &mock.ctxt,
		statsCache:          NewStatsCache(),
		statsByID:           make(map[uint64]*statspb.StatsInfo),
	}
	indexIDs := make([]uint64, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		oldName := index.IndexTableName
		name := catalog.SecondaryIndexTableNamePrefix + oldName
		index.IndexTableName = name
		indexTable := mock.ctxt.tables[oldName]
		indexTable.Cols[1].Typ = keyType
		indexTable.Name = name
		mock.ctxt.tables[name] = indexTable
		object := *mock.ctxt.objects[oldName]
		object.ObjName = name
		mock.ctxt.objects[name] = &object
		mock.ctxt.id2name[indexTable.TblId] = name
		stats := &statspb.StatsInfo{TableCnt: 50000, BlockNumber: 7}
		ctx.statsByID[indexTable.TblId] = stats
		ctx.statsCache.Set(indexTable.TblId, stats)
		indexIDs = append(indexIDs, indexTable.TblId)
	}
	stmt, err := mysql.ParseOne(t.Context(), "UPDATE index_hint_t SET a = 9 WHERE "+predicate, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	built, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	probes := make(map[uint64]*planpb.Node)
	buildTags := make(map[int32]bool)
	for id := range cteReachablePlanNodes(query) {
		node := query.Nodes[id]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil {
			probes[node.TableDef.TblId] = node
		}
		for _, spec := range node.RuntimeFilterBuildList {
			buildTags[spec.Tag] = true
		}
	}
	for _, id := range indexIDs {
		probe := probes[id]
		require.NotNil(t, probe, "the maintained index scan must be reachable")
		if !want {
			require.Empty(t, probe.RuntimeFilterProbeList)
			continue
		}
		require.Len(t, probe.RuntimeFilterProbeList, 1, "both index scans should use a runtime filter")
		require.True(t, buildTags[probe.RuntimeFilterProbeList[0].Tag], "probe must have a matching build")
	}
}

func newPointRuntimeFilterTestBuilder(t testing.TB) *QueryBuilder {
	t.Helper()
	b := newRuntimeFilterSingleTestBuilder(t, false)
	b.qry.StmtType = planpb.Query_SELECT
	b.qry.Steps = []int32{2}
	source, join := b.qry.Nodes[1], b.qry.Nodes[2]
	source.TableDef.TableType = catalog.SystemOrdinaryRel
	source.ObjRef = &planpb.ObjectRef{Obj: 42, ObjName: source.TableDef.Name}
	eq := makeRuntimeFilterTestEq(source.TableDef.Cols[0].Typ, 2, 3, 0, 0)
	eq.GetF().Args[1] = makePlan2Int64ConstExprWithType(42)
	source.FilterList = []*planpb.Expr{eq}
	join.JoinType = planpb.Node_INNER
	join.Stats.HashmapStats.HashOnPK = false
	join.Stats.HashmapStats.HashmapSize = 500
	return b
}

func TestPointRuntimeFilterAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*QueryBuilder)
		want   bool
	}{
		{name: "point", want: true},
		{name: "direct probe projection", want: true, mutate: func(b *QueryBuilder) {
			probe := b.qry.Nodes[0]
			probe.ProjectList = []*planpb.Expr{GetColExpr(probe.TableDef.Cols[0].Typ, 1, 0)}
		}},
		{name: "complete composite source key", want: true, mutate: func(b *QueryBuilder) {
			source := b.qry.Nodes[1]
			table := source.TableDef
			table.Cols = append(table.Cols, &planpb.ColDef{Name: "part", Typ: planpb.Type{Id: int32(types.T_int64)}})
			table.Name2ColIndex["part"] = 1
			table.Pkey.PkeyColName = catalog.CPrimaryKeyColName
			table.Pkey.Names = []string{"id", "part"}
			eq := makeRuntimeFilterTestEq(table.Cols[1].Typ, 2, 3, 1, 0)
			eq.GetF().Args[1] = makePlan2Int64ConstExprWithType(9)
			source.FilterList = append(source.FilterList, eq)
		}},
		{name: "no point predicate", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].FilterList = nil }},
		{name: "fake primary key", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].TableDef.Pkey.PkeyColName = catalog.FakePrimaryKeyColName }},
		{name: "partial composite key", mutate: func(b *QueryBuilder) {
			table := b.qry.Nodes[1].TableDef
			table.Cols = append(table.Cols, &planpb.ColDef{Name: "part", Typ: planpb.Type{Id: int32(types.T_int64)}})
			table.Name2ColIndex["part"] = 1
			table.Pkey.PkeyColName = catalog.CPrimaryKeyColName
			table.Pkey.Names = []string{"id", "part"}
		}},
		{name: "probe predicate", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[0].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "computed probe projection", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[0].ProjectList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "non-leading probe", mutate: func(b *QueryBuilder) {
			table := b.qry.Nodes[0].TableDef
			table.Cols = append(table.Cols, &planpb.ColDef{Name: "other", Typ: table.Cols[0].Typ})
			table.Name2ColIndex["other"] = 1
			table.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "other", Names: []string{"other"}}
		}},
		{name: "outer join", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = planpb.Node_LEFT }},
		{name: "right join", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].IsRightJoin = true }},
		{name: "large build guard", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].Stats.Outcnt = 5_000_001 }},
		{name: "hash on primary key guard", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[2].Stats.HashmapStats.HashOnPK = true
			b.qry.Nodes[1].Stats.Outcnt = 320_001
		}},
		{name: "disabled hint", mutate: func(b *QueryBuilder) { b.optimizerHints = &OptimizerHints{runtimeFilter: 1} }},
		{name: "multiple steps", mutate: func(b *QueryBuilder) { b.qry.Steps = append(b.qry.Steps, 1) }},
		{name: "source step", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].SourceStep = []int32{0} }},
		{name: "pending irregular maintenance", mutate: func(b *QueryBuilder) {
			b.irregularUpdateMaints = []irregularUpdateMaintenance{{}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newPointRuntimeFilterTestBuilder(t)
			if tc.mutate != nil {
				tc.mutate(b)
			}
			probe, join := b.qry.Nodes[0], b.qry.Nodes[2]
			before := DeepCopyStats(probe.Stats)
			b.generateRuntimeFilters(2)
			if !tc.want {
				require.Empty(t, probe.RuntimeFilterProbeList)
				return
			}
			require.Len(t, probe.RuntimeFilterProbeList, 1)
			require.Len(t, join.RuntimeFilterBuildList, 1)
			require.Equal(t, probe.RuntimeFilterProbeList[0].Tag, join.RuntimeFilterBuildList[0].Tag)
			require.EqualValues(t, 1, join.RuntimeFilterBuildList[0].UpperLimit)
			require.Equal(t, before, probe.Stats, "a benefit estimate must not rewrite statistics")
		})
	}
}

func TestPointRuntimeFilterPreservesOriginalPaths(t *testing.T) {
	t.Run("commuted source predicate", func(t *testing.T) {
		b := newPointRuntimeFilterTestBuilder(t)
		filter := b.qry.Nodes[1].FilterList[0]
		args := filter.GetF().Args
		args[0], args[1] = args[1], args[0]
		before := DeepCopyExpr(filter)
		b.generateRuntimeFilters(2)
		require.Len(t, b.qry.Nodes[0].RuntimeFilterProbeList, 1)
		require.True(t, exprStructuralEqual(before, filter), "the benefit check must not normalize the source predicate")
	})
	t.Run("ordinary cost admission", func(t *testing.T) {
		b := newPointRuntimeFilterTestBuilder(t)
		b.qry.Nodes[1].FilterList = nil
		b.qry.Nodes[2].Stats.HashmapStats.HashmapSize = 1
		b.generateRuntimeFilters(2)
		require.Len(t, b.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Greater(t, b.qry.Nodes[2].RuntimeFilterBuildList[0].UpperLimit, int32(1))
	})
}

func TestPointRuntimeFilterBenefitBoundary(t *testing.T) {
	for _, tc := range []struct {
		ndv  float64
		want bool
	}{{9, false}, {10, false}, {11, true}, {100, true}} {
		t.Run(fmt.Sprintf("ndv=%g", tc.ndv), func(t *testing.T) {
			b := newPointRuntimeFilterTestBuilder(t)
			probe := b.qry.Nodes[0]
			probe.TableDef.TblId = 41
			ctx := &statsCacheCompilerContext{MockCompilerContext: b.compCtx.(*MockCompilerContext), statsCache: NewStatsCache()}
			ctx.statsCache.Set(41, &statspb.StatsInfo{NdvMap: map[string]float64{"id": tc.ndv}})
			b.compCtx = ctx
			b.tag2Table = map[int32]*TableDef{1: probe.TableDef}
			b.generateRuntimeFilters(2)
			require.Equal(t, tc.want, len(probe.RuntimeFilterProbeList) == 1)
		})
	}
}

func TestPointRuntimeFilterNestedOwnership(t *testing.T) {
	b := newPointRuntimeFilterTestBuilder(t)
	probe := *b.qry.Nodes[0]
	probe.NodeId = 3
	probe.BindingTags = []int32{3}
	probe.Stats = DeepCopyStats(probe.Stats)
	typ := probe.TableDef.Cols[0].Typ
	outer := &planpb.Node{
		NodeId: 4, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{3, 2}, OnList: []*planpb.Expr{makeRuntimeFilterTestEq(typ, 3, 2, 0, 0)},
		Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{HashmapSize: 500}},
	}
	b.qry.Nodes = append(b.qry.Nodes, &probe, outer)
	b.qry.Steps = []int32{4}
	b.generateRuntimeFilters(4)
	inner := b.qry.Nodes[2]
	require.Len(t, inner.RuntimeFilterBuildList, 1)
	require.Len(t, outer.RuntimeFilterBuildList, 1)
	require.Len(t, probe.RuntimeFilterProbeList, 1)
	require.True(t, b.pointRuntimeFilterOwnedPair(inner, b.qry.Nodes[0]))
	b.qry.Nodes[0].RuntimeFilterProbeList[0].Tag++
	require.False(t, b.pointRuntimeFilterOwnedPair(inner, b.qry.Nodes[0]), "a foreign filter must not be treated as owned")
}

func TestPointRuntimeFilterRejectsSharedOrCyclicTree(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*QueryBuilder)
	}{
		{name: "shared scan", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].Children[1] = 0 }},
		{name: "cycle", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].Children[1] = 2 }},
		{name: "duplicate binding", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].BindingTags = []int32{1} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newPointRuntimeFilterTestBuilder(t)
			tc.mutate(b)
			ctx := &pointRuntimeFilterContext{root: 2}
			require.False(t, ctx.safe(b))
		})
	}
}
