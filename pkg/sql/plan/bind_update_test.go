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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIrregularIndexAffectedByUpdate(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Indexes: []*IndexDef{
			{
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexName:       "idx",
				IndexTableName:  "idx_entries",
				TableExist:      true,
				Parts:           []string{"vec"},
				IncludedColumns: []string{"title"},
			},
		},
	}

	for _, tt := range []struct {
		name string
		col  string
		want bool
	}{
		{name: "indexed part", col: "vec", want: true},
		{name: "included column", col: "title", want: true},
		{name: "unrelated column", col: "id", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			affected, err := irregularIndexAffectedByUpdate(
				tableDef, tableDef.Indexes[0], map[string]tree.Expr{tt.col: nil})
			require.NoError(t, err)
			require.Equal(t, tt.want, affected)
		})
	}
}

type updateIndexBuildTestContext struct {
	*MockCompilerContext
	statsCache *StatsCache
	statsByID  map[uint64]*statspb.StatsInfo
}

func (ctx *updateIndexBuildTestContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func (ctx *updateIndexBuildTestContext) Stats(obj *planpb.ObjectRef, _ *Snapshot) (*statspb.StatsInfo, error) {
	if obj == nil {
		return nil, nil
	}
	return ctx.statsByID[uint64(obj.Obj)], nil
}

type updateIndexBuildTestOptimizer struct {
	ctx *updateIndexBuildTestContext
}

func (optimizer *updateIndexBuildTestOptimizer) CurrentContext() CompilerContext {
	return optimizer.ctx
}

func (optimizer *updateIndexBuildTestOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	queryPlan, err := BuildPlan(optimizer.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return queryPlan.GetQuery(), nil
}

func findUpdateSecondaryIndexLookupJoin(t *testing.T, query *planpb.Query) *planpb.Node {
	t.Helper()
	visited := make(map[int32]struct{}, len(query.Nodes))
	var visit func(int32) *planpb.Node
	visit = func(nodeID int32) *planpb.Node {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			t.Fatalf("reachable plan references invalid node %d", nodeID)
		}
		if _, ok := visited[nodeID]; ok {
			return nil
		}
		visited[nodeID] = struct{}{}

		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_INNER && len(node.Children) == 2 {
			for _, childID := range node.Children {
				child := query.Nodes[childID]
				if child.NodeType == planpb.Node_TABLE_SCAN && child.TableDef != nil &&
					catalog.IsSecondaryIndexTable(child.TableDef.Name) {
					return node
				}
			}
		}
		for _, childID := range node.Children {
			if found := visit(childID); found != nil {
				return found
			}
		}
		return nil
	}

	for _, rootID := range query.Steps {
		if found := visit(rootID); found != nil {
			return found
		}
	}
	t.Fatal("missing reachable UPDATE secondary-index lookup join")
	return nil
}

func TestUpdateIndexLookupBuildSideUsesScanWidthBehindProject(t *testing.T) {
	newOptimizer := func() Optimizer {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)
		indexTableName := catalog.SecondaryIndexTableNamePrefix + "update_build_side"
		mainTable := mock.ctxt.tables["index_hint_t"]
		rowIDCol := mainTable.Cols[len(mainTable.Cols)-1]
		rowIDCol.ColId = 5
		mainTable.Cols = append(mainTable.Cols[:len(mainTable.Cols)-1],
			&planpb.ColDef{
				ColId: 3, Name: "k", OriginName: "k",
				Typ:     planpb.Type{Id: int32(types.T_varchar), Width: 512},
				Default: &planpb.Default{NullAbility: true},
			},
			&planpb.ColDef{
				ColId: 4, Name: "payload", OriginName: "payload",
				Typ:     planpb.Type{Id: int32(types.T_varchar), Width: 2_000},
				Default: &planpb.Default{NullAbility: true},
			},
			rowIDCol,
		)
		mainTable.Name2ColIndex["k"] = 3
		mainTable.Name2ColIndex["payload"] = 4
		mainTable.Indexes = []*planpb.IndexDef{{
			IndexName:      "idx_k",
			Parts:          []string{"k", catalog.CreateAlias("id")},
			IndexTableName: indexTableName,
			TableExist:     true,
		}}
		addIndexHintIndexTableForTest(mock, indexTableName, 25367)

		const (
			tableRows = uint64(20_000_000)
			indexRows = uint64(30_000_000)
		)
		mainStats := NewStatsInfo()
		mainStats.TableCnt = float64(tableRows)
		mainStats.BlockNumber = 2_500
		mainStats.SizeMap = map[string]uint64{
			"id": tableRows * 4, "a": tableRows * 4, "b": tableRows * 4,
			"k": tableRows * 256, "payload": tableRows * 2_000,
		}
		indexStats := NewStatsInfo()
		// UPDATE leaves old objects and tombstones in independently collected
		// table statistics. Model that transient drift: the logical full target
		// and its regular index still have one current row per base row.
		indexStats.TableCnt = float64(indexRows)
		indexStats.BlockNumber = 2_500
		indexStats.SizeMap = map[string]uint64{
			catalog.IndexTableIndexColName:   indexRows * 280,
			catalog.IndexTablePrimaryColName: indexRows * 16,
		}
		statsCache := NewStatsCache()
		statsCache.Set(mainTable.TblId, mainStats)
		statsCache.Set(25367, indexStats)
		return &updateIndexBuildTestOptimizer{ctx: &updateIndexBuildTestContext{
			MockCompilerContext: &mock.ctxt,
			statsCache:          statsCache,
			statsByID: map[uint64]*statspb.StatsInfo{
				mainTable.TblId: mainStats,
				25367:           indexStats,
			},
		}}
	}

	for _, test := range []struct {
		name           string
		sql            string
		wantIndexBuild bool
	}{
		{name: "expression assignment", sql: "UPDATE index_hint_t SET k = concat(k, 'x')", wantIndexBuild: true},
		{name: "constant assignment", sql: "UPDATE index_hint_t SET k = 'fixed'", wantIndexBuild: true},
		{name: "conditional assignment", sql: "UPDATE index_hint_t SET k = if(id > 0, k, 'fallback')", wantIndexBuild: true},
		{name: "multiple assignments", sql: "UPDATE index_hint_t SET k = concat(k, 'x'), payload = substring(payload, 1, 1000)", wantIndexBuild: true},
		{name: "constant true predicate", sql: "UPDATE index_hint_t SET k = concat(k, 'x') WHERE true", wantIndexBuild: true},
		{name: "selective predicate", sql: "UPDATE index_hint_t SET k = concat(k, 'x') WHERE id > 0"},
		{name: "ordered limit", sql: "UPDATE index_hint_t SET k = concat(k, 'x') ORDER BY id LIMIT 10"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(newOptimizer(), t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			join := findUpdateSecondaryIndexLookupJoin(t, query)
			build := query.Nodes[join.Children[1]]
			buildsIndex := build.NodeType == planpb.Node_TABLE_SCAN && build.TableDef != nil &&
				catalog.IsSecondaryIndexTable(build.TableDef.Name)
			require.Equal(t, test.wantIndexBuild, buildsIndex)
			if !test.wantIndexBuild {
				return
			}
			require.Greater(t, build.Stats.Rowsize, float64(100))
			targetProject := query.Nodes[join.Children[0]]
			require.Equal(t, planpb.Node_PROJECT, targetProject.NodeType)
			require.Equal(t, float64(100), targetProject.Stats.Rowsize)
			require.Len(t, targetProject.Children, 1)
			targetScan := query.Nodes[targetProject.Children[0]]
			for targetScan.NodeType == planpb.Node_PROJECT {
				require.Len(t, targetScan.Children, 1)
				targetScan = query.Nodes[targetScan.Children[0]]
			}
			require.Equal(t, planpb.Node_TABLE_SCAN, targetScan.NodeType)
			require.Greater(t, targetScan.Stats.Rowsize, build.Stats.Rowsize)
		})
	}
}

func TestUpdateIndexLookupBuildSidePreservesNarrowTarget(t *testing.T) {
	newOptimizer := func() Optimizer {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)
		mainTable := mock.ctxt.tables["index_hint_t"]
		mainTable.Indexes = mainTable.Indexes[:1]
		indexTableName := catalog.SecondaryIndexTableNamePrefix + "narrow_build_side"
		mainTable.Indexes[0].IndexTableName = indexTableName
		addIndexHintIndexTableForTest(mock, indexTableName, 25367)
		mock.ctxt.tables[indexTableName].Name2ColIndex[catalog.Row_ID] = 2

		const tableRows = uint64(20_000_000)
		mainStats := NewStatsInfo()
		mainStats.TableCnt = float64(tableRows)
		mainStats.BlockNumber = 2_500
		mainStats.SizeMap = map[string]uint64{
			"id": tableRows * 4, "a": tableRows * 4, "b": tableRows * 4,
		}
		indexStats := NewStatsInfo()
		indexStats.TableCnt = float64(tableRows)
		indexStats.BlockNumber = 2_500
		indexStats.SizeMap = map[string]uint64{
			catalog.IndexTableIndexColName:   tableRows * 32,
			catalog.IndexTablePrimaryColName: tableRows * 4,
		}
		statsCache := NewStatsCache()
		statsCache.Set(mainTable.TblId, mainStats)
		statsCache.Set(25367, indexStats)
		return &updateIndexBuildTestOptimizer{ctx: &updateIndexBuildTestContext{
			MockCompilerContext: &mock.ctxt,
			statsCache:          statsCache,
			statsByID: map[uint64]*statspb.StatsInfo{
				mainTable.TblId: mainStats,
				25367:           indexStats,
			},
		}}
	}

	logicPlan, err := runOneStmt(newOptimizer(), t, "UPDATE index_hint_t SET a = a + 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	join := findUpdateSecondaryIndexLookupJoin(t, query)

	probe := query.Nodes[join.Children[0]]
	require.Equal(t, planpb.Node_TABLE_SCAN, probe.NodeType)
	require.NotNil(t, probe.TableDef)
	require.True(t, catalog.IsSecondaryIndexTable(probe.TableDef.Name))
	require.Less(t, probe.Stats.Rowsize, float64(100))

	build := query.Nodes[join.Children[1]]
	require.Equal(t, planpb.Node_PROJECT, build.NodeType)
	require.Equal(t, float64(100), build.Stats.Rowsize)
	require.Len(t, build.Children, 1)
	targetScan := query.Nodes[build.Children[0]]
	require.Equal(t, planpb.Node_TABLE_SCAN, targetScan.NodeType)
	require.Less(t, targetScan.Stats.Rowsize, probe.Stats.Rowsize)
}

func TestSequentialUpdateProjectionLimit(t *testing.T) {
	require.True(t, withinSequentialUpdateProjectionLimit(3, 3))
	require.True(t, withinSequentialUpdateProjectionLimit(4096, 31))
	require.False(t, withinSequentialUpdateProjectionLimit(4096, 32))
	require.False(t, withinSequentialUpdateProjectionLimit(4096, 4096))
}

func TestClassifyIrregularIndexesForUpdate(t *testing.T) {
	newTableDef := func(indexes ...*IndexDef) *TableDef {
		return &TableDef{
			Pkey:    &PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
			Indexes: indexes,
		}
	}
	newIndex := func(name, algo, params string, parts ...string) *IndexDef {
		return &IndexDef{
			IndexName:       name,
			IndexTableName:  name + "_table",
			IndexAlgo:       algo,
			IndexAlgoParams: params,
			Parts:           parts,
			TableExist:      true,
		}
	}

	tests := []struct {
		name            string
		tableDef        *TableDef
		updateCols      map[string]tree.Expr
		wantInline      int
		wantUnsupported bool
		wantReject      bool
	}{
		{
			name:       "synchronous ivfflat indexed part",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
			wantInline: 1,
		},
		{
			name: "synchronous ivfflat include column keeps whole hidden group",
			tableDef: newTableDef(
				func() *IndexDef {
					idx := newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")
					idx.IncludedColumns = []string{"title"}
					return idx
				}(),
			),
			updateCols: map[string]tree.Expr{"title": nil},
			wantInline: 1,
		},
		{
			name:       "unrelated column",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"note": nil},
		},
		{
			name: "implicit on update indexed column",
			tableDef: func() *TableDef {
				tableDef := newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "updated_at"))
				tableDef.Cols = []*ColDef{
					{Name: "id"},
					{Name: "note"},
					{Name: "updated_at", OnUpdate: &planpb.OnUpdate{}},
				}
				tableDef.Name2ColIndex = map[string]int32{"id": 0, "note": 1, "updated_at": 2}
				return tableDef
			}(),
			updateCols: map[string]tree.Expr{"note": nil},
			wantInline: 1,
		},
		{
			name:       "async ivfflat is cdc only",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), `{"async":"true"}`, "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
		},
		{
			name:       "always async hnsw is cdc only",
			tableDef:   newTableDef(newIndex("hnsw", catalog.MoIndexHnswAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
		},
		{
			name:       "synchronous fulltext",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"body": nil},
			wantInline: 1,
		},
		{
			name:       "synchronous fulltext primary key remains rejected",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"id": nil},
			wantReject: true,
		},
		{
			name:       "async primary key stays modern",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), `{"async":"true"}`, "body")),
			updateCols: map[string]tree.Expr{"id": nil},
		},
		{
			name:       "synchronous master uses modern maintenance",
			tableDef:   newTableDef(newIndex("master", catalog.MOIndexMasterAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"body": nil},
			wantInline: 1,
		},
		{
			name:       "synchronous master primary key uses old-key maintenance",
			tableDef:   newTableDef(newIndex("master", catalog.MOIndexMasterAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"id": nil},
			wantInline: 1,
		},
		{
			name:            "unknown affected algorithm is rejected",
			tableDef:        newTableDef(newIndex("unknown", "UNKNOWN", "", "body")),
			updateCols:      map[string]tree.Expr{"body": nil},
			wantUnsupported: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inline, unsupported, err := classifyIrregularIndexesForUpdate(
				context.Background(), tt.tableDef, tt.updateCols,
			)
			if tt.wantReject {
				require.Error(t, err)
				var routeErr *updatePlannerRouteError
				require.True(t, errors.As(err, &routeErr))
				require.Equal(t, updatePlannerRejected, routeErr.route)
				require.True(t, moerr.IsMoErrCode(routeErr.err, moerr.ErrUnsupportedDML))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantUnsupported, unsupported)
			require.Len(t, inline, tt.wantInline)
		})
	}
}

func TestCoalesceRepeatedPhysicalTargetIndexes(t *testing.T) {
	regular := &IndexDef{IndexName: "idx", IndexTableName: "idx_table"}
	irregular := &IndexDef{IndexName: "ft", IndexTableName: "ft_table"}
	dmlCtx := &DMLContext{
		aliases: []string{"a", "b"},
		tableDefs: []*TableDef{
			{TblId: 100, Indexes: []*IndexDef{regular}},
			{TblId: 100, Indexes: []*IndexDef{regular}},
		},
		updateCol2Expr: []map[string]tree.Expr{
			{"x": nil},
			{"y": nil},
		},
	}

	regularNeedsUpdate := [][]bool{{false}, {true}}
	coalesceRepeatedPhysicalTargetRegularIndexes(dmlCtx, regularNeedsUpdate)
	require.Equal(t, []bool{true}, regularNeedsUpdate[0])
	require.Equal(t, []bool{false}, regularNeedsUpdate[1])

	irregularIndexes := [][]*IndexDef{{irregular}, {irregular}}
	coalesceRepeatedPhysicalTargetIrregularIndexes(dmlCtx, irregularIndexes)
	require.Equal(t, []*IndexDef{irregular}, irregularIndexes[0])
	require.Empty(t, irregularIndexes[1])
}

func TestPrimaryKeyUpdatedDetectsSingleAndCompositeKeys(t *testing.T) {
	tests := []struct {
		name       string
		tableDef   *TableDef
		updateCols map[string]tree.Expr
		want       bool
	}{
		{
			name: "single primary key updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "composite primary key part updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"tenant_id", "id"},
					PkeyColName: catalog.CPrimaryKeyColName,
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "unrelated column updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"title": nil},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, primaryKeyUpdated(tt.tableDef, tt.updateCols))
		})
	}
}
