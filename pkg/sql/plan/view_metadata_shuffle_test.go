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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type catalogOnlyStatsContext struct {
	*viewMetadataConsumerContext
	stats *StatsCache
}

func (c *catalogOnlyStatsContext) GetStatsCache() *StatsCache { return c.stats }

func (c *catalogOnlyStatsContext) BuildTableDefByMoColumns(db, name string) (*TableDef, error) {
	def, err := c.viewMetadataConsumerContext.BuildTableDefByMoColumns(db, name)
	if err != nil || def == nil {
		return def, err
	}
	// Match the frontend's catalog-only shape: identity and columns are known,
	// but primary-key, cluster-key, and column-index metadata is not loaded.
	return &TableDef{Name: def.Name, DbName: def.DbName, DbId: def.DbId,
		TblId: def.TblId, Version: def.Version, TableType: def.TableType, Cols: def.Cols}, nil
}

func TestViewMetadataTableLimitZeroWithCachedStats(t *testing.T) {
	mock := newViewMetadataConsumerContext(t)
	def := mock.tables["nation"]
	def.TblId, def.Version = 1234, 3
	stats := NewStatsInfo()
	stats.TableCnt = 1_000_000
	stats.NdvMap["n_nationkey"] = 100_000
	stats.MinValMap["n_nationkey"], stats.MaxValMap["n_nationkey"] = 1, 1_000_000
	cache := NewStatsCache()
	cache.Set(def.TblId, stats)
	ctx := &catalogOnlyStatsContext{viewMetadataConsumerContext: mock, stats: cache}

	var p *Plan
	require.NotPanics(t, func() {
		var err error
		p, err = buildViewMetadataConsumerPlan(t, ctx, "select * from nation limit 0", false)
		require.NoError(t, err)
	}, "a populated statistics cache must not require full metadata from the catalog-only path")
	require.NotNil(t, p)
	require.Equal(t, 1, ctx.fastCalls)
	columns := GetResultColumnsFromPlan(p)
	require.Len(t, columns, 4, "SELECT * must omit the mock catalog's hidden rowid")
	require.Equal(t, []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
		[]string{columns[0].Name, columns[1].Name, columns[2].Name, columns[3].Name})
	require.Len(t, p.GetQuery().CatalogDependencies, 1)
	dependency := p.GetQuery().CatalogDependencies[0]
	require.Equal(t, int64(def.TblId), dependency.Obj, "keep the real identity needed for table/View replacement invalidation")
	require.Equal(t, int64(def.Version), dependency.Server)
}

func TestDetermineShuffleForScanRequiresSortKeyEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pkey    *planpb.PrimaryKeyDef
		cluster *planpb.ClusterByDef
		noStats bool
		rangeOK bool
	}{
		{name: "catalog-only definition with cached stats"},
		{name: "catalog-only definition without stats", noStats: true},
		{name: "empty primary key", pkey: &planpb.PrimaryKeyDef{PkeyColName: "k"}},
		{name: "fake primary key", pkey: &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}},
		{name: "known primary key", pkey: &planpb.PrimaryKeyDef{PkeyColName: "k", Names: []string{"k"}}, rangeOK: true},
		{name: "cluster key without primary key", cluster: &planpb.ClusterByDef{Name: "k"}, rangeOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewStatsCache()
			if !tc.noStats {
				stats := NewStatsInfo()
				stats.TableCnt = 1_000_000
				stats.NdvMap["k"] = 100_000
				stats.MinValMap["k"], stats.MaxValMap["k"] = 1, 1_000_000
				cache.Set(1234, stats)
			}
			ctx := &statsCacheCompilerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: cache}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, Stats: DefaultStats(),
				TableDef: &TableDef{TblId: 1234, Pkey: tc.pkey, ClusterBy: tc.cluster,
					Name2ColIndex: map[string]int32{"k": 0},
					Cols:          []*ColDef{{Name: "k", Typ: Type{Id: int32(types.T_int64)}}}}}
			require.NotPanics(t, func() { determineShuffleForScan(node, builder) })
			require.True(t, node.Stats.HashmapStats.Shuffle)
			if tc.rangeOK {
				require.Equal(t, planpb.ShuffleType_Range, node.Stats.HashmapStats.ShuffleType)
				require.Equal(t, int64(1), node.Stats.HashmapStats.ShuffleColMin)
				require.Equal(t, int64(1_000_000), node.Stats.HashmapStats.ShuffleColMax)
			} else {
				require.Equal(t, planpb.ShuffleType_Hash, node.Stats.HashmapStats.ShuffleType)
			}
		})
	}
}
