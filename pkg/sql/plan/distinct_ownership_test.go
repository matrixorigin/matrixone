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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func distinctOwnershipTestBuilder(functionID int32) (*QueryBuilder, *planpb.Node) {
	arg := distinctAggTestCol(types.T_int64, 1, 1, 800_000)
	agg := distinctAggTestExpr(functionID, true, planpb.Type{Id: int32(types.T_int64)}, arg)
	builder, outer := newDistinctAggTestBuilder(1_000_000, 20_000, 800_000, []*planpb.Expr{agg})
	cache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 1_000_000
	stats.NdvMap["g"] = 20_000
	stats.NdvMap["v"] = 800_000
	cache.Set(1, stats)
	builder.compCtx = &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          cache,
	}
	builder.tag2Table[1] = &planpb.TableDef{TblId: 1, Cols: []*planpb.ColDef{
		{Name: "g", Typ: outer.GroupBy[0].Typ},
		{Name: "v", Typ: arg.Typ},
	}}
	return builder, outer
}

func TestDistinctRewritePreservesGroupCardinality(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
	}{{"count", function.COUNT}, {"sum", function.SUM}, {"avg", function.AVG}} {
		t.Run(tc.name, func(t *testing.T) {
			builder, outer := distinctOwnershipTestBuilder(tc.id)
			require.NoError(t, builder.optimizeDistinctAgg(1))
			ReCalcNodeStats(1, builder, false, false, true)
			require.Equal(t, 20_000.0, outer.Stats.Outcnt,
				"rewriting exact distinct must preserve the original grouping estimate")
			require.Equal(t, 20_000.0, getExprNdv(outer.GroupBy[0], builder))
		})
	}
}

func TestTupleDistinctRewritePreservesGroupCardinality(t *testing.T) {
	builder, outer := distinctOwnershipTestBuilder(function.COUNT)
	stats := builder.compCtx.GetStatsCache().Get(1)
	stats.GetStats().NdvMap["g"] = 4
	outer.GroupBy[0].Ndv = 4
	outer.Stats.Outcnt = 4
	outer.Stats.HashmapStats.HashmapSize = 4
	arg := outer.AggList[0].GetF().Args[0]
	arg.Typ.NotNullable = true
	outer.GroupBy[0].Typ.NotNullable = true
	builder.tag2Table[1].Cols[0].Typ.NotNullable = true
	builder.tag2Table[1].Cols[1].Typ.NotNullable = true
	outer.AggList[0].GetF().Args = append(outer.AggList[0].GetF().Args, outer.GroupBy[0])
	require.NoError(t, builder.optimizeDistinctAgg(1))
	require.Len(t, builder.qry.Nodes, 4, "the tuple must traverse both pair stages")
	ReCalcNodeStats(1, builder, false, false, true)
	require.Equal(t, 4.0, outer.Stats.Outcnt)
	finalPair := builder.qry.Nodes[outer.Children[0]]
	require.Equal(t, 800_000.0, getExprNdv(finalPair.GroupBy[1], builder))
}

func TestDistinctRewriteDoesNotTrustUnknownSourceEstimate(t *testing.T) {
	builder, outer := distinctOwnershipTestBuilder(function.COUNT)
	builder.compCtx.GetStatsCache().Delete(1)
	require.Greater(t, outer.GroupBy[0].Ndv, 0.0)
	require.NoError(t, builder.optimizeDistinctAgg(1))
	require.Equal(t, -1.0, getExprNdv(outer.GroupBy[0], builder),
		"an arbitrary carried estimate must not become trusted through a rewrite")
}

func TestAggregateReuseRequiresRetainedActivePartitionKey(t *testing.T) {
	for _, afterRemap := range []bool{false, true} {
		for _, tc := range []struct {
			name                             string
			partition                        int32
			inactive, stringRange, wantReuse bool
		}{
			{"compatible", 0, false, false, true},
			{"different key", 1, false, false, false},
			{"inactive key", 0, true, false, false},
			{"lossy string range", 0, false, true, false},
		} {
			t.Run(fmt.Sprintf("%s/remapped=%v", tc.name, afterRemap), func(t *testing.T) {
				builder, outer := distinctOwnershipTestBuilder(function.COUNT)
				require.NoError(t, builder.optimizeDistinctAgg(1))
				pair := builder.qry.Nodes[outer.Children[0]]
				pair.Stats.HashmapStats.Shuffle = true
				pair.Stats.HashmapStats.ShuffleType = planpb.ShuffleType_Hash
				pair.Stats.HashmapStats.ShuffleColIdx = tc.partition
				outer.Stats.HashmapStats.HashmapSize = 20_000
				// A failed proof must also clear a decision from an earlier pass.
				outer.Stats.HashmapStats.Shuffle = true
				if tc.inactive {
					outer.GroupingFlag = []bool{false}
				}
				if tc.stringRange {
					outer.GroupBy[0].Typ.Id = int32(types.T_varchar)
					pair.GroupBy[0].Typ.Id = int32(types.T_varchar)
					pair.Stats.HashmapStats.ShuffleType = planpb.ShuffleType_Range
				}
				if afterRemap {
					outer.GroupBy[0].GetCol().RelPos = 0
					pair.ProjectList = []*planpb.Expr{distinctAggTestCol(types.T_int32, -1, 0, 20_000)}
				}
				determineShuffleForGroupBy(outer, builder, afterRemap)
				require.Equal(t, tc.wantReuse, outer.Stats.HashmapStats.Shuffle)
				if tc.wantReuse {
					require.Equal(t, planpb.ShuffleMethod_Reuse, outer.Stats.HashmapStats.ShuffleMethod)
				} else {
					require.Equal(t, int32(-1), outer.Stats.HashmapStats.ShuffleColIdx)
				}
			})
		}
	}
}

func TestAggregateShuffleDoesNotExchangeBeforePartialReduction(t *testing.T) {
	builder, outer := distinctOwnershipTestBuilder(function.COUNT)
	require.NoError(t, builder.optimizeDistinctAgg(1))
	pair := builder.qry.Nodes[outer.Children[0]]
	pair.Stats.Outcnt = 10_000_000
	pair.Stats.HashmapStats.Shuffle = true
	pair.Stats.HashmapStats.ShuffleColIdx = 1
	outer.GroupBy[0].Ndv = 3_000_000
	outer.Stats.Outcnt = 3_000_000
	outer.Stats.HashmapStats.HashmapSize = 3_000_000
	determineShuffleForGroupBy(outer, builder, false)
	require.False(t, outer.Stats.HashmapStats.Shuffle,
		"high marginal NDV cannot justify moving a dominant group's unreduced state")
}
