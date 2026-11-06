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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Reuse the ordinary RF fixture, adding a second index lookup whose intermediate
// row estimate is inflated even though its key still comes from a bounded scan.
func newBoundedDeleteFilterFixture(t testing.TB) *QueryBuilder {
	b := newRuntimeFilterSingleTestBuilder(t, false)
	index, source, inner := b.qry.Nodes[0], b.qry.Nodes[1], b.qry.Nodes[2]
	index.TableDef.TblId, source.TableDef.TblId = 1, 2
	index.Stats.ForceOneCN = true // An independently existing placement constraint.
	source.Limit = makePlan2Uint64ConstExprWithType(10)
	source.Stats.Outcnt = 1000
	inner.JoinType = planpb.Node_INNER
	inner.Stats.Outcnt, inner.Stats.HashmapStats.HashmapSize = 1_000_000, 1000
	probe := &planpb.Node{
		NodeId: 3, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{3},
		TableDef: &planpb.TableDef{
			TblId: 3, Name: "outer_index", Cols: index.TableDef.Cols,
			Name2ColIndex: map[string]int32{"id": 0},
			Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		},
		Stats: &planpb.Stats{Outcnt: 1_000_000, TableCnt: 1_000_000, Cost: 1_000_000, Selectivity: 1, BlockNum: 123},
	}
	outer := &planpb.Node{
		NodeId: 4, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{3, 2},
		OnList: []*planpb.Expr{makeRuntimeFilterTestEq(probe.TableDef.Cols[0].Typ, 3, 2, 0, 0)},
		Stats:  &planpb.Stats{Outcnt: 1_000_000, HashmapStats: &planpb.HashMapStats{HashmapSize: 1_000_000}},
	}
	b.qry.Nodes = append(b.qry.Nodes, probe, outer,
		&planpb.Node{NodeId: 5, NodeType: planpb.Node_LOCK_OP, Children: []int32{4}, Stats: DefaultStats()},
		&planpb.Node{NodeId: 6, NodeType: planpb.Node_MULTI_UPDATE, Children: []int32{5}, Stats: DefaultStats(),
			UpdateCtxList: []*planpb.UpdateCtx{{TableDef: source.TableDef}}},
	)
	b.qry.StmtType, b.qry.Steps = planpb.Query_DELETE, []int32{6}
	b.deleteIndexScanTables = map[int32]deleteIndexScan{1: {tableID: 1}, 3: {tableID: 3}}
	cache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt, stats.NdvMap["id"] = 1_000_000, 1_000_000
	cache.Set(1, stats)
	cache.Set(3, stats)
	b.compCtx = &statsCacheCompilerContext{MockCompilerContext: b.compCtx.(*MockCompilerContext), statsCache: cache}
	b.tag2Table = map[int32]*TableDef{1: index.TableDef, 3: probe.TableDef}
	b.generateRuntimeFilters(6)
	require.Empty(t, outer.RuntimeFilterBuildList, "the original cost gate must reject the target")
	require.NotEmpty(t, inner.RuntimeFilterBuildList, "the control must retain an existing filter")
	return b
}

func TestBoundedDeleteIndexRuntimeFilter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*QueryBuilder)
		want   bool
	}{
		{name: "bounded keys", want: true},
		{name: "original rejection survives a lower late estimate", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].Stats.HashmapStats.HashmapSize = 1
		}},
		{name: "bound itself must pass despite a low late estimate", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].Stats.HashmapStats.HashmapSize = 1
			b.qry.Nodes[1].Limit = makePlan2Uint64ConstExprWithType(100_000)
		}},
		{name: "no original cost rejection", mutate: func(b *QueryBuilder) {
			identity := b.deleteIndexScanTables[3]
			identity.rejectedJoin, identity.rejectedScan = nil, nil
			b.deleteIndexScanTables[3] = identity
		}},
		{name: "replacement join cannot inherit rejection", mutate: func(b *QueryBuilder) { b.qry.Nodes[4] = DeepCopyNode(b.qry.Nodes[4]) }},
		{name: "replacement scan cannot inherit rejection", mutate: func(b *QueryBuilder) { b.qry.Nodes[3] = DeepCopyNode(b.qry.Nodes[3]) }},
		{name: "fanout does not invent source keys", want: true, mutate: func(b *QueryBuilder) { b.qry.Nodes[2].Stats.Outcnt = 2_000_000 }},
		{name: "source filter does not invalidate the key bound", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].FilterList = []*planpb.Expr{makeMixedSideRuntimeFilterResidual(b.qry.Nodes[1].TableDef.Cols[0].Typ, 2, 2)}
		}},
		{name: "ordinary query", mutate: func(b *QueryBuilder) { b.qry.StmtType = planpb.Query_SELECT }},
		{name: "no hard locality", mutate: func(b *QueryBuilder) { b.qry.Nodes[0].Stats.ForceOneCN = false }},
		{name: "multiple steps", mutate: func(b *QueryBuilder) { b.qry.Steps = append(b.qry.Steps, 6) }},
		{name: "changed table identity", mutate: func(b *QueryBuilder) {
			identity := b.deleteIndexScanTables[3]
			identity.tableID = 99
			b.deleteIndexScanTables[3] = identity
		}},
		{name: "unknown index sibling", mutate: func(b *QueryBuilder) { delete(b.deleteIndexScanTables, 1) }},
		{name: "missing limit", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].Limit = nil }},
		{name: "zero limit", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].Limit = makePlan2Uint64ConstExprWithType(0) }},
		{name: "dynamic limit", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].Limit = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		}},
		{name: "unsafe target filter", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].FilterList = []*planpb.Expr{makeMixedSideRuntimeFilterResidual(b.qry.Nodes[3].TableDef.Cols[0].Typ, 3, 3)}
		}},
		{name: "target projection", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].ProjectList = []*planpb.Expr{GetColExpr(b.qry.Nodes[3].TableDef.Cols[0].Typ, 3, 0)}
		}},
		{name: "source projection", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].ProjectList = []*planpb.Expr{GetColExpr(b.qry.Nodes[1].TableDef.Cols[0].Typ, 2, 0)}
		}},
		{name: "nullable source side", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = planpb.Node_LEFT }},
		{name: "row collecting hashmap", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].Stats.HashmapStats.HashOnPK = true }},
		{name: "index on build side", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].Children = []int32{2, 3}
		}},
		{name: "shuffle", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].Stats.HashmapStats.Shuffle = true }},
		{name: "large build retains original guard", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].Stats.Outcnt = 5_000_001 }},
		{name: "bound equals runtime budget", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].Stats.TableCnt = 1000
			limit := GetInFilterCardLimitOnPK(b.compCtx.GetProcess().GetService(), 1000)
			b.qry.Nodes[1].Limit = makePlan2Uint64ConstExprWithType(uint64(limit))
		}},
		{name: "bound above runtime budget", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].Limit = makePlan2Uint64ConstExprWithType(20_000)
			b.qry.Nodes[3].Stats.TableCnt = 1000
		}},
		{name: "multiple equalities", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].OnList = append(b.qry.Nodes[4].OnList, DeepCopyExpr(b.qry.Nodes[4].OnList[0]))
		}},
		{name: "CHAR equality is not a PK identity proof", mutate: func(b *QueryBuilder) {
			typ := planpb.Type{Id: int32(types.T_char), Width: 64}
			b.qry.Nodes[3].TableDef.Cols = []*planpb.ColDef{{Name: "id", Typ: typ}}
			b.qry.Nodes[1].TableDef.Cols = []*planpb.ColDef{{Name: "id", Typ: typ}}
			b.qry.Nodes[4].OnList = []*planpb.Expr{makeRuntimeFilterTestEq(typ, 3, 2, 0, 0)}
		}},
		{name: "wrong source type", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].TableDef.Cols[0] = &planpb.ColDef{Name: "id", Typ: planpb.Type{Id: int32(types.T_float32)}}
		}},
		{name: "shared source", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].Children[0] = 1 }},
		{name: "foreign dependency", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{{Tag: 999}} }},
		{name: "parent dependency on build input", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].RuntimeFilterProbeList = b.qry.Nodes[0].RuntimeFilterProbeList }},
		{name: "RF disabled hint", mutate: func(b *QueryBuilder) { b.optimizerHints = &OptimizerHints{runtimeFilter: 1} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBoundedDeleteFilterFixture(t)
			if tc.mutate != nil {
				tc.mutate(b)
			}
			before := make([]*Stats, len(b.qry.Nodes))
			innerFilters := append([]*planpb.RuntimeFilterSpec(nil), b.qry.Nodes[2].RuntimeFilterBuildList...)
			for i, n := range b.qry.Nodes {
				n.Stats.Dop, n.Stats.Sql = 3, "unchanged"
				if n.Stats.HashmapStats != nil {
					n.Stats.HashmapStats.Nullcnt = 7
					n.Stats.HashmapStats.Ranges = []float64{1, 2}
				}
				before[i] = proto.Clone(n.Stats).(*planpb.Stats)
			}
			b.addBoundedDeleteIndexRuntimeFilters(6)
			require.Equal(t, tc.want, len(b.qry.Nodes[4].RuntimeFilterBuildList) == 1)
			require.Equal(t, tc.want, len(b.qry.Nodes[3].RuntimeFilterProbeList) == 1)
			for i, n := range b.qry.Nodes {
				require.Equal(t, before[i], n.Stats, "late filtering must not rewrite any cost or placement")
			}
			require.Equal(t, innerFilters, b.qry.Nodes[2].RuntimeFilterBuildList)
			if tc.want {
				require.False(t, b.qry.Nodes[3].RuntimeFilterProbeList[0].NotOnPk)
			}
			b.addBoundedDeleteIndexRuntimeFilters(6)
			require.Equal(t, tc.want, len(b.qry.Nodes[4].RuntimeFilterBuildList) == 1, "idempotent")
		})
	}
}

func TestBoundedDeleteFilterPreservesOriginalRejection(t *testing.T) {
	b := newBoundedDeleteFilterFixture(t)
	join, probe := b.qry.Nodes[4], b.qry.Nodes[3]
	b.recordDeleteIndexCostRejection(DeepCopyNode(join), probe)
	b.recordDeleteIndexCostRejection(join, DeepCopyNode(probe))
	require.Same(t, join, b.deleteIndexScanTables[3].rejectedJoin)
	require.Same(t, probe, b.deleteIndexScanTables[3].rejectedScan)
	b.addBoundedDeleteIndexRuntimeFilters(6)
	require.Len(t, join.RuntimeFilterBuildList, 1)
}

func TestBoundedDeleteIndexFilterRejectsForcedDistribution(t *testing.T) {
	old := GetForceScanOnMultiCN()
	SetForceScanOnMultiCN(true)
	t.Cleanup(func() { SetForceScanOnMultiCN(old) })
	b := newBoundedDeleteFilterFixture(t)
	b.addBoundedDeleteIndexRuntimeFilters(6)
	require.Empty(t, b.qry.Nodes[4].RuntimeFilterBuildList)
}
