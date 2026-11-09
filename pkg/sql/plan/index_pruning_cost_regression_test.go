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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestJoinIndexCostsProbeFanout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ndv       float64
		hint      string
		residual  string
		wantIndex bool
	}{
		{name: "many matches from few build keys", ndv: 1_000},
		{name: "selective index control", ndv: 600_000, wantIndex: true},
		{name: "force retains broad lookup", ndv: 1_000, hint: "force index(idx_membership)", wantIndex: true},
		{name: "ignore retains scan", ndv: 600_000, hint: "ignore index(idx_membership)"},
		{name: "safe residual permits costing", ndv: 1_000, residual: " and amount > 0"},
		{name: "JSON cast retains evaluation domain", ndv: 1_000, residual: " and cast(payload as json) is not null", wantIndex: true},
		{name: "JSON function retains evaluation domain", ndv: 1_000, residual: " and json_extract(payload, '$.x') is not null", wantIndex: true},
		{name: "nested unsafe residual retains evaluation domain", ndv: 1_000, residual: " and (amount > 0 or cast(payload as json) is not null)", wantIndex: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(t, tc.ndv)
			// Isolate probe fanout from per-reader preparation costs and host CPU count.
			base := *ctx.MockCompilerContext
			ctx.ResolveVariableFunc = func(name string, system, global bool) (interface{}, error) {
				if name == "max_dop" {
					return int64(1), nil
				}
				return base.ResolveVariable(name, system, global)
			}
			activity := ctx.tables["cost_activity"]
			activity.Cols[2].Typ = activity.Cols[0].Typ
			activity.Name2ColIndex["payload"] = int32(len(activity.Cols))
			activity.Cols = append(activity.Cols, &planpb.ColDef{
				Name: "payload", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 16}, Default: &planpb.Default{},
			})
			activity.Indexes = nil
			addCostActivityRegularIndex(t, ctx, "idx_membership", []string{
				"state", catalog.CreateAlias(catalog.CPrimaryKeyColName),
			}, false)
			buildStats := ctx.statsByID[ctx.tables["cost_tags"].TblId]
			buildStats.TableCnt = 100_000
			buildStats.NdvMap["tenant_id"] = 50_000
			buildStats.MinValMap["tenant_id"] = 0
			buildStats.MaxValMap["tenant_id"] = 49_999
			ctx.statsCache.Set(ctx.tables["cost_tags"].TblId, buildStats)
			optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
			query, err := runOneStmt(optimizer, t, "select count(*) from cost_activity "+tc.hint+
				" where state in (select tenant_id from cost_tags where tenant_id < 100)"+tc.residual)
			require.NoError(t, err)
			require.Equal(t, int64(1), query.GetQuery().MaxDop)
			require.True(t, reachablePlanHasJoinType(query.GetQuery(), planpb.Node_SEMI))
			require.Equal(t, tc.wantIndex, planHasIndexJoin(query))
			if !tc.wantIndex {
				require.Empty(t, findFirstIndexScanName(query))
			}
		})
	}
}

func TestJoinIndexBackfillAdmission(t *testing.T) {
	for _, tc := range []struct {
		name         string
		ndv          float64
		buildRows    float64
		tableRows    float64
		secondNDV    float64
		unique       bool
		hiddenPK     bool
		force        bool
		missingStats bool
		defaultStats bool
		wantSkip     bool
	}{
		{name: "broad lookup preparation", ndv: 1_000, buildRows: 200, wantSkip: true},
		{name: "selective lookup", ndv: 1_000_000, buildRows: 200},
		{name: "unique lookup bounds fanout", ndv: 1_000, buildRows: 200, unique: true},
		{name: "correlated composite keys", ndv: 1_000, secondNDV: 1_000, buildRows: 200, wantSkip: true},
		{name: "selective composite component", ndv: 1_000, secondNDV: 1_000_000, buildRows: 200},
		{name: "hidden PK below capacity", ndv: 20_000, buildRows: 199, hiddenPK: true},
		{name: "hidden PK at capacity", ndv: 20_000, buildRows: 200, hiddenPK: true},
		{name: "hidden PK above capacity", ndv: 20_000, buildRows: 201, hiddenPK: true, wantSkip: true},
		{name: "small table preference", ndv: 10, buildRows: 200, tableRows: 10_000},
		{name: "force bypasses cost", ndv: 1_000, buildRows: 200, force: true},
		{name: "unknown NDV", buildRows: 200},
		{name: "negative NDV", ndv: -1, buildRows: 200},
		{name: "NaN NDV", ndv: math.NaN(), buildRows: 200},
		{name: "infinite NDV", ndv: math.Inf(1), buildRows: 200},
		{name: "inconsistent NDV", ndv: 1_000_001, buildRows: 200},
		{name: "missing statistics", ndv: 1_000, buildRows: 200, missingStats: true},
		{name: "default statistics", ndv: 1_000, buildRows: 200, defaultStats: true},
		{name: "empty build", ndv: 1_000},
		{name: "NaN build", ndv: 1_000, buildRows: math.NaN()},
		{name: "infinite build", ndv: 1_000, buildRows: math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.tableRows
			if rows == 0 {
				rows = 1_000_000
			}
			builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
				[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, nil,
				&planpb.Stats{TableCnt: rows, Outcnt: rows, Cost: rows, Selectivity: 1, BlockNum: int32(math.Ceil(rows / 8192))},
				map[int32]int{0: 1}, tc.force)
			builder.qry.MaxDop = 1
			node := builder.qry.Nodes[scanID]
			stats := builder.getStatsInfoByTableID(node.TableDef.TblId).GetStats()
			stats.TableCnt = rows
			stats.NdvMap = map[string]float64{"tenant_id": tc.ndv, "category": tc.secondNDV}
			if tc.secondNDV > 0 {
				index.Parts = []string{"tenant_id", "category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
			}
			if tc.unique {
				index.Unique = true
				index.Parts = []string{"tenant_id"}
			}
			if tc.hiddenPK {
				name := catalog.FakePrimaryKeyColName
				node.TableDef.Cols[4] = &planpb.ColDef{Name: name, Typ: planpb.Type{Id: int32(types.T_uint64)}}
				node.TableDef.Name2ColIndex[name] = 4
				node.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: name, Names: []string{name}}
				index.Parts[1] = catalog.CreateAlias(name)
			}
			if tc.missingStats {
				node.TableDef.TblId++
			}
			if tc.defaultStats {
				node.Stats = DefaultStats()
			}
			ctx := builder.newEncodedRegularIndexCostContext(node, refs)
			require.Equal(t, tc.wantSkip, ctx.shouldSkipJoinIndexBackfill(index, tc.buildRows))
		})
	}
}

func TestRegularIndexBackfillPhysicalCost(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     float64
		total    float64
		force    bool
		wantSkip bool
	}{
		{name: "single matching key", rows: 1, total: 1_000_000},
		{name: "broad equality reads many blocks", rows: 100_000, total: 1_000_000, wantSkip: true},
		{name: "force retains broad backfill", rows: 100_000, total: 1_000_000, force: true},
		{name: "small table retains preference", rows: 1_000, total: 10_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := makeRangeFilterExpr(0, 0, "=", 7)
			filter.Selectivity = tc.rows / tc.total
			builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
				[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
				[]*planpb.Expr{filter},
				&planpb.Stats{TableCnt: tc.total, Outcnt: tc.rows, Selectivity: filter.Selectivity, Cost: tc.total, BlockNum: int32(math.Ceil(tc.total / 8192))},
				map[int32]int{0: 1, 1: 1}, tc.force)
			node := builder.qry.Nodes[scanID]
			filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
			_, skip, valid := builder.newEncodedRegularIndexCostContext(node, refs).score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
			require.True(t, valid)
			require.Equal(t, tc.wantSkip, skip)
		})
	}
}

func TestRegularIndexBackfillPreparedLocalityRemainsOptimistic(t *testing.T) {
	known := makeIntBetweenFilterExpr(0, 0, 1, 1000)
	known.Selectivity = 0.001
	unknown := makeParamBetweenFilterExpr(0, 0, 0, 1)
	mixed := makeOrFilterExpr(known, unknown)
	mixed.Selectivity = 0.1
	builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
		[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{mixed},
		&planpb.Stats{TableCnt: 1_000_000, Outcnt: 1_000, Selectivity: 0.001, Cost: 1_000_000, BlockNum: 123},
		map[int32]int{0: 1, 1: 1}, false)
	node := builder.qry.Nodes[scanID]
	known.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	unknown.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	ctx := builder.newEncodedRegularIndexCostContext(node, refs)
	work, skip, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.Positive(t, work)
	require.InDelta(t, 0.001, ctx.unknownRangeLowerSelectivities[0], 0.0000001,
		"the known branch remains in the optimistic comparison")
	require.False(t, skip, "an empty unknown branch can leave adjacent known keys in one block")
}

func TestRegularIndexBackfillReaderEstimate(t *testing.T) {
	filter := makeRangeFilterExpr(0, 0, "=", 7)
	filter.Selectivity = 0.001
	builder, scanID, _, refs := newEncodedIndexCostTestCase(t,
		[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 1_000_000, Outcnt: 1_000, Cost: 1_000_000, BlockNum: 123},
		map[int32]int{0: 1, 1: 1}, false)
	node := builder.qry.Nodes[scanID]
	filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	builder.qry.MaxDop = 1
	require.Equal(t, 1.0, builder.newEncodedRegularIndexCostContext(node, refs).localScanReaders())
	builder.qry.MaxDop = 2
	readers := builder.newEncodedRegularIndexCostContext(node, refs).localScanReaders()
	require.GreaterOrEqual(t, readers, 1.0)
	require.LessOrEqual(t, readers, 2.0)
	node.Stats.BlockNum = 0
	require.Equal(t, 1.0, builder.newEncodedRegularIndexCostContext(node, refs).localScanReaders())
}

func TestRegularIndexBackfillEmptyKeys(t *testing.T) {
	require.Zero(t, regularIndexBackfillInputRows(1_000_000, 0))
	require.Zero(t, regularIndexInSearchComparisons(0))
}

func TestRegularIndexBackfillUnsortedPKCost(t *testing.T) {
	filter := makeRangeFilterExpr(0, 0, "=", 7)
	filter.Selectivity = 0.001
	builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
		[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 1_000_000, Outcnt: 1_000, Cost: 1_000_000, BlockNum: 123},
		map[int32]int{0: 1, 3: 1}, false)
	node := builder.qry.Nodes[scanID]
	filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	_, skip, valid := builder.newEncodedRegularIndexCostContext(node, refs).score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.False(t, skip, "ordered PK keys may be concentrated in a few blocks")

	// A different sort key makes the PK rows unordered; the scan must search
	// each surviving block rather than use ordered PK-row lookup.
	node.TableDef.ClusterBy = &planpb.ClusterByDef{Name: "tenant_id"}
	_, skip, valid = builder.newEncodedRegularIndexCostContext(node, refs).score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.True(t, skip)
}

func TestRegularIndexBackfillPackedKeyWidth(t *testing.T) {
	for _, tc := range []struct {
		oid  types.T
		want float64
	}{
		{oid: types.T_int32, want: 6}, {oid: types.T_uint32, want: 6},
		{oid: types.T_int64, want: 10}, {oid: types.T_uint64, want: 10},
	} {
		col := &planpb.ColDef{Name: "pk", Typ: planpb.Type{Id: int32(tc.oid)}}
		require.Equal(t, tc.want, estimatedRegularIndexPartBytes(col, nil, 1_000))
	}
	col := &planpb.ColDef{Name: "pk", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64}}
	require.Equal(t, 23.0, estimatedRegularIndexPartBytes(col, map[string]uint64{"pk": 20_000}, 1_000))
	require.Greater(t, regularIndexBackfillPrepareWork(100, 10, 2), regularIndexBackfillPrepareWork(100, 10, 1))
	require.Zero(t, regularIndexBackfillPrepareWork(0, 10, 2))
}

func TestRegularIndexBackfillBroadEqualityPlan(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(t, 10)
	table := ctx.tables["cost_activity"]
	table.Indexes = nil
	// A 28-byte encoded composite PK is realistic and keeps the automatic
	// rejection independent of the test machine's available reader count.
	ctx.statsByID[table.TblId].SizeMap[catalog.CPrimaryKeyColName] = 600_000 * 28
	addCostActivityRegularIndex(t, ctx, "idx_state_pk", []string{
		"state", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
	for _, tc := range []struct {
		name      string
		hint      string
		wantIndex bool
	}{
		{name: "automatic"},
		{name: "forced", hint: "force index(idx_state_pk)", wantIndex: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := runOneStmt(optimizer, t, "select sum(amount) from cost_activity "+tc.hint+" where state = 'READY'")
			require.NoError(t, err)
			require.Equal(t, tc.wantIndex, planHasIndexJoin(query))
			if tc.wantIndex {
				require.Equal(t, "idx_state_pk", findFirstIndexScanName(query))
			} else {
				require.Empty(t, findFirstIndexScanName(query))
			}
		})
	}
}

func TestRegularIndexBackfillInt32SingleReader(t *testing.T) {
	filter := makeStringEqFilterExpr(0, 2, "READY")
	filter.Selectivity = 0.1
	builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
		[]string{"category", catalog.CreateAlias("tenant_id")}, []*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 1_000_000, Outcnt: 100_000, Selectivity: 0.1, Cost: 1_000_000, BlockNum: 123},
		map[int32]int{2: 1, 3: 1}, false)
	builder.qry.MaxDop = 1
	node := builder.qry.Nodes[scanID]
	node.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "tenant_id", Names: []string{"tenant_id"}}
	filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	ctx := builder.newEncodedRegularIndexCostContext(node, refs)
	require.Equal(t, 1.0, ctx.localScanReaders())
	_, skip, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.True(t, skip, "broad int32 keys must include single-reader preparation CPU")

	filter.Selectivity = 0.001
	node.Stats.Outcnt = 1_000
	ctx = builder.newEncodedRegularIndexCostContext(node, refs)
	_, skip, valid = ctx.score(index, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.False(t, skip, "adjacent selective keys may still make backfill useful")
}

func TestRegularIndexCoveringCostWithoutBlockPruning(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		filter := makeStringEqFilterExpr(0, 2, "READY")
		if prefix {
			filter.GetF().Func.ObjName = "prefix_eq"
		}
		filter.Selectivity = 1.0 / 100_000
		builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
			[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{filter},
			&planpb.Stats{TableCnt: 100_000, Outcnt: 1, Cost: 100_000},
			map[int32]int{2: 1}, false)
		node := builder.qry.Nodes[scanID]
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		ctx := builder.newEncodedRegularIndexCostContext(node, refs)
		normalWork, normalReject, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.False(t, normalReject)
		builder.optimizerHints = &OptimizerHints{blockFilter: 2}
		ctx = builder.newEncodedRegularIndexCostContext(node, refs)
		work, reject, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.Equal(t, prefix, reject, "the additional admission guard applies only to new prefix access")
		require.Equal(t, normalWork, work, "all candidates retain the shared ranking model")
	}
}
