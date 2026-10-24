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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Reuse the hint catalog; only the primary-key representation differs.
func addPrimaryPrefixIndexTableForTest(mock *MockOptimizer) {
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	rowID := table.Cols[len(table.Cols)-1]
	table.Cols = table.Cols[:len(table.Cols)-1]
	keyType := pb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, NotNullable: true}
	key := &pb.ColDef{ColId: 4, Name: catalog.CPrimaryKeyColName, OriginName: catalog.CPrimaryKeyColName, Typ: keyType, Hidden: true, Default: &pb.Default{}}
	table.Name2ColIndex["payload"] = int32(len(table.Cols))
	table.Cols = append(table.Cols, &pb.ColDef{ColId: 3, Name: "payload", OriginName: "payload", Typ: pb.Type{Id: int32(types.T_int64)}, Default: &pb.Default{NullAbility: true}})
	table.Name2ColIndex[catalog.CPrimaryKeyColName] = int32(len(table.Cols))
	table.Cols = append(table.Cols, key)
	table.Cols = append(table.Cols, rowID)
	for i, col := range table.Cols {
		col.ColId = uint64(i)
	}
	table.Cols[0].Pkidx = 2
	table.Cols[1].Primary = true
	table.Cols[1].Pkidx = 1
	table.Cols[1].Typ.NotNullable = true
	table.Cols[1].Default.NullAbility = false
	table.Pkey = &pb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Cols: []uint64{1, 0}, Names: []string{"a", "id"}, CompPkeyCol: key}
	for _, idx := range table.Indexes {
		if !idx.Unique {
			idx.Parts[len(idx.Parts)-1] = catalog.CreateAlias(catalog.CPrimaryKeyColName)
		}
		mock.ctxt.tables[idx.IndexTableName].Cols[1].Typ = keyType
	}
}

// Missing statistics cannot prove a selective sorted prefix; use the existing
// small/unflushed-table ranking. Known estimates are tested separately.
func TestPrimaryKeyPrefixSecondaryPointAccess(t *testing.T) {
	for _, tc := range []struct{ name, sql, index string }{
		{"missing stats automatic", "select payload from index_hint_t where a=1 and b=2", "uk_ab"},
		{"prefix forced", "select payload from index_hint_t force index(uk_ab) where a=1 and b=2", "uk_ab"},
		{"missing stats locking", "select payload from index_hint_t where a=1 and b=2 for update", "uk_ab"},
		{"prefix forced locking", "select payload from index_hint_t force index(uk_ab) where a=1 and b=2 for update", "uk_ab"},
		{"full primary automatic", "select payload from index_hint_t where a=1 and id=9 and b=2", ""},
		{"full primary forced", "select payload from index_hint_t force index(uk_ab) where a=1 and id=9 and b=2", "uk_ab"},
		{"nonunique forced", "select payload from index_hint_t force index(idx_ab) where a=1 and b=2", "idx_ab"},
		{"incomplete unique", "select payload from index_hint_t use index(uk_ab) where a=1", ""},
		{"ignored candidates", "select payload from index_hint_t ignore index(idx_a,idx_ab,uk_ab,idx_id) where a=1 and b=2", ""},
		{"covering control", "select a,b from index_hint_t force index(uk_ab) where a=1 and b=2", "uk_ab"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addPrimaryPrefixIndexTableForTest(mock)
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.index, findFirstIndexScanName(p))
			if tc.index != "" && tc.name != "covering control" {
				require.True(t, planHasIndexJoin(p))
			}
		})
	}
}

func TestSortedPrimaryPrefixPointPreference(t *testing.T) {
	for _, tc := range []struct {
		name                                         string
		sortedSel, secondarySel, tableRows, scanRows float64
		rangeFilter, force, noStats                  bool
		wantIndex                                    bool
	}{
		{name: "selective point below threshold", sortedSel: 0.0009, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, wantIndex: true},
		{name: "selective point at threshold", sortedSel: 0.001, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, wantIndex: true},
		{name: "ordinary prefix above threshold", sortedSel: 0.0011, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, wantIndex: true},
		{name: "no better point", sortedSel: 0.001, secondarySel: 1, tableRows: 1e6, scanRows: 1},
		{name: "small table still needs better point", sortedSel: 0.001, secondarySel: 1, tableRows: 20000, scanRows: 1},
		{name: "unknown cost keeps sorted scan", sortedSel: 0.001, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, noStats: true},
		{name: "force bypasses sorted preference", sortedSel: 0.001, secondarySel: 1, tableRows: 1e6, scanRows: 1, force: true, wantIndex: true},
		{name: "known zero estimate keeps sorted scan", sortedSel: 0, secondarySel: 1, tableRows: 1e6, scanRows: 1},
		{name: "sorted range keeps base", sortedSel: 0.001, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, rangeFilter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addPrimaryPrefixIndexTableForTest(mock)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			table := mock.ctxt.tables["index_hint_t"]
			table.Indexes = []*pb.IndexDef{table.Indexes[2]}
			a := makeRangeFilterExpr(tag, 1, "=", 1)
			a.Selectivity = tc.sortedSel
			op := "="
			if tc.rangeFilter {
				op = ">"
			}
			b := makeRangeFilterExpr(tag, 2, op, 2)
			b.Selectivity = tc.secondarySel
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{a, b}}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			node = builder.qry.Nodes[scan]
			node.Stats = &pb.Stats{TableCnt: tc.tableRows, Outcnt: 1, Selectivity: 1 / tc.tableRows, Cost: tc.scanRows}
			if tc.noStats {
				node.Stats.Cost = 0
			}
			if tc.force {
				require.NoError(t, builder.recordIndexHints(scan, table, []*tree.IndexHint{{HintType: tree.HintForce, HintScope: tree.HintForScan, IndexNames: []string{"uk_ab"}}}))
			}
			got := builder.applyIndicesForFiltersRegularIndex(scan, node, map[[2]int32]int{{tag, 1}: 1, {tag, 2}: 1, {tag, table.Name2ColIndex["payload"]}: 1}, map[[2]int32]*pb.Expr{})
			if tc.wantIndex {
				require.NotEqual(t, scan, got)
				require.Equal(t, pb.Node_INDEX, builder.qry.Nodes[got].JoinType)
			} else {
				require.Equal(t, scan, got)
			}
		})
	}
}

func TestCompositeKeyFilterMergePhases(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		indexed, cluster, disableBlocks bool
	}{
		{name: "indexed primary", indexed: true},
		{name: "indexed cluster", indexed: true, cluster: true},
		{name: "nonindexed primary"},
		{name: "nonindexed cluster", cluster: true},
		{name: "block pruning disabled", indexed: true, disableBlocks: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
			if tc.disableBlocks {
				builder.optimizerHints = &OptimizerHints{blockFilter: 2}
			}
			tag := builder.genNewBindTag()
			table := makeExprOptCompositeSortKeyTableDef()
			if tc.cluster {
				table = makeExprOptCompositeClusterKeyTableDef()
			}
			if tc.indexed {
				table.Indexes = []*pb.IndexDef{{IndexName: "idx_c", Parts: []string{"c"}, TableExist: true}}
			}
			a := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
			c := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 2, "c"), 0)
			builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: table, FilterList: []*pb.Expr{a, c}}}
			node := builder.qry.Nodes[0]
			for range 2 {
				builder.mergeFiltersOnCompositeKey(0, true)
				if tc.indexed {
					requireFuncNames(t, node.FilterList, "=", ">")
					requireNoFuncNames(t, node.FilterList, "prefix_eq")
				} else {
					requireFuncNames(t, node.FilterList, "prefix_eq", ">")
				}
			}
			chosen := &pb.HashMapStats{HashmapSize: 17, Shuffle: true, ShuffleType: pb.ShuffleType_Range}
			node.Stats = &pb.Stats{HashmapStats: chosen}
			for range 2 {
				componentFilters := builder.collectCompositePartBlockFilters(0)
				builder.mergeFiltersOnCompositeKey(0, false)
				builder.retainConsumedCompositePartBlockFilters(componentFilters)
				builder.appendCompoundKeyBlockFilters(0)
				builder.appendCompositePartBlockFilters(componentFilters)
				requireFuncNames(t, node.FilterList, "prefix_eq", ">")
				require.Same(t, chosen, node.Stats.HashmapStats, "late lowering preserves chosen distribution")
			}
			if tc.disableBlocks {
				require.Empty(t, node.BlockFilterList)
			}
			refs := make(map[[2]int32]int)
			builder.countColRefs(0, refs)
			compoundPos, ok := compoundKeyColumn(table)
			require.True(t, ok)
			require.Positive(t, refs[[2]int32{tag, compoundPos}], "the new physical key is required after lowering")
			if tc.disableBlocks {
				require.Zero(t, refs[[2]int32{tag, 0}], "consumed logical row column is no longer referenced")
			}
		})
	}
}
