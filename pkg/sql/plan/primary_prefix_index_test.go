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
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	mock.ctxt.pks[table.Name] = []int{1, 0}
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
			mock := NewMockOptimizer(true)
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
		{name: "singleton ties sorted estimate", sortedSel: 0.000001, secondarySel: 1, tableRows: 1e6, scanRows: 1},
		{name: "small table singleton ties sorted estimate", sortedSel: 0.00005, secondarySel: 1, tableRows: 20000, scanRows: 1},
		{name: "selectivity heuristic does not require byte cost", sortedSel: 0.001, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, noStats: true, wantIndex: true},
		{name: "force bypasses sorted preference", sortedSel: 0.001, secondarySel: 1, tableRows: 1e6, scanRows: 1, force: true, wantIndex: true},
		{name: "known zero estimate keeps sorted scan", sortedSel: 0, secondarySel: 1, tableRows: 1e6, scanRows: 1},
		{name: "sorted range keeps base", sortedSel: 0.001, secondarySel: 0.0001, tableRows: 1e6, scanRows: 8192, rangeFilter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
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
			ctx := NewMockCompilerContext(true)
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
				parts, _ := builder.collectCompositePartBlockFilters(0, false)
				if tc.indexed {
					require.Empty(t, parts, "fresh deferred scans must not take discarded early snapshots")
				}
				builder.mergeFiltersOnCompositeKey(0, true)
				builder.retainConsumedCompositePartBlockFilters(parts)
				builder.appendCompoundKeyBlockFilters(0)
				builder.appendCompositePartBlockFilters(parts)
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
				componentFilters, needsLate := builder.collectCompositePartBlockFilters(0, true)
				require.Equal(t, tc.indexed, needsLate)
				builder.mergeFiltersOnCompositeKey(0, false)
				builder.retainConsumedCompositePartBlockFilters(componentFilters)
				builder.appendCompoundKeyBlockFilters(0)
				builder.appendCompositePartBlockFilters(componentFilters)
				requireFuncNames(t, node.FilterList, "prefix_eq", ">")
				require.Same(t, chosen, node.Stats.HashmapStats, "late lowering preserves chosen distribution")
			}
			if tc.disableBlocks {
				require.Empty(t, node.BlockFilterList)
			} else {
				requireFuncNames(t, node.BlockFilterList, "prefix_eq", "=")
				require.Len(t, node.BlockFilterList, 2, "compound and consumed component pruning survive without duplicates")
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

func TestUniquePointCardinalityAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                                                string
		rows                                                float64
		defaultStats, sorted, nonunique, partial, parameter bool
		siblings, reverse, zeroEstimate                     bool
		want                                                bool
	}{
		{name: "known singleton bypasses broad scan admission", rows: 1e6, want: true},
		{name: "parameter singleton beats sorted prefix", rows: 1e6, sorted: true, parameter: true, want: true},
		{name: "partial unique has no singleton bound", rows: 1e6, partial: true},
		{name: "nonunique has no singleton bound", rows: 1e6, nonunique: true},
		{name: "unflushed unique neutral fallback", defaultStats: true, want: true},
		{name: "unknown zero unique beats broad sibling", defaultStats: true, siblings: true, zeroEstimate: true, want: true},
		{name: "unknown zero unique stays ahead of broad sibling", defaultStats: true, siblings: true, reverse: true, zeroEstimate: true, want: true},
		{name: "neutral unique beats broad sibling", defaultStats: true, siblings: true, want: true},
		{name: "default sorted estimate is not evidence", defaultStats: true, sorted: true, want: true},
		{name: "unflushed nonunique fallback", defaultStats: true, nonunique: true, want: true},
		{name: "zero cardinality uses small table policy", rows: 0, sorted: true, want: true},
		{name: "infinite cardinality has no bound", rows: math.Inf(1), sorted: true},
		{name: "invalid cardinality has no bound", rows: math.NaN(), sorted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addPrimaryPrefixIndexTableForTest(mock)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			table := mock.ctxt.tables["index_hint_t"]
			idx := table.Indexes[2]
			if tc.nonunique {
				idx = table.Indexes[1]
			}
			broad := table.Indexes[0]
			table.Indexes = []*pb.IndexDef{idx}
			if tc.siblings {
				table.Indexes = []*pb.IndexDef{broad, idx}
				if tc.reverse {
					table.Indexes[0], table.Indexes[1] = table.Indexes[1], table.Indexes[0]
				}
			}
			a := makeRangeFilterExpr(tag, 1, "=", 1)
			b := makeRangeFilterExpr(tag, 2, "=", 2)
			a.Selectivity = 1
			b.Selectivity = 1
			if tc.zeroEstimate {
				a.Selectivity = 0
				b.Selectivity = 0
			}
			if tc.sorted {
				a.Selectivity = 0.001
			}
			if tc.parameter {
				b.GetF().Args[1] = &pb.Expr{Typ: pb.Type{Id: int32(types.T_int64)}, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
			}
			filters := []*pb.Expr{a, b}
			if tc.partial {
				filters = filters[:1]
			}
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			node = builder.qry.Nodes[scan]
			node.Stats = &pb.Stats{TableCnt: tc.rows, Cost: tc.rows, Outcnt: tc.rows, Selectivity: 1}
			if tc.defaultStats {
				node.Stats = DefaultStats()
			}
			got := builder.applyIndicesForFiltersRegularIndex(scan, node, map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1}, map[[2]int32]*pb.Expr{})
			if tc.want {
				require.NotEqual(t, scan, got)
				require.Equal(t, pb.Node_INDEX, builder.qry.Nodes[got].JoinType)
				if tc.siblings {
					chosen := builder.qry.Nodes[builder.qry.Nodes[got].Children[1]]
					require.Equal(t, "uk_ab", chosen.IndexScanInfo.IndexName)
				}
			} else {
				require.Equal(t, scan, got)
			}
		})
	}
}

func TestCheckIndexFilterRejectsSignedZeroFloatColumns(t *testing.T) {
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		t.Run(typ.String(), func(t *testing.T) {
			filters := make([]*pb.Expr, 0, 13)
			filters = append(filters, makeEqFilterExpr(1), makeParamInFilterExpr(0, 1, 2), makeParamBetweenFilterExpr(0, 1, 0, 1))
			for _, op := range []string{">", ">=", "<", "<="} {
				forward := makeRangeFilterExpr(0, 1, op, 0)
				reversed := makeRangeFilterExpr(0, 1, op, 0)
				reversed.GetF().Args[0], reversed.GetF().Args[1] = reversed.GetF().Args[1], reversed.GetF().Args[0]
				filters = append(filters, forward, reversed)
			}
			inRange := makeParamBetweenFilterExpr(0, 1, 0, 1)
			inRange.GetF().Func.ObjName = "in_range"
			orFilter := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "or"}, Args: []*pb.Expr{makeRangeFilterExpr(0, 1, ">=", 0), makeRangeFilterExpr(0, 1, "<", 1)}}}}
			filters = append(filters, inRange, orFilter)
			var typeColumns func(*pb.Expr)
			typeColumns = func(expr *pb.Expr) {
				if expr.GetCol() != nil {
					expr.Typ = pb.Type{Id: int32(typ)}
				}
				if fn := expr.GetF(); fn != nil {
					for _, arg := range fn.Args {
						typeColumns(arg)
					}
				}
			}
			for _, filter := range filters {
				typeColumns(filter)
				kind, col := checkIndexFilter(filter.GetF())
				require.Equal(t, UnsupportedIndexCondition, kind)
				require.Nil(t, col)
			}
			// Later covering-index parts must share the same eligibility owner.
			node := &pb.Node{TableDef: &pb.TableDef{Name2ColIndex: map[string]int32{"a": 0, "d": 1}}, FilterList: []*pb.Expr{makeEqFilterExpr(0), filters[0]}}
			require.Equal(t, []int32{0}, tryMatchMoreLeadingFilters(&pb.IndexDef{Parts: []string{"a", "d"}}, node, 0))
		})
	}
	kind, col := checkIndexFilter(makeEqFilterExpr(1).GetF())
	require.Equal(t, EqualIndexCondition, kind)
	require.NotNil(t, col)
}

func TestIndexOnlyByteStringLeadingPredicatesRetainExactResidual(t *testing.T) {
	index := &pb.IndexDef{Parts: []string{"a", catalog.CreateAlias(catalog.CPrimaryKeyColName)}}
	for _, typ := range []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob} {
		t.Run(typ.String(), func(t *testing.T) {
			filter := makeEqFilterExpr(0)
			filter.GetF().Args[0].Typ = pb.Type{Id: int32(typ), NotNullable: true}
			require.True(t, needsIndexOnlyResidualLeadingFilters(index, []*pb.Expr{filter}, []int32{0}))
		})
	}
	require.False(t, needsIndexOnlyResidualLeadingFilters(index, []*pb.Expr{makeEqFilterExpr(0)}, []int32{0}))
}

// Planning must succeed when only part of a residual expression is indexed.
func TestIndexBackfillResidualPlanning(t *testing.T) {
	for _, tc := range []struct{ name, hint, predicate string }{
		{"automatic mixed columns", "", "id<payload"},
		{"forced mixed columns", "force index(uk_ab)", "id<payload"},
		{"index against payload", "", "b<payload"},
		{"nested mixed columns", "force index(idx_ab)", "abs(id)<payload"},
		{"all mapped columns", "force index(uk_ab)", "id<b+10"},
		{"range residual", "force index(idx_ab)", "id<payload and b>0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addPrimaryPrefixIndexTableForTest(mock)
			// Use the same INT type as id: an implicit cast otherwise hides the
			// direct-column shape that exposed partial predicate rewriting.
			mock.ctxt.tables["index_hint_t"].Cols[3].Typ = pb.Type{Id: int32(types.T_int32)}
			p, err := runOneStmt(mock, t, "select payload from index_hint_t "+tc.hint+" where a=1 and b=2 and "+tc.predicate)
			require.NoError(t, err)
			require.True(t, planHasIndexJoin(p))
		})
	}
}

func TestIndexBackfillResidualMapping(t *testing.T) {
	for _, layout := range []string{"composite unique", "composite nonunique", "simple raw"} {
		t.Run(layout, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addPrimaryPrefixIndexTableForTest(mock)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			table := mock.ctxt.tables["index_hint_t"]
			idx := table.Indexes[2]
			if layout == "composite nonunique" {
				idx = table.Indexes[1]
			} else if layout == "simple raw" {
				table.Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
				idx = &pb.IndexDef{Parts: []string{"b"}, Unique: true, IndexTableName: idx.IndexTableName}
			}
			idxTable := DeepCopyTableDef(mock.ctxt.tables[idx.IndexTableName], true)
			if layout == "simple raw" {
				idxTable.Cols[0].Typ = table.Cols[2].Typ
				idxTable.Cols[1].Typ = table.Cols[0].Typ
			}
			mapping, err := builder.regularIndexColumnMap(idx, &pb.Node{TableDef: table, BindingTags: []int32{10}}, 20, idxTable)
			require.NoError(t, err)
			if layout == "simple raw" {
				require.Equal(t, int32(0), mapping[[2]int32{10, 2}].GetCol().ColPos)
				require.Equal(t, int32(1), mapping[[2]int32{10, 0}].GetCol().ColPos)
			} else {
				// b comes from serialized index key part 1; id comes from
				// serialized primary key part 1, independently of index layout.
				for _, pos := range []int32{2, 0} {
					fn := mapping[[2]int32{10, pos}].GetF()
					require.Equal(t, "serial_extract", fn.Func.ObjName)
					require.Equal(t, int64(1), fn.Args[1].GetLit().GetI64Val())
					wantSource := int32(0)
					if pos == 0 {
						wantSource = 1
					}
					require.Equal(t, wantSource, fn.Args[0].GetCol().ColPos)
				}
			}
			col := func(pos int32) *pb.Expr { return GetColExpr(table.Cols[pos].Typ, 10, pos) }
			bind := func(name string, args ...*pb.Expr) *pb.Expr {
				expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
				require.NoError(t, err)
				return expr
			}
			for _, tc := range []struct {
				name string
				expr *pb.Expr
				push bool
			}{
				{"literal", bind("<", col(0), MakePlan2Int64ConstExprWithType(20)), true},
				{"both columns", bind("<", col(0), col(2)), true},
				{"nested and repeated", bind("<", bind("abs", col(0)), bind("+", col(2), col(0))), true},
				{"parameter", bind("<", col(0), &pb.Expr{Typ: table.Cols[0].Typ, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}), true},
				{"unmapped second", bind("<", col(0), col(3)), false},
				{"unmapped nested", bind("<", bind("abs", col(0)), col(3)), false},
				{"foreign same position", bind("<", col(0), GetColExpr(table.Cols[2].Typ, 99, 2)), false},
				{"volatile", bind("<", col(0), bind("rand")), false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					original := DeepCopyExpr(tc.expr)
					node := &pb.Node{TableDef: table, BindingTags: []int32{10}, FilterList: []*pb.Expr{tc.expr}}
					indexNode := &pb.Node{TableDef: idxTable, BindingTags: []int32{20}}
					builder.applyExtraFiltersOnIndex(idx, node, indexNode, nil)
					require.True(t, proto.Equal(original, node.FilterList[0]), "base residual must remain unchanged")
					if !tc.push {
						require.Empty(t, indexNode.FilterList)
						return
					}
					require.Len(t, indexNode.FilterList, 1)
					var checkRefs func(*pb.Expr)
					checkRefs = func(expr *pb.Expr) {
						if ref := expr.GetCol(); ref != nil {
							require.Equal(t, int32(20), ref.RelPos)
							require.Contains(t, []int32{0, 1}, ref.ColPos)
						}
						if fn := expr.GetF(); fn != nil {
							for _, arg := range fn.Args {
								checkRefs(arg)
							}
						}
					}
					checkRefs(indexNode.FilterList[0])
					// The index copy must not alias the retained base expression.
					indexNode.FilterList[0].GetF().Args[0] = MakePlan2Int64ConstExprWithType(99)
					require.True(t, proto.Equal(original, node.FilterList[0]))
				})
			}
			// With no residual there is no map construction or allocation,
			// regardless of the number of unrelated table columns.
			node := &pb.Node{TableDef: table, BindingTags: []int32{10}, FilterList: []*pb.Expr{bind("=", col(2), MakePlan2Int64ConstExprWithType(2))}}
			indexNode := &pb.Node{TableDef: idxTable, BindingTags: []int32{20}}
			indices := []int32{0}
			for _, width := range []int{len(table.Cols), 1000} {
				for len(table.Cols) < width {
					table.Cols = append(table.Cols, &pb.ColDef{})
				}
				require.Zero(t, testing.AllocsPerRun(10, func() { builder.applyExtraFiltersOnIndex(idx, node, indexNode, indices) }))
			}
		})
	}
}

func TestIndexMappedExpressionDomain(t *testing.T) {
	col := GetColExpr(pb.Type{Id: int32(types.T_int32)}, 10, 0)
	mapping := map[[2]int32]*pb.Expr{{10, 0}: col}
	for _, tc := range []struct {
		name string
		expr *pb.Expr
		want bool
	}{
		{"constant list", &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{MakePlan2Int64ConstExprWithType(1)}}}}, true},
		{"column list", &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{col}}}}, false},
		{"correlated", &pb.Expr{Expr: &pb.Expr_Corr{Corr: &pb.CorrColRef{}}}, false},
		{"subquery", &pb.Expr{Expr: &pb.Expr_Sub{Sub: &pb.SubqueryRef{}}}, false},
		{"window", &pb.Expr{Expr: &pb.Expr_W{W: &pb.WindowSpec{}}}, false},
		{"raw", &pb.Expr{Expr: &pb.Expr_Raw{Raw: &pb.RawColRef{}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, exprUsesOnlyMappedCols(tc.expr, mapping)) })
	}
}

func TestIndexColumnMapRequiresCompleteValues(t *testing.T) {
	mock := NewMockOptimizer(true)
	addPrimaryPrefixIndexTableForTest(mock)
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	table := mock.ctxt.tables["index_hint_t"]
	table.Cols[2].Typ = pb.Type{Id: int32(types.T_varchar), Width: 16}
	table.Cols[1].Typ = table.Cols[2].Typ
	idx := table.Indexes[2]
	idx.IndexAlgoParams = `{"prefix_lengths":"b:1"}`
	node := &pb.Node{TableDef: table, BindingTags: []int32{10}}
	idxTable := mock.ctxt.tables[idx.IndexTableName]
	mapping, err := builder.regularIndexColumnMap(idx, node, 20, idxTable)
	require.NoError(t, err)
	require.NotContains(t, mapping, [2]int32{10, 2}, "a truncated index part is not a complete base value")
	require.Contains(t, mapping, [2]int32{10, 0}, "the full primary key is still available")
	require.Contains(t, mapping, [2]int32{10, 1}, "untruncated parts remain usable")
	// A prefix on a PK component must use the complete stored PK instead.
	idx.IndexAlgoParams = `{"prefix_lengths":"a:1"}`
	mapping, err = builder.regularIndexColumnMap(idx, node, 20, idxTable)
	require.NoError(t, err)
	require.Equal(t, int32(1), mapping[[2]int32{10, 1}].GetF().Args[0].GetCol().ColPos)
	idx.IndexAlgoParams = `{bad json`
	mapping, err = builder.regularIndexColumnMap(idx, node, 20, idxTable)
	require.Error(t, err)
	require.Nil(t, mapping)
}

func TestSecondaryDisjunctionBeforeCompositeLowering(t *testing.T) {
	for _, hint := range []string{"", "force index(idx_a)"} {
		for _, predicate := range []string{"b=1 or b=2", "b in (1,2)"} {
			t.Run(hint+predicate, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				proc := mock.ctxt.GetProcess()
				t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
				mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
				addPrimaryPrefixIndexTableForTest(mock)
				table := mock.ctxt.tables["index_hint_t"]
				idx := table.Indexes[0]
				idx.Parts = []string{"b", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
				table.Indexes = []*pb.IndexDef{idx}
				p, err := runOneStmt(mock, t, "select payload from index_hint_t "+hint+" where "+predicate)
				require.NoError(t, err)
				require.Equal(t, "idx_a", findFirstIndexScanName(p))
			})
		}
	}
}
func TestFullPrimaryPointBeforeSecondaryCosting(t *testing.T) {
	for _, hint := range []string{"", "force index(idx_ab)"} {
		t.Run(hint, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			proc := mock.ctxt.GetProcess()
			t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
			addPrimaryPrefixIndexTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			idx := table.Indexes[1]
			idx.Parts = []string{"a", "b", "id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
			table.Indexes = []*pb.IndexDef{idx}
			p, err := runOneStmt(mock, t, "select id from index_hint_t "+hint+" where a=1 and id=9")
			require.NoError(t, err)
			want := ""
			if hint != "" {
				want = "idx_ab"
			}
			require.Equal(t, want, findFirstIndexScanName(p))
		})
	}
}

func TestPrimaryEqualityAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	ctx.GetProcessFunc = func() *process.Process { return proc }
	bind := func(name string, args ...*pb.Expr) *pb.Expr {
		e, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
		require.NoError(t, err)
		return e
	}
	col := func(pos int32) *pb.Expr { return makeExprOptInt64Col(10, pos, "key") }
	value := MakePlan2Int64ConstExprWithType(1)
	a := bind("=", col(0), value)
	b := bind("=", col(1), value)
	reversed := bind("=", value, col(1))
	cast, err := appendCastBeforeExpr(ctx.GetContext(), col(1), pb.Type{Id: int32(types.T_varchar), Width: 32})
	require.NoError(t, err)
	floating := GetColExpr(pb.Type{Id: int32(types.T_float64)}, 10, 1)
	floatEq := bind("=", floating, &pb.Expr{Typ: floating.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Dval{Dval: 1}}}})
	for _, tc := range []struct {
		name    string
		filters []*pb.Expr
		want    bool
	}{
		{"complete", []*pb.Expr{a, b}, true},
		{"reversed", []*pb.Expr{a, reversed}, true},
		{"prefix", []*pb.Expr{a}, false},
		{"duplicate", []*pb.Expr{a, DeepCopyExpr(a)}, false},
		{"range", []*pb.Expr{a, bind(">", col(1), value)}, false},
		{"in", []*pb.Expr{a, makeExprOptInExpr(t, ctx, 10, 1, 1, 2)}, false},
		{"foreign tag", []*pb.Expr{a, bind("=", makeExprOptInt64Col(99, 1, "other"), value)}, false},
		{"column rhs", []*pb.Expr{a, bind("=", col(1), col(2))}, false},
		{"cast column", []*pb.Expr{a, bind("=", cast, makePlan2StringConstExprWithType("1"))}, false},
		{"float domain", []*pb.Expr{a, floatEq}, false},
		{"parameter", []*pb.Expr{a, bind("=", col(1), &pb.Expr{Typ: col(1).Typ, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}})}, true},
		{"malformed", []*pb.Expr{a, {Expr: &pb.Expr_F{F: &pb.Function{}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &pb.Node{TableDef: makeExprOptCompositeSortKeyTableDef(), BindingTags: []int32{10}, FilterList: tc.filters}
			require.Equal(t, tc.want, hasPrimaryKeyEquality(node))
			require.Zero(t, testing.AllocsPerRun(10, func() { hasPrimaryKeyEquality(node) }))
		})
	}
	node := &pb.Node{TableDef: makeExprOptCompositeSortKeyTableDef(), BindingTags: []int32{10}, FilterList: []*pb.Expr{a, b}}
	node.TableDef.Pkey.Names = []string{"a", "a"}
	require.False(t, hasPrimaryKeyEquality(node))
	node.TableDef.Pkey.Names = []string{"a", "missing"}
	require.False(t, hasPrimaryKeyEquality(node))
}

func TestDeferredLogicalNormalization(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	ctx.GetProcessFunc = func() *process.Process { return proc }
	builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	table := makeExprOptCompositeSortKeyTableDef()
	table.Indexes = []*pb.IndexDef{{IndexName: "idx_c", Parts: []string{"c"}, TableExist: true}}
	bind := func(name string, args ...*pb.Expr) *pb.Expr {
		e, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
		require.NoError(t, err)
		return e
	}
	a1 := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	a2 := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 2)
	b3 := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 1, "b"), 3)
	c1 := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 2, "c"), 1)
	c2 := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 2, "c"), 2)
	for _, tc := range []struct {
		expr *pb.Expr
		want string
	}{{bind("or", c1, c2), "in"}, {bind("or", a1, bind("and", a2, b3)), "or"}, {bind("or", a1, a2), "in"}} {
		expr := tc.expr
		chosen := &pb.HashMapStats{HashmapSize: 17, Shuffle: true}
		node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{expr}, Stats: &pb.Stats{HashmapStats: chosen}}
		builder.qry.Nodes = []*pb.Node{node}
		for range 2 {
			builder.mergeFiltersOnCompositeKey(0, true)
		}
		var check func(*pb.Expr)
		check = func(e *pb.Expr) {
			if ref := e.GetCol(); ref != nil {
				require.Less(t, ref.ColPos, int32(3))
				require.Equal(t, tag, ref.RelPos)
			}
			if fn := e.GetF(); fn != nil {
				require.NotContains(t, []string{"prefix_eq", "prefix_in", "serial", "serial_full"}, fn.Func.ObjName)
				for _, arg := range fn.Args {
					check(arg)
				}
			}
		}
		for _, e := range node.FilterList {
			check(e)
		}
		require.Same(t, chosen, node.Stats.HashmapStats)
		require.Equal(t, tc.want, node.FilterList[0].GetF().Func.ObjName)
		early, _ := builder.collectCompositePartBlockFilters(0, false)
		require.Empty(t, early)
		if fn := node.FilterList[0].GetF(); fn.Func.ObjName == "in" && fn.Args[0].GetCol().ColPos == 0 {
			late, needsLate := builder.collectCompositePartBlockFilters(0, true)
			require.True(t, needsLate)
			require.Len(t, late[0], 1)
			require.Equal(t, "in", late[0][0].copy.GetF().Func.ObjName)
			builder.mergeFiltersOnCompositeKey(0, false)
			builder.retainConsumedCompositePartBlockFilters(late)
			builder.appendCompoundKeyBlockFilters(0)
			builder.appendCompositePartBlockFilters(late)
			requireFuncNames(t, node.BlockFilterList, "prefix_in", "in")
		}
	}
	builder.optimizerHints = &OptimizerHints{blockFilter: 2}
	builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{a1}}}
	candidates, needsLate := builder.collectCompositePartBlockFilters(0, true)
	require.True(t, needsLate)
	require.Empty(t, candidates)
	builder.mergeFiltersOnCompositeKey(0, false)
	requireFuncNames(t, builder.qry.Nodes[0].FilterList, "prefix_eq")
	table.Indexes = nil
	candidates, needsLate = builder.collectCompositePartBlockFilters(0, true)
	require.False(t, needsLate)
	require.Empty(t, candidates)
}

// Real-PK plus CLUSTER BY is a metadata boundary, not currently legal DDL.
// Preserve the existing covering-before-point-veto policy for that boundary.
func TestPrimaryPointAdmissionRespectsPhysicalLayout(t *testing.T) {
	for _, tc := range []struct {
		name, projection, hint, want string
		cluster                      bool
	}{
		{"physical primary", "b", "", "", false},
		{"cluster covering", "b", "", "idx_ab", true},
		{"cluster point veto", "payload", "", "", true},
		{"cluster forced backfill", "payload", "force index(idx_ab)", "idx_ab", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			proc := mock.ctxt.GetProcess()
			t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
			addPrimaryPrefixIndexTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}, Cols: []uint64{0}}
			mock.ctxt.pks[table.Name] = []int{0}
			if tc.cluster {
				table.ClusterBy = &pb.ClusterByDef{Name: "a"}
			}
			idx := table.Indexes[1]
			idx.Parts = []string{"id", "b", catalog.CreateAlias("id")}
			table.Indexes = []*pb.IndexDef{idx}
			mock.ctxt.tables[idx.IndexTableName].Cols[1].Typ = table.Cols[0].Typ
			p, err := runOneStmt(mock, t, "select "+tc.projection+" from index_hint_t "+tc.hint+" where id=1")
			require.NoError(t, err)
			require.Equal(t, tc.want, findFirstIndexScanName(p))
		})
	}
}
