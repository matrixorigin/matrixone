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
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Use the existing planner fixture with a coherent analyzed population. The
// public SQL test separately verifies collection instead of trusting this input.
func likePrefixPlanOptimizer(mock *MockOptimizer) *encodedIndexPlanTestOptimizer {
	table := mock.ctxt.tables["index_hint_t"]
	stats := NewStatsInfo()
	stats.TableCnt = 500
	stats.TableName = table.Name
	stats.AccurateObjectNumber = 1
	for _, col := range table.Cols {
		stats.NdvMap[col.Name] = 500
		stats.SizeMap[col.Name] = 500 * 16
	}
	stats.ByteRangeMap = map[string]*statspb.ByteRangeDistribution{"a": prefixDistribution(table, 1, 500, 50)}
	cache := NewStatsCache()
	cache.Set(table.TblId, stats)
	return &encodedIndexPlanTestOptimizer{ctx: &encodedIndexPlanTestContext{MockCompilerContext: &mock.ctxt, statsCache: cache, statsByID: map[uint64]*statspb.StatsInfo{table.TblId: stats}}}
}

func prefixDistribution(table *planpb.TableDef, pos int, n, matched uint64) *statspb.ByteRangeDistribution {
	col := table.Cols[pos]
	d := &statspb.ByteRangeDistribution{Version: 1, TypeId: uint32(col.Typ.Id), Charset: col.Typ.Charset, CollationVersion: col.Typ.CollationVersion, TableVersion: table.Version, ColumnSeqnum: uint32(col.Seqnum), PopulationRows: n, SampleRows: n, CompleteSample: true}
	for i := uint64(0); i < n; i++ {
		value := "zzz"
		if i < matched {
			value = "abc"
		}
		d.Values = append(d.Values, []byte(value))
	}
	return d
}

func TestLikePrefixIndexAccess(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		indexed bool
	}{
		{"abc", true},
		{"abc%", true},
		{"abc%def", false},
		{"abc_def", false},
		{"%abc", false},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 40}
			table.Indexes = table.Indexes[:1]
			p, err := runOneStmt(likePrefixPlanOptimizer(mock), t, "select count(*) from index_hint_t where a like '"+tc.pattern+"'")
			require.NoError(t, err)
			if tc.indexed {
				require.Equal(t, "idx_a", findFirstIndexScanName(p))
			} else {
				require.Empty(t, findFirstIndexScanName(p))
			}
		})
	}
}

func TestLikeLiteralPrefix(t *testing.T) {
	for _, tc := range []struct {
		pattern, prefix   string
		wildcard, escaped bool
	}{
		{"abc%def", "abc", true, false}, {"abc_def", "abc", true, false},
		{`abc\_%`, "abc_", true, true}, {`abc\%%`, "abc%", true, true},
		{`abc\\%`, `abc\`, true, true}, {`abc\\\_%`, `abc\_`, true, true},
		{`abc\`, `abc\`, false, true}, {"%abc", "", true, false}, {"", "", false, false},
	} {
		prefix, wildcard, escaped, _ := likeLiteralPrefix(tc.pattern)
		require.Equal(t, tc.prefix, prefix, tc.pattern)
		require.Equal(t, tc.wildcard, wildcard, tc.pattern)
		require.Equal(t, tc.escaped, escaped, tc.pattern)
	}
}

func TestStringPrefixRangeContainsMatchingKeys(t *testing.T) {
	for _, tc := range []struct {
		prefix, upper  string
		binary, usable bool
	}{
		{"abc", "abd", false, true}, {"ab\x00", "ab\x01", false, true},
		{"\u007f", "\u0080", false, true}, {"\ud7ff", "\ue000", false, true},
		{"中文", "中斈", false, true}, {"a\U0010ffff", "b", false, true},
		{"\U0010ffff", "", false, false}, {"\ufffd", "", false, false},
		{"\xff", "", false, false}, {"a\xff", "b", true, true},
		{"\xfe", "\xff", true, true}, {"\xff", "", true, false}, {"", "", false, false},
	} {
		upper, ok := stringPrefixSuccessor(tc.prefix, tc.binary)
		require.Equal(t, tc.usable, ok, "%q", tc.prefix)
		require.Equal(t, tc.upper, upper, "%q", tc.prefix)
		if !ok {
			continue
		}
		packer := types.NewPacker()
		encode := func(s string, trailing bool) []byte {
			packer.Reset()
			packer.EncodeStringType([]byte(s))
			if trailing {
				packer.EncodeInt64(7)
			}
			return append([]byte(nil), packer.Bytes()...)
		}
		lowerKey, upperKey := encode(tc.prefix, false), encode(upper, false)
		for _, suffix := range []string{"", "a", "\x00", "\x00b", "文", "\xff"} {
			value := tc.prefix + suffix
			require.GreaterOrEqual(t, strings.Compare(value, tc.prefix), 0)
			require.Less(t, strings.Compare(value, upper), 0)
			key := encode(value, true)
			require.GreaterOrEqual(t, types.PrefixCompare(key, lowerKey), 0, "%q", value)
			require.Less(t, types.PrefixCompare(key, upperKey), 0, "%q", value)
		}
		packer.Close()
	}
}

func TestLogicalPrefixAdmission(t *testing.T) {
	makePrefix := func() *planpb.Expr {
		expr := makeStringEqFilterExpr(0, 1, "abc")
		expr.GetF().Func.ObjName = "prefix_eq"
		return expr
	}
	ordinary := &planpb.ColDef{Name: "name", Typ: planpb.Type{Id: int32(types.T_varchar)}}
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "id"}, ordinary}}
	require.True(t, logicalPrefixOnTable(makePrefix().GetF(), table))
	for _, name := range []string{catalog.CPrimaryKeyColName, catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.PrefixCBColName + "name"} {
		col := *ordinary
		col.Name = name
		table.Cols[1] = &col
		require.False(t, logicalPrefixOnTable(makePrefix().GetF(), table), name)
	}
	col := *ordinary
	col.Hidden = true
	table.Cols[1] = &col
	require.False(t, logicalPrefixOnTable(makePrefix().GetF(), table))
	for _, typ := range []planpb.Type{{Id: int32(types.T_varchar), PadSpace: true}, {Id: int32(types.T_char)}, {Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}} {
		expr := makePrefix()
		expr.GetF().Args[0].Typ = typ
		_, _, ok := logicalStringPrefix(expr.GetF())
		require.False(t, ok)
	}
	expr := makePrefix()
	expr.GetF().Args[1].GetLit().Src = makePlan2StringConstExprWithType("other")
	_, _, ok := logicalStringPrefix(expr.GetF())
	require.False(t, ok)
	expr = makePrefix()
	expr.GetF().Args[1] = makePlan2NullConstExprWithType()
	_, _, ok = logicalStringPrefix(expr.GetF())
	require.False(t, ok)
	require.Equal(t, UnsupportedIndexCondition, func() int {
		typ, _ := checkIndexFilter(makeOrFilterExpr(makePrefix(), makePrefix()).GetF())
		return typ
	}())
}

func TestPrefixSelectivityUsesCandidateRange(t *testing.T) {
	expr := makeStringEqFilterExpr(0, 1, "abc")
	expr.GetF().Func.ObjName = "prefix_eq"
	table := &planpb.TableDef{Version: 3, Cols: []*planpb.ColDef{{Name: "id"}, {Name: "name", Seqnum: 7, Typ: expr.GetF().Args[0].Typ}}}
	stats := NewStatsInfo()
	stats.TableCnt = 500
	scan := &planpb.Node{TableDef: table}
	_, ok := stringPrefixSelectivity(expr.GetF(), stats, scan)
	require.False(t, ok)
	stats.ByteRangeMap = map[string]*statspb.ByteRangeDistribution{"name": prefixDistribution(table, 1, 500, 150)}
	upper, ok := stringPrefixSelectivity(expr.GetF(), stats, scan)
	require.True(t, ok)
	require.Equal(t, 0.3, upper)
	for _, mutate := range []func(*statspb.ByteRangeDistribution){
		func(d *statspb.ByteRangeDistribution) { d.TableVersion++ },
		func(d *statspb.ByteRangeDistribution) { d.ColumnSeqnum++ },
		func(d *statspb.ByteRangeDistribution) { d.TypeId++ },
		func(d *statspb.ByteRangeDistribution) { d.Charset++ },
		func(d *statspb.ByteRangeDistribution) { d.CollationVersion++ },
		func(d *statspb.ByteRangeDistribution) { d.PopulationRows++ },
	} {
		d := prefixDistribution(table, 1, 500, 150)
		mutate(d)
		stats.ByteRangeMap["name"] = d
		_, ok := stringPrefixSelectivity(expr.GetF(), stats, scan)
		require.False(t, ok)
	}
	stats.TableCnt = 1 << 54
	stats.ByteRangeMap["name"] = prefixDistribution(table, 1, 500, 150)
	_, ok = stringPrefixSelectivity(expr.GetF(), stats, scan)
	require.False(t, ok)
}

func TestPrefixIndexRepresentations(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		typ                                       planpb.Type
		unique, trailing, declaredPrefix, indexed bool
	}{
		{name: "ordinary", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40}, indexed: true},
		{name: "unique", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40}, unique: true, indexed: true},
		{name: "trailing index part", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40}, trailing: true, indexed: true},
		{name: "binary VARCHAR", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40, Charset: uint32(types.CharsetBinary)}, indexed: true},
		{name: "CHAR retains LIKE", typ: planpb.Type{Id: int32(types.T_char), Width: 40}},
		{name: "UCA retains LIKE", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40, Charset: uint32(types.CharsetUTF8MB4UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}},
		{name: "lossy prefix index", typ: planpb.Type{Id: int32(types.T_varchar), Width: 40}, declaredPrefix: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols[1].Typ = tc.typ
			table.Indexes = table.Indexes[:1]
			if tc.unique {
				table.Indexes[0].Unique = true
				table.Indexes[0].Parts = []string{"a"}
				mock.ctxt.tables["idx_hint_a"].Cols[0].Typ = tc.typ
			}
			if tc.trailing {
				table.Indexes[0].Parts = []string{"a", "b", catalog.CreateAlias("id")}
			}
			if tc.declaredPrefix {
				table.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"a:2"}`
			}
			p, err := runOneStmt(likePrefixPlanOptimizer(mock), t, "select count(*) from index_hint_t where a like 'abc%'")
			require.NoError(t, err)
			scan := findFirstIndexScanNode(p)
			if !tc.indexed {
				require.Nil(t, scan)
				return
			}
			require.NotNil(t, scan)
			var names []string
			for _, f := range scan.FilterList {
				if f.GetF() != nil {
					names = append(names, f.GetF().Func.ObjName)
				}
			}
			require.NotContains(t, names, "prefix_eq", "an exact byte-prefix range needs no duplicate residual")
			require.NotContains(t, names, "like", "pure byte prefixes do not retain LIKE")
			if tc.unique {
				require.Contains(t, names, "in_range")
			} else {
				require.Contains(t, names, "prefix_in_range")
			}
		})
	}
}

func TestPrefixCandidateCostAndIndependentPruning(t *testing.T) {
	makeFixture := func() (*QueryBuilder, int32, *planpb.IndexDef, map[[2]int32]int) {
		f := makeStringEqFilterExpr(0, 2, "abc")
		f.GetF().Func.ObjName = "prefix_eq"
		f.Selectivity = 0.0001
		builder, id, idx, refs := newEncodedIndexCostTestCase(t, []string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*planpb.Expr{f}, &planpb.Stats{TableCnt: 800000, Outcnt: 80, Cost: 80, Selectivity: 0.0001}, map[int32]int{1: 1, 2: 1}, false)
		f.GetF().Args[0].GetCol().RelPos = builder.qry.Nodes[id].BindingTags[0]
		return builder, id, idx, refs
	}
	builder, id, idx, refs := makeFixture()
	node := builder.qry.Nodes[id]
	ctx := builder.newEncodedRegularIndexCostContext(node, refs)
	require.Equal(t, 800000.0, ctx.baseRows, "an unsorted row predicate is not a block-count estimate")
	require.False(t, ctx.prefixStatsUsable(0), "unknown frequency cannot admit a new automatic prefix path")
	require.Empty(t, ctx.matchRangeBackfills(idx)[0])
	testCtx := builder.compCtx.(*encodedIndexCostTestContext)
	stats := NewStatsInfo()
	stats.TableCnt = 800000
	sample := prefixDistribution(node.TableDef, 2, 1000, 1)
	sample.PopulationRows = 800000
	sample.CompleteSample = false
	stats.ByteRangeMap = map[string]*statspb.ByteRangeDistribution{"category": sample}
	testCtx.statsCache.Set(node.TableDef.TblId, stats)
	ctx = builder.newEncodedRegularIndexCostContext(node, refs)
	require.True(t, ctx.filterFacts[0].prefixKnown)
	require.Less(t, ctx.filterFacts[0].prefixUpper, 0.12)
	require.True(t, ctx.prefixStatsUsable(0))
	require.NotEmpty(t, ctx.matchRangeBackfills(idx)[0])
	historical := *node
	historical.ScanSnapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
	historicalCtx := builder.newEncodedRegularIndexCostContext(&historical, refs)
	require.False(t, historicalCtx.filterFacts[0].prefixKnown, "a sibling current scan's table-ID cache entry is not historical statistics")
	require.Empty(t, historicalCtx.matchRangeBackfills(idx)[0])

	residual := DeepCopyExpr(node.FilterList[0])
	residual.Selectivity = 0.2
	node.FilterList = append(node.FilterList, residual)
	correlated := builder.newEncodedRegularIndexCostContext(node, refs)
	work, _, valid := correlated.score(idx, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	residual.Selectivity = 1
	neutral := builder.newEncodedRegularIndexCostContext(node, refs)
	neutralWork, _, neutralValid := neutral.score(idx, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, neutralValid)
	require.Equal(t, neutralWork, work, "same-column necessary predicates must not independently reduce backfill work")
	node.FilterList = node.FilterList[:1]

	// Identical extrema cannot distinguish this broad distribution; samples can.
	sample.Values = sample.Values[:0]
	for i := 0; i < 1000; i++ {
		sample.Values = append(sample.Values, []byte("abc"))
	}
	ctx = builder.newEncodedRegularIndexCostContext(node, refs)
	require.Equal(t, 1.0, ctx.filterFacts[0].prefixUpper)
	require.True(t, ctx.prefixStatsUsable(0))
	require.NotEmpty(t, ctx.matchRangeBackfills(idx)[0])
	_, rejected, valid := ctx.score(idx, []int32{0}, nil, encodedRegularIndexCostBackfill)
	require.True(t, valid)
	require.True(t, rejected)

	// The same unsorted prefix must not erase a separately selective sorted key.
	builder, id, _, refs = makeFixture()
	node = builder.qry.Nodes[id]
	node.TableDef.ClusterBy = &planpb.ClusterByDef{Name: "event_id"}
	bound := makeRangeFilterExpr(node.BindingTags[0], 1, ">=", 799000)
	bound.Selectivity = 0.001
	node.FilterList = append(node.FilterList, bound)
	ctx = builder.newEncodedRegularIndexCostContext(node, refs)
	require.Less(t, ctx.baseRows, node.Stats.TableCnt, "preserve independent sorted-key pruning")
}

func TestPrefixEncodedRangeMatchesCostedInterval(t *testing.T) {
	values := make([]string, 0, 36)
	values = append(values, "", "中文", "中斈", "\x7f", "\x80", "\xc2\x80")
	alphabet := []byte{0, 'a', 'b', 254, 255}
	for _, a := range alphabet {
		values = append(values, string([]byte{a}))
		for _, b := range alphabet {
			values = append(values, string([]byte{a, b}))
		}
	}
	packer := types.NewPacker()
	defer packer.Close()
	encode := func(value string, pk bool) []byte {
		packer.Reset()
		packer.EncodeStringType([]byte(value))
		if pk {
			packer.EncodeInt64(7)
		}
		return bytes.Clone(packer.Bytes())
	}
	for _, prefix := range []string{"a", "a\x00", "\xfe", "中文", "\x7f"} {
		for _, binary := range []bool{false, true} {
			upper, ok := stringPrefixSuccessor(prefix, binary)
			if !ok {
				continue
			}
			lo, hi := encode(prefix, false), encode(upper, false)
			for _, value := range values {
				want := value >= prefix && value < upper
				key := encode(value, true)
				got := types.PrefixCompare(key, lo) >= 0 && types.PrefixCompare(key, hi) < 0
				require.Equal(t, want, got, "prefix=%q upper=%q value=%q", prefix, upper, value)
			}
		}
	}
}

func TestPrefixOutputWorkDoesNotMultiplyCorrelatedPredicates(t *testing.T) {
	first := makeStringEqFilterExpr(0, 2, "abc")
	first.GetF().Func.ObjName = "prefix_eq"
	first.Selectivity = 0.29
	second := DeepCopyExpr(first)
	builder, id, idx, refs := newEncodedIndexCostTestCase(t, []string{"category", "event_time", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*planpb.Expr{first, second}, &planpb.Stats{TableCnt: 1000, Outcnt: 84.1, Cost: 1000}, map[int32]int{2: 3, 3: 1}, false)
	node := builder.qry.Nodes[id]
	for _, f := range node.FilterList {
		f.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}
	stats := NewStatsInfo()
	stats.TableCnt = 1000
	stats.ByteRangeMap = map[string]*statspb.ByteRangeDistribution{"category": prefixDistribution(node.TableDef, 2, 1000, 290)}
	builder.compCtx.(*encodedIndexCostTestContext).statsCache.Set(node.TableDef.TblId, stats)
	for _, shape := range []encodedRegularIndexCostShape{encodedRegularIndexCostIndexOnly, encodedRegularIndexCostBackfill} {
		node.Stats.Outcnt = 84.1
		low := builder.newEncodedRegularIndexCostContext(node, refs)
		lowWork, _, ok := low.score(idx, []int32{0}, []int32{0}, shape)
		require.True(t, ok)
		node.Stats.Outcnt = 290
		upper := builder.newEncodedRegularIndexCostContext(node, refs)
		upperWork, _, ok := upper.score(idx, []int32{0}, []int32{0}, shape)
		require.True(t, ok)
		require.Equal(t, upperWork, lowWork, "correlation cannot discount projected tail decoding or join output, shape=%v", shape)
	}
}

func TestPrefixCostPreservesIndependentColumnReduction(t *testing.T) {
	equality := makeRangeFilterExpr(0, 0, "=", 7)
	equality.Selectivity = 0.1
	prefix := makeStringEqFilterExpr(0, 2, "abc")
	prefix.GetF().Func.ObjName = "prefix_eq"
	prefix.Selectivity = 0.2
	builder, id, idx, refs := newEncodedIndexCostTestCase(t, []string{"tenant_id", "category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*planpb.Expr{equality, prefix}, &planpb.Stats{TableCnt: 800000, Outcnt: 16000, Cost: 800000}, map[int32]int{0: 1, 2: 2}, false)
	node := builder.qry.Nodes[id]
	for _, f := range node.FilterList {
		f.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}
	ctx := builder.newEncodedRegularIndexCostContext(node, refs)
	work, _, ok := ctx.score(idx, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
	require.True(t, ok)
	// The pure LIKE and its exact byte-prefix equivalent have the same row
	// frequency and physical column dependencies, independent of a=7 access.
	like, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "like", []*planpb.Expr{DeepCopyExpr(prefix.GetF().Args[0]), makePlan2StringConstExprWithType("abc%")})
	require.NoError(t, err)
	like.Selectivity = prefix.Selectivity
	node.FilterList[1] = like
	control := builder.newEncodedRegularIndexCostContext(node, refs)
	controlWork, _, ok := control.score(idx, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
	require.True(t, ok)
	require.Equal(t, controlWork, work, "a prefix on another column must retain ordinary covering output selectivity")
}

func TestLikePrefixPlanCostScales(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		rows                    uint64
		unique, broad, noncover bool
	}{
		{name: "original census", rows: 500},
		{name: "ten million", rows: 10000000},
		{name: "old cap boundary", rows: 14000000},
		{name: "twenty million", rows: 20000000},
		{name: "direct unique", rows: 20000000, unique: true},
		{name: "broad encoded", rows: 20000000, broad: true},
		{name: "broad direct", rows: 20000000, unique: true, broad: true},
		{name: "scattered backfill", rows: 10000000, noncover: true},
		{name: "backfill budget", rows: 20000000, noncover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 40}
			table.Indexes = table.Indexes[:1]
			if tc.unique {
				table.Indexes[0].Unique = true
				table.Indexes[0].Parts = []string{"a"}
				mock.ctxt.tables["idx_hint_a"].Cols[0].Typ = table.Cols[1].Typ
			}
			optimizer := likePrefixPlanOptimizer(mock)
			stats := optimizer.ctx.statsByID[table.TblId]
			stats.TableCnt = float64(tc.rows)
			stats.SizeMap = map[string]uint64{"a": tc.rows * 24, "id": tc.rows * 4, "b": tc.rows * 4}
			n, m := min(tc.rows, uint64(2048)), uint64(2)
			if tc.rows == 500 {
				m = 150
			}
			if tc.broad {
				m = n
			}
			d := prefixDistribution(table, 1, n, m)
			d.PopulationRows = tc.rows
			d.CompleteSample = n == tc.rows
			stats.ByteRangeMap["a"] = d
			optimizer.ctx.statsCache.Set(table.TblId, stats)
			projection := "count(*)"
			if tc.noncover {
				projection = "sum(b)"
			}
			p, err := runOneStmt(optimizer, t, "select "+projection+" from index_hint_t where a like 'abc%'")
			require.NoError(t, err)
			if tc.broad || tc.noncover {
				require.Empty(t, findFirstIndexScanName(p))
				return
			}
			require.Equal(t, "idx_a", findFirstIndexScanName(p))
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_JOIN {
					require.NotEqual(t, planpb.Node_INDEX, node.JoinType, "covering must win by actual work")
				}
			}
		})
	}
}

func TestPrefixRangeExactnessAndResidualAgreement(t *testing.T) {
	for _, prefix := range []string{"a", "ab\x00", "中文", "\x7f", "\ud7ff", "a\U0010ffff"} {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		addIndexHintChoiceTableForTest(mock)
		table := mock.ctxt.tables["index_hint_t"]
		table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 40}
		idx := table.Indexes[0]
		f := makeStringEqFilterExpr(0, 1, prefix)
		f.GetF().Func.ObjName = "prefix_eq"
		f.GetF().Args[0].Typ = table.Cols[1].Typ
		_, upper, ok := logicalStringPrefix(f.GetF())
		require.True(t, ok)
		byteUpper, finite := stringPrefixSuccessor(prefix, true)
		exact := stringPrefixRangeIsExact(f.GetF())
		require.Equal(t, finite && upper == byteUpper, exact)
		lowerRange, ok := stringPrefixRange(mock.ctxt.GetContext(), f.GetF())
		require.True(t, ok)
		// Actual lookup materialization and its cost prediction use the same owner.
		builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
		lookup, err := builder.replaceNonEqualCondition(idx, f, 7, mock.ctxt.tables["idx_hint_a"])
		require.NoError(t, err)
		predicted := indexOnlyResidualLeadingFilterPositionsForPrefix(idx, table, []*planpb.Expr{f}, []int32{0}, indexOnlyLookupWillUsePrefixComparison(idx, []*planpb.Expr{f}, []int32{0}, NonEqualIndexCondition, false))
		actual := indexOnlyResidualLeadingFilterPositions(idx, table, []*planpb.Expr{f}, []int32{0}, lookup)
		require.Equal(t, predicted, actual)
		require.Equal(t, !exact, len(actual) > 0)
		require.Equal(t, "in_range", lowerRange.GetF().Func.ObjName)
		values := []string{"", prefix, prefix + "a", prefix + "\x00", prefix + "\xff", upper, upper + "\x00", "\x80", "\xc2\x80"}
		for _, value := range values {
			inRange := value >= prefix && value < upper
			if exact {
				require.Equal(t, strings.HasPrefix(value, prefix), inRange, "p=%q v=%q", prefix, value)
			}
		}
	}
}

func TestLikePrefixKeepsSelectiveINBackfill(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 40}
	table.Indexes = table.Indexes[:1]
	optimizer := likePrefixPlanOptimizer(mock)
	stats := optimizer.ctx.statsByID[table.TblId]
	stats.TableCnt = 100_000
	for _, col := range table.Cols {
		stats.NdvMap[col.Name] = 100_000
		stats.SizeMap[col.Name] = 100_000 * 8
	}
	stats.SizeMap["a"] = 100_000 * 24
	distribution := prefixDistribution(table, 1, 2048, 100)
	distribution.PopulationRows, distribution.CompleteSample = 100_000, false
	stats.ByteRangeMap["a"] = distribution
	for _, tc := range []struct {
		name, predicate, projection string
		backfill                    bool
	}{
		{name: "IN only", predicate: "a in ('abc1','abc2')", projection: "b", backfill: true},
		{name: "computed LIKE control", predicate: "a in ('abc1','abc2') and concat(a,'') like 'abc%'", projection: "b", backfill: true},
		{name: "prefix first", predicate: "a like 'abc%' and a in ('abc1','abc2')", projection: "b", backfill: true},
		{name: "IN first", predicate: "a in ('abc1','abc2') and a like 'abc%'", projection: "b", backfill: true},
		{name: "existing covering path", predicate: "a like 'abc%' and a in ('abc1','abc2')", projection: "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := runOneStmt(optimizer, t, "select "+tc.projection+" from index_hint_t where "+tc.predicate)
			require.NoError(t, err)
			require.Equal(t, "idx_a", findFirstIndexScanName(query))
			require.Equal(t, tc.backfill, planHasIndexJoin(query))
			index := findFirstIndexScanNode(query)
			require.NotNil(t, index)
			require.Equal(t, "prefix_in", index.FilterList[0].GetF().Func.ObjName)
		})
	}
	for _, sql := range []string{
		"select b from index_hint_t where a like 'abc%def' and a in ('abc1','abc2')",
		"prepare guarded_in from 'select b from index_hint_t where a like ''abc%'' and a in (?,?)'",
	} {
		query, err := runOneStmt(optimizer, t, sql)
		require.NoError(t, err)
		require.True(t, planHasIndexJoin(resolveQueryPlan(query)), "unproved prefix access preserves the ordinary IN candidate")
	}

	stats.ByteRangeMap = nil
	query, err := runOneStmt(optimizer, t, "select b from index_hint_t where a like 'abc%' and a in ('abc1','abc2')")
	require.NoError(t, err)
	require.True(t, planHasIndexJoin(query), "unavailable prefix statistics cannot hide ordinary IN")
}

func TestLikePrefixBackfillAlternativesUseBothCosts(t *testing.T) {
	for _, prefixFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "prefix first", false: "ordinary first"}[prefixFirst], func(t *testing.T) {
			prefix := makeStringEqFilterExpr(0, 2, "abc")
			prefix.GetF().Func.ObjName = "prefix_eq"
			prefix.Selectivity = 0.0001
			ordinary := makeStringInFilterExpr(0, 2, "abc1", "zzz")
			ordinary.Selectivity = 0.9
			extra := makeRangeFilterExpr(0, 3, "=", 7)
			extra.GetF().Args[1] = makeDatetimeConst("2026-08-10 12:00:00")
			extra.Selectivity = 0.00001
			builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
				[]string{"category", "event_time", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
				[]*planpb.Expr{prefix, ordinary, extra},
				&planpb.Stats{TableCnt: 100_000, Outcnt: 1, Cost: 100_000}, map[int32]int{1: 1}, false)
			node := builder.qry.Nodes[scanID]
			node.TableDef.Indexes = []*planpb.IndexDef{index}
			for i, predicate := range node.FilterList {
				predicate.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
				fn := predicate.GetF()
				argTypes := make([]types.Type, len(fn.Args))
				for j, arg := range fn.Args {
					argTypes[j] = makeTypeByPlan2Expr(arg)
				}
				overload, err := function.GetFunctionByName(builder.GetContext(), fn.Func.ObjName, argTypes)
				require.NoError(t, err)
				fn.Func.Obj = overload.GetEncodedOverloadID()
				node.FilterList[i] = predicate
			}
			wrapper := builder.compCtx.(*encodedIndexCostTestContext).statsCache.Get(encodedIndexCostUnitTableID)
			stats := wrapper.GetStats()
			for name, size := range stats.SizeMap {
				stats.SizeMap[name] = size / 800_000 * 100_000
			}
			stats.TableCnt = 100_000
			distribution := prefixDistribution(node.TableDef, 2, 2048, 1)
			distribution.PopulationRows, distribution.CompleteSample = 100_000, false
			stats.ByteRangeMap = map[string]*statspb.ByteRangeDistribution{"category": distribution}
			if !prefixFirst {
				node.FilterList[0], node.FilterList[1] = node.FilterList[1], node.FilterList[0]
			}
			ctx := builder.newEncodedRegularIndexCostContext(node, refs)
			matches := ctx.matchRangeBackfills(index)
			require.NotEmpty(t, matches[0])
			require.NotEmpty(t, matches[1])
			require.True(t, regularIndexFiltersHaveStableEvaluation(node))
			originalFilters := node.FilterList
			throwing, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "regexp_replace", []*planpb.Expr{
				DeepCopyExpr(prefix.GetF().Args[0]), makePlan2StringConstExprWithType("["), makePlan2StringConstExprWithType("y"),
			})
			require.NoError(t, err)
			unsafe, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{throwing, makePlan2StringConstExprWithType("x")})
			require.NoError(t, err)
			node.FilterList = append(slices.Clone(originalFilters), unsafe)
			checkHistorical := func(scenario string) {
				t.Helper()
				unsafeCtx := builder.newEncodedRegularIndexCostContext(node, refs)
				require.NotEmpty(t, unsafeCtx.matchRangeBackfills(index)[1], scenario+": both candidates reach the safety gate")
				require.False(t, regularIndexFiltersHaveStableEvaluation(node), scenario)
				selected := builder.applyIndicesForFiltersRegularIndex(scanID, node, refs, map[[2]int32]*planpb.Expr{})
				require.Equal(t, scanID, selected, scenario+": unsafe prefix must not displace the rejected ordinary baseline")
				distributions := stats.ByteRangeMap
				stats.ByteRangeMap = nil
				withoutStats := builder.applyIndicesForFiltersRegularIndex(scanID, node, refs, map[[2]int32]*planpb.Expr{})
				stats.ByteRangeMap = distributions
				require.Equal(t, scanID, withoutStats, scenario+": absent samples retain the same ordinary baseline")
			}
			checkHistorical("throwing residual")
			node.FilterList = slices.Clone(originalFilters)
			ordinaryPosition := 1
			if !prefixFirst {
				ordinaryPosition = 0
			}
			unsafeIN := DeepCopyExpr(originalFilters[ordinaryPosition])
			badBound, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "regexp_replace", []*planpb.Expr{
				makePlan2StringConstExprWithType("x"), makePlan2StringConstExprWithType("["), makePlan2StringConstExprWithType("y"),
			})
			require.NoError(t, err)
			unsafeIN.GetF().Args[1].GetList().List[0] = badBound
			node.FilterList[ordinaryPosition] = unsafeIN
			checkHistorical("throwing IN bound")
			node.FilterList = originalFilters
			prefixPosition := 0
			if !prefixFirst {
				prefixPosition = 1
			}
			prefixFilter := originalFilters[prefixPosition]
			ordinaryFilter := originalFilters[1-prefixPosition]
			lower := DeepCopyExpr(ordinaryFilter)
			lower.GetF().Func.ObjName = ">="
			lower.GetF().Args[1] = DeepCopyExpr(ordinaryFilter.GetF().Args[1].GetList().List[0])
			upper := DeepCopyExpr(lower)
			upper.GetF().Func.ObjName = "<"
			upper.GetF().Args[1].GetLit().Value = &planpb.Literal_Sval{Sval: "abd"}
			for _, bound := range []*planpb.Expr{lower, upper} {
				fn := bound.GetF()
				overload, err := function.GetFunctionByName(builder.GetContext(), fn.Func.ObjName, []types.Type{makeTypeByPlan2Expr(fn.Args[0]), makeTypeByPlan2Expr(fn.Args[1])})
				require.NoError(t, err)
				fn.Func.Obj = overload.GetEncodedOverloadID()
			}
			if prefixFirst {
				node.FilterList = []*planpb.Expr{prefixFilter, lower, upper, DeepCopyExpr(ordinaryFilter), DeepCopyExpr(prefixFilter)}
			} else {
				node.FilterList = []*planpb.Expr{lower, upper, DeepCopyExpr(ordinaryFilter), prefixFilter, DeepCopyExpr(prefixFilter)}
			}
			pairedCtx := builder.newEncodedRegularIndexCostContext(node, refs)
			require.True(t, regularIndexFiltersHaveStableEvaluation(node))
			paired := pairedCtx.matchRangeBackfills(index)
			if prefixFirst {
				require.Equal(t, [2][]int32{{0}, {1, 2}}, paired)
			} else {
				require.Equal(t, [2][]int32{{0, 1}, {3}}, paired)
			}
			// Later same-class predicates cannot displace the first usable
			// ordinary range pair or prefix. Restore the cost fixture below.
			node.FilterList = originalFilters
			for _, positions := range matches {
				_, reject, valid := ctx.score(index, positions, nil, encodedRegularIndexCostBackfill)
				require.True(t, valid)
				require.Equal(t, !ctx.filterFacts[positions[0]].logicalPrefix, reject)
			}
			result := builder.applyIndicesForFiltersRegularIndex(scanID, node, refs, map[[2]int32]*planpb.Expr{})
			require.NotEqual(t, scanID, result, "rejected broad IN must not hide useful prefix backfill")
			indexScan := builder.qry.Nodes[builder.qry.Nodes[result].Children[1]]
			require.Equal(t, "prefix_in_range", indexScan.FilterList[0].GetF().Func.ObjName)
		})
	}
}

func TestRegularIndexFixedInputsRejectsNestedDynamicValues(t *testing.T) {
	literal := func() *planpb.Expr { return makePlan2StringConstExprWithType("abc") }
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
		want bool
	}{
		{name: "literal", expr: literal(), want: true},
		{name: "literal list", expr: &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{literal()}}}}, want: true},
		{name: "parameter in list", expr: &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{{Expr: &planpb.Expr_P{P: &planpb.ParamRef{}}}}}}}},
		{name: "variable in list", expr: &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{{Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "value"}}}}}}}},
		{name: "unknown expression", expr: &planpb.Expr{}},
		{name: "nil"},
		{name: "literal vector", expr: &planpb.Expr{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{StringSource: uint32(types.StringSourceLiteral)}}}, want: true},
		{name: "parameter vector", expr: &planpb.Expr{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{StringSource: uint32(types.StringSourceCOMStmt)}}}},
		{name: "unknown vector source", expr: &planpb.Expr{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{}}}},
		{name: "nil vector", expr: &planpb.Expr{Expr: &planpb.Expr_Vec{}}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, regularIndexFixedInputs(tc.expr)) })
	}
	folded := literal()
	folded.GetLit().Src = literal()
	require.False(t, regularIndexFixedInputs(folded))
	require.False(t, regularIndexFixedInputs(makeOrFilterExpr(folded, literal())))
}

func TestRangeAlternativeLiteralStringINTotality(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock := NewMockOptimizer(true, proc)
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	node := &planpb.Node{BindingTags: []int32{7}}
	makeExpr := func(charset uint8) *planpb.Expr {
		typ := types.NewWithCharset(types.T_varchar, 40, 0, charset)
		values := vector.NewVec(typ)
		defer values.Free(proc.Mp())
		for _, value := range []string{"abc", "abc", "中文", "a\x00b", "\xff"} {
			require.NoError(t, vector.AppendBytes(values, []byte(value), false, proc.Mp()))
		}
		require.NoError(t, vector.AppendBytes(values, nil, true, proc.Mp()))
		data, err := values.MarshalBinary()
		require.NoError(t, err)
		left := &planpb.Expr{Typ: makePlan2Type(&typ), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7}}}
		expr := MakeInExpr(builder.GetContext(), left, int32(values.Length()), data, false)
		expr.GetF().Args[1].GetVec().StringSource = uint32(types.StringSourceLiteral)
		return expr
	}
	for _, charset := range []uint8{types.CharsetLegacy, types.CharsetBinary, types.CharsetUTF8MB4Bin, types.CharsetUTF8} {
		require.True(t, literalStringINIsTotal(makeExpr(charset), node), "charset %d", charset)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*planpb.Expr)
	}{
		{"unrelated overload", func(e *planpb.Expr) { e.GetF().Func.Obj = function.EqualFunctionEncodedID }},
		{"unknown overload", func(e *planpb.Expr) { e.GetF().Func.Obj = -1 }},
		{"unknown collation", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Charset = 250 }},
		{"staged collation", func(e *planpb.Expr) {
			left := e.GetF().Args[0]
			left.Typ.Charset, left.Typ.CollationVersion = 4, 1
			_, err := types.TypeFromPlan(left.Typ)
			require.NoError(t, err, "valid metadata is distinct from executable collation admission")
			payload := e.GetF().Args[1].GetVec()
			vec, _, ok := decodeLiteralVec(payload)
			require.True(t, ok)
			vec.GetType().Charset, vec.GetType().CollationVersion = 4, 1
			payload.Data, err = vec.MarshalBinary()
			require.NoError(t, err)
		}},
		{"UCA", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI) }},
		{"pad space", func(e *planpb.Expr) { e.GetF().Args[0].Typ.PadSpace = true }},
		{"CHAR", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Id = int32(types.T_char) }},
		{"width", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Width++ }},
		{"scale", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Scale++ }},
		{"collation mismatch", func(e *planpb.Expr) { e.GetF().Args[0].Typ.Charset = uint32(types.CharsetBinary) }},
		{"revision", func(e *planpb.Expr) { e.GetF().Args[0].Typ.CollationVersion++ }},
		{"different scan", func(e *planpb.Expr) { e.GetF().Args[0].GetCol().RelPos++ }},
		{"malformed payload", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().Data = []byte("bad") }},
		{"length", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().Len++ }},
		{"empty", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().Len = 0 }},
		{"expression source", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().StringSource = uint32(types.StringSourceExpression) }},
		{"prepared source", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().StringSource = uint32(types.StringSourceSQLPrepare) }},
		{"serialized key", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().IsSerialized = true }},
		{"decimal provenance", func(e *planpb.Expr) { e.GetF().Args[1].GetVec().DecimalLiteralRequiresV82 = true }},
		{"computed RHS", func(e *planpb.Expr) {
			e.GetF().Args[1] = makeOrFilterExpr(makePlan2BoolConstExprWithType(true), makePlan2BoolConstExprWithType(false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := makeExpr(types.CharsetUTF8)
			tc.mutate(e)
			require.False(t, literalStringINIsTotal(e, node))
			if tc.name == "serialized key" || tc.name == "decimal provenance" || tc.name == "expression source" || tc.name == "prepared source" {
				// Even the older legacy-domain proof must not bypass the
				// common provenance barrier, including nested predicates.
				legacy := makeExpr(types.CharsetLegacy)
				tc.mutate(legacy)
				require.True(t, isTruncationSafePredicateExpr(legacy), "otherwise the generic proof masks this guard")
				nested := makeOrFilterExpr(legacy, makePlan2BoolConstExprWithType(false))
				overload, err := function.GetFunctionByName(builder.GetContext(), "or", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()})
				require.NoError(t, err)
				nested.GetF().Func.Obj = overload.GetEncodedOverloadID()
				require.True(t, isTruncationSafePredicateExpr(nested))
				for _, predicate := range []*planpb.Expr{legacy, nested} {
					node.FilterList = []*planpb.Expr{predicate}
					require.False(t, regularIndexFiltersHaveStableEvaluation(node))
				}
			}
		})
	}
}

func TestLikeLiteralEqualitySelectivity(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, function string
		joined, unsafe, folded  bool
		selectivity             float64
	}{
		{name: "literal", pattern: "abc1", function: "=", selectivity: 0.00001},
		{name: "escaped literal", pattern: `abc\_`, function: "like", selectivity: 0.2},
		{name: "prefix", pattern: "abc%", function: "prefix_eq", selectivity: regularIndexUnknownRangeSel},
		{name: "joined scan", pattern: "abc1", function: "=", joined: true, selectivity: 0.2},
		{name: "throwing residual", pattern: "abc1", function: "=", unsafe: true, selectivity: 0.2},
		{name: "folded provenance", pattern: "abc1", function: "like", folded: true, selectivity: 0.2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, scanID, _, _ := newEncodedIndexCostTestCase(t, []string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, nil,
				&planpb.Stats{TableCnt: 100_000, Outcnt: 20_000, Cost: 100_000}, nil, false)
			node := builder.qry.Nodes[scanID]
			builder.tag2Table[node.BindingTags[0]] = node.TableDef
			wrapper := builder.compCtx.(*encodedIndexCostTestContext).statsCache.Get(encodedIndexCostUnitTableID)
			stats := wrapper.GetStats()
			stats.TableCnt = 100_000
			stats.NdvMap["category"] = 100_000
			col := &planpb.Expr{Typ: node.TableDef.Cols[2].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: node.BindingTags[0], ColPos: 2, Name: "category"}}}
			right := makePlan2StringConstExprWithType(tc.pattern)
			if tc.folded {
				right.GetLit().Src = makePlan2StringConstExprWithType(tc.pattern)
			}
			overload, err := function.GetFunctionByName(builder.GetContext(), "like", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
			require.NoError(t, err)
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Selectivity: 0.2, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "like", Obj: overload.GetEncodedOverloadID()}, Args: []*planpb.Expr{col, right},
			}}}
			node.FilterList = []*planpb.Expr{expr}
			if tc.unsafe {
				bad, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "regexp_replace", []*planpb.Expr{DeepCopyExpr(col), makePlan2StringConstExprWithType("["), makePlan2StringConstExprWithType("y")})
				require.NoError(t, err)
				residual, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{bad, makePlan2StringConstExprWithType("x")})
				require.NoError(t, err)
				node.FilterList = append(node.FilterList, residual)
			}
			root := scanID
			if tc.joined {
				other := int32(len(builder.qry.Nodes))
				builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{NodeType: planpb.Node_VALUE_SCAN})
				root = int32(len(builder.qry.Nodes))
				builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{NodeType: planpb.Node_JOIN, Children: []int32{scanID, other}})
			}
			builder.qry.Steps = []int32{root}
			builder.optimizeLikeExpr(root)
			require.Equal(t, tc.function, node.FilterList[0].GetF().Func.ObjName)
			require.InDelta(t, tc.selectivity, node.FilterList[0].Selectivity, 1e-12)
		})
	}
}
