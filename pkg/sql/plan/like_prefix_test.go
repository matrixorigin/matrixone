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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		{"abc%def", true},
		{"abc_def", true},
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
			require.Contains(t, names, "prefix_eq", "range candidates require an exact residual")
			require.NotContains(t, names, "like", "pure byte prefixes have an exact prefix_eq residual")
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
	require.True(t, ctx.skipPrefixIndexByStats(0), "unknown frequency cannot admit a new automatic prefix path")
	require.Empty(t, ctx.matchRangeBackfill(idx))
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
	require.False(t, ctx.skipPrefixIndexByStats(0))
	require.NotEmpty(t, ctx.matchRangeBackfill(idx))
	historical := *node
	historical.ScanSnapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
	historicalCtx := builder.newEncodedRegularIndexCostContext(&historical, refs)
	require.False(t, historicalCtx.filterFacts[0].prefixKnown, "a sibling current scan's table-ID cache entry is not historical statistics")
	require.Empty(t, historicalCtx.matchRangeBackfill(idx))

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
	require.True(t, ctx.skipPrefixIndexByStats(0))
	require.Empty(t, ctx.matchRangeBackfill(idx))

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
