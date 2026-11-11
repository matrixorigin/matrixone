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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func addCompositeUniqueInTableForTest(mock *MockOptimizer) {
	idType := pb.Type{Id: int32(types.T_int32), NotNullable: true}
	intType := pb.Type{Id: int32(types.T_int32)}
	table := &pb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     25400,
		Name:      "composite_unique_in_t",
		Cols: []*pb.ColDef{
			{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &pb.Default{}},
			{ColId: 1, Name: "a", OriginName: "a", Typ: intType, Default: &pb.Default{NullAbility: true}},
			{ColId: 2, Name: "b", OriginName: "b", Typ: intType, Default: &pb.Default{NullAbility: true}},
			{ColId: 3, Name: "c", OriginName: "c", Typ: intType, Default: &pb.Default{NullAbility: true}},
			{ColId: 4, Name: "payload", OriginName: "payload", Typ: pb.Type{Id: int32(types.T_int64)}, Default: &pb.Default{NullAbility: true}},
		},
		Pkey: &pb.PrimaryKeyDef{PkeyColName: "id", Cols: []uint64{0}, Names: []string{"id"}},
		Indexes: []*pb.IndexDef{
			{
				IndexName:      "idx_c",
				Parts:          []string{"c", catalog.CreateAlias("id")},
				IndexTableName: "composite_unique_in_idx_c",
				TableExist:     true,
			},
			{
				IndexName:      "uk_abc",
				Parts:          []string{"a", "b", "c"},
				IndexTableName: "composite_unique_in_uk_abc",
				TableExist:     true,
				Unique:         true,
			},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2, "c": 3, "payload": 4},
	}
	mock.ctxt.objects[table.Name] = &pb.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: 25400}
	mock.ctxt.tables[table.Name] = table
	mock.ctxt.id2name[25400] = table.Name
	mock.ctxt.pks[table.Name] = []int{0}
	addIndexHintIndexTableForTest(mock, "composite_unique_in_idx_c", 25401)
	addIndexHintIndexTableForTest(mock, "composite_unique_in_uk_abc", 25402)
}

func findCompositeUniqueInIndexJoin(p *Plan) *pb.Node {
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == pb.Node_JOIN && node.JoinType == pb.Node_INDEX {
			return node
		}
	}
	return nil
}

func TestCompositeUniqueTrailingInPointAccess(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fromAndWhere  string
		wantIndex     string
		wantLookup    string
		wantItems     int
		wantBaseFilts int
	}{
		{name: "equality control", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c=3", wantIndex: "uk_abc", wantLookup: "=", wantBaseFilts: 3},
		{name: "trailing IN", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,4,5)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 3},
		{name: "forced trailing IN", fromAndWhere: "composite_unique_in_t force index(uk_abc) where a=1 and b=2 and c in (3,4,5)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 3},
		{name: "forced trailing IN with residual and ORDER BY", fromAndWhere: "composite_unique_in_t force index(uk_abc) where a=1 and b=2 and c in (3,4,5) and payload=7 order by payload", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 4},
		{name: "duplicate and absent keys", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (99,99,100)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 2, wantBaseFilts: 3},
		{name: "NULL list item", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,null,5)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 3},
		{name: "residual payload predicate", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,4,5) and payload=7", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 4},
		{name: "incomplete unique prefix", fromAndWhere: "composite_unique_in_t use index(idx_c,uk_abc) where a=1 and c in (3,4,5)", wantIndex: "idx_c", wantBaseFilts: 2},
		{name: "IN on nontrailing part", fromAndWhere: "composite_unique_in_t use index(idx_c,uk_abc) where a in (1,2) and b=2 and c=3", wantIndex: "idx_c", wantBaseFilts: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addCompositeUniqueInTableForTest(mock)
			p, err := runOneStmt(mock, t, "select payload from "+tc.fromAndWhere)
			require.NoError(t, err)
			require.Equal(t, tc.wantIndex, findFirstIndexScanName(p))
			join := findCompositeUniqueInIndexJoin(p)
			require.NotNil(t, join)
			baseScan := p.GetQuery().Nodes[join.Children[0]]
			require.Len(t, baseScan.FilterList, tc.wantBaseFilts)
			if tc.wantIndex != "uk_abc" {
				return
			}

			indexScan := p.GetQuery().Nodes[join.Children[1]]
			require.Len(t, indexScan.FilterList, 1)
			lookup := indexScan.FilterList[0].GetF()
			require.NotNil(t, lookup)
			require.Equal(t, tc.wantLookup, lookup.Func.ObjName)
			if tc.wantItems == 0 {
				return
			}
			values := lookup.Args[1].GetList()
			require.NotNil(t, values)
			require.Len(t, values.List, tc.wantItems)
			for _, value := range values.List {
				serial := value.GetF()
				require.NotNil(t, serial)
				require.Equal(t, "serial", serial.Func.ObjName)
				require.Len(t, serial.Args, 3)
			}
		})
	}
}

func TestCompositeUniqueTrailingInRejectsPrefixLengths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
	}{
		{name: "leading key prefix", params: `{"prefix_lengths":"a:3"}`},
		{name: "trailing key prefix", params: `{"prefix_lengths":"c:3"}`},
		{name: "malformed prefix metadata", params: `{bad json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addCompositeUniqueInTableForTest(mock)
			table := mock.ctxt.tables["composite_unique_in_t"]
			for _, col := range table.Cols {
				if col.Name == "a" || col.Name == "c" {
					col.Typ = pb.Type{Id: int32(types.T_varchar), Width: 20}
				}
			}
			table.Indexes[1].IndexAlgoParams = tc.params
			p, err := runOneStmt(mock, t, "select payload from composite_unique_in_t force index(uk_abc) "+
				"where a='abcdef' and b=2 and c in ('ghijkl','mnopqr')")
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(p))
			require.Nil(t, findCompositeUniqueInIndexJoin(p))
		})
	}
}

func TestCompositeUniqueForcedOrderRejectsPrefixPointAccess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		params    string
		predicate string
	}{
		{name: "leading prefix equality", params: `{"prefix_lengths":"a:3"}`, predicate: "a='abcdef' and b=2 and c='ghijkl'"},
		{name: "leading prefix singleton IN", params: `{"prefix_lengths":"a:3"}`, predicate: "a='abcdef' and b=2 and c in ('ghijkl')"},
		{name: "trailing prefix equality", params: `{"prefix_lengths":"c:3"}`, predicate: "a='abcdef' and b=2 and c='ghijkl'"},
		{name: "trailing prefix singleton IN", params: `{"prefix_lengths":"c:3"}`, predicate: "a='abcdef' and b=2 and c in ('ghijkl')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addCompositeUniqueInTableForTest(mock)
			table := mock.ctxt.tables["composite_unique_in_t"]
			for _, col := range table.Cols {
				if col.Name == "a" || col.Name == "c" {
					col.Typ = pb.Type{Id: int32(types.T_varchar), Width: 20}
				}
			}
			table.Indexes[1].IndexAlgoParams = tc.params
			p, err := runOneStmt(mock, t, "select payload from composite_unique_in_t force index(uk_abc) where "+
				tc.predicate+" order by payload")
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(p))
			require.Nil(t, findCompositeUniqueInIndexJoin(p))
		})
	}
}

func addCompetingCompositeUniqueIndexesForTest(mock *MockOptimizer, reverse bool) *pb.TableDef {
	addCompositeUniqueInTableForTest(mock)
	table := mock.ctxt.tables["composite_unique_in_t"]
	table.Cols[2].Typ = pb.Type{Id: int32(types.T_varchar), Width: 20}
	prefix := &pb.IndexDef{
		IndexName: "uk_prefix", Parts: []string{"a", "b"}, Unique: true, TableExist: true,
		IndexTableName: "composite_unique_in_uk_prefix", IndexAlgoParams: `{"prefix_lengths":"b:3"}`,
	}
	full := &pb.IndexDef{
		IndexName: "uk_full", Parts: []string{"a", "c"}, Unique: true, TableExist: true,
		IndexTableName: "composite_unique_in_uk_full",
	}
	table.Indexes = []*pb.IndexDef{prefix, full}
	if reverse {
		table.Indexes[0], table.Indexes[1] = table.Indexes[1], table.Indexes[0]
	}
	addIndexHintIndexTableForTest(mock, prefix.IndexTableName, 25403)
	addIndexHintIndexTableForTest(mock, full.IndexTableName, 25404)
	return table
}

func TestCompositeUniqueForcedOrderRejectsCompetingPrefixAccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reverse    bool
		knownStats bool
	}{
		{name: "default statistics prefix first"},
		{name: "default statistics full first", reverse: true},
		{name: "known statistics prefix first", knownStats: true},
		{name: "known statistics full first", reverse: true, knownStats: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			table := addCompetingCompositeUniqueIndexesForTest(mock, tc.reverse)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			bFilter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*pb.Expr{
				GetColExpr(table.Cols[2].Typ, tag, 2), makePlan2StringConstExprWithType("abcdef"),
			})
			require.NoError(t, err)
			inFilter := makeParamInFilterExpr(tag, 3, 2)
			for i, item := range inFilter.GetF().Args[1].GetList().List {
				item.Expr = &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: int64(i + 3)}}}
			}
			filters := []*pb.Expr{makeRangeFilterExpr(tag, 1, "=", 1), bFilter, inFilter}
			filters[0].Selectivity = 0.01
			filters[1].Selectivity = 0
			filters[2].Selectivity = 1
			stats := DefaultStats()
			if tc.knownStats {
				stats = &pb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 1, Selectivity: 0.001}
			}
			node := &pb.Node{
				NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters, Stats: stats,
			}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			require.NoError(t, builder.recordIndexHints(scan, table, []*tree.IndexHint{{
				HintType: tree.HintForce, HintScope: tree.HintForScan, IndexNames: []string{"uk_prefix", "uk_full"},
			}}))
			colRefCnt := map[[2]int32]int{{tag, 4}: 1}
			idxColMap := map[[2]int32]*pb.Expr{}
			got, err := builder.applyForceIndexHintToScan(builder.qry.Nodes[scan], []forceIndexRequirement{{
				scope: forceIndexForOrder, columns: []*pb.Expr{GetColExpr(table.Cols[4].Typ, tag, 4)},
			}}, colRefCnt, idxColMap)
			require.NoError(t, err)
			require.Equal(t, scan, got)
			builder.applyIndicesForFilters(scan, builder.qry.Nodes[scan], colRefCnt, idxColMap)
			p := &pb.Plan{Plan: &pb.Plan_Query{Query: builder.qry}}
			selected := findFirstIndexScanName(p)
			t.Logf("selected index: %s", selected)
			require.NotEqual(t, "uk_prefix", selected)
		})
	}
}

func addFullValueCompetingUniqueIndexesForTest(mock *MockOptimizer, reverse bool) *pb.TableDef {
	addCompositeUniqueInTableForTest(mock)
	table := mock.ctxt.tables["composite_unique_in_t"]
	many := &pb.IndexDef{
		IndexName: "uk_many", Parts: []string{"a", "b"}, Unique: true, TableExist: true,
		IndexTableName: "composite_unique_in_uk_many",
	}
	one := &pb.IndexDef{
		IndexName: "uk_one", Parts: []string{"a", "c"}, Unique: true, TableExist: true,
		IndexTableName: "composite_unique_in_uk_one",
	}
	table.Indexes = []*pb.IndexDef{many, one}
	if reverse {
		table.Indexes[0], table.Indexes[1] = table.Indexes[1], table.Indexes[0]
	}
	addIndexHintIndexTableForTest(mock, many.IndexTableName, 25405)
	addIndexHintIndexTableForTest(mock, one.IndexTableName, 25406)
	return table
}

func TestCompositeUniqueMissingStatsPreferCompleteEquality(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reverse bool
	}{
		{name: "trailing IN index first"},
		{name: "complete equality index first", reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			table := addFullValueCompetingUniqueIndexesForTest(mock, tc.reverse)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			inFilter := makeParamInFilterExpr(tag, 2, 5000)
			for i, item := range inFilter.GetF().Args[1].GetList().List {
				item.Expr = &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: int64(i + 1)}}}
			}
			filters := []*pb.Expr{
				makeRangeFilterExpr(tag, 1, "=", 1),
				inFilter,
				makeRangeFilterExpr(tag, 3, "=", 1),
			}
			node := &pb.Node{
				NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters, Stats: DefaultStats(),
			}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			got := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan],
				map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1}, map[[2]int32]*pb.Expr{})
			require.NotEqual(t, scan, got)
			join := builder.qry.Nodes[got]
			require.Equal(t, pb.Node_INDEX, join.JoinType)
			indexScan := builder.qry.Nodes[join.Children[1]]
			require.Equal(t, "uk_one", indexScan.IndexScanInfo.IndexName)
			require.Len(t, indexScan.FilterList, 1)
			lookup := indexScan.FilterList[0].GetF()
			require.Equal(t, "=", lookup.Func.ObjName)
			require.Len(t, builder.qry.Nodes[join.Children[0]].FilterList, 3)
		})
	}
}

func TestCompositeUniqueTrailingInPreparedParameters(t *testing.T) {
	mock := NewMockOptimizer(true)
	addCompositeUniqueInTableForTest(mock)
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	tag := builder.genNewBindTag()
	table := mock.ctxt.tables["composite_unique_in_t"]
	table.Indexes = table.Indexes[1:]
	filters := []*pb.Expr{
		makeParamEqFilterExpr(tag, 1, 0),
		makeParamEqFilterExpr(tag, 2, 1),
		makeParamInFilterExpr(tag, 3, 2),
	}
	for i, item := range filters[2].GetF().Args[1].GetList().List {
		item.GetP().Pos = int32(i + 2)
	}
	node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: filters, Stats: DefaultStats()}
	scan := builder.appendNode(node, NewBindContext(builder, nil))
	got := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan], map[[2]int32]int{{tag, 4}: 1}, map[[2]int32]*pb.Expr{})
	require.NotEqual(t, scan, got)
	join := builder.qry.Nodes[got]
	require.Equal(t, pb.Node_INDEX, join.JoinType)
	indexScan := builder.qry.Nodes[join.Children[1]]
	require.Equal(t, "uk_abc", indexScan.IndexScanInfo.IndexName)
	lookup := indexScan.FilterList[0].GetF()
	require.Equal(t, "in", lookup.Func.ObjName)
	for i, value := range lookup.Args[1].GetList().List {
		serial := value.GetF()
		require.NotNil(t, serial)
		require.Equal(t, "serial", serial.Func.ObjName)
		require.Len(t, serial.Args, 3)
		require.Equal(t, int32(0), serial.Args[0].GetP().Pos)
		require.Equal(t, int32(1), serial.Args[1].GetP().Pos)
		require.Equal(t, int32(i+2), serial.Args[2].GetP().Pos)
	}
}

func makeCompositeUniqueInt32VecInFilterExpr(t *testing.T, relPos, colPos int32, values ...int32) *pb.Expr {
	t.Helper()
	mp := mpool.MustNew(t.Name())
	vec := vector.NewVec(types.T_int32.ToType())
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	typ := makePlan2Type(vec.GetType())
	vec.Free(mp)

	colType := pb.Type{Id: int32(types.T_int32)}
	return &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_F{F: &pb.Function{
		Func: &pb.ObjectRef{ObjName: "in"},
		Args: []*pb.Expr{
			{Typ: colType, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: relPos, ColPos: colPos}}},
			{Typ: typ, Expr: &pb.Expr_Vec{Vec: &pb.LiteralVec{Len: int32(len(values)), Data: data}}},
		},
	}}}
}

func TestCompositeUniqueTrailingInFoldedVector(t *testing.T) {
	mock := NewMockOptimizer(true)
	addCompositeUniqueInTableForTest(mock)
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	tag := builder.genNewBindTag()
	table := mock.ctxt.tables["composite_unique_in_t"]
	table.Indexes = table.Indexes[1:]
	filters := []*pb.Expr{
		makeRangeFilterExpr(tag, 1, "=", 1),
		makeRangeFilterExpr(tag, 2, "=", 2),
		makeCompositeUniqueInt32VecInFilterExpr(t, tag, 3, 3, 4, 5),
	}
	node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: filters, Stats: DefaultStats()}
	scan := builder.appendNode(node, NewBindContext(builder, nil))
	got := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan], map[[2]int32]int{{tag, 4}: 1}, map[[2]int32]*pb.Expr{})
	require.NotEqual(t, scan, got)
	join := builder.qry.Nodes[got]
	require.Same(t, filters[2], builder.qry.Nodes[join.Children[0]].FilterList[2])
	indexScan := builder.qry.Nodes[join.Children[1]]
	lookup := indexScan.FilterList[0].GetF()
	require.Equal(t, "in", lookup.Func.ObjName)
	require.Len(t, lookup.Args[1].GetList().List, 3)
	for _, value := range lookup.Args[1].GetList().List {
		require.Equal(t, "serial", value.GetF().Func.ObjName)
		require.Len(t, value.GetF().Args, 3)
	}
}

func TestCompositeUniqueTrailingInRejectsUnsupportedLists(t *testing.T) {
	for _, tc := range []struct {
		name       string
		makeFilter func(relPos int32) *pb.Expr
	}{
		{name: "non-runtime item", makeFilter: func(relPos int32) *pb.Expr {
			filter := makeParamInFilterExpr(relPos, 3, 3)
			filter.GetF().Args[1].GetList().List[1] = GetColExpr(pb.Type{Id: int32(types.T_int32)}, relPos, 4)
			return filter
		}},
		{name: "mixed item type", makeFilter: func(relPos int32) *pb.Expr {
			filter := makeParamInFilterExpr(relPos, 3, 3)
			filter.GetF().Args[1].GetList().List[1].Typ.Id = int32(types.T_int64)
			return filter
		}},
		{name: "floating key", makeFilter: func(relPos int32) *pb.Expr {
			filter := makeParamInFilterExpr(relPos, 3, 3)
			filter.GetF().Args[0].Typ.Id = int32(types.T_float64)
			return filter
		}},
		{name: "empty folded vector", makeFilter: func(relPos int32) *pb.Expr {
			return makeCompositeUniqueInt32VecInFilterExpr(t, relPos, 3)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addCompositeUniqueInTableForTest(mock)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			table := mock.ctxt.tables["composite_unique_in_t"]
			table.Indexes = table.Indexes[1:]
			filters := []*pb.Expr{
				makeRangeFilterExpr(tag, 1, "=", 1),
				makeRangeFilterExpr(tag, 2, "=", 2),
				tc.makeFilter(tag),
			}
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: filters, Stats: DefaultStats()}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			got := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan], map[[2]int32]int{{tag, 4}: 1}, map[[2]int32]*pb.Expr{})
			require.Equal(t, scan, got)
			require.Same(t, filters[2], builder.qry.Nodes[scan].FilterList[2])
		})
	}
}

func TestCompositeUniqueTrailingInCardinalityAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		outcnt      float64
		selectivity float64
		wantIndex   bool
	}{
		{name: "bounded key set", outcnt: 3, selectivity: 0.000003, wantIndex: true},
		{name: "broad scan estimate falls back", outcnt: 500000, selectivity: 0.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addCompositeUniqueInTableForTest(mock)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			table := mock.ctxt.tables["composite_unique_in_t"]
			table.Indexes = table.Indexes[1:]
			filters := []*pb.Expr{
				makeRangeFilterExpr(tag, 1, "=", 1),
				makeRangeFilterExpr(tag, 2, "=", 2),
				makeParamInFilterExpr(tag, 3, 100),
			}
			for _, filter := range filters {
				filter.Selectivity = 0.001
			}
			node := &pb.Node{
				NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters,
			}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			builder.qry.Nodes[scan].Stats = &pb.Stats{TableCnt: 1000000, Cost: 1000000, Outcnt: tc.outcnt, Selectivity: tc.selectivity}
			got := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan], map[[2]int32]int{{tag, 4}: 1}, map[[2]int32]*pb.Expr{})
			if tc.wantIndex {
				require.NotEqual(t, scan, got)
				require.Equal(t, pb.Node_INDEX, builder.qry.Nodes[got].JoinType)
			} else {
				require.Equal(t, scan, got)
			}
		})
	}
}
