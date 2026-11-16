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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		{name: "equality control", fromAndWhere: "composite_unique_in_t force index(uk_abc) where a=1 and b=2 and c=3", wantIndex: "uk_abc", wantLookup: "=", wantBaseFilts: 3},
		{name: "trailing IN", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,4,5)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 3},
		{name: "forced trailing IN", fromAndWhere: "composite_unique_in_t force index(uk_abc) where a=1 and b=2 and c in (3,4,5)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 3},
		{name: "forced trailing IN with residual and ORDER BY", fromAndWhere: "composite_unique_in_t force index(uk_abc) where a=1 and b=2 and c in (3,4,5) and payload=7 order by payload", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 4},
		{name: "duplicate and absent keys", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (99,99,100)", wantIndex: "uk_abc", wantLookup: "in", wantItems: 2, wantBaseFilts: 3},
		{name: "NULL list item uses ordinary access", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,null,5)", wantIndex: "idx_c", wantBaseFilts: 3},
		{name: "residual payload predicate", fromAndWhere: "composite_unique_in_t where a=1 and b=2 and c in (3,4,5) and payload=7", wantIndex: "uk_abc", wantLookup: "in", wantItems: 3, wantBaseFilts: 4},
		{name: "incomplete unique prefix", fromAndWhere: "composite_unique_in_t use index(idx_c,uk_abc) where a=1 and c in (3,4,5)", wantIndex: "idx_c", wantBaseFilts: 2},
		{name: "IN on leading part", fromAndWhere: "composite_unique_in_t use index(idx_c,uk_abc) where a in (1,2) and b=2 and c=3", wantIndex: "uk_abc", wantLookup: "in", wantItems: 2, wantBaseFilts: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
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
			values := lookup.Args[1].GetVec()
			require.NotNil(t, values)
			require.Equal(t, int32(tc.wantItems), values.Len)
			keys := vector.NewVec(types.T_any.ToType())
			require.NoError(t, keys.UnmarshalBinary(values.Data))
			require.Equal(t, tc.wantItems, keys.Length())
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
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
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
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
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
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := addCompetingCompositeUniqueIndexesForTest(mock, tc.reverse)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			bFilter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*pb.Expr{
				GetColExpr(table.Cols[2].Typ, tag, 2), makePlan2StringConstExprWithType("abcdef"),
			})
			require.NoError(t, err)
			inFilter := makeCompositeUniqueInt32VecInFilterExpr(t, tag, 3, 3, 4)
			filters := []*pb.Expr{makeCompositeUniqueInt32EqualFilterExpr(tag, 1, 1), bFilter, inFilter}
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
			_ = got
			_, err = builder.applyIndicesForFilters(scan, builder.qry.Nodes[scan], colRefCnt, idxColMap)
			require.NoError(t, err)
			p := &pb.Plan{Plan: &pb.Plan_Query{Query: builder.qry}}
			selected := findFirstIndexScanName(p)
			require.Empty(t, selected, "a mixed full-value/prefix hint set keeps the base scan protected")
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
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := addFullValueCompetingUniqueIndexesForTest(mock, tc.reverse)
			builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			inFilter := makeCompositeUniqueInt32VecInFilterExpr(t, tag, 2, 1, 2, 3)
			filters := []*pb.Expr{
				makeCompositeUniqueInt32EqualFilterExpr(tag, 1, 1),
				inFilter,
				makeCompositeUniqueInt32EqualFilterExpr(tag, 3, 1),
			}
			node := &pb.Node{
				NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters, Stats: DefaultStats(),
			}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			got, err := builder.applyIndicesForFiltersRegularIndex(scan, builder.qry.Nodes[scan],
				map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1}, map[[2]int32]*pb.Expr{})
			require.NoError(t, err)
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

func makeCompositeUniqueInt32EqualFilterExpr(relPos, colPos, value int32) *pb.Expr {
	typ := pb.Type{Id: int32(types.T_int32)}
	return &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_F{F: &pb.Function{
		Func: &pb.ObjectRef{ObjName: "="},
		Args: []*pb.Expr{
			GetColExpr(typ, relPos, colPos),
			{Typ: typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I32Val{I32Val: value}}}},
		},
	}}}
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

func findCompositeUniqueSerializedKeySet(p *Plan) *pb.Node {
	for _, node := range p.GetQuery().Nodes {
		if !node.IndexScanInfo.IsIndexScan {
			continue
		}
		for _, filter := range node.FilterList {
			if _, ok := exactUniqueKeySetSize(filter); ok {
				return node
			}
		}
	}
	return nil
}

func TestCompositeUniqueTrailingInIndexOnlyAccess(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addCompositeUniqueInTableForTest(mock)
	p, err := runOneStmt(mock, t, "select a,b,c from composite_unique_in_t force index(uk_abc) "+
		"where a=1 and b=2 and c in (3,4,5)")
	require.NoError(t, err)
	require.Nil(t, findCompositeUniqueInIndexJoin(p))
	scan := findCompositeUniqueSerializedKeySet(p)
	require.NotNil(t, scan)
	require.Equal(t, "uk_abc", scan.IndexScanInfo.IndexName)
	count, ok := exactUniqueKeySetSize(scan.FilterList[0])
	require.True(t, ok)
	require.Equal(t, 3, count)
}

func TestCompositeUniqueKeySetCardinalityAndFastPath(t *testing.T) {
	values := make([]string, maxCompositePoints+1)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	for _, tc := range []struct {
		name      string
		predicate string
		strings   bool
		want      int
	}{
		{name: "cardinality limit", predicate: "a=1 and b=2 and c in (" + strings.Join(values[:maxCompositePoints], ",") + ")", want: maxCompositePoints},
		{name: "above cardinality limit", predicate: "a=1 and b=2 and c in (" + strings.Join(values, ",") + ")"},
		{name: "fixed width fast path deduplicates", predicate: "a=1 and b=2 and c in (1,1,2,3,4,5,6,7,8)", want: 8},
		{name: "string fast path deduplicates", predicate: "a='lead' and b=2 and c in ('v0','v1','v2','v3','v4','v5','v6','v7','v7')", strings: true, want: 8},
		{name: "multiple set dimensions are not zipped", predicate: "a in (1,2) and b in (2,3) and c=4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addCompositeUniqueInTableForTest(mock)
			if tc.strings {
				table := mock.ctxt.tables["composite_unique_in_t"]
				table.Cols[1].Typ = pb.Type{Id: int32(types.T_varchar), Width: 20}
				table.Cols[3].Typ = pb.Type{Id: int32(types.T_varchar), Width: 20}
			}
			p, err := runOneStmt(mock, t, "select payload from composite_unique_in_t force index(uk_abc) where "+tc.predicate)
			require.NoError(t, err)
			scan := findCompositeUniqueSerializedKeySet(p)
			if tc.want == 0 {
				require.Nil(t, scan)
				return
			}
			require.NotNil(t, scan)
			require.Equal(t, "uk_abc", scan.IndexScanInfo.IndexName)
			count, ok := exactUniqueKeySetSize(scan.FilterList[0])
			require.True(t, ok)
			require.Equal(t, tc.want, count)
		})
	}
}

func TestFastExactKeySetMatchesSerialFolding(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIndexHintChoiceTableForTest(mock)
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	idxDef := &pb.IndexDef{Unique: true, Parts: []string{"a", "b"}}
	positions := []int32{0, 1}
	newInFilter := func(v *vector.Vector) *pb.Expr {
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		typ := makePlan2Type(v.GetType())
		return &pb.Expr{Selectivity: .5, Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "in"},
			Args: []*pb.Expr{GetColExpr(typ, 1, 1),
				{Typ: typ, Expr: &pb.Expr_Vec{Vec: &pb.LiteralVec{Len: int32(v.Length()), Data: data}}}},
		}}}
	}
	for _, tc := range []struct {
		name        string
		buildValues func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{name: "fixed width", buildValues: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
			v := vector.NewVec(types.T_int32.ToType())
			for _, value := range []int32{1, 1, 2, 3, 4, 5, 6, 7, 8} {
				require.NoError(t, vector.AppendFixed(v, value, false, mp))
			}
			return v
		}},
		{name: "string", buildValues: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
			v := vector.NewVec(types.T_varchar.ToType())
			for _, value := range []string{"v0", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"} {
				require.NoError(t, vector.AppendBytes(v, []byte(value), false, mp))
			}
			return v
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mock.ctxt.GetProcess().Mp()
			values := tc.buildValues(t, mp)
			defer values.Free(mp)
			equality := makeCompositeUniqueInt32EqualFilterExpr(1, 0, 1)
			equality.Selectivity = 1
			filters := []*pb.Expr{equality, newInFilter(values)}
			rowVec := vector.NewVec(*values.GetType())
			defer rowVec.Free(mp)

			actual, handled := builder.fastExactKeySet(
				idxDef, filters, positions, values, rowVec, types.T_varchar.ToType(), mp,
			)
			require.True(t, handled)
			require.NotNil(t, actual)
			defer actual.Free(mp)

			expected := vector.NewVec(types.T_varchar.ToType())
			defer expected.Free(mp)
			for row := 0; row < values.Length(); row++ {
				tuple := builder.foldKeySetTuple(idxDef, filters, positions, values, rowVec, row, mp)
				require.NotNil(t, tuple)
				require.NotNil(t, tuple.GetVec())
				decoded := vector.NewVec(types.T_any.ToType())
				require.NoError(t, decoded.UnmarshalBinary(tuple.GetVec().Data))
				require.Equal(t, types.T_varchar, decoded.GetType().Oid)
				require.Equal(t, 1, decoded.Length())
				require.False(t, decoded.IsNull(0))
				require.NoError(t, vector.AppendBytes(expected, decoded.GetBytesAt(0), false, mp))
			}
			require.NoError(t, expected.InplaceSortAndCompact(mp))
			require.NoError(t, actual.InplaceSortAndCompact(mp))
			require.Equal(t, 8, expected.Length())
			require.Equal(t, expected.Length(), actual.Length())
			for row := 0; row < expected.Length(); row++ {
				require.Equal(t, expected.GetBytesAt(row), actual.GetBytesAt(row), "key %d", row)
			}

			// The same nine raw values cross replaceKeySetCondition's fast-path threshold.
			got := builder.replaceKeySetCondition(idxDef, filters, positions, 7, mock.ctxt.tables["uk_hint_ab"])
			count, ok := exactUniqueKeySetSize(got)
			require.True(t, ok)
			require.Equal(t, 8, count)
		})
	}
}

func TestIndexKeySetLiteralBoundVariants(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.T
		value *pb.Literal
	}{
		{name: "bool", typ: types.T_bool, value: &pb.Literal{Value: &pb.Literal_Bval{Bval: true}}},
		{name: "default", typ: types.T_bool, value: &pb.Literal{Value: &pb.Literal_Defaultval{Defaultval: true}}},
		{name: "int8", typ: types.T_int8, value: &pb.Literal{Value: &pb.Literal_I8Val{I8Val: -8}}},
		{name: "int16", typ: types.T_int16, value: &pb.Literal{Value: &pb.Literal_I16Val{I16Val: -16}}},
		{name: "int32", typ: types.T_int32, value: &pb.Literal{Value: &pb.Literal_I32Val{I32Val: -32}}},
		{name: "int64", typ: types.T_int64, value: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: -64}}},
		{name: "uint8", typ: types.T_uint8, value: &pb.Literal{Value: &pb.Literal_U8Val{U8Val: 8}}},
		{name: "uint16", typ: types.T_uint16, value: &pb.Literal{Value: &pb.Literal_U16Val{U16Val: 16}}},
		{name: "uint32", typ: types.T_uint32, value: &pb.Literal{Value: &pb.Literal_U32Val{U32Val: 32}}},
		{name: "uint64", typ: types.T_uint64, value: &pb.Literal{Value: &pb.Literal_U64Val{U64Val: 64}}},
		{name: "float32", typ: types.T_float32, value: &pb.Literal{Value: &pb.Literal_Fval{Fval: 1.25}}},
		{name: "float64", typ: types.T_float64, value: &pb.Literal{Value: &pb.Literal_Dval{Dval: 2.5}}},
		{name: "date", typ: types.T_date, value: &pb.Literal{Value: &pb.Literal_Dateval{Dateval: 365}}},
		{name: "time", typ: types.T_time, value: &pb.Literal{Value: &pb.Literal_Timeval{Timeval: 123456}}},
		{name: "datetime", typ: types.T_datetime, value: &pb.Literal{Value: &pb.Literal_Datetimeval{Datetimeval: 123456}}},
		{name: "timestamp", typ: types.T_timestamp, value: &pb.Literal{Value: &pb.Literal_Timestampval{Timestampval: 123456}}},
		{name: "decimal64", typ: types.T_decimal64, value: &pb.Literal{Value: &pb.Literal_Decimal64Val{Decimal64Val: &pb.Decimal64{A: 1234}}}},
		{name: "decimal128", typ: types.T_decimal128, value: &pb.Literal{Value: &pb.Literal_Decimal128Val{Decimal128Val: &pb.Decimal128{A: 1234, B: 5}}}},
		{name: "enum", typ: types.T_enum, value: &pb.Literal{Value: &pb.Literal_EnumVal{EnumVal: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &pb.Expr{Typ: pb.Type{Id: int32(tc.typ)}, Expr: &pb.Expr_Lit{Lit: tc.value}}
			got, ok := indexKeySetLiteralBound(expr)
			require.True(t, ok)
			want, wantOK := function.SerialEncodedTypeSizeBound(tc.typ.ToType())
			require.True(t, wantOK)
			require.Equal(t, want, got)
		})
	}

	for _, tc := range []struct {
		name  string
		typ   types.T
		value string
	}{
		{name: "varchar counts bytes and NULs", typ: types.T_varchar, value: "a\x00\xc3\xa9"},
		{name: "empty varchar", typ: types.T_varchar},
		{name: "binary", typ: types.T_binary, value: "abc"},
		{name: "varbinary", typ: types.T_varbinary, value: "abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &pb.Expr{Typ: pb.Type{Id: int32(tc.typ)}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{
				Value: &pb.Literal_Sval{Sval: tc.value},
			}}}
			got, ok := indexKeySetLiteralBound(expr)
			require.True(t, ok)
			require.Equal(t, 2*uint64(len(tc.value))+3, got)
		})
	}
}

func TestIndexKeySetLiteralBoundRejectsInvalidInputs(t *testing.T) {
	literalExpr := func(typ types.T, value *pb.Literal) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(typ)}, Expr: &pb.Expr_Lit{Lit: value}}
	}
	for _, tc := range []struct {
		name  string
		typ   types.T
		value *pb.Literal
	}{
		{name: "nil expression literal", typ: types.T_int32},
		{name: "null", typ: types.T_int32, value: &pb.Literal{Isnull: true, Value: &pb.Literal_I32Val{I32Val: 1}}},
		{name: "runtime source", typ: types.T_int32, value: &pb.Literal{Src: &pb.Expr{}, Value: &pb.Literal_I32Val{I32Val: 1}}},
		{name: "unsupported literal variant", typ: types.T_json, value: &pb.Literal{Value: &pb.Literal_Jsonval{Jsonval: "{}"}}},
		{name: "string with non-point type", typ: types.T_char, value: &pb.Literal{Value: &pb.Literal_Sval{Sval: "abc"}}},
		{name: "time metadata mismatch", typ: types.T_datetime, value: &pb.Literal{Value: &pb.Literal_Timeval{Timeval: 1}}},
		{name: "datetime metadata mismatch", typ: types.T_time, value: &pb.Literal{Value: &pb.Literal_Datetimeval{Datetimeval: 1}}},
		{name: "decimal64 metadata mismatch", typ: types.T_decimal128, value: &pb.Literal{Value: &pb.Literal_Decimal64Val{Decimal64Val: &pb.Decimal64{A: 1}}}},
		{name: "decimal128 metadata mismatch", typ: types.T_decimal64, value: &pb.Literal{Value: &pb.Literal_Decimal128Val{Decimal128Val: &pb.Decimal128{A: 1}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := indexKeySetLiteralBound(literalExpr(tc.typ, tc.value))
			require.False(t, ok)
		})
	}

	maxStringBytes := int((maxIndexKeySetPlanningWork - 3) / 2)
	for _, tc := range []struct {
		name string
		size int
		want bool
	}{
		{name: "largest accepted string", size: maxStringBytes, want: true},
		{name: "string over planning budget", size: maxStringBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := strings.Repeat("x", tc.size)
			expr := literalExpr(types.T_varchar, &pb.Literal{Value: &pb.Literal_Sval{Sval: value}})
			got, ok := indexKeySetLiteralBound(expr)
			require.Equal(t, tc.want, ok)
			if tc.want {
				require.Equal(t, maxIndexKeySetPlanningWork-1, got)
			}
		})
	}
}

func TestIndexKeySetCostAndMalformedInputBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stats        *pb.Stats
		count        int
		forced, want bool
	}{
		{name: "small scan three", stats: &pb.Stats{TableCnt: 1000, Cost: 1000}, count: 3},
		{name: "at startup boundary", stats: &pb.Stats{TableCnt: 1596, Cost: 1000}, count: 3, want: true},
		{name: "original shape", stats: &pb.Stats{TableCnt: 5000, Cost: 303.03}, count: 3, want: true},
		{name: "single point unchanged", stats: &pb.Stats{TableCnt: 1, Cost: 1}, count: 1, want: true},
		{name: "existing cost boundary", stats: &pb.Stats{TableCnt: 60000, Cost: 1000}, count: 100, want: true},
		{name: "above existing cost boundary", stats: &pb.Stats{TableCnt: 60000, Cost: 1000}, count: 101},
		{name: "default three", stats: DefaultStats(), count: 3, want: true},
		{name: "default 128", stats: DefaultStats(), count: maxCompositePoints},
		{name: "nil stats", count: maxCompositePoints},
		{name: "unknown zero cardinality", stats: &pb.Stats{TableCnt: 0, Cost: 60000}, count: maxCompositePoints},
		{name: "force bypasses cost only", stats: &pb.Stats{TableCnt: 1000, Cost: 1000}, count: maxCompositePoints, forced: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, indexKeySetWorthwhile(tc.stats, tc.count, tc.forced))
		})
	}

	empty := makeCompositeUniqueInt32VecInFilterExpr(t, 1, 2)
	_, _, ok := indexKeySetFilter(empty)
	require.False(t, ok)
	malformed := makeCompositeUniqueInt32VecInFilterExpr(t, 1, 2, 1, 2)
	malformed.GetF().Args[1].GetVec().Data = []byte{1, 2, 3}
	require.False(t, newIndexKeySetWorkBudget().inputBound(malformed).ok)
}

func TestIndexKeySetWorkBudgetAndDynamicValues(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addCompositeUniqueInTableForTest(mock)
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	tag := builder.genNewBindTag()
	filters := []*pb.Expr{
		makeCompositeUniqueInt32EqualFilterExpr(tag, 1, 1),
		makeCompositeUniqueInt32VecInFilterExpr(t, tag, 3, 1, 2, 3),
	}
	positions := []int32{0, 1}
	budget := newIndexKeySetWorkBudget(builder.GetContext())
	budget.proc = builder.compCtx.GetProcess()
	work, ok := budget.estimate(filters, positions)
	require.True(t, ok)
	require.Positive(t, work)
	budget.remaining = work
	_, ok = budget.estimate(filters, positions)
	require.True(t, ok, "the exact budget is inclusive")
	budget.remaining = work - 1
	_, ok = budget.estimate(filters, positions)
	require.False(t, ok)

	budget.remaining = work
	require.True(t, budget.charge(work))
	require.Zero(t, budget.remaining)
	stats := &pb.Stats{TableCnt: 5000, Cost: 5000}
	require.True(t, budget.revalidate(filters, positions, work, stats, false))
	stats.TableCnt, stats.Cost = 1000, 1000
	require.False(t, budget.revalidate(filters, positions, work, stats, false))
	require.True(t, budget.revalidate(filters, positions, work, stats, true))

	value := filters[0].GetF().Args[1]
	value.GetLit().Src = &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
	_, ok = newIndexKeySetWorkBudget(builder.GetContext()).estimate(filters, positions)
	require.False(t, ok, "a prepared equality cannot be frozen into the plan")
	value.GetLit().Src = nil
	value.GetLit().Isnull = true
	_, ok = newIndexKeySetWorkBudget(builder.GetContext()).estimate(filters, positions)
	require.False(t, ok)
}

type compositeUniqueKeySetResolver struct {
	*MockCompilerContext
	failureTarget string
	failure       error
	before        func(string)
	resolved      []string
}

func (c *compositeUniqueKeySetResolver) ResolveIndexTableByRef(
	ref *ObjectRef, name string, snapshot *Snapshot, output *TableResolution,
) (*ObjectRef, *TableDef, error) {
	if output != nil {
		output.Reset()
	}
	c.resolved = append(c.resolved, name)
	if c.before != nil {
		c.before(name)
	}
	if name == c.failureTarget {
		if c.failure != nil {
			return nil, nil, c.failure
		}
		return nil, nil, errors.New("injected composite key-set resolution failure")
	}
	return c.MockCompilerContext.ResolveIndexTableByRef(ref, name, snapshot, output)
}

func TestIndexKeySetResolutionRetrySharesBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		roomForSecond bool
		changeInput   bool
		wantIndex     string
	}{
		{name: "failed candidate spends its reservation"},
		{name: "affordable alternate remains available", roomForSecond: true, wantIndex: "uk_one"},
		{name: "changed input revalidates before alternate", roomForSecond: true, changeInput: true, wantIndex: "uk_one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := addFullValueCompetingUniqueIndexesForTest(mock, false)
			resolver := &compositeUniqueKeySetResolver{
				MockCompilerContext: &mock.ctxt,
				failureTarget:       table.Indexes[0].IndexTableName,
			}
			builder := NewQueryBuilder(pb.Query_SELECT, resolver, false, true)
			tag := builder.genNewBindTag()
			filters := []*pb.Expr{
				makeCompositeUniqueInt32EqualFilterExpr(tag, 1, 1),
				makeCompositeUniqueInt32VecInFilterExpr(t, tag, 2, 1, 2, 3),
				makeCompositeUniqueInt32VecInFilterExpr(t, tag, 3, 1, 2, 3),
			}
			node := &pb.Node{
				NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters,
				Stats: &pb.Stats{TableCnt: 5000, Cost: 5000, Outcnt: 500, Selectivity: .1},
			}
			scan := builder.appendNode(node, NewBindContext(builder, nil))
			budget := newIndexKeySetWorkBudget(builder.GetContext())
			budget.proc = builder.compCtx.GetProcess()
			workMany, ok := budget.estimate(filters, []int32{0, 1})
			require.True(t, ok)
			workOne, ok := budget.estimate(filters, []int32{0, 2})
			require.True(t, ok)
			budget.remaining = workMany
			if tc.roomForSecond {
				budget.remaining += workOne
			}
			if tc.changeInput {
				values := make([]int32, maxCompositePoints)
				for i := range values {
					values[i] = int32(i)
				}
				replacement := makeCompositeUniqueInt32VecInFilterExpr(t, tag, 2, values...)
				resolver.before = func(name string) {
					if name == table.Indexes[0].IndexTableName {
						filters[1].GetF().Args[1].GetVec().Data = replacement.GetF().Args[1].GetVec().Data
						filters[1].GetF().Args[1].GetVec().Len = maxCompositePoints
					}
				}
			}
			got, err := builder.applyIndicesForFiltersRegularIndexAttempt(
				scan, node, map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1},
				map[[2]int32]*pb.Expr{}, false, budget, nil,
			)
			require.NoError(t, err)
			require.Contains(t, resolver.resolved, table.Indexes[0].IndexTableName)
			if tc.wantIndex == "" {
				require.Equal(t, scan, got)
				require.Len(t, builder.qry.Nodes, 1)
				require.Len(t, resolver.resolved, 1)
				return
			}
			require.NotEqual(t, scan, got)
			join := builder.qry.Nodes[got]
			require.Equal(t, pb.Node_INDEX, join.JoinType)
			indexScan := builder.qry.Nodes[join.Children[1]]
			require.Equal(t, tc.wantIndex, indexScan.IndexScanInfo.IndexName)
			count, ok := exactUniqueKeySetSize(indexScan.FilterList[0])
			require.True(t, ok)
			require.Equal(t, 3, count)
			require.Contains(t, resolver.resolved, table.Indexes[1].IndexTableName)
		})
	}
}

func TestIndexKeySetCancellationIsNotADecline(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := addFullValueCompetingUniqueIndexesForTest(mock, false)
	resolver := &compositeUniqueKeySetResolver{
		MockCompilerContext: &mock.ctxt,
		failureTarget:       table.Indexes[0].IndexTableName,
		failure:             context.Canceled,
	}
	builder := NewQueryBuilder(pb.Query_SELECT, resolver, false, true)
	tag := builder.genNewBindTag()
	node := &pb.Node{
		NodeType: pb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
		BindingTags: []int32{tag}, FilterList: []*pb.Expr{
			makeCompositeUniqueInt32EqualFilterExpr(tag, 1, 1),
			makeCompositeUniqueInt32VecInFilterExpr(t, tag, 2, 1, 2, 3),
			makeCompositeUniqueInt32VecInFilterExpr(t, tag, 3, 1, 2, 3),
		},
		Stats: &pb.Stats{TableCnt: 5000, Cost: 5000, Outcnt: 500, Selectivity: .1},
	}
	scan := builder.appendNode(node, NewBindContext(builder, nil))
	require.PanicsWithValue(t, context.Canceled, func() {
		_, _ = builder.applyIndicesForFiltersRegularIndex(
			scan, node, map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1}, map[[2]int32]*pb.Expr{},
		)
	})
	require.Len(t, builder.qry.Nodes, 1)
	require.Equal(t, []string{table.Indexes[0].IndexTableName}, resolver.resolved)
}
