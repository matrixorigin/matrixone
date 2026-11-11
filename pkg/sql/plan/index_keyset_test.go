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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func compositeKeySetScan(p *Plan) *planpb.Node {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	for _, node := range p.GetQuery().Nodes {
		if !node.IndexScanInfo.IsIndexScan || len(node.FilterList) == 0 {
			continue
		}
		fn := node.FilterList[0].GetF()
		if fn == nil || fn.Func.ObjName != "prefix_in" || len(fn.Args) != 2 {
			continue
		}
		if list := fn.Args[1].GetList(); list != nil && len(list.List) > 0 {
			return node
		}
		serial := fn.Args[1].GetF()
		if serial != nil && len(serial.Args) > 1 {
			return node
		}
	}
	return nil
}

func addCompositeKeySetPayloadColumn(mock *MockOptimizer) {
	table := mock.ctxt.tables["index_hint_t"]
	payload := *table.Cols[1]
	payload.ColId = table.Cols[len(table.Cols)-1].ColId + 1
	payload.Name = "payload"
	payload.OriginName = "payload"
	table.Name2ColIndex[payload.Name] = int32(len(table.Cols))
	table.Cols = append(table.Cols, &payload)
}

func TestCompositeIndexKeySetAccess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		predicate string
		index     string
		points    int
	}{
		{name: "unique trailing IN", predicate: "a = 1 and b in (2, 3)", index: "uk_ab", points: 2},
		{name: "unique leading IN and equality suffix", predicate: "a in (1, 2) and b = 3", index: "uk_ab", points: 2},
		{name: "nonunique full user key", predicate: "a = 1 and b in (2, 3)", index: "idx_ab", points: 2},
		{name: "duplicate IN values", predicate: "a = 1 and b in (2, 2, 3)", index: "uk_ab", points: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			addCompositeKeySetPayloadColumn(mock)
			p, err := runOneStmt(mock, t, fmt.Sprintf(
				"select * from index_hint_t force index(%s) where %s", tc.index, tc.predicate,
			))
			require.NoError(t, err)
			scan := compositeKeySetScan(p)
			require.NotNil(t, scan)
			require.Equal(t, tc.index, scan.IndexScanInfo.IndexName)
			require.True(t, planHasIndexJoin(p))
			tuples := scan.FilterList[0].GetF().Args[1].GetList()
			require.NotNil(t, tuples)
			require.Len(t, tuples.List, tc.points)
			for _, tuple := range tuples.List {
				require.NotNil(t, tuple.GetVec())
				require.NotEmpty(t, tuple.GetVec().GetData())
			}
		})
	}
}

// The cardinality limit is inclusive; N+1 is covered by the fallback test.
func TestCompositeIndexKeySetAtCardinalityLimit(t *testing.T) {
	values := make([]string, maxCompositePoints)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	addCompositeKeySetPayloadColumn(mock)
	p, err := runOneStmt(mock, t,
		"select * from index_hint_t force index(uk_ab) where a = 1 and b in ("+strings.Join(values, ",")+")")
	require.NoError(t, err)
	scan := compositeKeySetScan(p)
	require.NotNil(t, scan)
	require.Len(t, scan.FilterList[0].GetF().Args[1].GetList().List, maxCompositePoints)
}

func TestCompositeIndexKeySetFallbacks(t *testing.T) {
	values := make([]string, maxCompositePoints+1)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	for _, tc := range []struct {
		name      string
		predicate string
	}{
		{name: "multiple set dimensions are not zipped", predicate: "a in (1, 2) and b in (3, 4)"},
		{name: "NULL IN value", predicate: "a = 1 and b in (2, null)"},
		{name: "oversized IN list", predicate: "a = 1 and b in (" + strings.Join(values, ",") + ")"},
		{name: "incomplete unique key", predicate: "b in (2, 3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t,
				"select * from index_hint_t force index(uk_ab) where "+tc.predicate,
			)
			require.NoError(t, err)
			require.Nil(t, compositeKeySetScan(p))
		})
	}
}

func TestCompositeIndexKeySetKeepsEqualityLowering(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	p, err := runOneStmt(mock, t,
		"select * from index_hint_t force index(uk_ab) where a = 1 and b = 2",
	)
	require.NoError(t, err)
	scan := findFirstIndexScanNode(p)
	require.NotNil(t, scan)
	require.Equal(t, "uk_ab", scan.IndexScanInfo.IndexName)
	require.Nil(t, compositeKeySetScan(p))
	require.Equal(t, "=", scan.FilterList[0].GetF().Func.ObjName)
}

func TestCompositeIndexKeySetRejectsPrefixMetadata(t *testing.T) {
	for _, tc := range []struct {
		name      string
		params    string
		supported bool
	}{
		{name: "full values", supported: true},
		{name: "legacy prefix", params: `{"prefix_lengths":"b:4"}`},
		{name: "v2 prefix", params: `{"prefix_lengths_v2":"{\"b\":4}"}`},
		{name: "malformed metadata", params: `{"prefix_lengths":"b:not-a-number"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := &IndexDef{Parts: []string{"a", "b"}, IndexAlgoParams: tc.params}
			require.Equal(t, tc.supported, keySetIndexSupported(idx))
		})
	}
}

func TestCompositeIndexKeySetPrefixFallback(t *testing.T) {
	for _, tc := range []struct {
		name             string
		prefixPart       string
		expectPrefixScan bool
	}{
		{name: "truncated leading part", prefixPart: "a"},
		{name: "truncated trailing part", prefixPart: "b", expectPrefixScan: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			addCompositeKeySetPayloadColumn(mock)
			for _, idx := range mock.ctxt.tables["index_hint_t"].Indexes {
				if idx.IndexName == "idx_ab" {
					idx.IndexAlgoParams = fmt.Sprintf(`{"prefix_lengths":"%s:4"}`, tc.prefixPart)
				}
			}
			p, err := runOneStmt(mock, t,
				"select * from index_hint_t force index(idx_ab) where a = 1 and b in (2, 3)")
			require.NoError(t, err)
			require.Nil(t, compositeKeySetScan(p))
			scan := findFirstIndexScanNode(p)
			if tc.expectPrefixScan {
				require.NotNil(t, scan)
				require.Equal(t, "prefix_eq", scan.FilterList[0].GetF().Func.ObjName)
			} else {
				require.Nil(t, scan)
			}
			for _, node := range p.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					require.NotNil(t, filter)
				}
			}
		})
	}
}

func TestMarshalKeySetElementCompactsVarlena(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	mp := proc.Mp()
	typ := types.T_varchar.ToType()
	src := vector.NewVec(typ)
	defer src.Free(mp)
	scratch := vector.NewVec(typ)
	defer scratch.Free(mp)

	values := make([]string, maxCompositePoints)
	for i := range values {
		values[i] = fmt.Sprintf("%03d%s", i, strings.Repeat("x", 8<<10))
		require.NoError(t, vector.AppendBytes(src, []byte(values[i]), false, mp))
	}
	for i, want := range values {
		data, err := marshalKeySetElement(src, scratch, i, mp)
		require.NoError(t, err)
		// A borrowed window contains the entire source area and fails this
		// bound. Each serialized element should contain only its own value.
		require.Less(t, len(data), len(want)+1024)
		got := vector.NewVec(typ)
		require.NoError(t, got.UnmarshalBinary(data))
		require.Equal(t, 1, got.Length())
		require.Equal(t, want, string(got.GetBytesAt(0)))
	}
}

func TestCompositeIndexKeySetPreparedFallback(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	addCompositeKeySetPayloadColumn(mock)
	table := mock.ctxt.tables["index_hint_t"]
	// The parameter remains on the value side of this text comparison.
	table.Cols[table.Name2ColIndex["a"]].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 16}
	stmts, err := mysql.Parse(mock.ctxt.GetContext(),
		"select payload from index_hint_t force index(idx_ab) where a=cast(? as varchar(16)) and b in (2,3)", 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	p, err := BuildPlan(&mock.ctxt, stmts[0], true)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery())
	scan := findFirstIndexScanNode(p)
	require.NotNil(t, scan)
	require.Equal(t, "idx_ab", scan.IndexScanInfo.IndexName)
	require.NotEmpty(t, scan.FilterList)
	require.NotNil(t, scan.FilterList[0])
	require.Equal(t, "prefix_eq", scan.FilterList[0].GetF().Func.ObjName)
	var parameterEquality, literalSet bool
	for _, node := range p.GetQuery().Nodes {
		for _, filter := range node.FilterList {
			require.NotNil(t, filter)
			if node.TableDef == nil || node.TableDef.Name != "index_hint_t" || node.IndexScanInfo.IsIndexScan {
				continue
			}
			fn := filter.GetF()
			if fn != nil && fn.Func != nil && fn.Func.ObjName == "=" && len(fn.Args) == 2 &&
				fn.Args[0].GetCol() != nil && containsDynamicParam(fn.Args[1]) {
				parameterEquality = true
			}
			if _, _, ok := indexKeySetFilter(filter); ok {
				literalSet = true
			}
		}
	}
	require.True(t, parameterEquality, "prepared comparison must retain its runtime parameter")
	require.True(t, literalSet, "the original constant IN must be eligible for key-set matching")
}

func TestKeySetCostGatePreservesUniqueEquality(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	addCompositeKeySetPayloadColumn(mock)
	table := mock.ctxt.tables["index_hint_t"]
	var secondary, unique *IndexDef
	for _, idx := range table.Indexes {
		switch idx.IndexName {
		case "idx_ab":
			copy := *idx
			copy.IndexName = "idx_a_payload"
			copy.Parts = []string{"a", "payload", catalog.CreateAlias("id")}
			secondary = &copy
		case "uk_ab":
			unique = idx
		}
	}
	require.NotNil(t, secondary)
	require.NotNil(t, unique)
	table.Indexes = []*IndexDef{secondary, unique}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	tag := builder.genNewBindTag()
	aPos, bPos, payloadPos := table.Name2ColIndex["a"], table.Name2ColIndex["b"], table.Name2ColIndex["payload"]
	makeEquality := func(pos int32, value int32) *planpb.Expr {
		typ := table.Cols[pos].Typ
		require.EqualValues(t, types.T_int32, typ.Id)
		literal := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_I32Val{I32Val: value},
		}}}
		expr, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "=", []*planpb.Expr{
			GetColExpr(typ, tag, pos), literal,
		})
		require.NoError(t, err)
		return expr
	}
	a := makeEquality(aPos, 1)
	b := makeEquality(bPos, 2)
	a.Selectivity, b.Selectivity = 0.1, 0.5
	payloadType := table.Cols[payloadPos].Typ
	require.EqualValues(t, types.T_int32, payloadType.Id)
	vec := vector.NewVec(types.T(payloadType.Id).ToType())
	defer vec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(vec, int32(3), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, int32(4), false, proc.Mp()))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	decoded := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, []int32{3, 4}, vector.MustFixedColWithTypeCheck[int32](decoded))
	in, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "in", []*planpb.Expr{
		GetColExpr(payloadType, tag, payloadPos),
		{Typ: payloadType, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(len(data)), Data: data}}},
	})
	require.NoError(t, err)
	in.Selectivity = 1e-8
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
		BindingTags: []int32{tag}, FilterList: []*planpb.Expr{a, b, in}}
	scanID := builder.appendNode(node, NewBindContext(builder, nil))
	node.Stats = &planpb.Stats{TableCnt: 1e6, Cost: 1e6, Outcnt: 1e6, Selectivity: 1}
	candidate, _, count := builder.getMostSelectiveIndexForPointSelect(table.Indexes, node, false, 1)
	require.Equal(t, 0, candidate)
	require.Equal(t, 2, count)
	oldCandidate, _, _ := builder.getMostSelectiveIndexForPointSelectWithKeySets(table.Indexes, node, false, 1, false)
	require.Equal(t, 1, oldCandidate)
	result := builder.applyIndicesForFiltersRegularIndex(scanID, node,
		map[[2]int32]int{{tag, aPos}: 1, {tag, bPos}: 1, {tag, payloadPos}: 1}, map[[2]int32]*planpb.Expr{})
	require.NotEqual(t, scanID, result)
	join := builder.qry.Nodes[result]
	require.Equal(t, planpb.Node_INDEX, join.JoinType)
	require.Equal(t, "uk_ab", builder.qry.Nodes[join.Children[1]].IndexScanInfo.IndexName)
}

func TestIndexKeySetFailedLoweringDoesNotPublishScan(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	keyType := planpb.Type{Id: int32(types.T_int64)}
	indexType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	ctx.tables["keyset_failure_index"] = &TableDef{Cols: []*planpb.ColDef{{Typ: indexType}}}
	ctx.objects["keyset_failure_index"] = &ObjectRef{}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, true)
	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(vec, int64(2), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, int64(3), false, proc.Mp()))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	col := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: keyType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: pos}}}
	}
	param := &planpb.Expr{Typ: keyType, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	equal := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{col(0), param},
	}}}
	in := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{col(1), {
			Typ: keyType, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(len(data)), Data: data}},
		}},
	}}}
	limit, offset := &planpb.Expr{}, &planpb.Expr{}
	node := &planpb.Node{ObjRef: &ObjectRef{}, FilterList: []*planpb.Expr{equal, in}, Limit: limit, Offset: offset}
	builder.qry.Nodes = []*planpb.Node{node}
	idx := &IndexDef{IndexTableName: "keyset_failure_index", Parts: []string{"a", "b", "id"}}
	filterIdx := []int32{0, 1}
	_, listSize, supported := indexKeySetFilter(in)
	require.True(t, supported)
	require.Equal(t, 2, listSize)
	require.True(t, isRuntimeConstExpr(param))
	require.True(t, containsDynamicParam(param))

	// Runtime parameters cannot be folded into a plan-time literal key set.
	require.Nil(t, builder.replaceKeySetCondition(idx, node.FilterList, filterIdx, 1, ctx.tables[idx.IndexTableName]))
	joinID, scanID := builder.applyIndexJoin(idx, node, KeySetIndexCondition, filterIdx, &Snapshot{})
	require.EqualValues(t, -1, joinID)
	require.EqualValues(t, -1, scanID)
	require.Len(t, builder.qry.Nodes, 1)
	require.Same(t, limit, node.Limit)
	require.Same(t, offset, node.Offset)
}

func BenchmarkMarshalKeySetElementVarlen(b *testing.B) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	b.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	mp := proc.Mp()
	typ := types.T_varchar.ToType()
	src := vector.NewVec(typ)
	defer src.Free(mp)
	scratch := vector.NewVec(typ)
	defer scratch.Free(mp)
	value := []byte(strings.Repeat("x", 64<<10))
	for i := 0; i < maxCompositePoints; i++ {
		if err := vector.AppendBytes(src, value, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(value) * maxCompositePoints))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < maxCompositePoints; row++ {
			if _, err := marshalKeySetElement(src, scratch, row, mp); err != nil {
				b.Fatal(err)
			}
		}
	}
}
