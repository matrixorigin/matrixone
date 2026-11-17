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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUniqueKeySetUsesExactSerializedIN(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	addCompositeKeySetPayloadColumn(mock)
	p, err := runOneStmt(mock, t,
		"select payload from index_hint_t force index(uk_ab) where a=1 and b in (3,2,2)")
	require.NoError(t, err)
	scan := findFirstIndexScanNode(p)
	require.NotNil(t, scan)
	require.Equal(t, "uk_ab", scan.IndexScanInfo.IndexName)
	fn := scan.FilterList[0].GetF()
	require.NotNil(t, fn)
	require.Equal(t, "in", fn.Func.ObjName)
	require.NotNil(t, fn.Args[1].GetVec())
	keys := vector.NewVec(types.T_any.ToType())
	require.NoError(t, keys.UnmarshalBinary(fn.Args[1].GetVec().Data))
	require.Equal(t, 2, keys.Length())
	require.False(t, keys.HasNull())
	packer := types.NewPacker()
	defer packer.Close()
	for i, b := range []int32{2, 3} {
		packer.Reset()
		packer.EncodeInt32(1)
		packer.EncodeInt32(b)
		require.Equal(t, packer.Bytes(), keys.GetBytesAt(i))
	}
}

func TestUniqueKeySetOwnsLongEncodedKeys(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	addCompositeKeySetPayloadColumn(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Cols[table.Name2ColIndex["b"]].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 512}
	first, second := strings.Repeat("a", 128), strings.Repeat("b", 128)
	p, err := runOneStmt(mock, t, "select payload from index_hint_t force index(uk_ab) where a=1 and b in ('"+second+"','"+first+"')")
	require.NoError(t, err)
	scan := findFirstIndexScanNode(p)
	require.NotNil(t, scan)
	fn := scan.FilterList[0].GetF()
	require.Equal(t, "in", fn.Func.ObjName)
	require.NotNil(t, fn.Args[1].GetVec())
	keys := vector.NewVec(types.T_any.ToType())
	require.NoError(t, keys.UnmarshalBinary(fn.Args[1].GetVec().Data))
	require.Equal(t, 2, keys.Length())
	packer := types.NewPacker()
	defer packer.Close()
	for i, value := range []string{first, second} {
		packer.Reset()
		packer.EncodeInt32(1)
		packer.EncodeStringType([]byte(value))
		require.Equal(t, packer.Bytes(), keys.GetBytesAt(i))
	}
}

func TestExactUniqueKeySetSizeRejectsInvalidVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	makeExpr := func(typ types.Type, values [][]byte, nulls []bool, sort bool) *planpb.Expr {
		v := vector.NewVec(typ)
		defer v.Free(mp)
		for i, value := range values {
			isNull := len(nulls) > i && nulls[i]
			require.NoError(t, vector.AppendBytes(v, value, isNull, mp))
		}
		if sort {
			v.InplaceSortAndCompact()
		}
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{{}, {Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(v.Length()), Data: data}}}},
		}}}
	}
	valid := makeExpr(types.T_varchar.ToType(), [][]byte{[]byte("b"), []byte("a"), []byte("a")}, nil, true)
	k, ok := exactUniqueKeySetSize(valid)
	require.True(t, ok)
	require.Equal(t, 2, k)
	for name, expr := range map[string]*planpb.Expr{
		"empty":      makeExpr(types.T_varchar.ToType(), nil, nil, true),
		"null":       makeExpr(types.T_varchar.ToType(), [][]byte{[]byte("a")}, []bool{true}, true),
		"wrong type": makeExpr(types.T_varbinary.ToType(), [][]byte{[]byte("a")}, nil, true),
		"unsorted":   makeExpr(types.T_varchar.ToType(), [][]byte{[]byte("b"), []byte("a")}, nil, false),
	} {
		t.Run(name, func(t *testing.T) { _, ok := exactUniqueKeySetSize(expr); require.False(t, ok) })
	}
	badCount := DeepCopyExpr(valid)
	badCount.GetF().Args[1].GetVec().Len++
	_, ok = exactUniqueKeySetSize(badCount)
	require.False(t, ok)
	badData := DeepCopyExpr(valid)
	badData.GetF().Args[1].GetVec().Data = []byte{1, 2, 3}
	_, ok = exactUniqueKeySetSize(badData)
	require.False(t, ok)
	require.Zero(t, mp.CurrNB())
}

func TestUniqueKeySetBroadScanGate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		competing  bool
		parameter  bool
		nullValue  bool
		alternate  bool
		sorted     bool
		wantKeySet bool
	}{
		{name: "large scan uses complete unique set", wantKeySet: true},
		{name: "rejected nonunique candidate does not mask unique set", competing: true, wantKeySet: true},
		{name: "dynamic equality falls back without publishing a scan", parameter: true},
		{name: "NULL equality falls back without publishing a scan", nullValue: true},
		{name: "failed unique lowering does not mask another unique set", parameter: true, alternate: true, wantKeySet: true},
		{name: "more selective physical ordering remains preferred", sorted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			proc := mock.ctxt.GetProcess()
			t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
			addIndexHintChoiceTableForTest(mock)
			addCompositeKeySetPayloadColumn(mock)
			table := mock.ctxt.tables["index_hint_t"]
			var unique, secondary *IndexDef
			for _, idx := range table.Indexes {
				switch idx.IndexName {
				case "uk_ab":
					unique = idx
				case "idx_ab":
					copy := *idx
					copy.IndexName = "idx_a_payload"
					copy.Parts = []string{"a", "payload", catalog.CreateAlias("id")}
					secondary = &copy
				}
			}
			require.NotNil(t, unique)
			table.Indexes = []*IndexDef{unique}
			if tc.competing {
				require.NotNil(t, secondary)
				table.Indexes = []*IndexDef{secondary, unique}
			}
			if tc.alternate {
				copy := *unique
				copy.IndexName = "uk_payload_b"
				copy.Parts = []string{"payload", "b"}
				table.Indexes = []*IndexDef{unique, &copy}
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
			tag := builder.genNewBindTag()
			makeLiteral := func(pos int32, value int32) *planpb.Expr {
				return &planpb.Expr{Typ: table.Cols[pos].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: value}}}}
			}
			makeIn := func(pos int32, selectivity float64) *planpb.Expr {
				typ := table.Cols[pos].Typ
				v := vector.NewVec(types.T_int32.ToType())
				defer v.Free(proc.Mp())
				require.NoError(t, vector.AppendFixed(v, int32(2), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(v, int32(3), false, proc.Mp()))
				data, err := v.MarshalBinary()
				require.NoError(t, err)
				e, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "in", []*planpb.Expr{
					GetColExpr(typ, tag, pos),
					{Typ: typ, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 2, Data: data}}},
				})
				require.NoError(t, err)
				e.Selectivity = selectivity
				return e
			}
			aPos, bPos, payloadPos := table.Name2ColIndex["a"], table.Name2ColIndex["b"], table.Name2ColIndex["payload"]
			value := makeLiteral(aPos, 1)
			if tc.parameter {
				value = &planpb.Expr{Typ: table.Cols[aPos].Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			} else if tc.nullValue {
				value = &planpb.Expr{Typ: table.Cols[aPos].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
			}
			equal, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[aPos].Typ, tag, aPos), value})
			require.NoError(t, err)
			equal.Selectivity = 1
			filters := []*planpb.Expr{equal, makeIn(bPos, 0.5)}
			if tc.competing {
				filters = append(filters, makeIn(payloadPos, 1e-8))
			}
			if tc.alternate {
				alt, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[payloadPos].Typ, tag, payloadPos), makeLiteral(payloadPos, 4)})
				require.NoError(t, err)
				alt.Selectivity = 1
				filters = append(filters, alt)
			}
			if tc.sorted {
				idPos := table.Name2ColIndex["id"]
				ordered, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), ">", []*planpb.Expr{GetColExpr(table.Cols[idPos].Typ, tag, idPos), makeLiteral(idPos, 100)})
				require.NoError(t, err)
				ordered.Selectivity = 1e-6
				filters = append(filters, ordered)
			}
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table,
				BindingTags: []int32{tag}, FilterList: filters}
			scanID := builder.appendNode(node, NewBindContext(builder, nil))
			node.Stats = &planpb.Stats{TableCnt: 60000, Cost: 60000, Outcnt: 30000, Selectivity: 0.5}
			before := *node.Stats
			limit, offset := &planpb.Expr{}, &planpb.Expr{}
			node.Limit, node.Offset = limit, offset
			if tc.competing {
				candidate, _, count := builder.getMostSelectiveIndexForPointSelect(table.Indexes, node, false, 1)
				require.Equal(t, 0, candidate)
				require.Equal(t, 2, count)
			}
			result := builder.applyIndicesForFiltersRegularIndex(scanID, node,
				map[[2]int32]int{{tag, aPos}: 1, {tag, bPos}: 1, {tag, payloadPos}: 1}, map[[2]int32]*planpb.Expr{})
			require.Equal(t, before, *node.Stats)
			if !tc.wantKeySet {
				require.Equal(t, scanID, result)
				require.Len(t, builder.qry.Nodes, 1)
				require.Same(t, limit, node.Limit)
				require.Same(t, offset, node.Offset)
				return
			}
			require.NotEqual(t, scanID, result)
			join := builder.qry.Nodes[result]
			require.Equal(t, planpb.Node_INDEX, join.JoinType)
			scan := builder.qry.Nodes[join.Children[1]]
			wantIndex := "uk_ab"
			if tc.alternate {
				wantIndex = "uk_payload_b"
			}
			require.Equal(t, wantIndex, scan.IndexScanInfo.IndexName)
			require.Equal(t, "in", scan.FilterList[0].GetF().Func.ObjName)
		})
	}
}
