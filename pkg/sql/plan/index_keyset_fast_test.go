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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func keySetTestEquality(typ planpb.Type, lit *planpb.Expr, col int32) *planpb.Expr {
	return &planpb.Expr{Selectivity: 1, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{GetColExpr(typ, 1, col), lit},
	}}}
}

func keySetTestIn(t *testing.T, v *vector.Vector) *planpb.Expr {
	t.Helper()
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	typ := makePlan2Type(v.GetType())
	return &planpb.Expr{Selectivity: .5, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{GetColExpr(typ, 1, 1),
			{Typ: typ, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(v.Length()), Data: data}}}},
	}}}
}

func TestFastExactKeySetWireAndMiddleComponent(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	mp := proc.Mp()
	typ := types.T_varchar.ToType()
	typ.Width = 64
	v := vector.NewVec(typ)
	defer v.Free(mp)
	for i := 0; i <= 32; i++ {
		require.NoError(t, vector.AppendBytes(v, []byte(strings.Repeat("a", i)), false, mp))
	}
	for _, value := range [][]byte{{'a', 0}, {'a', 0, 255}, {'a', 0}, {255}} {
		require.NoError(t, vector.AppendBytes(v, value, false, mp))
	}
	in := keySetTestIn(t, v)
	charType := planpb.Type{Id: int32(types.T_char), Width: 8}
	charLit := &planpb.Expr{Typ: charType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "q"}}}}
	intType := planpb.Type{Id: int32(types.T_int32)}
	intLit := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 42}}}}
	filters := []*planpb.Expr{keySetTestEquality(charType, charLit, 0), in, keySetTestEquality(intType, intLit, 2)}
	positions := []int32{0, 1, 2}
	idx := &IndexDef{Unique: true, Parts: []string{"a", "b", "c"}}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	rowVec := vector.NewVec(typ)
	defer rowVec.Free(mp)
	fast, handled := builder.fastExactKeySet(idx, filters, positions, v, rowVec, types.T_varchar.ToType(), mp)
	require.True(t, handled)
	require.NotNil(t, fast)
	defer fast.Free(mp)
	expected := make(map[string]struct{})
	found23, found24 := false, false
	for row := 0; row < v.Length(); row++ {
		tuple := builder.foldKeySetTuple(idx, filters, positions, v, rowVec, row, mp)
		require.NotNil(t, tuple)
		require.NotNil(t, tuple.GetVec())
		decoded := vector.NewVec(types.T_any.ToType())
		require.NoError(t, decoded.UnmarshalBinary(tuple.GetVec().Data))
		key := decoded.GetBytesAt(0)
		found23 = found23 || len(key) == types.VarlenaInlineSize
		found24 = found24 || len(key) == types.VarlenaInlineSize+1
		expected[string(key)] = struct{}{}
	}
	require.True(t, found23, "test must cross the inline boundary")
	require.True(t, found24, "test must cross the inline boundary")
	fast.InplaceSortAndCompact()
	require.True(t, fast.GetSorted())
	require.Equal(t, len(expected), fast.Length())
	for row := 0; row < fast.Length(); row++ {
		_, ok := expected[string(fast.GetBytesAt(row))]
		require.True(t, ok)
		if row > 0 {
			require.Less(t, bytes.Compare(fast.GetBytesAt(row-1), fast.GetBytesAt(row)), 0)
		}
	}
	got := builder.replaceKeySetCondition(idx, filters, positions, 7, mock.ctxt.tables["uk_hint_ab"])
	size, ok := exactUniqueKeySetSize(got)
	require.True(t, ok)
	require.Equal(t, len(expected), size)
}

func TestKeySetSerializedBudgetArithmetic(t *testing.T) {
	cases := []struct {
		total, size, want int
		ok                bool
	}{
		{0, 0, 0, true}, {0, maxCompositePointBytes, maxCompositePointBytes, true},
		{maxCompositePointBytes - 1, 1, maxCompositePointBytes, true},
		{maxCompositePointBytes, 1, 0, false}, {0, maxCompositePointBytes + 1, 0, false},
		{math.MaxInt, 1, 0, false}, {1, math.MaxInt, 0, false}, {-1, 0, 0, false}, {0, -1, 0, false},
	}
	for _, tc := range cases {
		got, ok := addKeySetBytes(tc.total, tc.size)
		require.Equal(t, tc.ok, ok)
		require.Equal(t, tc.want, got)
	}
}

func TestFastExactKeySetUnsafeEqualityFallback(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	typ := types.T_varchar.ToType()
	typ.Width = 32
	v := vector.NewVec(typ)
	defer v.Free(proc.Mp())
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendBytes(v, []byte("x"), false, proc.Mp()))
	}
	in := keySetTestIn(t, v)
	intType := planpb.Type{Id: int32(types.T_int32)}
	good := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
	source := DeepCopyExpr(good)
	source.GetLit().Src = &planpb.Expr{}
	nullable := DeepCopyExpr(good)
	nullable.GetLit().Isnull = true
	datalinkType := planpb.Type{Id: int32(types.T_datalink)}
	datalink := &planpb.Expr{Typ: datalinkType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "stage://example/path"}}}}
	param := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	idx := &IndexDef{Unique: true, Parts: []string{"a", "b"}}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	rowVec := vector.NewVec(typ)
	defer rowVec.Free(proc.Mp())
	for name, rhs := range map[string]*planpb.Expr{"parameter": param, "source": source, "null": nullable, "datalink": datalink} {
		t.Run(name, func(t *testing.T) {
			filters := []*planpb.Expr{keySetTestEquality(rhs.Typ, rhs, 0), in}
			got, handled := builder.fastExactKeySet(idx, filters, []int32{0, 1}, v, rowVec, types.T_varchar.ToType(), proc.Mp())
			require.False(t, handled)
			require.Nil(t, got)
		})
	}
}

// A bounded off-heap row scratch rejects a single oversized value; the helper
// must decline and release any partially allocated scratch state.
func TestFastExactKeySetAllocationFailureReleasesScratch(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	typ := types.T_varchar.ToType()
	typ.Width = 3 << 20
	v, err := vector.NewConstBytes(typ, []byte(strings.Repeat("x", 2<<20)), 8, proc.Mp())
	require.NoError(t, err)
	defer v.Free(proc.Mp())
	in := keySetTestIn(t, v)
	intType := planpb.Type{Id: int32(types.T_int32)}
	lit := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
	filters := []*planpb.Expr{keySetTestEquality(intType, lit, 0), in}
	idx := &IndexDef{Unique: true, Parts: []string{"a", "b"}}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	small, err := mpool.NewMPool("keyset-fast-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(small)
	rowVec := vector.NewVec(typ)
	rowVec.SetOffHeap(true)
	defer rowVec.Free(small)
	keys, handled := builder.fastExactKeySet(idx, filters, []int32{0, 1}, v, rowVec, types.T_varchar.ToType(), small)
	if keys != nil {
		keys.Free(small)
	}
	require.False(t, handled)
	require.Nil(t, keys)
	rowVec.Free(small)
	require.Zero(t, small.CurrNB())
}

func TestKeySetNonuniquePrefixWithDuplicateValues(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	typ := types.T_varchar.ToType()
	typ.Width = 32
	v := vector.NewVec(typ)
	defer v.Free(proc.Mp())
	for _, value := range [][]byte{{'a'}, {'a', 0}, {'a'}, {'b'}, {255}, {'b'}, {'a', 0}, {'c'}} {
		require.NoError(t, vector.AppendBytes(v, value, false, proc.Mp()))
	}
	in := keySetTestIn(t, v)
	intType := planpb.Type{Id: int32(types.T_int32)}
	lit := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
	filters := []*planpb.Expr{keySetTestEquality(intType, lit, 0), in}
	idx := &IndexDef{Parts: []string{"a", "b"}}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	got := builder.replaceKeySetCondition(idx, filters, []int32{0, 1}, 7, mock.ctxt.tables["uk_hint_ab"])
	require.NotNil(t, got)
	require.Equal(t, "prefix_in", got.GetF().Func.ObjName)
	tuples := got.GetF().Args[1].GetList()
	require.NotNil(t, tuples)
	require.Len(t, tuples.List, v.Length())
	rowVec := vector.NewVec(typ)
	defer rowVec.Free(proc.Mp())
	for row, actual := range tuples.List {
		original := builder.foldKeySetTuple(idx, filters, []int32{0, 1}, v, rowVec, row, proc.Mp())
		require.NotNil(t, original)
		require.NotNil(t, actual.GetVec())
		require.NotNil(t, original.GetVec())
		require.Equal(t, original.GetVec().Data, actual.GetVec().Data)
	}
}

func TestKeySetInputCountAndNullFallbackAtFastThreshold(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	typ := types.T_varchar.ToType()
	typ.Width = 32
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	intType := planpb.Type{Id: int32(types.T_int32)}
	lit := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
	idx := &IndexDef{Unique: true, Parts: []string{"a", "b"}}
	for _, tc := range []struct {
		name    string
		count   int
		hasNull bool
	}{
		{"128_duplicates", maxCompositePoints, false}, {"129_duplicates", maxCompositePoints + 1, false}, {"eight_with_null", 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := vector.NewVec(typ)
			defer v.Free(proc.Mp())
			for row := 0; row < tc.count; row++ {
				require.NoError(t, vector.AppendBytes(v, []byte("x"), tc.hasNull && row == 3, proc.Mp()))
			}
			in := keySetTestIn(t, v)
			filters := []*planpb.Expr{keySetTestEquality(intType, lit, 0), in}
			got := builder.replaceKeySetCondition(idx, filters, []int32{0, 1}, 7, mock.ctxt.tables["uk_hint_ab"])
			if tc.count > maxCompositePoints || tc.hasNull {
				require.Nil(t, got)
				return
			}
			count, ok := exactUniqueKeySetSize(got)
			require.True(t, ok)
			require.Equal(t, 1, count)
		})
	}
}

func TestKeySetPoolFailureDoesNotPublishIndex(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	originalPool := proc.Mp()
	typ := types.T_int32.ToType()
	values := vector.NewVec(typ)
	defer values.Free(originalPool)
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(values, int32(i), false, originalPool))
	}
	in := keySetTestIn(t, values)
	stringType := planpb.Type{Id: int32(types.T_varchar), Width: 3 << 20}
	lit := &planpb.Expr{Typ: stringType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: strings.Repeat("x", 2<<20)}}}}
	filters := []*planpb.Expr{keySetTestEquality(stringType, lit, 0), in}
	limit, offset := &planpb.Expr{}, &planpb.Expr{}
	node := &planpb.Node{ObjRef: &planpb.ObjectRef{}, FilterList: filters, Limit: limit, Offset: offset}
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	builder.qry.Nodes = []*planpb.Node{node}
	idx := &IndexDef{Unique: true, IndexTableName: "uk_hint_ab", Parts: []string{"a", "b"}}
	small, err := mpool.NewMPool("keyset-publish-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(small)
	proc.SetMPool(small)
	defer proc.SetMPool(originalPool)
	ret, scan := builder.applyIndexJoin(idx, node, KeySetIndexCondition, []int32{0, 1}, &Snapshot{})
	require.EqualValues(t, -1, ret)
	require.EqualValues(t, -1, scan)
	require.Len(t, builder.qry.Nodes, 1)
	require.Same(t, limit, node.Limit)
	require.Same(t, offset, node.Offset)
	require.Zero(t, small.CurrNB())
}
