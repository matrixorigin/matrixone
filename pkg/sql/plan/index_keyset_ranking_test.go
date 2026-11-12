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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func appendKeySetFixedForTest[T any](t *testing.T, v *vector.Vector, mp *mpool.MPool, values ...T) {
	t.Helper()
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(v, value, false, mp))
	}
}

// The independent oracle serializes each full tuple through the existing
// lowering path, in both orders. This checks the component-count proof for
// every accepted scalar class and for duplicate/escaped byte strings.
func TestKeySetComponentCountMatchesFullSerializedTuples(t *testing.T) {
	cases := []struct {
		name   string
		typ    types.Type
		append func(*testing.T, *vector.Vector, *mpool.MPool)
	}{
		{"bool", types.T_bool.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, false, true, false)
		}},
		{"bit", types.T_bit.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, uint64(0), uint64(math.MaxUint64), uint64(0))
		}},
		{"int8", types.T_int8.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, int8(math.MinInt8), int8(math.MaxInt8), int8(math.MinInt8))
		}},
		{"int16", types.T_int16.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, int16(math.MinInt16), int16(math.MaxInt16), int16(math.MinInt16))
		}},
		{"int32", types.T_int32.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, int32(math.MinInt32), int32(math.MaxInt32), int32(math.MinInt32))
		}},
		{"int64", types.T_int64.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, int64(math.MinInt64), int64(math.MaxInt64), int64(math.MinInt64))
		}},
		{"uint8", types.T_uint8.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, uint8(0), uint8(math.MaxUint8), uint8(0))
		}},
		{"uint16", types.T_uint16.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, uint16(0), uint16(math.MaxUint16), uint16(0))
		}},
		{"uint32", types.T_uint32.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, uint32(0), uint32(math.MaxUint32), uint32(0))
		}},
		{"uint64", types.T_uint64.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, uint64(0), uint64(math.MaxUint64), uint64(0))
		}},
		{"decimal64", types.T_decimal64.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Decimal64(0), types.Decimal64(math.MaxUint64), types.Decimal64(0))
		}},
		{"decimal128", types.T_decimal128.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Decimal128{}, types.Decimal128{B0_63: math.MaxUint64, B64_127: math.MaxUint64}, types.Decimal128{})
		}},
		{"decimal256", types.T_decimal256.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Decimal256{}, types.Decimal256{B0_63: math.MaxUint64, B64_127: math.MaxUint64, B128_191: math.MaxUint64, B192_255: math.MaxUint64}, types.Decimal256{})
		}},
		{"date", types.T_date.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Date(0), types.Date(730000), types.Date(0))
		}},
		{"time", types.T_time.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Time(-1), types.Time(1), types.Time(-1))
		}},
		{"datetime", types.T_datetime.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Datetime(1), types.Datetime(1000000), types.Datetime(1))
		}},
		{"timestamp", types.T_timestamp.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Timestamp(-1), types.Timestamp(1000000), types.Timestamp(-1))
		}},
		{"year", types.T_year.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.MoYear(0), types.MoYear(2155), types.MoYear(0))
		}},
		{"uuid", types.T_uuid.ToType(), func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
			appendKeySetFixedForTest(t, v, mp, types.Uuid{}, types.Uuid{15: 255}, types.Uuid{})
		}},
	}
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary} {
		typ := oid.ToType()
		typ.Width = 16
		cases = append(cases, struct {
			name   string
			typ    types.Type
			append func(*testing.T, *vector.Vector, *mpool.MPool)
		}{
			oid.String(), typ, func(t *testing.T, v *vector.Vector, mp *mpool.MPool) {
				for _, s := range [][]byte{nil, {'a'}, {'a', 0}, {'a', 0, 255}, {'a'}, {'a', 0}} {
					require.NoError(t, vector.AppendBytes(v, s, false, mp))
				}
			},
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			proc := mock.ctxt.GetProcess()
			defer func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) }()
			addIndexHintChoiceTableForTest(mock)
			v := vector.NewVec(tc.typ)
			defer v.Free(proc.Mp())
			tc.append(t, v, proc.Mp())
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			typ := makePlan2Type(&tc.typ)
			in := &planpb.Expr{Selectivity: 0.5, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{GetColExpr(typ, 1, 1), {Typ: typ, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(v.Length()), Data: data}}}}}}}
			count, ok := distinctKeySetComponentCount(in)
			require.True(t, ok)
			equalType := makePlan2Type(&[]types.Type{types.T_int32.ToType()}[0])
			equal := &planpb.Expr{Selectivity: 1, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{GetColExpr(equalType, 1, 2), {Typ: equalType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 42}}}}}}}}
			encoder, err := function.NewSerialValueEncoder(v)
			require.NoError(t, err)
			for _, inFirst := range []bool{false, true} {
				filters := []*planpb.Expr{equal, in}
				if inFirst {
					filters = []*planpb.Expr{in, equal}
				}
				b := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
				idx := &IndexDef{Unique: true, Parts: []string{"left", "right"}}
				got := b.replaceKeySetCondition(idx, filters, []int32{0, 1}, 7, mock.ctxt.tables["uk_hint_ab"])
				require.NotNil(t, got, "inFirst=%v", inFirst)
				actualCount, ok := exactUniqueKeySetSize(got)
				require.True(t, ok)
				require.Equal(t, count, actualCount)
				keys := vector.NewVec(types.T_any.ToType())
				require.NoError(t, keys.UnmarshalBinary(got.GetF().Args[1].GetVec().Data))
				expected := make(map[string]struct{})
				packer := types.NewPacker()
				for row := 0; row < v.Length(); row++ {
					packer.Reset()
					if !inFirst {
						packer.EncodeInt32(42)
					}
					encoder(v, row, packer)
					if inFirst {
						packer.EncodeInt32(42)
					}
					require.NoError(t, packer.Err())
					expected[string(packer.GetBuf())] = struct{}{}
				}
				packer.Close()
				require.Len(t, expected, keys.Length())
				for row := 0; row < keys.Length(); row++ {
					_, exists := expected[string(keys.GetBytesAt(row))]
					require.True(t, exists)
					if row > 0 {
						require.Less(t, bytes.Compare(keys.GetBytesAt(row-1), keys.GetBytesAt(row)), 0)
					}
				}
			}
		})
	}
}

type keySetRankingContext struct {
	*MockCompilerContext
	fail          map[string]bool
	beforeResolve func(string)
}

func (c *keySetRankingContext) ResolveIndexTableByRef(ref *ObjectRef, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if c.beforeResolve != nil {
		c.beforeResolve(name)
	}
	if c.fail[name] {
		return nil, nil, moerr.NewInternalErrorNoCtx("injected index resolution failure")
	}
	return c.MockCompilerContext.ResolveIndexTableByRef(ref, name, snapshot)
}

type keySetRankingFixture struct {
	ctx           *keySetRankingContext
	builder       *QueryBuilder
	node          *planpb.Node
	indexes       []*IndexDef
	inB, inC      *planpb.Expr
	limit, offset *planpb.Expr
}

func makeKeySetRankingFixture(t *testing.T, mode string) *keySetRankingFixture {
	t.Helper()
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	c := *table.Cols[table.Name2ColIndex["b"]]
	c.Name, c.OriginName, c.ColId = "c", "c", 100
	table.Name2ColIndex["c"] = int32(len(table.Cols))
	table.Cols = append(table.Cols, &c)
	table.Indexes = nil
	for i, def := range []struct {
		name  string
		parts []string
	}{
		{"ab", []string{"a", "b"}}, {"ba", []string{"b", "a"}}, {"ac", []string{"a", "c"}},
	} {
		tableName := "keyset_" + def.name
		addIndexHintIndexTableForTest(mock, tableName, uint64(28000+i))
		table.Indexes = append(table.Indexes, &IndexDef{IndexName: def.name, IndexTableName: tableName, Parts: def.parts, Unique: true, TableExist: true})
	}
	ctx := &keySetRankingContext{MockCompilerContext: &mock.ctxt, fail: map[string]bool{}}
	b := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	tag := b.genNewBindTag()
	aPos, bPos, cPos := table.Name2ColIndex["a"], table.Name2ColIndex["b"], table.Name2ColIndex["c"]
	lit := &planpb.Expr{Typ: table.Cols[aPos].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
	if mode == "parameter" {
		lit = &planpb.Expr{Typ: table.Cols[aPos].Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	}
	if mode == "null" {
		lit = &planpb.Expr{Typ: table.Cols[aPos].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	}
	eq, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[aPos].Typ, tag, aPos), lit})
	require.NoError(t, err)
	eq.Selectivity = 1
	makeIn := func(pos int32, values ...int32) *planpb.Expr {
		vec := vector.NewVec(types.T_int32.ToType())
		defer vec.Free(proc.Mp())
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(vec, value, false, proc.Mp()))
		}
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		e, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{GetColExpr(table.Cols[pos].Typ, tag, pos), {Typ: table.Cols[pos].Typ, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(len(values)), Data: data}}}})
		require.NoError(t, err)
		e.Selectivity = .5
		return e
	}
	inB := makeIn(bPos, 2, 2, 2)
	inC := makeIn(cPos, 9, 8)
	if mode == "tie" {
		inB = makeIn(bPos, 3, 2, 2)
		table.Indexes[0], table.Indexes[1] = table.Indexes[1], table.Indexes[0]
	}
	if mode == "estimate" {
		inC.Selectivity = .000001
	}
	if mode == "resolve_failure" {
		ctx.fail["keyset_ab"] = true
	}
	if mode == "all_failure" {
		for _, idx := range table.Indexes {
			ctx.fail[idx.IndexTableName] = true
		}
	}
	if mode == "lowering_failure" {
		ctx.tables["keyset_ab"].Cols[0].Typ = planpb.Type{Id: int32(types.T_int32)}
	}
	limit, offset := &planpb.Expr{}, &planpb.Expr{}
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: ctx.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: []*planpb.Expr{eq, inB, inC}, Limit: limit, Offset: offset}
	b.appendNode(node, NewBindContext(b, nil))
	node.Stats = &planpb.Stats{TableCnt: 60000, Cost: 60000, Outcnt: 30000, Selectivity: .5}
	return &keySetRankingFixture{ctx: ctx, builder: b, node: node, indexes: table.Indexes, inB: inB, inC: inC, limit: limit, offset: offset}
}

func TestBoundedUniqueKeySetRankingMatchesExhaustive(t *testing.T) {
	for _, tc := range []struct {
		name, mode, want string
		sorted           float64
	}{
		{"duplicates alter ranking", "duplicates", "ab", 1},
		{"stable tie preserves catalog order for reversed index", "tie", "ba", 1},
		{"filter estimate can beat smaller key count", "estimate", "ac", 1},
		{"preferred candidate resolution failure tries next", "resolve_failure", "ba", 1},
		{"first candidate lowering failure tries next", "lowering_failure", "ba", 1},
		{"all candidate resolutions fail", "all_failure", "", 1},
		{"sort just below key set score", "duplicates", "", math.Nextafter(1.0/60000, 0)},
		{"sort equal to key set score", "duplicates", "", 1.0 / 60000},
		{"sort just above key set score", "duplicates", "ab", math.Nextafter(1.0/60000, math.Inf(1))},
		{"parameter uses exhaustive fallback", "parameter", "", 1},
		{"NULL uses exhaustive fallback", "null", "", 1},
		{"sorted scan remains preferred", "duplicates", "", .000001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast := makeKeySetRankingFixture(t, tc.mode)
			slow := makeKeySetRankingFixture(t, tc.mode)
			fastResult, fastScan := fast.builder.applyBoundedUniqueKeySet(fast.indexes, fast.node, &Snapshot{}, tc.sorted)
			slowResult, slowScan := slow.builder.applyBoundedUniqueKeySetExhaustive(slow.indexes, slow.node, &Snapshot{}, tc.sorted)
			if tc.want == "" {
				require.Equal(t, int32(-1), fastResult)
				require.Equal(t, int32(-1), fastScan)
				require.Equal(t, int32(-1), slowResult)
				require.Equal(t, int32(-1), slowScan)
				require.Len(t, fast.builder.qry.Nodes, 1)
				require.Same(t, fast.limit, fast.node.Limit)
				require.Same(t, fast.offset, fast.node.Offset)
				return
			}
			require.GreaterOrEqual(t, fastResult, int32(0))
			require.GreaterOrEqual(t, slowResult, int32(0))
			fscan, sscan := fast.builder.qry.Nodes[fastScan], slow.builder.qry.Nodes[slowScan]
			require.Equal(t, tc.want, fscan.IndexScanInfo.IndexName)
			require.Equal(t, sscan.IndexScanInfo.IndexName, fscan.IndexScanInfo.IndexName)
			require.Equal(t, sscan.FilterList[0].GetF().Args[1].GetVec().Data, fscan.FilterList[0].GetF().Args[1].GetVec().Data)
		})
	}
}

// Inject a changed IN vector between ranking and lowering. The mismatch must
// trigger exhaustive reranking while the original scan is still unpublished.
func TestBoundedUniqueKeySetCountMismatchFallsBackBeforePublish(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	dataVec := vector.NewVec(types.T_int32.ToType())
	defer dataVec.Free(f.ctx.GetProcess().Mp())
	appendKeySetFixedForTest(t, dataVec, f.ctx.GetProcess().Mp(), int32(1), int32(2), int32(3))
	data, err := dataVec.MarshalBinary()
	require.NoError(t, err)
	calls := 0
	f.ctx.beforeResolve = func(name string) {
		calls++
		require.Len(t, f.builder.qry.Nodes, 1)
		require.Same(t, f.limit, f.node.Limit)
		require.Same(t, f.offset, f.node.Offset)
		if calls == 1 {
			require.Equal(t, "keyset_ab", name)
			f.inB.GetF().Args[1].GetVec().Data = data
		}
	}
	result, scan := f.builder.applyBoundedUniqueKeySet(f.indexes, f.node, &Snapshot{}, 1)
	require.GreaterOrEqual(t, result, int32(0))
	require.GreaterOrEqual(t, scan, int32(0))
	require.GreaterOrEqual(t, calls, 4) // one ranked attempt plus exhaustive candidates
	require.Equal(t, "ac", f.builder.qry.Nodes[scan].IndexScanInfo.IndexName)
	count, ok := exactUniqueKeySetSize(f.builder.qry.Nodes[scan].FilterList[0])
	require.True(t, ok)
	require.Equal(t, 2, count)
}

func TestDistinctKeySetComponentRejectsUnsafeInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	makeIn := func(colType, vectorType types.Type, count int, null bool) *planpb.Expr {
		v := vector.NewVec(vectorType)
		defer v.Free(mp)
		for i := 0; i < count; i++ {
			require.NoError(t, vector.AppendFixed(v, int32(i), null && i == 0, mp))
		}
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		typ := makePlan2Type(&colType)
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{GetColExpr(typ, 1, 1), {Typ: typ, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(count), Data: data}}}}}}}
	}
	intType := types.T_int32.ToType()
	for _, tc := range []struct {
		name  string
		count int
		null  bool
		want  int
		ok    bool
	}{
		{"empty", 0, false, 0, false}, {"one", 1, false, 1, true}, {"maximum", maxCompositePoints, false, maxCompositePoints, true}, {"above maximum", maxCompositePoints + 1, false, 0, false}, {"nullable", 2, true, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, ok := distinctKeySetComponentCount(makeIn(intType, intType, tc.count, tc.null))
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.want, count)
			}
		})
	}
	bad := makeIn(intType, intType, 1, false)
	bad.GetF().Args[1].GetVec().Data = []byte{1, 2, 3}
	_, ok := distinctKeySetComponentCount(bad)
	require.False(t, ok)
	_, ok = distinctKeySetComponentCount(makeIn(types.T_float32.ToType(), intType, 1, false))
	require.False(t, ok)
	_, ok = distinctKeySetComponentCount(makeIn(types.T_uint32.ToType(), intType, 1, false))
	require.False(t, ok)
	require.Zero(t, mp.CurrNB())
}

func TestDistinctKeySetConstantVectors(t *testing.T) {
	for _, oid := range []types.T{types.T_int32, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary} {
		t.Run(oid.String(), func(t *testing.T) {
			f := makeKeySetRankingFixture(t, "duplicates")
			mp := f.ctx.GetProcess().Mp()
			typ := oid.ToType()
			if typ.IsVarlen() {
				typ.Width = 16
			}
			var v *vector.Vector
			var err error
			if oid == types.T_int32 {
				v, err = vector.NewConstFixed(typ, int32(2), 4, mp)
			} else {
				v, err = vector.NewConstBytes(typ, []byte{'a', 0, 255}, 4, mp)
			}
			require.NoError(t, err)
			defer v.Free(mp)
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			pTyp := makePlan2Type(&typ)
			in := &planpb.Expr{Selectivity: .5, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{GetColExpr(pTyp, 1, 1), {Typ: pTyp, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 4, Data: data}}}}}}}
			count, ok := distinctKeySetComponentCount(in)
			require.True(t, ok)
			require.Equal(t, 1, count)
			for _, first := range []bool{false, true} {
				filters := []*planpb.Expr{f.node.FilterList[0], in}
				if first {
					filters[0], filters[1] = filters[1], filters[0]
				}
				got := f.builder.replaceKeySetCondition(&IndexDef{Unique: true, Parts: []string{"left", "right"}}, filters, []int32{0, 1}, 7, f.ctx.tables["keyset_ab"])
				require.NotNil(t, got)
				actual, ok := exactUniqueKeySetSize(got)
				require.True(t, ok)
				require.Equal(t, count, actual)
			}
			vnull := vector.NewConstNull(typ, 4, mp)
			defer vnull.Free(mp)
			nullData, err := vnull.MarshalBinary()
			require.NoError(t, err)
			in.GetF().Args[1].GetVec().Data = nullData
			_, ok = distinctKeySetComponentCount(in)
			require.False(t, ok)
		})
	}
}

func TestBoundedUniqueKeySetRankingDifferentialMatrix(t *testing.T) {
	const seed int64 = 7321
	rng := rand.New(rand.NewSource(seed))
	selectivities := []float64{0, 1e-6, 1e-5, .5, 1, math.NaN(), math.Inf(1), -1}
	for i := 0; i < 96; i++ {
		bvals, cvals := make([]int32, 1+rng.Intn(8)), make([]int32, 1+rng.Intn(8))
		for j := range bvals {
			bvals[j] = int32(rng.Intn(7) - 3)
		}
		for j := range cvals {
			cvals[j] = int32(rng.Intn(7) - 3)
		}
		bs, cs := selectivities[rng.Intn(len(selectivities))], selectivities[rng.Intn(len(selectivities))]
		order := rng.Perm(3)
		failBits := rng.Intn(8)
		sorted := []float64{0, 1e-6, 1.0 / 60000, 2.0 / 60000, 1}[rng.Intn(5)]
		t.Run(fmt.Sprintf("seed_%d_case_%d", seed, i), func(t *testing.T) {
			fast, slow := makeKeySetRankingFixture(t, "duplicates"), makeKeySetRankingFixture(t, "duplicates")
			for _, f := range []*keySetRankingFixture{fast, slow} {
				for j, vals := range [][]int32{bvals, cvals} {
					v := vector.NewVec(types.T_int32.ToType())
					appendKeySetFixedForTest(t, v, f.ctx.GetProcess().Mp(), vals...)
					data, err := v.MarshalBinary()
					v.Free(f.ctx.GetProcess().Mp())
					require.NoError(t, err)
					e := []*planpb.Expr{f.inB, f.inC}[j]
					e.GetF().Args[1].GetVec().Data = data
					e.GetF().Args[1].GetVec().Len = int32(len(vals))
					e.Selectivity = []float64{bs, cs}[j]
				}
				original := append([]*IndexDef(nil), f.indexes...)
				for j, k := range order {
					f.indexes[j] = original[k]
				}
				for j, name := range []string{"keyset_ab", "keyset_ba", "keyset_ac"} {
					f.ctx.fail[name] = failBits&(1<<j) != 0
				}
			}
			fid, fs := fast.builder.applyBoundedUniqueKeySet(fast.indexes, fast.node, &Snapshot{}, sorted)
			sid, ss := slow.builder.applyBoundedUniqueKeySetExhaustive(slow.indexes, slow.node, &Snapshot{}, sorted)
			require.Equal(t, fid >= 0, sid >= 0)
			if fid < 0 {
				require.Len(t, fast.builder.qry.Nodes, 1)
				require.Same(t, fast.limit, fast.node.Limit)
				require.Same(t, fast.offset, fast.node.Offset)
				return
			}
			fn, sn := fast.builder.qry.Nodes[fs], slow.builder.qry.Nodes[ss]
			require.Equal(t, sn.IndexScanInfo.IndexName, fn.IndexScanInfo.IndexName)
			require.Equal(t, sn.FilterList[0].GetF().Args[1].GetVec().Data, fn.FilterList[0].GetF().Args[1].GetVec().Data)
		})
	}
}
