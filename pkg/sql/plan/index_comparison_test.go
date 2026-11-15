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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSerializedIndexComparisonDomains(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key, value types.Type
		usable     bool
	}{
		{"datetime", types.T_datetime.ToTypeWithScale(6), types.T_datetime.ToTypeWithScale(3), true},
		{"timestamp", types.T_timestamp.ToTypeWithScale(3), types.T_timestamp.ToTypeWithScale(6), true},
		{"datetime_timestamp", types.T_datetime.ToTypeWithScale(6), types.T_timestamp.ToTypeWithScale(3), false},
		{"timestamp_datetime", types.T_timestamp.ToTypeWithScale(3), types.T_datetime.ToTypeWithScale(6), false},
		{"integer", types.T_int64.ToType(), types.T_int64.ToType(), true},
		{"signed_unsigned", types.T_int64.ToType(), types.T_uint64.ToType(), false},
		{"float_zero", types.T_float64.ToType(), types.T_float64.ToType(), false},
		{"decimal", types.New(types.T_decimal64, 10, 2), types.New(types.T_decimal64, 18, 2), true},
		{"runtime_decimal_scale", types.New(types.T_decimal64, 10, 2), types.New(types.T_decimal64, 18, 6), false},
		{"raw_string", types.New(types.T_varbinary, 20, 0), types.New(types.T_varchar, 10, 0), true},
		{"json_order", types.T_json.ToType(), types.T_json.ToType(), false},
		{"char_padding", types.New(types.T_char, 8, 0), types.New(types.T_char, 8, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := GetColExpr(makePlan2Type(&tc.key), 0, 0)
			bound := &planpb.Expr{Typ: makePlan2Type(&tc.value), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			for _, op := range []string{"=", "<", "<=", ">", ">="} {
				for _, reverse := range []bool{false, true} {
					args := []*planpb.Expr{col, bound}
					if reverse {
						args = []*planpb.Expr{bound, col}
					}
					fn := &planpb.Function{Func: &planpb.ObjectRef{ObjName: op}, Args: args}
					require.Equal(t, tc.usable, serializedIndexComparisonUsable(fn, col.Typ), "%s reverse=%v", op, reverse)
				}
			}
			for _, op := range []string{"between", "in_range"} {
				fn := &planpb.Function{Func: &planpb.ObjectRef{ObjName: op}, Args: []*planpb.Expr{col, bound, bound}}
				if op == "in_range" {
					fn.Args = append(fn.Args, MakePlan2Uint8ConstExprWithType(0))
				}
				require.Equal(t, tc.usable, serializedIndexComparisonUsable(fn, col.Typ), op)
			}
			in := &planpb.Function{Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{col, {Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{bound}}}}}}
			require.Equal(t, tc.usable, serializedIndexComparisonUsable(in, col.Typ), "IN must inspect members, not the list's relabeled type")
		})
	}
	d := types.T_datetime.ToTypeWithScale(6)
	stamp := types.T_timestamp.ToTypeWithScale(6)
	col := GetColExpr(makePlan2Type(&d), 0, 0)
	bound := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{Typ: makePlan2Type(&typ), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	}
	comparison := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "<"}, Args: []*planpb.Expr{col, bound(typ)}}}}
	}
	for _, peer := range []types.Type{d, stamp} {
		or := &planpb.Function{Func: &planpb.ObjectRef{ObjName: "or"}, Args: []*planpb.Expr{comparison(d), {Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "or"}, Args: []*planpb.Expr{comparison(d), comparison(peer)}}}}}}
		require.Equal(t, peer.Oid == d.Oid, serializedIndexComparisonUsable(or, col.Typ), "every nested OR arm needs a candidate superset")
	}
}

func TestDecimalIndexNormalizationRequiresExactInRangeValue(t *testing.T) {
	ctx := context.Background()
	key := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	for _, tc := range []struct {
		name, text   string
		oid          types.T
		scale, width int32
		usable       bool
	}{
		{"exact", "99.990000", types.T_decimal64, 6, 18, true},
		{"negative exact", "-99.990000", types.T_decimal64, 6, 18, true},
		{"positive rounding", "99.991000", types.T_decimal64, 6, 18, false},
		{"negative rounding", "-99.991000", types.T_decimal64, 6, 18, false},
		{"positive overflow", "100000000.000000", types.T_decimal64, 6, 18, false},
		{"negative overflow", "-100000000.000000", types.T_decimal64, 6, 18, false},
		{"negative edge", "-99999999.990000", types.T_decimal64, 6, 18, true},
		{"wide trailing zeros", "99.990000000000000000000", types.T_decimal128, 21, 38, true},
		{"wide nonzero tail", "99.990000000000000000001", types.T_decimal128, 21, 38, false},
		{"decimal256 exact", "99.9900000000000000000000000000000000000000", types.T_decimal256, 40, 76, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := planpb.Type{Id: int32(tc.oid), Width: tc.width, Scale: tc.scale}
			var value *planpb.Expr
			switch tc.oid {
			case types.T_decimal64:
				v, err := types.ParseDecimal64(tc.text, tc.width, tc.scale)
				require.NoError(t, err)
				value = MakePlan2Decimal64ExprWithType(v, &typ)
			case types.T_decimal128:
				v, err := types.ParseDecimal128(tc.text, tc.width, tc.scale)
				require.NoError(t, err)
				value = MakePlan2Decimal128ExprWithType(v, &typ)
			case types.T_decimal256:
				value = &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: tc.text}}}}
			}
			col := GetColExpr(key, 1, 0)
			filter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: ">="}, Args: []*planpb.Expr{col, value},
			}}}
			original := DeepCopyExpr(filter)
			normalized, err := normalizeSerializedIndexComparison(ctx, filter, key)
			if tc.usable {
				require.NoError(t, err)
				require.Equal(t, key.Id, normalized.GetF().Args[1].Typ.Id)
				require.Equal(t, key.Scale, normalized.GetF().Args[1].Typ.Scale)
			} else {
				require.Error(t, err)
				require.Nil(t, normalized)
			}
			require.Equal(t, original, filter, "do not mutate the SQL residual")
		})
	}
}

func TestDecimalTrailingZeroProofRejectsUnprovedCast(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 4}
	inner := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{
		Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "99.99100"}},
	}}
	cast := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "cast"}, Args: []*planpb.Expr{inner},
	}}}
	key := types.New(types.T_decimal64, 10, 2)
	require.False(t, losslessIndexDecimalBound(cast, key), "source scale differs from cast target")
}

func TestDecimalCastProofRejectsUncertainInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  string
		width  int32
		binary bool
		proven bool
	}{
		{name: "valid literal cast", value: "99.990", width: 5, proven: true},
		{name: "overflowing literal cast", value: "99.990", width: 4},
		{name: "invalid cast syntax", value: "99. 990", width: 5},
		{name: "binary literal", value: "99.990", width: 5, binary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			literal := makePlan2StringConstExprWithType(tc.value, tc.binary)
			typ := planpb.Type{Id: int32(types.T_decimal64), Width: tc.width, Scale: 3}
			cast := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "cast"}, Args: []*planpb.Expr{literal},
			}}}
			key := types.New(types.T_decimal64, 10, 2)
			require.Equal(t, tc.proven, losslessIndexDecimalBound(cast, key))
		})
	}
}

func TestIndexOnlyLiteralVectorNullResidual(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(v, int64(1), false, proc.Mp()))
	for _, tc := range []struct {
		name     string
		withNull bool
	}{
		{name: "without null"},
		{name: "with null", withNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.withNull {
				require.NoError(t, vector.AppendFixed(v, int64(0), true, proc.Mp()))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			rhs := &planpb.Expr{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(v.Length()), Data: data}}}
			require.Equal(t, tc.withNull, runtimeConstMayBeNull(rhs))
		})
	}
	malformed := &planpb.Expr{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 1}}}
	require.True(t, runtimeConstMayBeNull(malformed))
}

func TestDirectIndexComparisonPreservesScalarOverload(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	typ := planpb.Type{Id: int32(types.T_datetime), Scale: 6}
	bound, err := appendCastBeforeExpr(builder.GetContext(), makePlan2StringConstExprWithType("2024-01-02 00:00:00"), typ)
	require.NoError(t, err)
	original, err := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*planpb.Expr{bound, GetColExpr(typ, 0, 0)})
	require.NoError(t, err)
	idx := &IndexDef{Unique: true, Parts: []string{"d"}}
	rewritten := builder.replaceNonEqualCondition(idx, original, 1, &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "key", Typ: typ}}})
	require.NotNil(t, rewritten)
	expected, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<", []*planpb.Expr{GetColExpr(typ, 1, 0), bound})
	require.NoError(t, err)
	require.Equal(t, expected.GetF().Func.Obj, rewritten.GetF().Func.Obj, "the executable overload, not only its name, must be inverted")
	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(makeTypeByPlan2Type(typ))
	for _, value := range []string{"2024-01-01 00:00:00", "2024-01-02 00:00:00", "2024-01-03 00:00:00"} {
		dt, err := types.ParseDatetime(value, 6)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(input.Vecs[0], dt, false, proc.Mp()))
	}
	input.SetRowCount(3)
	for _, expr := range []*planpb.Expr{original, rewritten} {
		func() {
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer exec.Free()
			got, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{true, false, false}, vector.MustFixedColWithTypeCheck[bool](got))
		}()
	}
}

func TestCompositeKeyComparisonKeepsMixedTemporalPredicates(t *testing.T) {
	for _, key := range []struct {
		name  string
		table *planpb.TableDef
	}{{"primary", makeExprOptCompositeSortKeyTableDef()}, {"cluster", makeExprOptCompositeClusterKeyTableDef()}} {
		t.Run(key.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, false)
			tag := builder.genNewBindTag()
			key.table.Cols[1].Typ = planpb.Type{Id: int32(types.T_datetime), Scale: 6}
			first := makeExprOptBinaryInt64Expr(t, builder.compCtx.(*MockCompilerContext), "=", makeExprOptInt64Col(tag, 0, "a"), 1)
			stamp, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "now", nil)
			require.NoError(t, err)
			for _, ops := range [][]string{{"="}, {">=", "<="}} {
				filters := []*planpb.Expr{DeepCopyExpr(first)}
				for _, op := range ops {
					f, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*planpb.Expr{GetColExpr(key.table.Cols[1].Typ, tag, 1), DeepCopyExpr(stamp)})
					require.NoError(t, err)
					filters = append(filters, f)
				}
				originals := DeepCopyExprList(filters[1:])
				got := builder.doMergeFiltersOnCompositeKey(true, key.table, tag, filters...)
				for i, original := range originals {
					require.Contains(t, got, original, "mixed predicate survives any safe prefix lowering")
					require.Equal(t, original, filters[i+1], "no narrowing pre-merge cast")
				}
			}
		})
	}
}

func TestCompositeKeyComparisonRequiresExactResidual(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	table := makeExprOptThreePartCompositeSortKeyTableDef()
	table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varbinary), Width: 8}
	first := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	for _, oid := range []types.T{types.T_varbinary, types.T_text, types.T_blob} {
		table.Cols[1].Typ.Id = int32(oid)
		bound, err := appendCastBeforeExpr(builder.GetContext(), makePlan2StringConstExprWithType("a"), table.Cols[1].Typ)
		require.NoError(t, err)
		for _, op := range []string{"=", ">"} {
			original, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*planpb.Expr{GetColExpr(table.Cols[1].Typ, tag, 1), DeepCopyExpr(bound)})
			require.NoError(t, err)
			got := builder.doMergeFiltersOnCompositeKey(true, table, tag, DeepCopyExpr(first), original)
			require.Len(t, got, 2)
			residual := DeepCopyExpr(original)
			residual.Selectivity = 1
			require.Equal(t, residual, got[1], "prefix ambiguity requires original SQL evaluation")
			if op == ">" {
				require.Equal(t, "prefix_in_range", got[0].GetF().Func.ObjName)
				require.Equal(t, uint32(0), got[0].GetF().Args[3].GetLit().GetU8Val(), "open string lower endpoint must admit longer embedded-NUL values")
			}
		}
	}
	cluster := makeExprOptCompositeClusterKeyTableDef()
	for _, op := range []string{"<", "<="} {
		upper := makeExprOptBinaryInt64Expr(t, ctx, op, makeExprOptInt64Col(tag, 1, "b"), 5)
		got := builder.doMergeFiltersOnCompositeKey(true, cluster, tag, DeepCopyExpr(first), upper)
		require.Len(t, got, 2)
		residual := DeepCopyExpr(upper)
		residual.Selectivity = 1
		require.Equal(t, residual, got[1], "serialized cluster NULL must not satisfy a SQL upper bound")
	}
}

func TestCompositeKeyOrPreservesExactArms(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	for _, oid := range []types.T{types.T_int64, types.T_varbinary} {
		table := makeExprOptCompositeSortKeyTableDef()
		typ := oid.ToType()
		table.Cols[0].Typ = makePlan2Type(&typ)
		arms := make([]*planpb.Expr, 0, 2)
		for i, text := range []string{"a", "b"} {
			var value *planpb.Expr
			if oid == types.T_int64 {
				value = MakePlan2Int64ConstExprWithType(int64(i + 1))
			} else {
				var err error
				value, err = appendCastBeforeExpr(builder.GetContext(), makePlan2StringConstExprWithType(text), table.Cols[0].Typ)
				require.NoError(t, err)
			}
			arm, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[0].Typ, tag, 0), value})
			require.NoError(t, err)
			arms = append(arms, arm)
		}
		original, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", arms)
		require.NoError(t, err)
		got := builder.doMergeFiltersOnCompositeKey(true, table, tag, DeepCopyExpr(original))
		require.Len(t, got, 1)
		if oid == types.T_varbinary {
			require.Equal(t, original, got[0])
		} else {
			require.Equal(t, "prefix_in", got[0].GetF().Func.ObjName)
		}
	}
}

func TestCompositeStringRangeFlags(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	for _, prefix := range []bool{false, true} {
		table := makeExprOptCompositeSortKeyTableDef()
		if prefix {
			table = makeExprOptThreePartCompositeSortKeyTableDef()
		}
		table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varbinary), Width: 8}
		first := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
		bounds := make([]*planpb.Expr, 0, 2)
		for _, text := range []string{"a", "b"} {
			value, err := appendCastBeforeExpr(builder.GetContext(), makePlan2StringConstExprWithType(text), table.Cols[1].Typ)
			require.NoError(t, err)
			bounds = append(bounds, value)
		}
		original, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "in_range", []*planpb.Expr{
			GetColExpr(table.Cols[1].Typ, tag, 1), bounds[0], bounds[1], MakePlan2Uint8ConstExprWithType(1),
		})
		require.NoError(t, err)
		got := builder.doMergeFiltersOnCompositeKey(true, table, tag, DeepCopyExpr(first), original)
		if prefix {
			require.Len(t, got, 2)
			require.Equal(t, uint32(0), got[0].GetF().Args[3].GetLit().GetU8Val())
		} else {
			require.Len(t, got, 1)
			require.Equal(t, "in_range", got[0].GetF().Func.ObjName)
			require.Equal(t, uint32(1), got[0].GetF().Args[3].GetLit().GetU8Val())
		}
	}
}
