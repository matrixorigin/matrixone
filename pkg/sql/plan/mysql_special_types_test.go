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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestFuncCastForEnumTypeKeepsMatchingErrorMember(t *testing.T) {
	target := plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,"}
	expr := &plan.Expr{
		Typ: target,
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_EnumVal{EnumVal: 0},
		}},
	}

	got, err := funcCastForEnumType(context.Background(), expr, target)
	require.NoError(t, err)
	require.Same(t, expr, got)
	require.Equal(t, uint32(0), got.GetLit().GetEnumVal())
}

func TestInsertIgnoreMySQLSpecialTypeLiteralHelpers(t *testing.T) {
	ctx := context.Background()
	enumType := plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,"}
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,y,z"}
	yearType := plan.Type{Id: int32(types.T_year)}

	for _, tc := range []struct {
		name     string
		target   plan.Type
		value    *tree.NumVal
		wantEnum uint32
		wantSet  uint64
		handled  bool
	}{
		{"enum valid label", enumType, tree.NewNumVal("b", "b", false, tree.P_char), 2, 0, true},
		{"enum invalid label", enumType, tree.NewNumVal("bad", "bad", false, tree.P_char), 0, 0, true},
		{"enum numeric ordinal", enumType, tree.NewNumVal(int64(1), "1", false, tree.P_int64), 1, 0, true},
		{"enum invalid numeric ordinal", enumType, tree.NewNumVal(uint64(9), "9", false, tree.P_uint64), 0, 0, true},
		{"set label drops invalid member", setType, tree.NewNumVal("x,bad", "x,bad", false, tree.P_char), 0, 1, true},
		{"set numeric masks unknown bits", setType, tree.NewNumVal(uint64(99), "99", false, tree.P_uint64), 0, 3, true},
		{"set negative numeric becomes empty", setType, tree.NewNumVal(int64(-1), "-1", false, tree.P_int64), 0, 0, true},
		{"invalid year becomes zero", yearType, tree.NewNumVal("2156", "2156", false, tree.P_char), 0, 0, true},
		{"valid year uses normal conversion", yearType, tree.NewNumVal("2024", "2024", false, tree.P_char), 0, 0, false},
		{"null uses normal conversion", enumType, tree.NewNumVal("", "", false, tree.P_null), 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, handled, err := makeInsertIgnoreMySQLSpecialTypeConstExpr(ctx, tc.value, tc.target)
			require.NoError(t, err)
			require.Equal(t, tc.handled, handled)
			if !handled {
				require.Nil(t, expr)
				return
			}
			require.NotNil(t, expr)
			if tc.target.Id == int32(types.T_enum) {
				require.Equal(t, tc.wantEnum, expr.GetLit().GetEnumVal())
			}
			if tc.target.Id == int32(types.T_uint64) {
				require.Equal(t, tc.wantSet, expr.GetLit().GetU64Val())
			}
		})
	}

	require.True(t, mysqlYearLiteralIsValid(tree.NewNumVal(int64(2024), "2024", false, tree.P_int64)))
	require.False(t, mysqlYearLiteralIsValid(tree.NewNumVal(uint64(^uint64(0)), "18446744073709551615", false, tree.P_uint64)))
	require.True(t, mysqlYearLiteralIsValid(tree.NewNumVal("ignored", "ignored", false, tree.P_hexnum)))
	require.Equal(t, uint64(7), mysqlSetValidBitmap("x,y,z"))
	require.Equal(t, ^uint64(0), mysqlSetValidBitmap(strings.Repeat("x,", types.MaxSetMembers-1)+"x"))
}

func TestMySQLSpecialOrderTypeReversibility(t *testing.T) {
	enum := &plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,c"}
	emptyLabelEnum := &plan.Type{Id: int32(types.T_enum), Enumvalues: ",a"}
	duplicateEnum := &plan.Type{Id: int32(types.T_enum), Enumvalues: "a,A"}
	set := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,y"}
	ambiguousSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,"}
	emptyFirstSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: ",x"}
	emptyMiddleSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,,y"}

	require.True(t, mysqlSpecialOrderTypeReversible(enum))
	require.True(t, mysqlSpecialNumericTypeReversible(enum))
	require.True(t, mysqlSpecialOrderTypeReversible(emptyLabelEnum))
	require.False(t, mysqlSpecialNumericTypeReversible(emptyLabelEnum))
	require.False(t, mysqlSpecialOrderTypeReversible(duplicateEnum))
	require.False(t, mysqlSpecialNumericTypeReversible(duplicateEnum))
	require.True(t, mysqlSpecialOrderTypeReversible(set))
	require.True(t, mysqlSpecialNumericTypeReversible(set))
	require.False(t, mysqlSpecialOrderTypeReversible(ambiguousSet))
	require.False(t, mysqlSpecialNumericTypeReversible(ambiguousSet))
	require.True(t, setTypeHasEmptyMember(emptyFirstSet))
	require.True(t, setTypeHasEmptyMember(emptyMiddleSet))
	require.True(t, setTypeHasEmptyMember(ambiguousSet))
	require.False(t, setTypeHasEmptyMember(set))
	require.False(t, mysqlSpecialOrderTypeReversible(&plan.Type{Id: int32(types.T_varchar)}))
	require.Equal(t, enumFoldKey("K"), enumFoldKey("K"))
	require.True(t, mysqlSpecialOrderTypesCompatible(enum, DeepCopyType(enum)))
	require.False(t, mysqlSpecialOrderTypesCompatible(enum, set))
	require.Error(t, newNonReversibleMySQLSpecialOrderError(context.Background()))
}

func TestFindInSetSetBindingUsesStoredBitmap(t *testing.T) {
	ctx := context.Background()
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "z,a,m"}
	display, err := makeEnumOrSetDisplayValue(ctx, &plan.Expr{Typ: setType})
	require.NoError(t, err)

	bound, err := BindFuncExprImplByPlanExpr(ctx, "find_in_set", []*plan.Expr{
		makePlan2StringConstExprWithType("a"), display,
	})
	require.NoError(t, err)
	fn := bound.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 3)
	require.Equal(t, int32(types.T_uint64), fn.Args[1].Typ.Id)
	require.Empty(t, fn.Args[1].Typ.Enumvalues)
	require.Equal(t, "z,a,m", fn.Args[2].GetLit().GetSval())

	_, err = BindFuncExprImplByPlanExpr(ctx, "find_in_set", []*plan.Expr{
		makePlan2StringConstExprWithType("a"),
		makePlan2StringConstExprWithType("a,b"),
		makePlan2StringConstExprWithType("not-public"),
	})
	require.Error(t, err)
}

func TestBitwiseAggregateSetBindingUsesStoredBitmap(t *testing.T) {
	ctx := context.Background()
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c"}
	bitmap := &plan.Expr{
		Typ:  setType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	}
	display, err := makeEnumOrSetDisplayValue(ctx, bitmap)
	require.NoError(t, err)

	bound, err := BindFuncExprImplByPlanExpr(ctx, "bit_and", []*plan.Expr{display})
	require.NoError(t, err)
	fn := bound.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 1)
	require.Equal(t, int32(types.T_uint64), fn.Args[0].Typ.Id)
	require.Empty(t, fn.Args[0].Typ.Enumvalues)
	require.NotNil(t, fn.Args[0].GetCol())
	require.Equal(t, int32(1), fn.Args[0].GetCol().RelPos)
	require.False(t, isBitwiseAggregatePrivateCast(fn.Args[0]))
}

func TestFindInSetRewriteHelpersRejectInvalidProvenance(t *testing.T) {
	search := makePlan2StringConstExprWithType("a")
	plain := makePlan2StringConstExprWithType("a,b")
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "z,a,m"}
	rawSet := &plan.Expr{
		Typ:  setType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	}

	for _, tc := range []struct {
		name string
		args []*plan.Expr
	}{
		{name: "wrong arity", args: []*plan.Expr{search}},
		{name: "nil operand", args: []*plan.Expr{search, nil}},
		{name: "ordinary string", args: []*plan.Expr{search, plain}},
		{
			name: "malformed display wrapper",
			args: []*plan.Expr{search, {
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: moSetCastIndexToValueFun},
					Args: []*plan.Expr{makePlan2StringConstExprWithType(setType.Enumvalues)},
				}},
			}},
		},
		{
			name: "display wrapper with non SET storage",
			args: []*plan.Expr{search, {
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: moSetCastIndexToValueFun},
					Args: []*plan.Expr{
						makePlan2StringConstExprWithType(setType.Enumvalues),
						makePlan2StringConstExprWithType("a"),
					},
				}},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, rewritten := rewriteFindInSetStoredOperand(tc.args)
			require.False(t, rewritten)
			require.Equal(t, tc.args, got)
		})
	}

	rewritten, ok := rewriteFindInSetStoredOperand([]*plan.Expr{search, rawSet})
	require.True(t, ok)
	require.Len(t, rewritten, 3)
	require.Equal(t, int32(types.T_uint64), rewritten[1].Typ.Id)
	require.Empty(t, rewritten[1].Typ.Enumvalues)
	require.Equal(t, setType.Enumvalues, rewritten[2].GetLit().GetSval())
	require.Equal(t, setType.Enumvalues, rawSet.Typ.Enumvalues)
}

func TestFindInSetSetProvenanceThroughBindingBoundary(t *testing.T) {
	ctx := context.Background()
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "z,a,m"}
	bindCtx := NewBindContext(nil, nil)
	bindCtx.bindingByTag[7] = &Binding{
		mysqlSpecialOrderTypes: []*plan.Type{&setType},
	}
	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 7, ColPos: 0}},
	}
	args := []*plan.Expr{makePlan2StringConstExprWithType("a"), column}

	rewritten, ok, err := rewriteFindInSetSetProvenance(ctx, nil, bindCtx, args)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rewritten, 3)
	require.Equal(t, moSetCastValueToIndexFun, rewritten[1].GetF().GetFunc().GetObjName())
	require.Empty(t, rewritten[1].Typ.Enumvalues)
	require.Equal(t, setType.Enumvalues, rewritten[2].GetLit().GetSval())

	unchanged, didRewrite, err := rewriteFindInSetSetProvenance(ctx, nil, nil, args)
	require.NoError(t, err)
	require.False(t, didRewrite)
	require.Equal(t, args, unchanged)

	ordinaryContext := NewBindContext(nil, nil)
	unchanged, didRewrite, err = rewriteFindInSetSetProvenance(ctx, nil, ordinaryContext, args)
	require.NoError(t, err)
	require.False(t, didRewrite)
	require.Equal(t, args, unchanged)
}

func TestFindInSetInternalArityIsPlannerOnly(t *testing.T) {
	ctx := context.Background()
	internalArgs := []*plan.Expr{
		makePlan2StringConstExprWithType("a"),
		makePlan2Uint64ConstExprWithType(2),
		makePlan2StringConstExprWithType("z,a,m"),
	}

	_, err := BindFuncExprImplByPlanExpr(ctx, "find_in_set", internalArgs)
	require.Error(t, err)

	bound, err := bindFuncExprImplByPlanExpr(ctx, "find_in_set", internalArgs, true, nil, nil, true)
	require.NoError(t, err)
	require.Len(t, bound.GetF().GetArgs(), 3)

	bound, err = bindBoundFuncExprAndConstFoldWithInternalFunctionArgs(ctx, nil, "find_in_set", internalArgs)
	require.NoError(t, err)
	require.Len(t, bound.GetF().GetArgs(), 3)

	_, err = BindFuncExprImplByPlanExpr(ctx, "find_in_set", []*plan.Expr{internalArgs[0]})
	require.Error(t, err)
}

func TestFindInSetPlannerPreservesSetContractAcrossQueryBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		def      string
		wantType types.T
	}{
		{name: "direct", sql: "select find_in_set('a', s) from enum_order_t", def: "red,green,blue", wantType: types.T_uint64},
		{name: "derived", sql: "select find_in_set('a', s) from (select s from enum_order_t) d", def: "red,green,blue", wantType: types.T_uint64},
		{name: "derived empty member", sql: "select find_in_set('', s) from (select s from set_empty_member_t) d", def: ",a", wantType: types.T_uint64},
		{name: "cte empty member", sql: "with c as (select s from set_empty_member_t) select find_in_set('', s) from c", def: ",a", wantType: types.T_uint64},
		{name: "union empty member", sql: "select find_in_set('', s) from (select s from set_empty_member_t union all select s from set_empty_member_t) d", def: ",a", wantType: types.T_uint64},
		{name: "union empty member then null", sql: "select find_in_set('', s) from (select s from set_empty_member_t union all select null as s) d", def: ",a", wantType: types.T_uint64},
		{name: "union null then empty member", sql: "select find_in_set('', s) from (select null as s union all select s from set_empty_member_t) d", def: ",a", wantType: types.T_uint64},
		{name: "ordered derived empty member", sql: "select find_in_set('', s) from (select s from set_empty_member_t order by s) d", def: ",a", wantType: types.T_uint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneExprStmt(newMySQLSpecialOrderMock(), t, tc.sql)
			require.NoError(t, err)
			findInSet := findPlanFunctionExpr(logicPlan, "find_in_set")
			require.NotNil(t, findInSet, logicPlan.String())
			require.Len(t, findInSet.GetF().GetArgs(), 3)
			require.Equal(t, int32(tc.wantType), findInSet.GetF().GetArgs()[1].Typ.Id)
			require.Empty(t, findInSet.GetF().GetArgs()[1].Typ.Enumvalues)
			require.Equal(t, tc.def, findInSet.GetF().GetArgs()[2].GetLit().GetSval())
			if strings.Contains(tc.name, "union") && strings.Contains(tc.name, "null") {
				require.NotNil(t, findInSet.GetF().GetArgs()[1].GetCol(), logicPlan.String())
				require.False(t, findInSet.GetF().GetArgs()[1].Typ.NotNullable)
			}
			if tc.name == "derived empty member" {
				raw := findInSet.GetF().GetArgs()[1].GetCol()
				require.NotNil(t, raw)
				require.Equal(t, int32(0), raw.ColPos)
			}
		})
	}
}

// TestGeomFromTextSRIDInResultType verifies that a constant SRID argument to
// ST_GeomFromText lands in the result type's Width (since geometry cells store
// bare WKB and SRID lives in the type).
func TestGeomFromTextSRIDInResultType(t *testing.T) {
	ctx := context.Background()
	wktArg := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "POINT(1 2)"}}},
	}
	sridArg := makePlan2Int64ConstExprWithType(4326)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*plan.Expr{wktArg, sridArg})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), expr.Typ.Id)
	srid, defined := decodeGeometrySRIDWidth(expr.Typ.Width)
	require.True(t, defined)
	require.Equal(t, uint32(4326), srid)

	// Without an SRID argument, the result type carries no SRID.
	expr2, err := BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*plan.Expr{wktArg})
	require.NoError(t, err)
	_, defined2 := decodeGeometrySRIDWidth(expr2.Typ.Width)
	require.False(t, defined2)
}

func TestGeometrySRIDOverloadsAndPreparedMetadata(t *testing.T) {
	ctx := context.Background()
	wkbArg := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "wkb"}}},
	}
	geometryArg := &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_geometry32),
			Scale: 1,
			Width: encodeGeometrySRIDWidth(4326, true),
		},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "POINT(1 2)"}}},
	}

	for _, name := range []string{"st_geomfromwkb", "st_geometryfromwkb"} {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*plan.Expr{
			wkbArg,
			makePlan2Int64ConstExprWithType(3857),
		})
		require.NoError(t, err, name)
		srid, defined := decodeGeometrySRIDWidth(expr.Typ.Width)
		require.True(t, defined, name)
		require.Equal(t, uint32(3857), srid, name)
	}

	setter, err := BindFuncExprImplByPlanExpr(ctx, "st_srid", []*plan.Expr{
		geometryArg,
		makePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry32), setter.Typ.Id)
	require.Equal(t, int32(1), setter.Typ.Scale)
	srid, defined := decodeGeometrySRIDWidth(setter.Typ.Width)
	require.True(t, defined)
	require.Zero(t, srid)

	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	prepared, err := BindFuncExprImplByPlanExpr(ctx, "st_srid", []*plan.Expr{
		geometryArg,
		param,
	})
	require.NoError(t, err)
	_, defined = decodeGeometrySRIDWidth(prepared.Typ.Width)
	require.False(t, defined)

	rule := NewResetParamRefRule(ctx, []*plan.Expr{param})
	rule.SetParamValues([]any{ParamValue{
		Value:          int64(4326),
		RuntimeType:    types.T_int64.ToType(),
		HasRuntimeType: true,
	}})
	rebound, err := rule.ApplyExpr(DeepCopyExpr(prepared))
	require.NoError(t, err)
	srid, defined = decodeGeometrySRIDWidth(rebound.Typ.Width)
	require.True(t, defined)
	require.Equal(t, uint32(4326), srid)

	rule.SetParamValues([]any{ParamValue{
		RuntimeType:    types.T_int64.ToType(),
		HasRuntimeType: true,
	}})
	nullRebound, err := rule.ApplyExpr(DeepCopyExpr(prepared))
	require.NoError(t, err)
	_, defined = decodeGeometrySRIDWidth(nullRebound.Typ.Width)
	require.False(t, defined)

	rule.SetParamValues([]any{ParamValue{
		Value:          int64(-1),
		RuntimeType:    types.T_int64.ToType(),
		HasRuntimeType: true,
	}})
	_, err = rule.ApplyExpr(DeepCopyExpr(prepared))
	require.Error(t, err)
}

func TestPreparedGeometrySRIDPlanIsValueSpecialized(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select st_srid(st_geomfromtext('POINT(1 2)'), ?)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	prepared, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx, prepared))
	fn := findPlanFunctionExpr(prepared, "st_srid")
	require.NotNil(t, fn)
	require.Len(t, fn.GetF().Args, 2)
	_, hasParam := preparedParamPosition(fn.GetF().Args[1])
	require.True(t, hasParam)
	_, defined := decodeGeometrySRIDWidth(fn.Typ.Width)
	require.False(t, defined)
	require.Equal(t, []int32{0}, PreparedPlanGeometrySRIDParamPositions(prepared))
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepared))

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, prepared,
		[]any{ParamValue{Value: int64(4326), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}})
	require.NoError(t, err)
	require.True(t, specialized)
	filledFn := findPlanFunctionExpr(filled, "st_srid")
	require.NotNil(t, filledFn)
	srid, defined := decodeGeometrySRIDWidth(filledFn.Typ.Width)
	require.True(t, defined)
	require.Equal(t, uint32(4326), srid)

	key := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{Value: int64(4326), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
	})
	otherKey := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{Value: int64(3857), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
	})
	require.NotEmpty(t, key)
	require.NotEqual(t, key, otherKey)
	nullKey := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
	})
	sentinelKey := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{Value: "<null>", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
	})
	require.NotEqual(t, nullKey, sentinelKey,
		"a typed NULL must not alias a user value equal to the old NULL sentinel")
}

func TestPreparedGeometrySRIDSemanticKeyTracksFixedSRIDSource(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select st_geomfromwkb(?, 4326)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	prepared, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx, prepared))
	require.Empty(t, PreparedPlanGeometrySRIDParamPositions(prepared),
		"the fixed SRID literal is not itself a runtime parameter")

	nullKey := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{RuntimeType: types.T_blob.ToType(), HasRuntimeType: true},
	})
	valueKey := PreparedPlanGeometrySRIDSemanticKey(prepared, []any{
		ParamValue{Value: "wkb", RuntimeType: types.T_blob.ToType(), HasRuntimeType: true},
	})
	require.NotEmpty(t, nullKey)
	require.NotEqual(t, nullKey, valueKey,
		"a fixed SRID still needs source NULL state in the runtime cache key")
}

// TestFuncCastForGeometrySRID verifies that SRID compatibility is enforced at
// bind time from the value/column types.
func TestFuncCastForGeometrySRID(t *testing.T) {
	ctx := context.Background()
	mkGeom := func(srid uint32, defined bool) *plan.Expr {
		return &plan.Expr{Typ: *geometryPlanType(types.T_geometry, "POINT", srid, defined)}
	}

	// Matching SRID is accepted.
	col4326 := *geometryPlanType(types.T_geometry, "POINT", 4326, true)
	_, err := funcCastForGeometryType(ctx, mkGeom(4326, true), col4326)
	require.NoError(t, err)

	// Mismatched SRID is rejected.
	_, err = funcCastForGeometryType(ctx, mkGeom(0, true), col4326)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SRID of the geometry does not match")

	// A value with no SRID into a SRID-constrained column is rejected.
	_, err = funcCastForGeometryType(ctx, mkGeom(0, false), col4326)
	require.Error(t, err)

	// An unconstrained (no-SRID) column accepts any SRID.
	colAny := *geometryPlanType(types.T_geometry, "POINT", 0, false)
	_, err = funcCastForGeometryType(ctx, mkGeom(4326, true), colAny)
	require.NoError(t, err)
}

func geometryPlanType(id types.T, subtype string, srid uint32, sridDefined bool) *plan.Type {
	return &plan.Type{
		Id:    int32(id),
		Scale: int32(geometrySubtypeEnum(subtype)),
		Width: encodeGeometrySRIDWidth(srid, sridDefined),
	}
}

func TestGeometryPlanTypeHelpers(t *testing.T) {
	// Subtype is read from Scale, SRID from Width.
	typ := geometryPlanType(types.T_geometry, "POINT", 0, false)
	require.True(t, isGeometryPlanType(typ))
	require.Equal(t, "POINT", geometrySubtypeName(typ))
	_, ok := geometrySRIDValue(typ)
	require.False(t, ok) // SRID not defined

	// GENERIC geometry has no subtype name.
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry)}))
	// Non-geometry types yield nothing.
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_varchar)}))
	// T_geometry32 is also a geometry plan type.
	require.True(t, isGeometryPlanType(geometryPlanType(types.T_geometry32, "POINT", 0, false)))

	typ = geometryPlanType(types.T_geometry, "POINT", 4326, true)
	require.Equal(t, "POINT", geometrySubtypeName(typ))
	srid, ok := geometrySRIDValue(typ)
	require.True(t, ok)
	require.Equal(t, uint32(4326), srid)

	require.Equal(t, "POINT;SRID=4326", geometryMetadataString("POINT", 4326, true))
	require.Equal(t, "SRID=0", geometryMetadataString("", 0, true))
}

func TestGeometrySubtypeEnumRoundTrip(t *testing.T) {
	for _, name := range []string{"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"} {
		require.Equal(t, name, geometrySubtypeNameFromEnum(geometrySubtypeEnum(name)), name)
	}
	// GENERIC / GEOMETRY map to the empty (no-constraint) name.
	require.Equal(t, "", geometrySubtypeNameFromEnum(geometrySubtypeEnum("GEOMETRY")))
	require.Equal(t, "", geometrySubtypeNameFromEnum(geometrySubtypeEnum("")))
}

func TestGeometrySRIDWidthEncoding(t *testing.T) {
	// Undefined SRID encodes to 0 and decodes back to (0, false).
	srid, ok := decodeGeometrySRIDWidth(encodeGeometrySRIDWidth(0, false))
	require.False(t, ok)
	require.Equal(t, uint32(0), srid)

	// Defined SRID 0 is distinct from undefined.
	w0 := encodeGeometrySRIDWidth(0, true)
	require.Equal(t, int32(1), w0)
	srid, ok = decodeGeometrySRIDWidth(w0)
	require.True(t, ok)
	require.Equal(t, uint32(0), srid)

	// Defined SRID 4326 round-trips.
	srid, ok = decodeGeometrySRIDWidth(encodeGeometrySRIDWidth(4326, true))
	require.True(t, ok)
	require.Equal(t, uint32(4326), srid)
}

func TestGeometrySRIDLiteralValueCoversPlanIntegerDomains(t *testing.T) {
	tests := []struct {
		name string
		lit  *plan.Literal
		want int64
	}{
		{name: "i8", lit: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: -8}}, want: -8},
		{name: "i16", lit: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: -16}}, want: -16},
		{name: "i32", lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: -32}}, want: -32},
		{name: "i64", lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -64}}, want: -64},
		{name: "u8", lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 8}}, want: 8},
		{name: "u16", lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 16}}, want: 16},
		{name: "u32", lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 32}}, want: 32},
		{name: "u64", lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 64}}, want: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, isNull, ok := geometrySRIDLiteralValue(test.lit)
			require.True(t, ok)
			require.False(t, isNull)
			require.Equal(t, test.want, got)
		})
	}

	got, isNull, ok := geometrySRIDLiteralValue(nil)
	require.False(t, ok)
	require.False(t, isNull)
	require.Zero(t, got)
	got, isNull, ok = geometrySRIDLiteralValue(&plan.Literal{Isnull: true})
	require.True(t, ok)
	require.True(t, isNull)
	require.Zero(t, got)
	got, isNull, ok = geometrySRIDLiteralValue(&plan.Literal{Value: &plan.Literal_Sval{Sval: "4326"}})
	require.False(t, ok)
	require.False(t, isNull)
	require.Zero(t, got)
}

func TestGeometrySRIDRuntimeValueValidation(t *testing.T) {
	valid := []struct {
		name  string
		value any
		want  uint32
	}{
		{name: "nil", value: nil, want: 0},
		{name: "int8", value: int8(8), want: 8},
		{name: "int16", value: int16(16), want: 16},
		{name: "int32", value: int32(32), want: 32},
		{name: "int64", value: int64(64), want: 64},
		{name: "uint8", value: uint8(8), want: 8},
		{name: "uint16", value: uint16(16), want: 16},
		{name: "uint32", value: uint32(32), want: 32},
		{name: "uint64", value: uint64(64), want: 64},
		{name: "string", value: " 4326 ", want: 4326},
		{name: "bytes", value: []byte("4326"), want: 4326},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			got, isNull, err := geometrySRIDRuntimeValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.Equal(t, test.value == nil, isNull)
		})
	}

	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "negative int8", value: int8(-1)},
		{name: "negative int16", value: int16(-1)},
		{name: "negative int32", value: int32(-1)},
		{name: "negative int64", value: int64(-1)},
		{name: "negative text", value: "-1"},
		{name: "empty text", value: ""},
		{name: "invalid text", value: "4326.0"},
		{name: "oversized text", value: "2147483647"},
		{name: "oversized uint", value: uint64(geo.MaxSRID) + 1},
		{name: "fraction", value: 4326.0},
		{name: "boolean", value: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, isNull, err := geometrySRIDRuntimeValue(test.value)
			require.False(t, isNull)
			require.Error(t, err)
		})
	}
}

func TestGeometrySubtypeCompatible(t *testing.T) {
	require.True(t, geometrySubtypeCompatible("", "POINT"))
	require.True(t, geometrySubtypeCompatible("GEOMETRY", "POINT"))
	require.True(t, geometrySubtypeCompatible("POINT", "GEOMETRY"))
	require.True(t, geometrySubtypeCompatible("POINT", "POINT"))
	require.False(t, geometrySubtypeCompatible("POINT", ""))
	require.False(t, geometrySubtypeCompatible("POINT", "LINESTRING"))
}

func TestFuncCastForGeometryTypeNull(t *testing.T) {
	target := *geometryPlanType(types.T_geometry, "GEOMETRY", 0, false)
	for _, expr := range []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_any)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Isnull: true},
			},
		},
		{
			Typ: plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Isnull: true},
			},
		},
	} {
		casted, err := funcCastForGeometryType(context.Background(), expr, target)
		require.NoError(t, err)
		require.Equal(t, target.Id, casted.Typ.Id)
		require.Equal(t, target.Enumvalues, casted.Typ.Enumvalues)
		lit, ok := casted.Expr.(*plan.Expr_Lit)
		require.True(t, ok)
		require.True(t, lit.Lit.Isnull)
	}
}

func mockGeometryPreparedDMLPlan(t *testing.T, sql string, srid uint32, sridDefined bool) *plan.Plan {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	table := ctx.tables["emp"]
	for _, col := range table.Cols {
		if col.Name == "sal" {
			col.Typ = *geometryPlanType(types.T_geometry, "POINT", srid, sridDefined)
		}
	}
	ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx.GetContext(), p))
	return p
}

func mockInetNtoaPreparedDMLPlan(t *testing.T, sql string) *plan.Plan {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	table := ctx.tables["emp"]
	for _, col := range table.Cols {
		if col.Name == "sal" {
			col.Typ = plan.Type{Id: int32(types.T_varchar), Width: 31}
		}
	}
	ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx.GetContext(), p))
	return p
}

func findPlanFunctionExprInDMLWriteData(queryPlan *plan.Plan, name string) *plan.Expr {
	var find func(*plan.Expr) *plan.Expr
	find = func(expr *plan.Expr) *plan.Expr {
		if expr == nil {
			return nil
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func != nil && fn.Func.GetObjName() == name {
				return expr
			}
			for _, arg := range fn.Args {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		return nil
	}

	if query := queryPlan.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			if node == nil || node.RowsetData == nil {
				continue
			}
			for _, col := range node.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						if found := find(row.Expr); found != nil {
							return found
						}
					}
				}
			}
		}
	}
	return nil
}

func TestPreparedInetNtoaDMLWriteRebindsWithinPreservedRoot(t *testing.T) {
	for _, test := range []struct {
		name           string
		isBinary       bool
		wantSourceType types.Type
	}{
		{name: "SQL EXECUTE source", wantSourceType: types.T_json.ToType()},
		{name: "COM_STMT source", isBinary: true, wantSourceType: types.T_json.ToType()},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := mockInetNtoaPreparedDMLPlan(t,
				"insert into constraint_test.emp (sal) values (inet_ntoa(?))")
			original := proto.Clone(prepared).(*plan.Plan)

			filled, specialized, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
				context.Background(), prepared, []any{ParamValue{
					Value: "1.6", IsBinaryProtocol: test.isBinary,
					InetNtoaSourceType: test.wantSourceType, HasInetNtoaSourceType: true,
				}})
			require.NoError(t, err)
			require.True(t, specialized)
			inetNtoa := findPlanFunctionExprInDMLWriteData(filled, "inet_ntoa")
			require.NotNil(t, inetNtoa, filled.String())
			_, overloadID := planfunction.DecodeOverloadID(inetNtoa.GetF().GetFunc().GetObj())
			require.Equal(t, int32(15), overloadID, inetNtoa.String())
			require.Equal(t, types.T_json, types.T(inetNtoa.GetF().GetArgs()[0].Typ.Id))
			require.Equal(t, types.T_varchar, types.T(inetNtoa.Typ.Id))
			require.Equal(t, int32(31), inetNtoa.Typ.Width)
			require.True(t, proto.Equal(original, prepared),
				"execute-time specialization must not mutate the cached DML plan")
		})
	}
}

func preparedGeometryDMLWriteExpr(p *plan.Plan) *plan.Expr {
	var result *plan.Expr
	_ = plan.VisitExpressionsInOwner(p, func(expr *plan.Expr) error {
		if result != nil {
			return nil
		}
		if fn := expr.GetF(); fn != nil && fn.Func != nil &&
			strings.EqualFold(fn.Func.GetObjName(), moGeometryCastToSubtypeFun) {
			result = expr
		}
		return nil
	})
	return result
}

func TestPreparedGeometrySRIDDMLAssignmentRevalidatesAtExecute(t *testing.T) {
	for _, sql := range []string{
		"insert into constraint_test.emp (sal) values (st_srid(st_geomfromtext('POINT(1 2)'), ?))",
		"update constraint_test.emp set sal = st_srid(st_geomfromtext('POINT(1 2)'), ?) where empno = 1",
	} {
		t.Run(strings.Split(sql, " ")[0], func(t *testing.T) {
			prepared := mockGeometryPreparedDMLPlan(t, sql, 4326, true)
			original := proto.Clone(prepared).(*plan.Plan)
			require.True(t, proto.Equal(original, prepared), "snapshot must preserve the prepared DML plan")

			matching, specialized, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
				context.Background(), prepared, []any{ParamValue{
					Value: int64(4326), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true,
				}})
			require.NoError(t, err)
			require.True(t, specialized)
			root := preparedGeometryDMLWriteExpr(matching)
			require.NotNil(t, root, matching.String())
			srid, defined := decodeGeometrySRIDWidth(root.Typ.Width)
			require.True(t, defined)
			require.Equal(t, uint32(4326), srid)
			require.True(t, proto.Equal(original, prepared),
				"successful specialization must not mutate the cached DML plan")

			_, _, err = FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
				context.Background(), prepared, []any{ParamValue{
					Value: int64(3857), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true,
				}})
			require.Error(t, err, "a constrained target must reject an execute-time SRID mismatch")
			require.True(t, proto.Equal(original, prepared),
				"execute-time specialization must not mutate the cached DML plan")
		})
	}

	for _, sql := range []string{
		"insert into constraint_test.emp (sal) values (st_srid(st_geomfromtext('POINT(1 2)'), ?))",
		"update constraint_test.emp set sal = st_srid(st_geomfromtext('POINT(1 2)'), ?) where empno = 1",
		"insert into constraint_test.emp (sal) values (st_geomfromwkb(st_aswkb(st_geomfromtext('POINT(1 2)')), ?))",
		"update constraint_test.emp set sal = st_geomfromwkb(st_aswkb(st_geomfromtext('POINT(1 2)')), ?) where empno = 1",
	} {
		t.Run("null/"+strings.Split(sql, " ")[0]+"/"+strings.Split(sql, "(")[0], func(t *testing.T) {
			prepared := mockGeometryPreparedDMLPlan(t, sql, 4326, true)
			_, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
				context.Background(), prepared, []any{ParamValue{Value: nil}})
			require.NoError(t, err, "a NULL setter/constructor result is valid for a nullable constrained target")
		})
	}

	// SRID 0 is a defined constraint, distinct from an unconstrained geometry.
	prepared := mockGeometryPreparedDMLPlan(t,
		"insert into constraint_test.emp (sal) values (st_srid(st_geomfromtext('POINT(1 2)'), ?))",
		0, true)
	_, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
		context.Background(), prepared, []any{ParamValue{
			Value: int64(4326), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true,
		}})
	require.Error(t, err, "SRID 0 must not be treated as an unconstrained target")
}

func TestGeometrySRIDNullShortCircuit(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   string
		arg  *plan.Expr
	}{
		{
			name: "setter",
			fn:   "st_srid",
			arg:  &plan.Expr{Typ: *geometryPlanType(types.T_geometry, "POINT", 0, false), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
		},
		{
			name: "wkb constructor",
			fn:   "st_geomfromwkb",
			arg:  &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), test.fn, []*plan.Expr{
				test.arg,
				makePlan2Int64ConstExprWithType(-1),
			})
			require.NoError(t, err)
			_, defined := decodeGeometrySRIDWidth(expr.Typ.Width)
			require.False(t, defined)
		})
	}

	target := *geometryPlanType(types.T_geometry, "POINT", 4326, true)
	for _, name := range []string{"st_srid", "st_geomfromwkb"} {
		t.Run("assignment/"+name, func(t *testing.T) {
			argType := types.T_varchar
			if name == "st_srid" {
				argType = types.T_geometry
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*plan.Expr{
				{Typ: plan.Type{Id: int32(argType)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
				makePlan2Int64ConstExprWithType(-1),
			})
			require.NoError(t, err)
			_, err = funcCastForGeometryType(context.Background(), expr, target)
			require.NoError(t, err, "NULL result must short-circuit SRID mismatch validation")
		})
	}
}
