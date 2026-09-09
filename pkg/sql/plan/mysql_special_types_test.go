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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	duplicateEnum := &plan.Type{Id: int32(types.T_enum), Enumvalues: "a,A"}
	set := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,y"}
	ambiguousSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,"}
	emptyFirstSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: ",x"}
	emptyMiddleSet := &plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,,y"}

	require.True(t, mysqlSpecialOrderTypeReversible(enum))
	require.False(t, mysqlSpecialOrderTypeReversible(duplicateEnum))
	require.True(t, mysqlSpecialOrderTypeReversible(set))
	require.False(t, mysqlSpecialOrderTypeReversible(ambiguousSet))
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

	bound, err := bindFuncExprImplByPlanExpr(ctx, "find_in_set", internalArgs, true, nil, true)
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
		{name: "ordered derived empty member", sql: "select find_in_set('', s) from (select s from set_empty_member_t order by s) d", def: ",a", wantType: types.T_uint64},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneExprStmt(newMySQLSpecialOrderMock(), t, tc.sql)
			require.NoError(t, err)
			findInSet := findPlanFunctionExpr(logicPlan, "find_in_set")
			require.NotNil(t, findInSet, logicPlan.String())
			require.Len(t, findInSet.GetF().GetArgs(), 3)
			require.Equal(t, int32(tc.wantType), findInSet.GetF().GetArgs()[1].Typ.Id)
			require.Empty(t, findInSet.GetF().GetArgs()[1].Typ.Enumvalues)
			require.Equal(t, tc.def, findInSet.GetF().GetArgs()[2].GetLit().GetSval())
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
