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

	require.True(t, mysqlSpecialOrderTypeReversible(enum))
	require.False(t, mysqlSpecialOrderTypeReversible(duplicateEnum))
	require.True(t, mysqlSpecialOrderTypeReversible(set))
	require.False(t, mysqlSpecialOrderTypeReversible(ambiguousSet))
	require.False(t, mysqlSpecialOrderTypeReversible(&plan.Type{Id: int32(types.T_varchar)}))
	require.Equal(t, enumFoldKey("K"), enumFoldKey("K"))
	require.True(t, mysqlSpecialOrderTypesCompatible(enum, DeepCopyType(enum)))
	require.False(t, mysqlSpecialOrderTypesCompatible(enum, set))
	require.Error(t, newNonReversibleMySQLSpecialOrderError(context.Background()))
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
