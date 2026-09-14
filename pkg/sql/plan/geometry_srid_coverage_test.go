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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGeometrySRIDLiteralValueMatrix(t *testing.T) {
	cases := []struct {
		name    string
		literal *planpb.Literal
		want    int64
		isNull  bool
		ok      bool
	}{
		{"nil", nil, 0, false, false},
		{"null", &planpb.Literal{Isnull: true}, 0, true, true},
		{"i8", &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: -1}}, -1, false, true},
		{"i16", &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 16}}, 16, false, true},
		{"i32", &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 32}}, 32, false, true},
		{"i64", &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 64}}, 64, false, true},
		{"u8", &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 8}}, 8, false, true},
		{"u16", &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 16}}, 16, false, true},
		{"u32", &planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 32}}, 32, false, true},
		{"u64", &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 64}}, 64, false, true},
		{"unsupported", &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "4326"}}, 0, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, isNull, ok := geometrySRIDLiteralValue(tc.literal)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.isNull, isNull)
			require.Equal(t, tc.ok, ok)
		})
	}
}

func TestGeometrySRIDRuntimeValueMatrix(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		want    uint32
		isNull  bool
		wantErr bool
	}{
		{"nil", nil, 0, true, false},
		{"int8", int8(8), 8, false, false},
		{"int16", int16(16), 16, false, false},
		{"int32", int32(32), 32, false, false},
		{"int64", int64(64), 64, false, false},
		{"uint8", uint8(8), 8, false, false},
		{"uint16", uint16(16), 16, false, false},
		{"uint32", uint32(32), 32, false, false},
		{"uint64", uint64(64), 64, false, false},
		{"max", uint64(geo.MaxSRID), uint32(geo.MaxSRID), false, false},
		{"string", " 4326 ", 4326, false, false},
		{"bytes", []byte("0"), 0, false, false},
		{"negative int8", int8(-1), 0, false, true},
		{"negative int16", int16(-1), 0, false, true},
		{"negative int32", int32(-1), 0, false, true},
		{"negative int64", int64(-1), 0, false, true},
		{"negative text", "-1", 0, false, true},
		{"empty text", "", 0, false, true},
		{"fractional text", "1.5", 0, false, true},
		{"text overflow", "999999999999999999999999", 0, false, true},
		{"numeric overflow", uint64(geo.MaxSRID) + 1, 0, false, true},
		{"unsupported", true, 0, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, isNull, err := geometrySRIDRuntimeValue(tc.value)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.isNull, isNull)
		})
	}
}

func geometrySRIDFunctionExpr(name string, args ...*planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_geometry)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: args,
		}},
	}
}

func geometrySRIDParamExpr(pos int32) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
}

func TestGeometrySRIDExpressionClassification(t *testing.T) {
	param := geometrySRIDParamExpr(0)
	require.False(t, isDirectPreparedGeometrySRIDArg(nil))
	require.True(t, isDirectPreparedGeometrySRIDArg(param))

	for _, name := range []string{"st_srid", "ST_GEOMFROMWKB", "st_geomfrombinary", "st_geometryfromwkb"} {
		require.True(t, isPreparedGeometrySRIDFunction(name), name)
	}
	require.False(t, isPreparedGeometrySRIDFunction("st_geomfromtext"))

	deferred := geometrySRIDFunctionExpr("st_srid", &planpb.Expr{}, param)
	require.True(t, geometryExprHasDeferredSRID(deferred))
	require.True(t, geometryExprHasDeferredSRID(&planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{deferred}}}}))
	require.True(t, geometryExprHasDeferredSRID(&planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: deferred}}}))
	require.False(t, geometryExprHasDeferredSRID(&planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "abs"}}}}))

	null := &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	require.True(t, geometrySRIDSourceIsStaticNull(null))
	require.True(t, geometrySRIDSourceIsStaticNull(geometrySRIDFunctionExpr("cast", null)))
	require.True(t, geometrySRIDSourceIsStaticNull(geometrySRIDFunctionExpr("st_srid", null, param)))
	require.False(t, geometrySRIDSourceIsStaticNull(geometrySRIDFunctionExpr("abs", null)))
}

func TestGeometrySRIDBinderBoundaryCases(t *testing.T) {
	ctx := context.Background()
	text := func(isNull bool) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: isNull, Value: &planpb.Literal_Sval{Sval: "POINT(1 2)"}}}}
	}
	numeric := func(value int64) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}}}
	}
	nullSRID := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}

	got, err := BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*planpb.Expr{text(false), nullSRID})
	require.NoError(t, err)
	_, defined := decodeGeometrySRIDWidth(got.Typ.Width)
	require.False(t, defined)

	got, err = BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*planpb.Expr{text(true), numeric(-1)})
	require.NoError(t, err)
	_, defined = decodeGeometrySRIDWidth(got.Typ.Width)
	require.False(t, defined)

	_, err = BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*planpb.Expr{text(false), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 1.5}}}}})
	require.Error(t, err)
	setter, err := BindFuncExprImplByPlanExpr(ctx, "st_srid", []*planpb.Expr{
		{Typ: *geometryPlanType(types.T_geometry, "POINT", 0, false)}, numeric(4326),
	})
	require.NoError(t, err)
	srid, defined := decodeGeometrySRIDWidth(setter.Typ.Width)
	require.True(t, defined)
	require.Equal(t, uint32(4326), srid)

	// A prepared SRID is admitted only for the two value-specialized APIs;
	// text constructors must retain their bind-time constant contract.
	_, err = BindFuncExprImplByPlanExpr(ctx, "st_geomfromtext", []*planpb.Expr{text(false), geometrySRIDParamExpr(0)})
	require.Error(t, err)
	_, err = BindFuncExprImplByPlanExpr(ctx, "st_srid", []*planpb.Expr{{Typ: *geometryPlanType(types.T_geometry, "POINT", 0, false)}, geometrySRIDParamExpr(0)})
	require.NoError(t, err)
}

func TestGeometrySRIDNumericLimitsRemainExplicit(t *testing.T) {
	for _, srid := range []int64{0, 4326, int64(geo.MaxSRID)} {
		require.NoError(t, validateGeometrySRID(srid))
	}
	for _, srid := range []int64{-1, int64(geo.MaxSRID) + 1, math.MaxInt64} {
		require.Error(t, validateGeometrySRID(srid))
	}
}
