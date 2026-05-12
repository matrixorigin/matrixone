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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIsGeometryNullLiteralExpr(t *testing.T) {
	require.False(t, isGeometryNullLiteralExpr(nil))

	nonNull := &Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "POINT(1 2)"}},
		},
	}
	require.False(t, isGeometryNullLiteralExpr(nonNull))

	nullExpr := &Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Isnull: true},
		},
	}
	require.True(t, isGeometryNullLiteralExpr(nullExpr))

	colExpr := &Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	require.False(t, isGeometryNullLiteralExpr(colExpr))
}

func TestFuncCastForGeometryType(t *testing.T) {
	ctx := context.Background()

	t.Run("non-geometry target returns expr unchanged", func(t *testing.T) {
		expr := &Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "hello"}},
			},
		}
		target := plan.Type{Id: int32(types.T_varchar)}
		result, err := funcCastForGeometryType(ctx, expr, target)
		require.NoError(t, err)
		require.Equal(t, expr, result)
	})

	t.Run("T_any expr gets target type assigned", func(t *testing.T) {
		expr := &Expr{
			Typ: plan.Type{Id: int32(types.T_any)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: ""}},
			},
		}
		target := plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}
		result, err := funcCastForGeometryType(ctx, expr, target)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_geometry), result.Typ.Id)
	})

	t.Run("null literal expr gets target type assigned", func(t *testing.T) {
		expr := &Expr{
			Typ: plan.Type{Id: int32(types.T_geometry)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Isnull: true},
			},
		}
		target := plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}
		result, err := funcCastForGeometryType(ctx, expr, target)
		require.NoError(t, err)
		require.Equal(t, "POINT", result.Typ.Enumvalues)
	})

	t.Run("same geometry subtype skips cast", func(t *testing.T) {
		expr := &Expr{
			Typ: plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: 0},
			},
		}
		target := plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}
		result, err := funcCastForGeometryType(ctx, expr, target)
		require.NoError(t, err)
		require.Equal(t, "POINT", result.Typ.Enumvalues)
	})

	t.Run("different geometry subtype generates cast function", func(t *testing.T) {
		expr := &Expr{
			Typ: plan.Type{Id: int32(types.T_geometry), Enumvalues: ""},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: 0},
			},
		}
		target := plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}
		result, err := funcCastForGeometryType(ctx, expr, target)
		require.NoError(t, err)
		require.NotNil(t, result)
		fn := result.GetF()
		require.NotNil(t, fn)
		require.Equal(t, "cast_geometry_to_subtype", fn.Func.ObjName)
	})
}

func TestIsGeometryPlanType(t *testing.T) {
	require.False(t, isGeometryPlanType(nil))

	varcharType := &plan.Type{Id: int32(types.T_varchar)}
	require.False(t, isGeometryPlanType(varcharType))

	geomType := &plan.Type{Id: int32(types.T_geometry)}
	require.True(t, isGeometryPlanType(geomType))
}

func TestGeometrySubtypeName(t *testing.T) {
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_varchar)}))
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry), Enumvalues: ""}))
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry), Enumvalues: "GEOMETRY"}))
	require.Equal(t, "POINT", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}))
	require.Equal(t, "LINESTRING", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry), Enumvalues: "linestring"}))
}

func TestNormalizeGeometrySubtype(t *testing.T) {
	require.Equal(t, "POINT", normalizeGeometrySubtype("point"))
	require.Equal(t, "LINESTRING", normalizeGeometrySubtype("LINESTRING"))
	require.Equal(t, "POLYGON", normalizeGeometrySubtype("Polygon"))
	require.Equal(t, "MULTIPOINT", normalizeGeometrySubtype("multipoint"))
	require.Equal(t, "MULTILINESTRING", normalizeGeometrySubtype("MULTILINESTRING"))
	require.Equal(t, "MULTIPOLYGON", normalizeGeometrySubtype("MultiPolygon"))
	require.Equal(t, "GEOMETRYCOLLECTION", normalizeGeometrySubtype("geometrycollection"))
	require.Equal(t, "", normalizeGeometrySubtype("GEOMETRY"))
	require.Equal(t, "", normalizeGeometrySubtype(""))
	require.Equal(t, "", normalizeGeometrySubtype("INVALID"))
}
