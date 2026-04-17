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

func TestGeometryPlanTypeHelpers(t *testing.T) {
	typ := &plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT"}
	require.True(t, isGeometryPlanType(typ))
	require.Equal(t, "POINT", geometrySubtypeName(typ))
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_geometry)}))
	require.Equal(t, "", geometrySubtypeName(&plan.Type{Id: int32(types.T_varchar)}))

	typ = &plan.Type{Id: int32(types.T_geometry), Enumvalues: "POINT;SRID=4326"}
	require.Equal(t, "POINT", geometrySubtypeName(typ))
	srid, ok := geometrySRIDValue(typ)
	require.True(t, ok)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, "POINT;SRID=4326", geometryMetadataString("POINT", 4326, true))
	require.Equal(t, "SRID=0", geometryMetadataString("", 0, true))
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
	target := plan.Type{Id: int32(types.T_geometry), Enumvalues: "GEOMETRY"}
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
