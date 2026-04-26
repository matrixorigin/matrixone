// Copyright 2024 Matrix Origin
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func Test_replaceFuncId(t *testing.T) {
	case1 := &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{
					ObjName: "current_timestamp",
					Obj:     function.CURRENT_TIMESTAMP,
				},
				Args: []*Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 10,
								Name:   "a",
							},
						},
					},
				},
			},
		},
	}

	err := replaceFuncId(context.Background(), case1)
	assert.NoError(t, err)

	case1ColDef := &plan.ColDef{
		Default: &plan.Default{
			Expr: case1,
		},
	}
	case1Expr, err := getDefaultExpr(context.Background(), case1ColDef)
	assert.NoError(t, err)
	assert.NotNil(t, case1Expr)
}

func TestGetTypeFromAstMySQLCompatibilityTypes(t *testing.T) {
	ctx := context.Background()

	yearType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid: uint32(defines.MYSQL_TYPE_YEAR),
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_year), yearType.Id)
	require.Equal(t, int32(4), yearType.Width)

	decimalType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:         uint32(defines.MYSQL_TYPE_DECIMAL),
		DisplayWith: 65,
		Scale:       30,
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), decimalType.Id)
	require.Equal(t, int32(65), decimalType.Width)
	require.Equal(t, int32(30), decimalType.Scale)

	setType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:        uint32(defines.MYSQL_TYPE_SET),
		EnumValues: []string{"read", "write", "execute"},
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), setType.Id)
	require.Equal(t, "read,write,execute", setType.Enumvalues)
	require.Equal(t, "SET('read','write','execute')", FormatColType(setType))

	geometryType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:          uint32(defines.MYSQL_TYPE_GEOMETRY),
		FamilyString: "geometry",
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), geometryType.Id)
	require.Equal(t, "GEOMETRY", FormatColType(geometryType))
}

func TestMakePlan2DecimalExprWithTypeUsesDecimal256(t *testing.T) {
	expr, err := makePlan2DecimalExprWithType(context.Background(), "123456789012345678901234567890123456789")
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(0), expr.Typ.Scale)
}
