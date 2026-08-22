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
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExplicitCastUsesDedicatedOverload(t *testing.T) {
	ctx := context.Background()
	source := makePlan2StringConstExprWithType("1")
	targetType := types.T_int64.ToType()
	target := makePlan2TypeValue(&targetType)

	ordinary, err := appendCastBeforeExpr(ctx, DeepCopyExpr(source), target)
	require.NoError(t, err)
	explicit, err := appendExplicitCastBeforeExpr(ctx, DeepCopyExpr(source), target)
	require.NoError(t, err)
	fixed, err := appendFixedCastBeforeExpr(ctx, DeepCopyExpr(source), target)
	require.NoError(t, err)

	ordinaryFunction := ordinary.GetF().GetFunc()
	explicitFunction := explicit.GetF().GetFunc()
	fixedFunction := fixed.GetF().GetFunc()
	require.Equal(t, "cast", ordinaryFunction.GetObjName())
	require.Equal(t, "cast", explicitFunction.GetObjName())
	require.Equal(t, "cast", fixedFunction.GetObjName())
	_, ordinaryOverload := function.DecodeOverloadID(ordinaryFunction.GetObj())
	_, explicitOverload := function.DecodeOverloadID(explicitFunction.GetObj())
	_, fixedOverload := function.DecodeOverloadID(fixedFunction.GetObj())
	require.Equal(t, int32(0), ordinaryOverload)
	require.Equal(t, int32(1), explicitOverload)
	require.Equal(t, int32(2), fixedOverload)
}

func collectCastOverloads(expr *planpb.Expr, overloads map[int32]struct{}) {
	if expr == nil {
		return
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == "cast" {
			_, overload := function.DecodeOverloadID(fn.Func.GetObj())
			overloads[overload] = struct{}{}
		}
		for _, arg := range fn.Args {
			collectCastOverloads(arg, overloads)
		}
	}
}

func TestFixedCastProvenanceIsPreparedOnly(t *testing.T) {
	ordinary, err := runOneStmt(NewMockOptimizer(false), t,
		"select cast(n_nationkey as bigint) + 1 from nation")
	require.NoError(t, err)
	ordinaryOverloads := make(map[int32]struct{})
	for _, node := range ordinary.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectCastOverloads(expr, ordinaryOverloads)
		}
	}
	require.Contains(t, ordinaryOverloads, int32(0))
	require.NotContains(t, ordinaryOverloads, int32(2))

	prepared := buildPreparedAggregatePlan(t,
		"select cast(? as bigint) + 1")
	preparedOverloads := make(map[int32]struct{})
	for _, node := range prepared.Plan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectCastOverloads(expr, preparedOverloads)
		}
	}
	require.Contains(t, preparedOverloads, int32(2))

	fixedSibling := buildPreparedAggregatePlan(t,
		"select ? + cast(n_nationkey as bigint) from nation")
	fixedSiblingOverloads := make(map[int32]struct{})
	for _, node := range fixedSibling.Plan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectCastOverloads(expr, fixedSiblingOverloads)
		}
	}
	require.Contains(t, fixedSiblingOverloads, int32(2))

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared.Plan, []any{
			ParamValue{Value: 2.5, RuntimeType: types.T_float64.ToType(), HasRuntimeType: true},
		})
	require.NoError(t, err)
	// Replacing the payload below a fixed CAST does not change the plan shape,
	// so the cached parameterized plan remains reusable.
	require.False(t, specialized)
	filledOverloads := make(map[int32]struct{})
	for _, node := range filled.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectCastOverloads(expr, filledOverloads)
		}
	}
	require.Contains(t, filledOverloads, int32(2))
}

func TestExplicitCastOverloadID(t *testing.T) {
	tests := []struct {
		name string
		typ  tree.InternalType
		want int32
	}{
		{name: "signed", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONGLONG), FamilyString: "signed"}, want: 1},
		{name: "signed integer", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONGLONG), FamilyString: "integer"}, want: 1},
		{name: "unsigned", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONGLONG), Unsigned: true}, want: 1},
		{name: "decimal", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_NEWDECIMAL), FamilyString: "decimal"}, want: 1},
		{name: "char", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_STRING), FamilyString: "char"}, want: 1},
		{name: "varchar", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_VARCHAR), FamilyString: "varchar"}, want: 1},
		{name: "text", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_TEXT), FamilyString: "text"}, want: 1},
		{name: "binary", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_STRING), FamilyString: "binary"}, want: 1},
		{name: "varbinary", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_VAR_STRING), FamilyString: "varbinary"}, want: 1},
		{name: "blob", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_BLOB), FamilyString: "blob"}, want: 1},
		{name: "tinyint", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_TINY), FamilyString: "tinyint"}, want: 2},
		{name: "smallint", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_SHORT), FamilyString: "smallint"}, want: 2},
		{name: "int", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONG), FamilyString: "int"}, want: 2},
		{name: "bigint", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONGLONG), FamilyString: "bigint"}, want: 2},
		{name: "bigint unsigned", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_LONGLONG), FamilyString: "bigint", Unsigned: true}, want: 2},
		{name: "float", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_FLOAT), FamilyString: "float"}, want: 2},
		{name: "double", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_DOUBLE), FamilyString: "double"}, want: 2},
		{name: "bit", typ: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_BIT), FamilyString: "bit"}, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, explicitCastOverloadID(&tree.T{InternalType: test.typ}))
		})
	}
}
