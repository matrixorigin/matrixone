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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func decimalArithmeticTestColumn(typ types.Type, position int32) *planpb.Expr {
	return GetColExpr(makePlan2Type(&typ), 0, position)
}

func TestDecimalArithmeticExplicitCastPreservesDeclaredDomain(t *testing.T) {
	ctx := context.Background()
	leftType := types.New(types.T_decimal128, 38, 0)
	rightType := types.New(types.T_decimal128, 38, 18)
	right, err := appendExplicitCastBeforeExpr(
		ctx, makePlan2StringConstExprWithType("0.5"), makePlan2Type(&rightType))
	require.NoError(t, err)

	result, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		decimalArithmeticTestColumn(leftType, 0), right,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), result.Typ.Id)
	require.Equal(t, int32(57), result.Typ.Width)
	require.Equal(t, int32(18), result.Typ.Scale)
	require.Equal(t, int32(38), result.GetF().Args[1].Typ.Width)
	require.Equal(t, int32(18), result.GetF().Args[1].Typ.Scale)
}

func TestDecimalArithmeticWeakStringLiteralUsesCanonicalPrecision(t *testing.T) {
	ctx := context.Background()
	peerType := types.New(types.T_decimal128, 20, 0)
	for _, test := range []struct {
		name      string
		value     string
		scale     int32
		wantWidth int32
	}{
		{name: "positive exponent", value: "1e2", wantWidth: 3},
		{name: "negative exponent", value: "1e-2", scale: 4, wantWidth: 4},
		{name: "leading and trailing zeroes", value: "0001.20", scale: 2, wantWidth: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			literalType := types.New(types.T_decimal128, 38, test.scale)
			literal, err := appendCastBeforeExpr(
				ctx, makePlan2StringConstExprWithType(test.value), makePlan2Type(&literalType))
			require.NoError(t, err)
			lookup := refineDecimalArithmeticLiteralLookupTypes(
				"+",
				[]*planpb.Expr{decimalArithmeticTestColumn(peerType, 0), literal},
				[]types.Type{peerType, literalType},
			)
			require.Equal(t, test.wantWidth, lookup[1].Width)
			require.Equal(t, test.scale, lookup[1].Scale)
		})
	}
}

func TestDecimalArithmeticSmallIntegerLiteralKeepsDecimal128(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 0)
	result, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(2),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(38), result.Typ.Width)
}

func TestDecimalMultiplyIntegerLiteralUsesRefinedCast(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 10, 2)
	result, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(8),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(2), result.Typ.Scale)

	args := result.GetF().Args
	require.Len(t, args, 2)
	require.Equal(t, int32(types.T_decimal64), args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal64), args[1].Typ.Id)
	require.Equal(t, int32(0), args[1].Typ.Scale)
	require.NotNil(t, args[1].GetF(), "the integer literal must be physically cast")
}

func TestDecimalAddIntegerLiteralRetainsDecimal128Coercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 10, 2)
	result, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(8),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(2), result.Typ.Scale)

	args := result.GetF().Args
	require.Len(t, args, 2)
	require.Equal(t, int32(types.T_decimal128), args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal128), args[1].Typ.Id)
}

func TestWideDecimalProductRemainsWideInComparison(t *testing.T) {
	ctx := context.Background()
	leftType := types.New(types.T_decimal128, 38, 0)
	rightType := types.New(types.T_decimal128, 28, 0)
	product, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(leftType, 0),
		decimalArithmeticTestColumn(rightType, 1),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), product.Typ.Id)
	require.Equal(t, int32(65), product.Typ.Width)

	comparison, err := BindFuncExprImplByPlanExpr(ctx, ">", []*planpb.Expr{
		product, decimalArithmeticTestColumn(leftType, 0),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), comparison.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal256), comparison.GetF().Args[1].Typ.Id)
}

func TestCTASPublishesDecimal256ProductBoundary(t *testing.T) {
	logicPlan, err := buildSingleStmt(NewMockOptimizer(false), t,
		"create table decimal_product_boundary as select "+
			"cast(n_nationkey as decimal(38,0)) * "+
			"cast(n_nationkey as decimal(27,0)) as product from nation")
	require.NoError(t, err)

	columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.NotEmpty(t, columns)
	require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
	require.Equal(t, int32(65), columns[0].Typ.Width)
	require.Equal(t, int32(0), columns[0].Typ.Scale)
}
