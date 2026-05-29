// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// makeDecimal128ConstExpr creates a DECIMAL128 constant expression with the given value and scale.
func makeDecimal128ConstExpr(val string, width int32, scale int32) *plan.Expr {
	dec, _, err := types.Parse128(val)
	if err != nil {
		panic(err)
	}
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       width,
			Scale:       scale,
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(dec.B0_63),
						B: int64(dec.B64_127),
					},
				},
			},
		},
	}
}

// makeInt64ConstPlanExpr creates an int64 constant plan.Expr.
func makeInt64ConstPlanExpr(val int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_I64Val{
					I64Val: val,
				},
			},
		},
	}
}

// makeInt32ConstPlanExpr creates an int32 constant plan.Expr.
func makeInt32ConstPlanExpr(val int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_I32Val{
					I32Val: val,
				},
			},
		},
	}
}

// isCastExpr checks if an expression is a cast function wrapping.
func isCastExpr(expr *plan.Expr) bool {
	if f := expr.GetF(); f != nil {
		return f.Func.GetObjName() == "cast"
	}
	return false
}

// TestBindFuncExprImplByPlanExpr_LeadLagDefaultCast tests that the default value
// (3rd argument) of lead/lag window functions is cast to match the type of the
// value expression (1st argument). This fixes decimal scaling issues where
// e.g., default -1 with DECIMAL(10,4) is stored as raw -1 (displaying as -0.0001)
// instead of -10000 (representing -1.0000).
func TestBindFuncExprImplByPlanExpr_LeadLagDefaultCast(t *testing.T) {
	ctx := context.Background()

	// Test lead with DECIMAL value and INT default
	t.Run("lead decimal(10,4) with int default -1", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(-1)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr, "result should be a function expression")
		require.Equal(t, 3, len(funcExpr.Args), "lead should have 3 arguments")

		// The default argument should be cast to DECIMAL128
		arg2 := funcExpr.Args[2]
		require.True(t, isCastExpr(arg2), "default value should be wrapped in a cast expression")
		require.Equal(t, int32(types.T_decimal128), arg2.Typ.Id, "default type should be DECIMAL128")
		require.Equal(t, int32(4), arg2.Typ.Scale, "default scale should match value scale")
	})

	// Test lag with DECIMAL value and INT default
	t.Run("lag decimal(10,4) with int default -1", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(-1)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lag", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr, "result should be a function expression")
		require.Equal(t, 3, len(funcExpr.Args), "lag should have 3 arguments")

		arg2 := funcExpr.Args[2]
		require.True(t, isCastExpr(arg2), "default value should be wrapped in a cast expression")
		require.Equal(t, int32(types.T_decimal128), arg2.Typ.Id, "default type should be DECIMAL128")
		require.Equal(t, int32(4), arg2.Typ.Scale, "default scale should match value scale")
	})

	// Test lead with DECIMAL and default 0 (zero is unaffected by scaling, but should still be cast)
	t.Run("lead decimal(10,4) with int default 0", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(0)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.True(t, isCastExpr(arg2), "default 0 should also be cast for consistency")
		require.Equal(t, int32(types.T_decimal128), arg2.Typ.Id)
		require.Equal(t, int32(4), arg2.Typ.Scale)
	})

	// Test lead with no default value (2 args) — should not panic
	t.Run("lead decimal(10,4) with no default", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 2, len(funcExpr.Args), "lead should have 2 arguments when no default")
	})

	// Test lead with only 1 arg — should not panic
	t.Run("lead decimal(10,4) with only value", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 1, len(funcExpr.Args))
	})
}

// TestBindFuncExprImplByPlanExpr_LeadLagDefaultNoCast tests that the cast is
// NOT applied when the default value type already matches the value type.
func TestBindFuncExprImplByPlanExpr_LeadLagDefaultNoCast(t *testing.T) {
	ctx := context.Background()

	// Test lead with integer value and matching integer default
	t.Run("lead int64 with int64 default", func(t *testing.T) {
		valueExpr := makeInt64ConstPlanExpr(100)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(-1)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.False(t, isCastExpr(arg2), "default value should NOT be wrapped in a cast when types match")
		require.Equal(t, int32(types.T_int64), arg2.Typ.Id, "default type should remain int64")
	})

	// Test lag with integer value and matching integer default
	t.Run("lag int64 with int64 default", func(t *testing.T) {
		valueExpr := makeInt64ConstPlanExpr(100)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(0)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lag", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.False(t, isCastExpr(arg2), "default value should NOT be wrapped in a cast when types match")
		require.Equal(t, int32(types.T_int64), arg2.Typ.Id)
	})

	// Test lead with DECIMAL value and DECIMAL default — should not cast
	t.Run("lead decimal(10,4) with decimal default", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeDecimal128ConstExpr("-1.0000", 10, 4)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.False(t, isCastExpr(arg2), "default should NOT be cast when already DECIMAL")
		require.Equal(t, int32(types.T_decimal128), arg2.Typ.Id)
		require.Equal(t, int32(4), arg2.Typ.Scale)
	})

	// Test lead with INT32 value and INT64 default — should cast because types differ
	t.Run("lead int32 with int64 default", func(t *testing.T) {
		valueExpr := makeInt32ConstPlanExpr(100)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeInt64ConstPlanExpr(-1)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.True(t, isCastExpr(arg2), "default int64 should be cast to int32")
		require.Equal(t, int32(types.T_int32), arg2.Typ.Id)
	})
}

// TestBindFuncExprImplByPlanExpr_LeadLagDefaultCastDifferentDecimalScale tests
// that when the value and default are both DECIMAL but with different scales,
// the default is cast to match the value's scale (important for accurate scaling).
func TestBindFuncExprImplByPlanExpr_LeadLagDefaultCastDifferentDecimalScale(t *testing.T) {
	ctx := context.Background()

	// DECIMAL(10,4) value with DECIMAL(10,2) default → should cast default to scale 4
	t.Run("lead decimal(10,4) with decimal(10,2) default", func(t *testing.T) {
		valueExpr := makeDecimal128ConstExpr("100.1234", 10, 4)
		offsetExpr := makeInt64ConstPlanExpr(1)
		defaultExpr := makeDecimal128ConstExpr("-1.00", 10, 2) // DECIMAL(10,2)

		result, err := BindFuncExprImplByPlanExpr(ctx, "lead", []*plan.Expr{valueExpr, offsetExpr, defaultExpr})
		require.NoError(t, err)
		require.NotNil(t, result)

		funcExpr := result.GetF()
		require.NotNil(t, funcExpr)
		require.Equal(t, 3, len(funcExpr.Args))

		arg2 := funcExpr.Args[2]
		require.True(t, isCastExpr(arg2), "default should be cast when scale differs")
		require.Equal(t, int32(types.T_decimal128), arg2.Typ.Id)
		require.Equal(t, int32(4), arg2.Typ.Scale, "default scale should match value scale (4)")
	})
}
