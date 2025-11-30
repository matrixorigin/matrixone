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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Helper function to create a DATETIME constant expression
func makeDatetimeConst(dtStr string) *plan.Expr {
	dt, _ := types.ParseDatetime(dtStr, 6)
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Datetimeval{
					Datetimeval: int64(dt),
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_datetime),
			NotNullable: true,
			Scale:       6,
		},
	}
}

// Helper function to create a float64 constant expression
func makeFloat64Const(val float64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Dval{
					Dval: val,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
		},
	}
}

// Helper function to create a decimal64 constant expression
func makeDecimal64Const(val float64, scale int32) *plan.Expr {
	d64, _ := types.Decimal64FromFloat64(val, 18, scale)
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Decimal64Val{
					Decimal64Val: &plan.Decimal64{A: int64(d64)},
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_decimal64),
			NotNullable: true,
			Scale:       scale,
		},
	}
}

// Helper function to create an int64 constant expression
func makeInt64Const(val int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_I64Val{
					I64Val: val,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
}

// Helper function to create a string constant expression
func makeStringConst(s string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Sval{
					Sval: s,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_char),
			NotNullable: true,
			Width:       int32(len(s)),
		},
	}
}

// Helper function to create INTERVAL expression
func makeIntervalExpr(valueExpr *plan.Expr, unit string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					valueExpr,
					makeStringConst(unit),
				},
			},
		},
		Typ: plan.Type{
			Id: int32(types.T_interval),
		},
	}
}

// Helper to extract int64 value from a constant expression
// Handles both literal expressions and cast function expressions
func extractInt64Value(expr *plan.Expr) int64 {
	if lit, ok := expr.Expr.(*plan.Expr_Lit); ok {
		if i64val, ok := lit.Lit.Value.(*plan.Literal_I64Val); ok {
			return i64val.I64Val
		}
	} else if funcExpr, ok := expr.Expr.(*plan.Expr_F); ok && funcExpr.F != nil {
		// For cast functions, try to extract from the first argument
		if len(funcExpr.F.Args) > 0 {
			return extractInt64Value(funcExpr.F.Args[0])
		}
	}
	return 0
}

// TestResetDateFunctionArgsDecimalInterval tests that resetDateFunctionArgs correctly handles
// decimal/float interval values by converting them to microseconds and setting the interval type.
func TestResetDateFunctionArgsDecimalInterval(t *testing.T) {
	ctx := context.Background()

	dateExpr := makeDatetimeConst("2000-01-01 01:00:00")

	testCases := []struct {
		name                 string
		intervalValueExpr    *plan.Expr
		intervalUnit         string
		expectedIntervalVal  int64
		expectedIntervalType types.IntervalType
	}{
		{
			name:                 "DATETIME - INTERVAL 1.1 SECOND",
			intervalValueExpr:    makeFloat64Const(1.1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL 1.000009 SECOND",
			intervalValueExpr:    makeFloat64Const(1.000009),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1000009, // math.Round(1.000009 * 1000000) = 1000009 (not 1000008 due to rounding)
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL -0.1 SECOND",
			intervalValueExpr:    makeFloat64Const(-0.1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  -100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL 1.5 MINUTE",
			intervalValueExpr:    makeFloat64Const(1.5),
			intervalUnit:         "MINUTE",
			expectedIntervalVal:  90000000, // 1.5 * 60 * 1000000
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL 0.5 HOUR",
			intervalValueExpr:    makeFloat64Const(0.5),
			intervalUnit:         "HOUR",
			expectedIntervalVal:  1800000000, // 0.5 * 3600 * 1000000
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL 1.1 SECOND (decimal64)",
			intervalValueExpr:    makeDecimal64Const(1.1, 1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "DATETIME - INTERVAL 1 SECOND (integer, no conversion)",
			intervalValueExpr:    makeInt64Const(1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1,
			expectedIntervalType: types.Second, // Should remain Second, not MicroSecond
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			intervalExpr := makeIntervalExpr(tc.intervalValueExpr, tc.intervalUnit)

			args, err := resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
			require.NoError(t, err)
			require.Len(t, args, 3)

			// Verify the date expression is unchanged
			require.Equal(t, dateExpr, args[0])

			// Verify the interval value
			intervalValue := extractInt64Value(args[1])
			// After using math.Round, 1.000009 * 1000000 should be exactly 1000009
			require.Equal(t, tc.expectedIntervalVal, intervalValue, "Interval value should match expected (math.Round handles precision)")

			// Verify the interval type
			intervalType := extractInt64Value(args[2])
			require.Equal(t, int64(tc.expectedIntervalType), intervalType, "Interval type mismatch")
		})
	}
}

// TestResetDateFunctionArgsDecimalIntervalWithCast tests that resetDateFunctionArgs correctly handles
// decimal/float interval values that are wrapped in a cast function (which happens when SQL parser
// parses decimal literals like 1.1 as decimal128 and makePlan2DecimalExprWithType wraps them in cast).
func TestResetDateFunctionArgsDecimalIntervalWithCast(t *testing.T) {
	ctx := context.Background()

	// Helper function to create a cast function expression (simulating what makePlan2DecimalExprWithType does)
	// This simulates the case where 1.1 is parsed as decimal128 and wrapped in a cast function
	makeCastExprForDecimal := func(val string) *plan.Expr {
		// Parse the decimal to get scale
		_, scale, _ := types.Parse128(val)
		var typ plan.Type
		if scale < 18 && len(val) < 18 {
			typ = plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       18,
				Scale:       scale,
				NotNullable: true,
			}
		} else {
			typ = plan.Type{
				Id:          int32(types.T_decimal128),
				Width:       38,
				Scale:       scale,
				NotNullable: true,
			}
		}

		// Create a cast function expression (simulating appendCastBeforeExpr)
		return &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						Obj:     0, // cast function ID
						ObjName: "cast",
					},
					Args: []*plan.Expr{
						makeStringConst(val), // First arg: string literal
						{
							Typ: typ,
							Expr: &plan.Expr_T{
								T: &plan.TargetType{},
							},
						},
					},
				},
			},
			Typ: typ,
		}
	}

	// Test case: DATE_SUB(datetime, INTERVAL 1.1 SECOND) where 1.1 is wrapped in cast function
	// This simulates the real scenario where SQL parser parses 1.1 as decimal128
	t.Run("DATETIME - INTERVAL 1.1 SECOND (wrapped in cast)", func(t *testing.T) {
		dateExpr := makeDatetimeConst("2000-01-01 01:00:00")
		// Create cast expression that wraps the decimal string (simulating makePlan2DecimalExprWithType)
		castExpr := makeCastExprForDecimal("1.1")
		intervalExpr := makeIntervalExpr(castExpr, "second")

		args, err := resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
		require.NoError(t, err)
		require.Len(t, args, 3, "Should return 3 arguments: dateExpr, interval value, interval type")

		// Verify interval value is converted to microseconds
		intervalValue := extractInt64Value(args[1])
		// Use math.Round to match the implementation
		expectedMicroseconds := int64(math.Round(1.1 * float64(types.MicroSecsPerSec)))
		require.Equal(t, expectedMicroseconds, intervalValue, "Interval value should be converted to microseconds (1100000) even when wrapped in cast")

		// Verify interval type is MicroSecond
		intervalType := extractInt64Value(args[2])
		require.Equal(t, int64(types.MicroSecond), intervalType, "Interval type should be MicroSecond, not Second")
	})

	// Test case: DATE_SUB(datetime, INTERVAL 1.000009 SECOND) where 1.000009 is wrapped in cast function
	t.Run("DATETIME - INTERVAL 1.000009 SECOND (wrapped in cast)", func(t *testing.T) {
		dateExpr := makeDatetimeConst("2000-01-01 01:00:00")
		castExpr := makeCastExprForDecimal("1.000009")
		intervalExpr := makeIntervalExpr(castExpr, "second")

		args, err := resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
		require.NoError(t, err)

		intervalValue := extractInt64Value(args[1])
		val := float64(1.000009) * float64(types.MicroSecsPerSec)
		// Use math.Round to match the implementation (1.000009 * 1000000 = 1000008.9999999999, rounds to 1000009)
		expectedMicroseconds := int64(math.Round(val))
		require.Equal(t, expectedMicroseconds, intervalValue, "Interval value should be converted to microseconds (1000009) even when wrapped in cast")

		intervalType := extractInt64Value(args[2])
		require.Equal(t, int64(types.MicroSecond), intervalType, "Interval type should be MicroSecond")
	})
}
