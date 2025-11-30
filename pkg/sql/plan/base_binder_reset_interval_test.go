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

// Helper function to create a float32 constant expression
func makeFloat32Const(val float32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Fval{
					Fval: val,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_float32),
			NotNullable: true,
		},
	}
}

// Helper function to create a decimal128 constant expression
func makeDecimal128Const(val float64, scale int32) *plan.Expr {
	d128, _ := types.Decimal128FromFloat64(val, 38, scale)
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(d128.B0_63),
						B: int64(d128.B64_127),
					},
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_decimal128),
			NotNullable: true,
			Scale:       scale,
		},
	}
}

// Helper function to create a varchar constant expression
func makeVarcharConst(s string) *plan.Expr {
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
			Id:          int32(types.T_varchar),
			NotNullable: true,
			Width:       int32(len(s)),
		},
	}
}

// Helper function to create a NULL literal expression
func makeNullConst() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: true,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: false,
		},
	}
}

// Helper to extract int64 value from expression
func extractInt64FromExpr(expr *plan.Expr) int64 {
	if lit, ok := expr.Expr.(*plan.Expr_Lit); ok {
		if i64val, ok := lit.Lit.Value.(*plan.Literal_I64Val); ok {
			return i64val.I64Val
		}
	} else if funcExpr, ok := expr.Expr.(*plan.Expr_F); ok && funcExpr.F != nil {
		// For cast functions, try to extract from the first argument
		if len(funcExpr.F.Args) > 0 {
			return extractInt64FromExpr(funcExpr.F.Args[0])
		}
	}
	return 0
}

// TestResetIntervalFunctionArgsComprehensive tests resetIntervalFunctionArgs with comprehensive test cases
func TestResetIntervalFunctionArgsComprehensive(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                 string
		intervalValueExpr    *plan.Expr
		intervalUnit         string
		expectedIntervalVal  int64
		expectedIntervalType types.IntervalType
		expectError          bool
		errorContains        string
	}{
		// Test varchar/char string interval values
		{
			name:                 "INTERVAL '1' SECOND (varchar)",
			intervalValueExpr:    makeVarcharConst("1"),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1,
			expectedIntervalType: types.Second,
		},
		{
			name:                 "INTERVAL '1' DAY (char)",
			intervalValueExpr:    makeStringConst("1"),
			intervalUnit:         "DAY",
			expectedIntervalVal:  1,
			expectedIntervalType: types.Day,
		},
		{
			name:                 "INTERVAL 'invalid' SECOND (varchar, invalid string)",
			intervalValueExpr:    makeVarcharConst("invalid"),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  math.MaxInt64, // Invalid string returns MaxInt64
			expectedIntervalType: types.Second,
		},
		// Test float64 time units
		{
			name:                 "INTERVAL 1.1 SECOND (float64)",
			intervalValueExpr:    makeFloat64Const(1.1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 1.000009 SECOND (float64)",
			intervalValueExpr:    makeFloat64Const(1.000009),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1000009,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 1.5 MINUTE (float64)",
			intervalValueExpr:    makeFloat64Const(1.5),
			intervalUnit:         "MINUTE",
			expectedIntervalVal:  90000000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 0.5 HOUR (float64)",
			intervalValueExpr:    makeFloat64Const(0.5),
			intervalUnit:         "HOUR",
			expectedIntervalVal:  1800000000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 1.1 DAY (float64)",
			intervalValueExpr:    makeFloat64Const(1.1),
			intervalUnit:         "DAY",
			expectedIntervalVal:  95040000000, // 1.1 * 86400 * 1000000
			expectedIntervalType: types.MicroSecond,
		},
		// Test float32 time units
		{
			name:                 "INTERVAL 1.1 SECOND (float32)",
			intervalValueExpr:    makeFloat32Const(1.1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 2.5 MINUTE (float32)",
			intervalValueExpr:    makeFloat32Const(2.5),
			intervalUnit:         "MINUTE",
			expectedIntervalVal:  150000000, // 2.5 * 60 * 1000000
			expectedIntervalType: types.MicroSecond,
		},
		// Test decimal64 time units
		{
			name:                 "INTERVAL 1.1 SECOND (decimal64)",
			intervalValueExpr:    makeDecimal64Const(1.1, 1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 1.5 MINUTE (decimal64)",
			intervalValueExpr:    makeDecimal64Const(1.5, 1),
			intervalUnit:         "MINUTE",
			expectedIntervalVal:  90000000,
			expectedIntervalType: types.MicroSecond,
		},
		// Test decimal128 time units
		{
			name:                 "INTERVAL 1.1 SECOND (decimal128)",
			intervalValueExpr:    makeDecimal128Const(1.1, 1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1100000,
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 0.5 HOUR (decimal128)",
			intervalValueExpr:    makeDecimal128Const(0.5, 1),
			intervalUnit:         "HOUR",
			expectedIntervalVal:  1800000000,
			expectedIntervalType: types.MicroSecond,
		},
		// Test int64 (no conversion for time units, goes through cast path)
		{
			name:                 "INTERVAL 1 SECOND (int64)",
			intervalValueExpr:    makeInt64Const(1),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1,
			expectedIntervalType: types.Second,
		},
		{
			name:                 "INTERVAL 2 MINUTE (int64)",
			intervalValueExpr:    makeInt64Const(2),
			intervalUnit:         "MINUTE",
			expectedIntervalVal:  2,
			expectedIntervalType: types.Minute,
		},
		// Test non-time units with float64 (should go through cast path, not convert to microseconds)
		// Note: For non-time units, the function returns a cast expression, not a direct value
		// So we just verify the structure and type
		{
			name:                 "INTERVAL 1.5 MONTH (float64, non-time unit)",
			intervalValueExpr:    makeFloat64Const(1.5),
			intervalUnit:         "MONTH",
			expectedIntervalVal:  0, // Returns cast expression, not direct value
			expectedIntervalType: types.Month,
		},
		{
			name:                 "INTERVAL 1.5 YEAR (float64, non-time unit)",
			intervalValueExpr:    makeFloat64Const(1.5),
			intervalUnit:         "YEAR",
			expectedIntervalVal:  0, // Returns cast expression, not direct value
			expectedIntervalType: types.Year,
		},
		{
			name:                 "INTERVAL 1.5 WEEK (float64, non-time unit)",
			intervalValueExpr:    makeFloat64Const(1.5),
			intervalUnit:         "WEEK",
			expectedIntervalVal:  0, // Returns cast expression, not direct value
			expectedIntervalType: types.Week,
		},
		// Test NULL value with time unit and decimal/float type (should not convert, hasValue=false)
		{
			name:                 "INTERVAL NULL SECOND (null float64 literal)",
			intervalValueExpr:    makeNullConst(),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  0, // NULL will be handled at execution time, goes through cast path
			expectedIntervalType: types.Second,
		},
		// Test NULL float32 with time unit
		{
			name: "INTERVAL NULL SECOND (null float32 literal)",
			intervalValueExpr: func() *plan.Expr {
				expr := makeNullConst()
				expr.Typ.Id = int32(types.T_float32)
				return expr
			}(),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  0,
			expectedIntervalType: types.Second,
		},
		// Test NULL decimal64 with time unit
		{
			name: "INTERVAL NULL SECOND (null decimal64 literal)",
			intervalValueExpr: func() *plan.Expr {
				expr := makeNullConst()
				expr.Typ.Id = int32(types.T_decimal64)
				expr.Typ.Scale = 1
				return expr
			}(),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  0,
			expectedIntervalType: types.Second,
		},
		// Test NULL decimal128 with time unit
		{
			name: "INTERVAL NULL SECOND (null decimal128 literal)",
			intervalValueExpr: func() *plan.Expr {
				expr := makeNullConst()
				expr.Typ.Id = int32(types.T_decimal128)
				expr.Typ.Scale = 1
				return expr
			}(),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  0,
			expectedIntervalType: types.Second,
		},
		// Test decimal with scale=0 (edge case for scale < 0 check)
		// Note: scale=0 means no decimal places, so 1.1 becomes 1
		{
			name:                 "INTERVAL 1.1 SECOND (decimal64, scale=0)",
			intervalValueExpr:    makeDecimal64Const(1.1, 0),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1000000, // 1.1 with scale=0 becomes 1, so 1 * 1000000 = 1000000
			expectedIntervalType: types.MicroSecond,
		},
		{
			name:                 "INTERVAL 1.1 SECOND (decimal128, scale=0)",
			intervalValueExpr:    makeDecimal128Const(1.1, 0),
			intervalUnit:         "SECOND",
			expectedIntervalVal:  1000000, // 1.1 with scale=0 becomes 1, so 1 * 1000000 = 1000000
			expectedIntervalType: types.MicroSecond,
		},
		// Test error cases
		{
			name:              "Invalid interval unit",
			intervalValueExpr: makeInt64Const(1),
			intervalUnit:      "INVALID_UNIT",
			expectError:       true,
			errorContains:     "invalid interval type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			intervalExpr := makeIntervalExpr(tc.intervalValueExpr, tc.intervalUnit)

			args, err := resetIntervalFunctionArgs(ctx, intervalExpr)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, args)
			require.Len(t, args, 2, "resetIntervalFunctionArgs should return 2 expressions")

			// Verify the interval value
			intervalValue := extractInt64FromExpr(args[0])
			// For non-time units with float64, the function returns a cast expression
			// So we only check the value if it's not a cast expression
			if tc.expectedIntervalVal != 0 || intervalValue != 0 {
				// Only verify if we expect a non-zero value or got a non-zero value
				if _, isCast := args[0].Expr.(*plan.Expr_F); !isCast {
					require.Equal(t, tc.expectedIntervalVal, intervalValue,
						"Interval value mismatch for %s", tc.name)
				}
			}

			// Verify the interval type
			intervalType := extractInt64FromExpr(args[1])
			require.Equal(t, int64(tc.expectedIntervalType), intervalType,
				"Interval type mismatch for %s", tc.name)
		})
	}
}

// TestResetIntervalFunctionArgsNullHandling tests NULL value handling in resetIntervalFunctionArgs
func TestResetIntervalFunctionArgsNullHandling(t *testing.T) {
	ctx := context.Background()

	// Test NULL float64 with time unit (should not convert, goes through cast path)
	nullFloatExpr := makeNullConst()
	nullFloatExpr.Typ.Id = int32(types.T_float64)
	intervalExpr := makeIntervalExpr(nullFloatExpr, "SECOND")

	args, err := resetIntervalFunctionArgs(ctx, intervalExpr)
	require.NoError(t, err)
	require.Len(t, args, 2)

	// NULL values are handled at execution time, so we just verify the structure
	require.NotNil(t, args[0])
	require.NotNil(t, args[1])
}

// TestResetIntervalFunctionArgsNonLiteral tests non-literal expressions (should go through cast path)
func TestResetIntervalFunctionArgsNonLiteral(t *testing.T) {
	ctx := context.Background()

	// Test non-literal int64 expression
	colRefExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}

	intervalExpr := makeIntervalExpr(colRefExpr, "SECOND")
	args, err := resetIntervalFunctionArgs(ctx, intervalExpr)
	require.NoError(t, err)
	require.Len(t, args, 2)
	require.NotNil(t, args[0])
	require.NotNil(t, args[1])

	// Test non-literal float64 expression (should not convert, GetLit() is nil)
	colRefFloatExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 1,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
		},
	}

	intervalExpr2 := makeIntervalExpr(colRefFloatExpr, "SECOND")
	args2, err := resetIntervalFunctionArgs(ctx, intervalExpr2)
	require.NoError(t, err)
	require.Len(t, args2, 2)
	require.NotNil(t, args2[0])
	require.NotNil(t, args2[1])

	// Test non-literal decimal64 expression
	colRefDecimalExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 2,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_decimal64),
			NotNullable: true,
			Scale:       1,
		},
	}

	intervalExpr3 := makeIntervalExpr(colRefDecimalExpr, "SECOND")
	args3, err := resetIntervalFunctionArgs(ctx, intervalExpr3)
	require.NoError(t, err)
	require.Len(t, args3, 2)
	require.NotNil(t, args3[0])
	require.NotNil(t, args3[1])
}

// TestResetIntervalFunction tests the wrapper function resetIntervalFunction
func TestResetIntervalFunction(t *testing.T) {
	ctx := context.Background()

	// Test that resetIntervalFunction correctly calls resetIntervalFunctionArgs
	testCases := []struct {
		name              string
		intervalValueExpr *plan.Expr
		intervalUnit      string
	}{
		{
			name:              "INTERVAL 1 SECOND (int64)",
			intervalValueExpr: makeInt64Const(1),
			intervalUnit:      "SECOND",
		},
		{
			name:              "INTERVAL 1.1 SECOND (float64)",
			intervalValueExpr: makeFloat64Const(1.1),
			intervalUnit:      "SECOND",
		},
		{
			name:              "INTERVAL '1' SECOND (varchar)",
			intervalValueExpr: makeVarcharConst("1"),
			intervalUnit:      "SECOND",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			intervalExpr := makeIntervalExpr(tc.intervalValueExpr, tc.intervalUnit)

			// Test resetIntervalFunction
			args, err := resetIntervalFunction(ctx, intervalExpr)
			require.NoError(t, err)
			require.Len(t, args, 2)
			require.NotNil(t, args[0])
			require.NotNil(t, args[1])

			// Verify it returns the same result as resetIntervalFunctionArgs
			args2, err2 := resetIntervalFunctionArgs(ctx, intervalExpr)
			require.NoError(t, err2)
			require.Equal(t, len(args), len(args2))
			require.Equal(t, extractInt64FromExpr(args[0]), extractInt64FromExpr(args2[0]))
			require.Equal(t, extractInt64FromExpr(args[1]), extractInt64FromExpr(args2[1]))
		})
	}
}

// Helper function to create a datetime constant expression
func makeDatetimeConstForResetDate(dtStr string) *plan.Expr {
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

// Helper function to create a date constant expression
func makeDateConstForResetDate(dateStr string) *plan.Expr {
	d, _ := types.ParseDateCast(dateStr)
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Dateval{
					Dateval: int32(d),
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_date),
			NotNullable: true,
		},
	}
}

// Helper function to create an int64 constant expression
func makeInt64ConstForResetDate(val int64) *plan.Expr {
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

// TestResetDateFunction tests resetDateFunction with various scenarios
func TestResetDateFunction(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		dateExpr      *plan.Expr
		intervalExpr  *plan.Expr
		expectError   bool
		errorContains string
		checkFunc     func(t *testing.T, args []*plan.Expr, err error)
	}{
		{
			name:     "NULL interval - should return syntax error",
			dateExpr: makeDatetimeConstForResetDate("2000-01-01 00:00:00"),
			intervalExpr: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: true,
					},
				},
				Typ: plan.Type{
					Id: int32(types.T_any),
				},
			},
			expectError:   true,
			errorContains: "syntax",
		},
		{
			name:         "Expr_List interval - should call resetDateFunctionArgs directly",
			dateExpr:     makeDatetimeConstForResetDate("2000-01-01 00:00:00"),
			intervalExpr: makeIntervalExpr(makeInt64ConstForResetDate(1), "SECOND"),
			expectError:  false,
			checkFunc: func(t *testing.T, args []*plan.Expr, err error) {
				require.NoError(t, err)
				require.Len(t, args, 3)
				require.NotNil(t, args[0])
				require.NotNil(t, args[1])
				require.NotNil(t, args[2])
				// For int64 interval value, it doesn't get converted to microseconds
				// The value remains 1, and the type is Second
				intervalVal := extractInt64FromExpr(args[1])
				require.Equal(t, int64(1), intervalVal)
				// Verify interval type is Second
				intervalType := extractInt64FromExpr(args[2])
				require.Equal(t, int64(types.Second), intervalType)
			},
		},
		{
			name:         "Non-Expr_List interval - should create default 'day' interval",
			dateExpr:     makeDatetimeConstForResetDate("2000-01-01 00:00:00"),
			intervalExpr: makeInt64ConstForResetDate(5),
			expectError:  false,
			checkFunc: func(t *testing.T, args []*plan.Expr, err error) {
				require.NoError(t, err)
				require.Len(t, args, 3)
				require.NotNil(t, args[0])
				require.NotNil(t, args[1])
				require.NotNil(t, args[2])
				// Verify interval value is 5 days
				intervalVal := extractInt64FromExpr(args[1])
				require.Equal(t, int64(5), intervalVal)
				// Verify interval type is Day (default)
				intervalType := extractInt64FromExpr(args[2])
				require.Equal(t, int64(types.Day), intervalType)
			},
		},
		{
			name:         "DATE type with Expr_List interval - should handle date type conversion",
			dateExpr:     makeDateConstForResetDate("2000-01-01"),
			intervalExpr: makeIntervalExpr(makeInt64ConstForResetDate(1), "HOUR"),
			expectError:  false,
			checkFunc: func(t *testing.T, args []*plan.Expr, err error) {
				require.NoError(t, err)
				require.Len(t, args, 3)
				// DATE with HOUR interval should be converted to DATETIME
				require.NotNil(t, args[0])
				// For int64 interval value, it doesn't get converted to microseconds
				// The value remains 1, and the type is Hour
				intervalVal := extractInt64FromExpr(args[1])
				require.Equal(t, int64(1), intervalVal)
				// Verify interval type is Hour
				intervalType := extractInt64FromExpr(args[2])
				require.Equal(t, int64(types.Hour), intervalType)
			},
		},
		{
			name:         "DATE type with DAY interval - should not convert to DATETIME",
			dateExpr:     makeDateConstForResetDate("2000-01-01"),
			intervalExpr: makeIntervalExpr(makeInt64ConstForResetDate(1), "DAY"),
			expectError:  false,
			checkFunc: func(t *testing.T, args []*plan.Expr, err error) {
				require.NoError(t, err)
				require.Len(t, args, 3)
				// DATE with DAY interval should remain DATE
				require.NotNil(t, args[0])
				// Verify interval value is 1 day
				intervalVal := extractInt64FromExpr(args[1])
				require.Equal(t, int64(1), intervalVal)
				// Verify interval type is Day
				intervalType := extractInt64FromExpr(args[2])
				require.Equal(t, int64(types.Day), intervalType)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args, err := resetDateFunction(ctx, tc.dateExpr, tc.intervalExpr)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, args, err)
				}
			}
		})
	}
}
