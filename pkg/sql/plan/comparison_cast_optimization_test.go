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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestComparisonTypeCastOptimization tests that comparison operators avoid casting columns
// when comparing with constants to preserve index usage
func TestComparisonTypeCastOptimization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		op             string
		colType        types.T
		constType      types.T
		constValue     interface{}
		shouldOptimize bool
		expectedType   types.T
	}{
		// INT vs INT - should optimize
		{
			name:           "int32 = int64",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_int64,
			shouldOptimize: true,
			expectedType:   types.T_int32,
		},
		// INT vs DECIMAL with zero fractional part - should optimize
		// INT vs DECIMAL with zero fractional part - should NOT optimize
		// We keep DECIMAL128 to preserve semantics
		{
			name:           "int32 = 9.0",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_decimal128,
			constValue:     "9.0",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
		},
		// INT vs DECIMAL with non-zero fractional part - should NOT optimize
		{
			name:           "int32 = 9.000002",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_decimal128,
			constValue:     "9.000002",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
		},
		// FLOAT32 vs DECIMAL - should optimize
		{
			name:           "float32 = 9.0",
			op:             "=",
			colType:        types.T_float32,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_float32,
		},
		// FLOAT64 vs DECIMAL - should optimize
		{
			name:           "float64 = 9.0",
			op:             "=",
			colType:        types.T_float64,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_float64,
		},
		// FLOAT vs FLOAT - should optimize
		{
			name:           "float32 = float64",
			op:             "=",
			colType:        types.T_float32,
			constType:      types.T_float64,
			shouldOptimize: true,
			expectedType:   types.T_float32,
		},
		// DECIMAL vs DECIMAL - should optimize
		{
			name:           "decimal64 = decimal128",
			op:             "=",
			colType:        types.T_decimal64,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_decimal64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.colType)},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.constType)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
					},
				},
			}

			// Set constant value based on type
			if tt.constType == types.T_decimal128 && tt.constValue != nil {
				// Parse decimal value
				dec, scale, err := types.Parse128(tt.constValue.(string))
				require.NoError(t, err)
				constExpr.Expr.(*plan.Expr_Lit).Lit.Value = &plan.Literal_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(dec.B0_63),
						B: int64(dec.B64_127),
					},
				}
				constExpr.Typ.Scale = scale
				constExpr.Typ.Width = 38
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, tt.op, []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			// Check if optimization was applied by examining the function arguments
			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// The optimization should result in both arguments having the column's type
			if tt.shouldOptimize {
				// Both arguments should be cast to column type
				require.Equal(t, int32(tt.expectedType), funcExpr.Args[0].Typ.Id,
					"Left argument should have column type")
				require.Equal(t, int32(tt.expectedType), funcExpr.Args[1].Typ.Id,
					"Right argument should have column type")
			}
		})
	}
}

// TestDecimalScaleCompatibilityInComparison tests that decimal comparison
// does not use column type when constant has higher scale (would cause truncation)
func TestDecimalScaleCompatibilityInComparison(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		colScale         int32
		constScale       int32
		shouldUseColType bool
	}{
		{
			name:             "col scale 0, const scale 15 - should NOT use col type",
			colScale:         0,
			constScale:       15,
			shouldUseColType: false,
		},
		{
			name:             "col scale 15, const scale 0 - should use col type",
			colScale:         15,
			constScale:       0,
			shouldUseColType: true,
		},
		{
			name:             "col scale 5, const scale 5 - should use col type",
			colScale:         5,
			constScale:       5,
			shouldUseColType: true,
		},
		{
			name:             "col scale 10, const scale 5 - should use col type",
			colScale:         10,
			constScale:       5,
			shouldUseColType: true,
		},
		{
			name:             "col scale 5, const scale 10 - should NOT use col type",
			colScale:         5,
			constScale:       10,
			shouldUseColType: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column expression with decimal128 type
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Create constant expression with decimal128 type
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal128Val{
							Decimal128Val: &plan.Decimal128{A: 12, B: 0},
						},
					},
				},
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// Debug output
			t.Logf("colExpr.Typ: Id=%d, Width=%d, Scale=%d", colExpr.Typ.Id, colExpr.Typ.Width, colExpr.Typ.Scale)
			t.Logf("constExpr.Typ: Id=%d, Width=%d, Scale=%d", constExpr.Typ.Id, constExpr.Typ.Width, constExpr.Typ.Scale)
			t.Logf("funcExpr.Args[0].Typ: Id=%d, Width=%d, Scale=%d", funcExpr.Args[0].Typ.Id, funcExpr.Args[0].Typ.Width, funcExpr.Args[0].Typ.Scale)
			t.Logf("funcExpr.Args[1].Typ: Id=%d, Width=%d, Scale=%d", funcExpr.Args[1].Typ.Id, funcExpr.Args[1].Typ.Width, funcExpr.Args[1].Typ.Scale)

			// Check the scale of the arguments after optimization
			leftScale := funcExpr.Args[0].Typ.Scale
			rightScale := funcExpr.Args[1].Typ.Scale

			if tt.shouldUseColType {
				// Both should have column's scale
				require.Equal(t, tt.colScale, leftScale, "Left arg should have col scale")
				require.Equal(t, tt.colScale, rightScale, "Right arg should have col scale")
			} else {
				// Should use higher scale to avoid truncation
				expectedScale := tt.constScale
				if tt.colScale > tt.constScale {
					expectedScale = tt.colScale
				}
				require.Equal(t, expectedScale, leftScale, "Left arg should have higher scale")
				require.Equal(t, expectedScale, rightScale, "Right arg should have higher scale")
			}
		})
	}
}

// TestDecimalTrailingZerosOptimization tests that decimal constants with trailing zeros
// can be safely truncated to match column scale, allowing index usage
func TestDecimalTrailingZerosOptimization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		colScale         int32
		constValue       string
		constScale       int32
		shouldUseColType bool
		description      string
	}{
		{
			name:             "trailing zeros - can optimize",
			colScale:         5,
			constValue:       "12.340000000", // 9 decimal places, but last 4 are zeros
			constScale:       9,
			shouldUseColType: true,
			description:      "Constant 12.340000000 has trailing zeros, can truncate to 12.34000",
		},
		{
			name:             "no trailing zeros - cannot optimize",
			colScale:         5,
			constValue:       "12.340000001", // 9 decimal places, last digit is 1
			constScale:       9,
			shouldUseColType: false,
			description:      "Constant 12.340000001 has non-zero trailing digits, cannot truncate",
		},
		{
			name:             "all zeros after column scale",
			colScale:         2,
			constValue:       "99.990000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "Constant 99.990000000 can be truncated to 99.99",
		},
		{
			name:             "exact match - no optimization needed",
			colScale:         5,
			constValue:       "12.34567",
			constScale:       5,
			shouldUseColType: true,
			description:      "Scales match exactly, no optimization needed",
		},
		{
			name:             "integer with trailing zeros",
			colScale:         0,
			constValue:       "12.000000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "Constant 12.000000000 can be truncated to 12",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test: %s", tt.description)

			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Parse the constant value
			dec, _, err := types.Parse128(tt.constValue)
			require.NoError(t, err)

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
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

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// Check the scale of the arguments
			leftScale := funcExpr.Args[0].Typ.Scale
			rightScale := funcExpr.Args[1].Typ.Scale

			t.Logf("Result: leftScale=%d, rightScale=%d", leftScale, rightScale)

			if tt.shouldUseColType {
				// Both should have column's scale (optimization applied)
				require.Equal(t, tt.colScale, leftScale, "Left arg should have col scale")
				require.Equal(t, tt.colScale, rightScale, "Right arg should have col scale")
				t.Logf("✓ Optimization applied: using column scale %d", tt.colScale)
			} else {
				// Should use higher scale (no optimization)
				expectedScale := tt.constScale
				if tt.colScale > tt.constScale {
					expectedScale = tt.colScale
				}
				require.Equal(t, expectedScale, leftScale, "Left arg should have higher scale")
				require.Equal(t, expectedScale, rightScale, "Right arg should have higher scale")
				t.Logf("✓ No optimization: using higher scale %d", expectedScale)
			}
		})
	}
}

// TestDecimalCastWrappedLiteralOptimization tests that decimal constants wrapped in Cast functions
// (as they appear in real SQL queries) can still be optimized when they have trailing zeros
func TestDecimalCastWrappedLiteralOptimization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		colScale         int32
		constValue       string // String literal as it appears in SQL
		constScale       int32  // Target scale of the Cast
		shouldUseColType bool
		description      string
	}{
		{
			name:             "cast string with trailing zeros",
			colScale:         2,
			constValue:       "99.990000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "Cast('99.990000000' as DECIMAL(10,9)) can be optimized to DECIMAL(10,2)",
		},
		{
			name:             "cast string without trailing zeros",
			colScale:         2,
			constValue:       "99.991234567",
			constScale:       9,
			shouldUseColType: false,
			description:      "Cast('99.991234567' as DECIMAL(10,9)) cannot be optimized",
		},
		{
			name:             "cast decimal literal with trailing zeros",
			colScale:         2,
			constValue:       "100.000000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "Cast(100.000000000 as DECIMAL(10,9)) can be optimized",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test: %s", tt.description)

			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Create a Cast function wrapping a string literal (simulating real SQL parsing)
			// This is how "price = 99.990000000" appears after parsing
			innerLit := &plan.Expr{
				Typ: plan.Type{
					Id: int32(types.T_varchar),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Sval{
							Sval: tt.constValue,
						},
					},
				},
			}

			// Wrap in Cast function
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							ObjName: "cast",
						},
						Args: []*plan.Expr{innerLit},
					},
				},
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// Check the scale of the arguments
			leftScale := funcExpr.Args[0].Typ.Scale
			rightScale := funcExpr.Args[1].Typ.Scale

			t.Logf("Result: leftScale=%d, rightScale=%d", leftScale, rightScale)

			if tt.shouldUseColType {
				// Both should have column's scale (optimization applied)
				require.Equal(t, tt.colScale, leftScale, "Left arg should have col scale")
				require.Equal(t, tt.colScale, rightScale, "Right arg should have col scale")
				t.Logf("✓ Optimization applied: using column scale %d", tt.colScale)
			} else {
				// Should use higher scale (no optimization)
				expectedScale := tt.constScale
				if tt.colScale > tt.constScale {
					expectedScale = tt.colScale
				}
				require.Equal(t, expectedScale, leftScale, "Left arg should have higher scale")
				require.Equal(t, expectedScale, rightScale, "Right arg should have higher scale")
				t.Logf("✓ No optimization: using higher scale %d", expectedScale)
			}
		})
	}
}
