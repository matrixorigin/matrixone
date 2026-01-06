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
	"strconv"
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
			constValue:     int64(100),
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
		// FLOAT32 vs DECIMAL - should NOT optimize (conservative, avoid precision loss)
		{
			name:           "float32 = 9.0",
			op:             "=",
			colType:        types.T_float32,
			constType:      types.T_decimal128,
			constValue:     "9.0",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
		},
		// FLOAT64 vs DECIMAL - should NOT optimize (conservative, avoid precision loss)
		{
			name:           "float64 = 9.0",
			op:             "=",
			colType:        types.T_float64,
			constType:      types.T_decimal128,
			constValue:     "9.0",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
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
			if tt.constValue != nil {
				switch v := tt.constValue.(type) {
				case int64:
					constExpr.Expr.(*plan.Expr_Lit).Lit.Value = &plan.Literal_I64Val{I64Val: v}
				case string:
					// Parse decimal value
					dec, scale, err := types.Parse128(v)
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
		// Note: Removed "col scale X, const scale Y - should NOT use col type" test
		// because it conflicts with early false detection optimization.
		// When constScale > colScale with non-zero trailing digits, early false detection
		// may return FALSE directly instead of generating a comparison expression.
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
		// Note: Removed "col scale 5, const scale 10 - should NOT use col type" test
		// because it conflicts with early false detection optimization.
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
			// All remaining test cases have shouldUseColType=true or constScale <= colScale
			// So we can use simple trailing zeros
			constValue := int64(12)
			for i := int32(0); i < tt.constScale; i++ {
				constValue *= 10
			}

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
							Decimal128Val: &plan.Decimal128{A: constValue, B: 0},
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
		// Note: Removed "no trailing zeros - cannot optimize" test case
		// because it triggers early false detection optimization which returns FALSE directly
		// instead of generating a comparison expression.
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
		// Note: Removed "cast string without trailing zeros" test case
		// because it triggers early false detection optimization.
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

// TestDecimalLiteralSmartTypeSelection tests that decimal literals are assigned
// the smallest type that can hold them (decimal64 vs decimal128)
func TestDecimalLiteralSmartTypeSelection(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		colType      types.T
		colScale     int32
		constValue   string
		expectedType types.T
		description  string
	}{
		{
			name:         "small decimal fits in decimal64",
			colType:      types.T_decimal64,
			colScale:     2,
			constValue:   "99.99",
			expectedType: types.T_decimal64,
			description:  "99.99 should be decimal64, no type promotion needed",
		},
		{
			name:         "large decimal needs decimal128",
			colType:      types.T_decimal64,
			colScale:     2,
			constValue:   "9999999999999999999.99", // 19 digits
			expectedType: types.T_decimal128,
			description:  "Large value needs decimal128, type promotion expected",
		},
		{
			name:         "decimal64 column with decimal64 constant",
			colType:      types.T_decimal64,
			colScale:     5,
			constValue:   "12345.67890",
			expectedType: types.T_decimal64,
			description:  "Both decimal64, should use index",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test: %s", tt.description)

			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(tt.colType),
					Width: 18,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Parse constant to get its natural type
			dec, scale, err := types.Parse128(tt.constValue)
			require.NoError(t, err)

			// Determine if it fits in decimal64
			maxDecimal64 := uint64(999999999999999999)
			isDecimal64 := dec.B64_127 == 0 && dec.B0_63 <= maxDecimal64 && scale <= 18

			var constExpr *plan.Expr
			if isDecimal64 {
				constExpr = &plan.Expr{
					Typ: plan.Type{
						Id:    int32(types.T_decimal64),
						Width: 18,
						Scale: scale,
					},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: int64(dec.B0_63)},
							},
						},
					},
				}
			} else {
				constExpr = &plan.Expr{
					Typ: plan.Type{
						Id:    int32(types.T_decimal128),
						Width: 38,
						Scale: scale,
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

			// Verify the constant got the expected type
			require.Equal(t, int32(tt.expectedType), constExpr.Typ.Id,
				"Constant should have type %v", tt.expectedType)

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			t.Logf("✓ Constant assigned type: %v", types.T(constExpr.Typ.Id))
		})
	}
}

// TestDecimalEarlyFalseDetection tests the early false detection optimization
func TestDecimalEarlyFalseDetection(t *testing.T) {
	tests := []struct {
		name          string
		columnType    types.Type
		constantValue string
		constantScale int32
		expectFalse   bool
		description   string
	}{
		{
			name:          "Always false - precision mismatch",
			columnType:    types.New(types.T_decimal64, 10, 2),
			constantValue: "99991234567", // 99.991234567
			constantScale: 9,
			expectFalse:   true,
			description:   "Column DECIMAL(10,2) can only hold 99.99, constant is 99.991234567",
		},
		{
			name:          "Not always false - trailing zeros",
			columnType:    types.New(types.T_decimal64, 10, 2),
			constantValue: "99990000000", // 99.990000000
			constantScale: 9,
			expectFalse:   false,
			description:   "Has trailing zeros, can be optimized to 99.99",
		},
		{
			name:          "Not always false - same scale",
			columnType:    types.New(types.T_decimal64, 10, 2),
			constantValue: "9999", // 99.99
			constantScale: 2,
			expectFalse:   false,
			description:   "Same scale, normal comparison",
		},
		{
			name:          "Not always false - lower scale",
			columnType:    types.New(types.T_decimal64, 10, 2),
			constantValue: "99", // 99
			constantScale: 0,
			expectFalse:   false,
			description:   "Constant scale < column scale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create constant expression
			val, _ := strconv.ParseInt(tt.constantValue, 10, 64)
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal64),
					Scale: tt.constantScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal64Val{
							Decimal64Val: &plan.Decimal64{A: val},
						},
					},
				},
			}

			constType := types.New(types.T_decimal64, 18, tt.constantScale)
			result := isDecimalComparisonAlwaysFalseCore(constExpr, constType, tt.columnType.Scale)

			if result != tt.expectFalse {
				t.Errorf("%s: expected %v, got %v - %s",
					tt.name, tt.expectFalse, result, tt.description)
			}
		})
	}
}

// TestUnwrapCast tests the unwrapCast helper function
func TestUnwrapCast(t *testing.T) {
	tests := []struct {
		name     string
		expr     *plan.Expr
		expected string
	}{
		{
			name: "Direct literal - no unwrap needed",
			expr: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value:  &plan.Literal_I64Val{I64Val: 100},
					},
				},
			},
			expected: "literal",
		},
		{
			name: "Direct column - no unwrap needed",
			expr: &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{Name: "price"},
				},
			},
			expected: "column",
		},
		{
			name: "Cast(Literal) - should unwrap",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "cast"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Isnull: false,
										Value:  &plan.Literal_I64Val{I64Val: 100},
									},
								},
							},
						},
					},
				},
			},
			expected: "literal",
		},
		{
			name: "Cast(Column) - should unwrap",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "cast"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{Name: "price"},
								},
							},
						},
					},
				},
			},
			expected: "column",
		},
		{
			name: "Non-cast function - no unwrap",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "abs"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Isnull: false,
										Value:  &plan.Literal_I64Val{I64Val: 100},
									},
								},
							},
						},
					},
				},
			},
			expected: "function",
		},
		{
			name:     "Nil expression",
			expr:     nil,
			expected: "nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unwrapCast(tt.expr)

			if tt.expected == "nil" {
				if result != nil {
					t.Errorf("Expected nil, got non-nil")
				}
				return
			}

			if result == nil {
				t.Errorf("Expected non-nil, got nil")
				return
			}

			switch tt.expected {
			case "literal":
				if result.GetLit() == nil {
					t.Errorf("Expected literal, got %T", result.Expr)
				}
			case "column":
				if result.GetCol() == nil {
					t.Errorf("Expected column, got %T", result.Expr)
				}
			case "function":
				if result.GetF() == nil {
					t.Errorf("Expected function, got %T", result.Expr)
				}
			}
		})
	}
}

// TestEarlyFalseDetectionWithCast tests early false detection with Cast-wrapped expressions
func TestEarlyFalseDetectionWithCast(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		setupExprs  func() (*plan.Expr, *plan.Expr)
		expectFalse bool
		description string
	}{
		{
			name: "Cast(Column) = Literal - always false",
			setupExprs: func() (*plan.Expr, *plan.Expr) {
				// Cast(price DECIMAL(10,2) AS DECIMAL(10,9))
				colExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 9},
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{ObjName: "cast"},
							Args: []*plan.Expr{
								{
									Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{Name: "price"},
									},
								},
							},
						},
					},
				}

				// Literal: 99.991234567
				constExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 9},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: 99991234567},
							},
						},
					},
				}

				return colExpr, constExpr
			},
			expectFalse: true,
			description: "Column scale=2, constant scale=9 with non-zero trailing digits",
		},
		{
			name: "Cast(Column) = Literal with trailing zeros - not always false",
			setupExprs: func() (*plan.Expr, *plan.Expr) {
				// Cast(price DECIMAL(10,2) AS DECIMAL(10,9))
				colExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 9},
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{ObjName: "cast"},
							Args: []*plan.Expr{
								{
									Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{Name: "price"},
									},
								},
							},
						},
					},
				}

				// Literal: 99.990000000 (trailing zeros)
				constExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 9},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: 99990000000},
							},
						},
					},
				}

				return colExpr, constExpr
			},
			expectFalse: false,
			description: "Has trailing zeros, should use trailing zeros optimization instead",
		},
		{
			name: "Non-decimal comparison - not always false",
			setupExprs: func() (*plan.Expr, *plan.Expr) {
				// Integer column
				colExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "id"},
					},
				}

				// Integer literal
				constExpr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value:  &plan.Literal_I64Val{I64Val: 100},
						},
					},
				}

				return colExpr, constExpr
			},
			expectFalse: false,
			description: "Non-decimal types should not trigger early false detection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr1, expr2 := tt.setupExprs()
			result := isDecimalComparisonAlwaysFalse(ctx, expr1, expr2)

			if result != tt.expectFalse {
				t.Errorf("%s: expected %v, got %v - %s",
					tt.name, tt.expectFalse, result, tt.description)
			}
		})
	}
}

// TestDecimalNotEqualAlwaysTrue tests the early true detection for <> operator
func TestDecimalNotEqualAlwaysTrue(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		colScale    int32
		constValue  string
		constScale  int32
		expectTrue  bool
		description string
	}{
		{
			name:        "Always true - precision mismatch",
			colScale:    2,
			constValue:  "99991234567", // 99.991234567
			constScale:  9,
			expectTrue:  true,
			description: "Column DECIMAL(10,2) cannot equal 99.991234567, so <> is always true",
		},
		{
			name:        "Not always true - trailing zeros",
			colScale:    2,
			constValue:  "99990000000", // 99.990000000
			constScale:  9,
			expectTrue:  false,
			description: "Has trailing zeros, can match after truncation",
		},
		{
			name:        "Not always true - same scale",
			colScale:    2,
			constValue:  "9999", // 99.99
			constScale:  2,
			expectTrue:  false,
			description: "Same scale, normal comparison",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal64),
					Width: 10,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   "price",
					},
				},
			}

			// Create constant expression
			val, _ := strconv.ParseInt(tt.constValue, 10, 64)
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal64),
					Width: 18,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal64Val{
							Decimal64Val: &plan.Decimal64{A: val},
						},
					},
				},
			}

			// Call BindFuncExprImplByPlanExpr with "<>"
			result, err := BindFuncExprImplByPlanExpr(ctx, "<>", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			// Check if result is a boolean constant TRUE
			if tt.expectTrue {
				lit := result.GetLit()
				require.NotNil(t, lit, "Expected literal result for always-true case")
				bval, ok := lit.Value.(*plan.Literal_Bval)
				require.True(t, ok, "Expected boolean literal")
				require.True(t, bval.Bval, "Expected TRUE for <> with incompatible precision")
			} else {
				// Should be a function expression, not a constant
				funcExpr := result.GetF()
				require.NotNil(t, funcExpr, "Expected function expression for non-always-true case")
				require.Equal(t, "<>", funcExpr.Func.ObjName)
			}
		})
	}
}

// TestIntegerRangeCheckInComparison tests that integer comparisons check value ranges
// to prevent out-of-range errors
func TestIntegerRangeCheckInComparison(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		colType     types.T
		constValue  int64
		shouldCast  bool
		description string
	}{
		{
			name:        "uint8 column with in-range value",
			colType:     types.T_uint8,
			constValue:  100,
			shouldCast:  false,
			description: "100 fits in uint8 (0-255), should use column type",
		},
		{
			name:        "uint8 column with out-of-range value",
			colType:     types.T_uint8,
			constValue:  20000,
			shouldCast:  true,
			description: "20000 exceeds uint8 max (255), should cast to larger type",
		},
		{
			name:        "uint8 column with negative value",
			colType:     types.T_uint8,
			constValue:  -1,
			shouldCast:  true,
			description: "-1 is negative, uint8 is unsigned, should cast",
		},
		{
			name:        "uint32 column with negative value",
			colType:     types.T_uint32,
			constValue:  -1,
			shouldCast:  true,
			description: "-1 is negative, uint32 is unsigned, should cast",
		},
		{
			name:        "int8 column with in-range value",
			colType:     types.T_int8,
			constValue:  100,
			shouldCast:  false,
			description: "100 fits in int8 (-128 to 127), should use column type",
		},
		{
			name:        "int8 column with out-of-range positive",
			colType:     types.T_int8,
			constValue:  200,
			shouldCast:  true,
			description: "200 exceeds int8 max (127), should cast",
		},
		{
			name:        "int8 column with out-of-range negative",
			colType:     types.T_int8,
			constValue:  -200,
			shouldCast:  true,
			description: "-200 below int8 min (-128), should cast",
		},
		{
			name:        "uint16 column with max value",
			colType:     types.T_uint16,
			constValue:  65535,
			shouldCast:  false,
			description: "65535 is uint16 max, should use column type",
		},
		{
			name:        "uint16 column with overflow",
			colType:     types.T_uint16,
			constValue:  65536,
			shouldCast:  true,
			description: "65536 exceeds uint16 max (65535), should cast",
		},
		{
			name:        "bit(8) column with in-range value",
			colType:     types.T_bit,
			constValue:  200,
			shouldCast:  false,
			description: "200 fits in bit(8) (0-255), should use column type",
		},
		{
			name:        "bit(8) column with out-of-range value",
			colType:     types.T_bit,
			constValue:  256,
			shouldCast:  true,
			description: "256 exceeds bit(8) max (255), should cast",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column expression
			colTyp := plan.Type{Id: int32(tt.colType)}
			if tt.colType == types.T_bit {
				colTyp.Width = 8 // bit(8)
			}

			colExpr := &plan.Expr{
				Typ: colTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   "col",
					},
				},
			}

			// Create constant expression (int64)
			constExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value:  &plan.Literal_I64Val{I64Val: tt.constValue},
					},
				},
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// Check if cast was applied
			leftType := funcExpr.Args[0].Typ.Id
			rightType := funcExpr.Args[1].Typ.Id

			if tt.shouldCast {
				// Should cast to a larger type (not column type)
				require.NotEqual(t, int32(tt.colType), rightType,
					"Expected cast for %s: constant %d should not use column type %v",
					tt.description, tt.constValue, tt.colType)
			} else {
				// Should use column type (no cast needed)
				require.Equal(t, int32(tt.colType), leftType,
					"Expected no cast for %s: constant %d should use column type %v",
					tt.description, tt.constValue, tt.colType)
				require.Equal(t, int32(tt.colType), rightType,
					"Expected no cast for %s: constant %d should use column type %v",
					tt.description, tt.constValue, tt.colType)
			}
		})
	}
}


// TestFloatPrecisionCheck tests that float precision limits are correctly enforced
func TestFloatPrecisionCheck(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		colType      types.T
		constValue   int64
		shouldCast   bool
		description  string
	}{
		{
			name:         "float32 with safe integer (within 2^24)",
			colType:      types.T_float32,
			constValue:   1000000,
			shouldCast:   false,
			description:  "1000000 < 2^24 (16777216), safe for float32",
		},
		{
			name:         "float32 at boundary (2^24)",
			colType:      types.T_float32,
			constValue:   16777216,
			shouldCast:   false,
			description:  "16777216 = 2^24, exactly representable in float32",
		},
		{
			name:         "float32 beyond safe range",
			colType:      types.T_float32,
			constValue:   16777217,
			shouldCast:   true,
			description:  "16777217 > 2^24, may lose precision in float32",
		},
		{
			name:         "float32 with large value",
			colType:      types.T_float32,
			constValue:   100000000,
			shouldCast:   true,
			description:  "100000000 >> 2^24, will lose precision in float32",
		},
		{
			name:         "float32 negative at boundary",
			colType:      types.T_float32,
			constValue:   -16777216,
			shouldCast:   false,
			description:  "-16777216 = -2^24, exactly representable in float32",
		},
		{
			name:         "float32 negative beyond safe range",
			colType:      types.T_float32,
			constValue:   -16777217,
			shouldCast:   true,
			description:  "-16777217 < -2^24, may lose precision in float32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.colType)},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   "col",
					},
				},
			}

			constExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value:  &plan.Literal_I64Val{I64Val: tt.constValue},
					},
				},
			}

			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			leftType := funcExpr.Args[0].Typ.Id
			rightType := funcExpr.Args[1].Typ.Id

			if tt.shouldCast {
				require.NotEqual(t, int32(tt.colType), rightType,
					"Expected cast for %s: constant %d should not use column type %v",
					tt.description, tt.constValue, tt.colType)
			} else {
				require.Equal(t, int32(tt.colType), leftType,
					"Expected no cast for %s: constant %d should use column type %v",
					tt.description, tt.constValue, tt.colType)
				require.Equal(t, int32(tt.colType), rightType,
					"Expected no cast for %s: constant %d should use column type %v",
					tt.description, tt.constValue, tt.colType)
			}
		})
	}
}
