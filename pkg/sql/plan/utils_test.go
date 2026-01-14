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

func TestHasTrailingZeros(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		constScale   int32
		columnScale  int32
		expectResult bool
	}{
		{
			name:         "99.990000000 scale=9 to scale=2 - has trailing zeros",
			value:        "99.990000000",
			constScale:   9,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "99.991234567 scale=9 to scale=2 - no trailing zeros",
			value:        "99.991234567",
			constScale:   9,
			columnScale:  2,
			expectResult: false,
		},
		{
			name:         "100.000000000 scale=9 to scale=0 - has trailing zeros",
			value:        "100.000000000",
			constScale:   9,
			columnScale:  0,
			expectResult: true,
		},
		{
			name:         "12.340000000 scale=9 to scale=5 - has trailing zeros",
			value:        "12.340000000",
			constScale:   9,
			columnScale:  5,
			expectResult: true,
		},
		{
			name:         "12.340000001 scale=9 to scale=5 - no trailing zeros",
			value:        "12.340000001",
			constScale:   9,
			columnScale:  5,
			expectResult: false,
		},
		{
			name:         "12.34567 scale=5 to scale=5 - exact match",
			value:        "12.34567",
			constScale:   5,
			columnScale:  5,
			expectResult: false, // constScale <= columnScale, returns false
		},
		{
			name:         "12.34567 scale=5 to scale=10 - column has higher scale",
			value:        "12.34567",
			constScale:   5,
			columnScale:  10,
			expectResult: false, // constScale <= columnScale, returns false
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the decimal value
			dec, _, err := types.Parse128(tt.value)
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

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			// Test hasTrailingZeros
			result := hasTrailingZeros(constExpr, constType, tt.columnScale)

			t.Logf("Value: %s, ConstScale: %d, ColumnScale: %d", tt.value, tt.constScale, tt.columnScale)
			t.Logf("Decimal128: A=%d, B=%d", dec.B0_63, dec.B64_127)
			t.Logf("Expected: %v, Got: %v", tt.expectResult, result)

			require.Equal(t, tt.expectResult, result, "hasTrailingZeros result mismatch")
		})
	}
}

func TestCheckNoNeedCastWithTrailingZeros(t *testing.T) {
	tests := []struct {
		name         string
		constValue   string
		constScale   int32
		columnScale  int32
		expectResult bool
	}{
		{
			name:         "trailing zeros - should not need cast",
			constValue:   "99.990000000",
			constScale:   9,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "no trailing zeros - should need cast",
			constValue:   "99.991234567",
			constScale:   9,
			columnScale:  2,
			expectResult: false,
		},
		{
			name:         "exact scale match - should not need cast",
			constValue:   "99.99",
			constScale:   2,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "column scale higher - should not need cast",
			constValue:   "99.99",
			constScale:   2,
			columnScale:  5,
			expectResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the decimal value
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

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			columnType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.columnScale,
			}

			// Test checkNoNeedCast
			result := checkNoNeedCast(constType, columnType, constExpr)

			t.Logf("ConstValue: %s, ConstScale: %d, ColumnScale: %d", tt.constValue, tt.constScale, tt.columnScale)
			t.Logf("Expected: %v, Got: %v", tt.expectResult, result)

			require.Equal(t, tt.expectResult, result, "checkNoNeedCast result mismatch")
		})
	}
}

func TestBindFuncExprWithTrailingZeros(t *testing.T) {
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
			name:             "trailing zeros - should use col type",
			colScale:         2,
			constValue:       "99.990000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "99.990000000 has trailing zeros, should use column scale 2",
		},
		// Note: Removed "no trailing zeros - should NOT use col type" test case
		// because it triggers early false detection optimization which returns FALSE directly.
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

			t.Logf("Input: colScale=%d, constValue=%s, constScale=%d", tt.colScale, tt.constValue, tt.constScale)
			t.Logf("Result: leftScale=%d, rightScale=%d", leftScale, rightScale)

			// Check if cast is present
			leftHasCast := funcExpr.Args[0].GetF() != nil && funcExpr.Args[0].GetF().Func.GetObjName() == "cast"
			rightHasCast := funcExpr.Args[1].GetF() != nil && funcExpr.Args[1].GetF().Func.GetObjName() == "cast"
			t.Logf("Left has cast: %v, Right has cast: %v", leftHasCast, rightHasCast)

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

// TestDecimal128HasTrailingZeros tests the decimal128HasTrailingZeros function
// specifically for large values that use the high 64 bits
func TestDecimal128HasTrailingZeros(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		constScale     int32
		columnScale    int32
		expectTrailing bool
		description    string
	}{
		{
			name:           "Large value with trailing zeros",
			value:          "12345678901234567890.000000",
			constScale:     6,
			columnScale:    0,
			expectTrailing: true,
			description:    "20-digit integer with 6 trailing zeros",
		},
		{
			name:           "Large value without trailing zeros",
			value:          "12345678901234567890.123456",
			constScale:     6,
			columnScale:    0,
			expectTrailing: false,
			description:    "20-digit value with non-zero fractional part",
		},
		{
			name:           "Large value partial trailing zeros",
			value:          "12345678901234567890.123000",
			constScale:     6,
			columnScale:    3,
			expectTrailing: true,
			description:    "20-digit value with 3 trailing zeros (scale 6 to 3)",
		},
		{
			name:           "Large value partial no trailing zeros",
			value:          "12345678901234567890.123456",
			constScale:     6,
			columnScale:    3,
			expectTrailing: false,
			description:    "20-digit value without trailing zeros (scale 6 to 3)",
		},
		{
			name:           "Max Decimal128 range with trailing zeros",
			value:          "99999999999999999999999999999999.00000",
			constScale:     5,
			columnScale:    0,
			expectTrailing: true,
			description:    "Near max value with trailing zeros",
		},
		{
			name:           "Small value high bits zero",
			value:          "123.000000",
			constScale:     6,
			columnScale:    0,
			expectTrailing: true,
			description:    "Small value (high bits = 0) with trailing zeros",
		},
		{
			name:           "Small value high bits zero no trailing",
			value:          "123.456789",
			constScale:     6,
			columnScale:    0,
			expectTrailing: false,
			description:    "Small value (high bits = 0) without trailing zeros",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, _, err := types.Parse128(tt.value)
			require.NoError(t, err)

			t.Logf("Value: %s", tt.value)
			t.Logf("Decimal128: low=%d, high=%d", dec.B0_63, dec.B64_127)
			t.Logf("ConstScale: %d, ColumnScale: %d", tt.constScale, tt.columnScale)

			// Test decimal128HasTrailingZeros directly
			trailingDigits := tt.constScale - tt.columnScale
			result := decimal128HasTrailingZeros(int64(dec.B0_63), int64(dec.B64_127), trailingDigits)

			t.Logf("TrailingDigits: %d, Expected: %v, Got: %v", trailingDigits, tt.expectTrailing, result)
			require.Equal(t, tt.expectTrailing, result, tt.description)

			// Also test through hasTrailingZeros wrapper
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

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			wrapperResult := hasTrailingZeros(constExpr, constType, tt.columnScale)
			require.Equal(t, tt.expectTrailing, wrapperResult, "hasTrailingZeros wrapper result mismatch")
		})
	}
}
