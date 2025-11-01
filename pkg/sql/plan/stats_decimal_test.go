// Copyright 2025 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestGetDecimalLiteralValue tests the conversion of decimal literals with proper scale handling
func TestGetDecimalLiteralValue(t *testing.T) {
	t.Run("decimal64 positive value", func(t *testing.T) {
		// Test: 5.00 with scale=2
		// Internal representation: 500
		// Expected result: 5.0
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: 500},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 2)
		require.True(t, ok)
		require.InDelta(t, 5.0, result, 0.001)
	})

	t.Run("decimal64 negative value", func(t *testing.T) {
		// Test: -10.50 with scale=2
		// Internal representation: -1050
		// Expected result: -10.5
		dec, _ := types.Decimal64FromFloat64(-10.5, 10, 2)
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: int64(dec)},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 2)
		require.True(t, ok)
		require.InDelta(t, -10.5, result, 0.001)
	})

	t.Run("decimal64 scale=0", func(t *testing.T) {
		// Test: 100 with scale=0
		// Internal representation: 100
		// Expected result: 100.0
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: 100},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 0)
		require.True(t, ok)
		require.InDelta(t, 100.0, result, 0.001)
	})

	t.Run("decimal64 large scale", func(t *testing.T) {
		// Test: 1.23456 with scale=5
		// Internal representation: 123456
		// Expected result: 1.23456
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: 123456},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 5)
		require.True(t, ok)
		require.InDelta(t, 1.23456, result, 0.00001)
	})

	t.Run("decimal128 positive value", func(t *testing.T) {
		// Test: 12345.67 with scale=2
		dec, _ := types.Decimal128FromFloat64(12345.67, 20, 2)
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal128Val{
				Decimal128Val: &plan.Decimal128{
					A: int64(dec.B0_63),
					B: int64(dec.B64_127),
				},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 2)
		require.True(t, ok)
		require.InDelta(t, 12345.67, result, 0.01)
	})

	t.Run("non-decimal literal", func(t *testing.T) {
		// Test: non-decimal literal should return false
		lit := &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 123},
		}

		result, ok := getDecimalLiteralValue(lit, 2)
		require.False(t, ok)
		require.Equal(t, 0.0, result)
	})
}

// TestCalcSelectivityByMinMaxForDecimal tests selectivity calculation for decimal range conditions
func TestCalcSelectivityByMinMaxForDecimal(t *testing.T) {
	t.Run("greater than or equal", func(t *testing.T) {
		// Column: l_quantity range [1.0, 50.0]
		// Filter: l_quantity >= 5.0
		// Expected: (50 - 5 + 1) / (50 - 1) = 46/49 ≈ 0.939
		expr := makeDecimalComparisonExpr(">=", 5.0, 2)

		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)

		expected := (50.0 - 5.0 + 1) / (50.0 - 1.0)
		require.InDelta(t, expected, result, 0.01)
		require.Greater(t, result, 0.9) // Should be close to 1
	})

	t.Run("less than or equal", func(t *testing.T) {
		// Column: l_quantity range [1.0, 50.0]
		// Filter: l_quantity <= 15.0
		// Expected: (15 - 1 + 1) / (50 - 1) = 15/49 ≈ 0.306
		expr := makeDecimalComparisonExpr("<=", 15.0, 2)

		result := calcSelectivityByMinMaxForDecimal("<=", 1.0, 50.0, expr)

		expected := (15.0 - 1.0 + 1) / (50.0 - 1.0)
		require.InDelta(t, expected, result, 0.01)
	})

	t.Run("between", func(t *testing.T) {
		// Column: l_quantity range [1.0, 50.0]
		// Filter: l_quantity BETWEEN 5.0 AND 15.0
		// Expected: (15 - 5 + 1) / (50 - 1) = 11/49 ≈ 0.224
		expr := makeDecimalBetweenExpr(5.0, 15.0, 2)

		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)

		expected := (15.0 - 5.0 + 1) / (50.0 - 1.0)
		require.InDelta(t, expected, result, 0.01)
	})

	t.Run("value out of range - too high", func(t *testing.T) {
		// Column: l_quantity range [1.0, 50.0]
		// Filter: l_quantity >= 100.0 (out of range)
		// Expected: very low selectivity (近似0或0.1)
		expr := makeDecimalComparisonExpr(">=", 100.0, 2)

		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)

		// Should return small value (out of range)
		require.Less(t, result, 0.001)
	})

	t.Run("value out of range - too low", func(t *testing.T) {
		// Column: l_quantity range [1.0, 50.0]
		// Filter: l_quantity <= 0.5 (out of range)
		// Expected: very low selectivity
		expr := makeDecimalComparisonExpr("<=", 0.5, 2)

		result := calcSelectivityByMinMaxForDecimal("<=", 1.0, 50.0, expr)

		// Should return small value (out of range)
		require.Less(t, result, 0.001)
	})

	t.Run("negative values", func(t *testing.T) {
		// Column: c_acctbal range [-999.99, 9999.99]
		// Filter: c_acctbal >= -500.0
		// Expected: (9999.99 - (-500) + 1) / (9999.99 - (-999.99)) ≈ 0.954
		expr := makeDecimalComparisonExpr(">=", -500.0, 2)

		result := calcSelectivityByMinMaxForDecimal(">=", -999.99, 9999.99, expr)

		expected := (9999.99 - (-500.0) + 1) / (9999.99 - (-999.99))
		require.InDelta(t, expected, result, 0.01)
	})

	t.Run("max < min - invalid range", func(t *testing.T) {
		// Invalid range: max < min
		// Should return default 0.1
		expr := makeDecimalComparisonExpr(">=", 5.0, 2)
		result := calcSelectivityByMinMaxForDecimal(">=", 50.0, 1.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("expr.GetF() returns nil", func(t *testing.T) {
		// Invalid expr - no function
		expr := &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{Name: "test_col"},
			},
		}
		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("expr with insufficient args", func(t *testing.T) {
		// Invalid expr - only 1 arg (needs 2)
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{Name: "test_col"},
							},
						},
						// Missing second arg
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("literal is nil", func(t *testing.T) {
		// Expr with nil literal
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{Name: "test_col"},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: nil, // nil literal
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("getDecimalLiteralValue returns false", func(t *testing.T) {
		// Literal that getDecimalLiteralValue can't handle (non-decimal)
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{Name: "test_col"},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_I64Val{I64Val: 123}, // int literal, not decimal
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("greater than (single >)", func(t *testing.T) {
		// Test ">" (not ">=")
		// Note: The current implementation treats ">" and ">=" the same
		// Both use: (max - val1 + 1) / (max - min)
		expr := makeDecimalComparisonExpr(">", 5.0, 2)
		result := calcSelectivityByMinMaxForDecimal(">", 1.0, 50.0, expr)
		// Actual implementation uses same formula as >=
		expected := (50.0 - 5.0 + 1) / (50.0 - 1.0)
		require.InDelta(t, expected, result, 0.01)
	})

	t.Run("less than (single <)", func(t *testing.T) {
		// Test "<" (not "<=")
		// Note: The current implementation treats "<" and "<=" the same
		expr := makeDecimalComparisonExpr("<", 15.0, 2)
		result := calcSelectivityByMinMaxForDecimal("<", 1.0, 50.0, expr)
		// Actual implementation uses same formula as <=
		expected := (15.0 - 1.0 + 1) / (50.0 - 1.0)
		require.InDelta(t, expected, result, 0.01)
	})

	t.Run("between with nil lit1", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "between"},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "test_col"}},
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{Lit: nil}, // nil lit1
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Decimal64Val{
										Decimal64Val: &plan.Decimal64{A: 1500}, // 15.0
									},
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("between with nil lit2", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "between"},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "test_col"}},
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Decimal64Val{
										Decimal64Val: &plan.Decimal64{A: 500}, // 5.0
									},
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{Lit: nil}, // nil lit2
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("between with invalid lit1", func(t *testing.T) {
		// lit1 that getDecimalLiteralValue can't handle
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "between"},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "test_col"}},
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_I64Val{I64Val: 123}, // wrong type
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Decimal64Val{
										Decimal64Val: &plan.Decimal64{A: 1500},
									},
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("between with invalid lit2", func(t *testing.T) {
		// lit2 that getDecimalLiteralValue can't handle
		dec, _ := types.Decimal64FromFloat64(5.0, 18, 2)
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "between"},
					Args: []*plan.Expr{
						{
							Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "test_col"}},
							Typ:  plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Decimal64Val{
										Decimal64Val: &plan.Decimal64{A: int64(dec)},
									},
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
						{
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_I64Val{I64Val: 123}, // wrong type
								},
							},
							Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 2},
						},
					},
				},
			},
		}
		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("unknown funcName defaults", func(t *testing.T) {
		// Unknown function name should return 0.1
		expr := makeDecimalComparisonExpr("unknown", 5.0, 2)
		result := calcSelectivityByMinMaxForDecimal("unknown", 1.0, 50.0, expr)
		require.Equal(t, 0.1, result)
	})

	t.Run("ret > 1 clamped to 1.0", func(t *testing.T) {
		// Scenario where calculation would give > 1
		// This happens when between range (val2 - val1 + 1) > (max - min)
		// e.g., val1=1, val2=100, but max-min=49
		expr := makeDecimalBetweenExpr(1.0, 100.0, 2) // val2 way beyond max
		result := calcSelectivityByMinMaxForDecimal("between", 1.0, 50.0, expr)
		// Should be clamped to 1.0
		require.Equal(t, 1.0, result)
	})

	t.Run("ret < 0 returns low value", func(t *testing.T) {
		// Scenario where calculation gives negative result
		// This can happen in edge cases with between when range is inverted
		// Use a between with val2 < val1 and very small range
		expr := makeDecimalBetweenExpr(50.0, 1.0, 2)                             // val2 < val1
		result := calcSelectivityByMinMaxForDecimal("between", 50.0, 50.0, expr) // min=max edge case
		// Should return low value (0.00000001)
		require.Less(t, result, 0.001)
	})
}

// TestDecimalSelectivityRegression tests the specific Q19 scenario that caused performance regression
func TestDecimalSelectivityRegression(t *testing.T) {
	t.Run("Q19 l_quantity filter should have reasonable selectivity", func(t *testing.T) {
		// TPC-H Q19 scenario:
		// l_quantity range: [1.0, 50.0]
		// Filter: (l_quantity >= 5 AND l_quantity <= 15) OR
		//         (l_quantity >= 14 AND l_quantity <= 24) OR
		//         (l_quantity >= 28 AND l_quantity <= 38)

		// First condition: >= 5 AND <= 15
		expr1 := makeDecimalComparisonExpr(">=", 5.0, 2)
		sel1 := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr1)
		require.Greater(t, sel1, 0.5, "l_quantity >= 5 should have >50%% selectivity")

		expr2 := makeDecimalComparisonExpr("<=", 15.0, 2)
		sel2 := calcSelectivityByMinMaxForDecimal("<=", 1.0, 50.0, expr2)
		require.Greater(t, sel2, 0.2, "l_quantity <= 15 should have >20%% selectivity")

		// Combined selectivity should be reasonable (not near zero!)
		// This was the bug: it was calculated as 0.000000
		combined := andSelectivity(sel1, sel2)
		require.Greater(t, combined, 0.15, "Combined selectivity should be >15%%")
		require.Less(t, combined, 0.4, "Combined selectivity should be <40%%")
	})

	t.Run("l_extendedprice range should not cause extreme selectivity", func(t *testing.T) {
		// l_extendedprice range: [900.91, 104949.50]
		// Any reasonable filter should have selectivity > 0.001
		min := 900.91
		max := 104949.50

		// Filter: >= 10000
		expr := makeDecimalComparisonExpr(">=", 10000.0, 2)
		sel := calcSelectivityByMinMaxForDecimal(">=", min, max, expr)

		require.Greater(t, sel, 0.001)
		require.Less(t, sel, 1.0)
	})
}

// Helper functions to construct test expressions

func makeDecimalComparisonExpr(op string, value float64, scale int32) *plan.Expr {
	// Convert float to decimal internal representation
	dec, _ := types.Decimal64FromFloat64(value, 18, scale)

	// Simple expr structure without triggering shuffle logic
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: op},
				Args: []*plan.Expr{
					// Column reference - simplified to avoid triggering shuffle
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 0,
								ColPos: 0,
								Name:   "test_col",
							},
						},
						Typ: plan.Type{
							Id:    int32(types.T_decimal64),
							Width: 18,
							Scale: scale,
						},
					},
					// Literal value
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_Decimal64Val{
									Decimal64Val: &plan.Decimal64{A: int64(dec)},
								},
							},
						},
						Typ: plan.Type{
							Id:    int32(types.T_decimal64),
							Width: 18,
							Scale: scale,
						},
					},
				},
			},
		},
		Typ: plan.Type{Id: int32(types.T_bool)},
	}
}

func makeDecimalBetweenExpr(val1, val2 float64, scale int32) *plan.Expr {
	dec1, _ := types.Decimal64FromFloat64(val1, 18, scale)
	dec2, _ := types.Decimal64FromFloat64(val2, 18, scale)

	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "between"},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{Name: "test_col", ColPos: 0},
						},
						Typ: plan.Type{Id: int32(types.T_decimal64), Scale: scale},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_Decimal64Val{
									Decimal64Val: &plan.Decimal64{A: int64(dec1)},
								},
							},
						},
						Typ: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: scale},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_Decimal64Val{
									Decimal64Val: &plan.Decimal64{A: int64(dec2)},
								},
							},
						},
						Typ: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: scale},
					},
				},
			},
		},
		Typ: plan.Type{Id: int32(types.T_bool)},
	}
}

// TestDecimalLiteralScaleBugScenario tests the exact bug scenario that caused Q19 performance regression
func TestDecimalLiteralScaleBugScenario(t *testing.T) {
	t.Run("bug scenario: l_quantity >= 5.00 with scale=2", func(t *testing.T) {
		// This is the exact scenario from TPC-H Q19 that caused the bug

		// Column stats: l_quantity ∈ [1.0, 50.0]
		min := 1.0
		max := 50.0

		// Filter condition: l_quantity >= 5.00
		// Literal internal value: 500 (5.00 * 100)
		// If scale is not applied: val1 = 500.0 → sel = (50-500+1)/49 = negative!
		// With correct scale: val1 = 5.0 → sel = (50-5+1)/49 = 0.939 ✓

		expr := makeDecimalComparisonExpr(">=", 5.0, 2)
		result := calcSelectivityByMinMaxForDecimal(">=", min, max, expr)

		// Verify the selectivity is reasonable (NOT near zero!)
		require.Greater(t, result, 0.5,
			"BUG: Selectivity for 'l_quantity >= 5' should be >50%%, got %.6f", result)
		require.Less(t, result, 1.0)

		// More precise check
		expected := (max - 5.0 + 1) / (max - min)
		require.InDelta(t, expected, result, 0.01,
			"Expected selectivity %.4f, got %.4f", expected, result)
	})

	t.Run("bug scenario: complex OR condition from Q19", func(t *testing.T) {
		// Q19 has complex OR: (>= 5 AND <= 15) OR (>= 14 AND <= 24) OR (>= 28 AND <= 38)
		// With the bug, each condition returned ~0, making final selectivity 0
		// With the fix, should return reasonable selectivity

		min := 1.0
		max := 50.0

		// Condition 1: >= 5 AND <= 15
		ge5 := makeDecimalComparisonExpr(">=", 5.0, 2)
		sel_ge5 := calcSelectivityByMinMaxForDecimal(">=", min, max, ge5)

		le15 := makeDecimalComparisonExpr("<=", 15.0, 2)
		sel_le15 := calcSelectivityByMinMaxForDecimal("<=", min, max, le15)

		sel1 := andSelectivity(sel_ge5, sel_le15)

		// Each individual selectivity should be reasonable
		require.Greater(t, sel_ge5, 0.8)
		require.Greater(t, sel_le15, 0.2)
		require.Greater(t, sel1, 0.15, "AND selectivity should be >15%%")

		// Similar for other conditions...
		// The key is that none of them should be near zero!
	})
}

// TestDecimal128Selectivity tests decimal128 handling
func TestDecimal128Selectivity(t *testing.T) {
	t.Run("decimal128 large values", func(t *testing.T) {
		// Test with larger decimal128 values
		dec, _ := types.Decimal128FromFloat64(1000000.50, 38, 2)
		lit := &plan.Literal{
			Value: &plan.Literal_Decimal128Val{
				Decimal128Val: &plan.Decimal128{
					A: int64(dec.B0_63),
					B: int64(dec.B64_127),
				},
			},
		}

		result, ok := getDecimalLiteralValue(lit, 2)
		require.True(t, ok)
		require.InDelta(t, 1000000.50, result, 0.01)
	})
}

// TestDecimalSelectivityEdgeCases tests edge cases and boundary conditions
func TestDecimalSelectivityEdgeCases(t *testing.T) {
	t.Run("min equals max", func(t *testing.T) {
		// When column has only one distinct value
		expr := makeDecimalComparisonExpr(">=", 5.0, 2)
		result := calcSelectivityByMinMaxForDecimal(">=", 10.0, 10.0, expr)

		// Should handle gracefully (not panic or return extreme values)
		require.Greater(t, result, 0.0)
		require.LessOrEqual(t, result, 1.0)
	})

	t.Run("exact match on min", func(t *testing.T) {
		// Filter value exactly equals min
		expr := makeDecimalComparisonExpr(">=", 1.0, 2)
		result := calcSelectivityByMinMaxForDecimal(">=", 1.0, 50.0, expr)

		// Should return ~1.0 (all values match)
		require.Greater(t, result, 0.98)
	})

	t.Run("exact match on max", func(t *testing.T) {
		// Filter value exactly equals max
		expr := makeDecimalComparisonExpr("<=", 50.0, 2)
		result := calcSelectivityByMinMaxForDecimal("<=", 1.0, 50.0, expr)

		// Should return ~1.0 (all values match)
		require.Greater(t, result, 0.98)
	})
}
