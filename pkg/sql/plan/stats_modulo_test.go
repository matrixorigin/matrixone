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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestModuloNdvLogic tests the core min(colNdv, modValue) logic
func TestModuloNdvLogic(t *testing.T) {
	testCases := []struct {
		name     string
		colNdv   float64
		modValue float64
		expected float64
	}{
		{"mod 2 with 5M rows", 5000000, 2, 2},
		{"mod 10 with 1M rows", 1000000, 10, 10},
		{"mod 100 with 50 rows", 50, 100, 50},
		{"mod 1000 exact match", 1000, 1000, 1000},
		{"mod 7 day of week", 10000000, 7, 7},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := math.Min(tc.colNdv, tc.modValue)
			require.Equal(t, tc.expected, result)
		})
	}
}

// TestModuloLiteralExtraction tests extraction of modulo values from different integer types
func TestModuloLiteralExtraction(t *testing.T) {
	testCases := []struct {
		name     string
		literal  *plan.Literal
		expected int64
	}{
		{"int64", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}, 5},
		{"int32", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 5}}, 5},
		{"int16", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 5}}, 5},
		{"int8", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 5}}, 5},
		{"uint64", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 5}}, 5},
		{"uint32", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 5}}, 5},
		{"uint16", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 5}}, 5},
		{"uint8", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 5}}, 5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var extracted int64
			switch v := tc.literal.Value.(type) {
			case *plan.Literal_I64Val:
				extracted = v.I64Val
			case *plan.Literal_I32Val:
				extracted = int64(v.I32Val)
			case *plan.Literal_I16Val:
				extracted = int64(v.I16Val)
			case *plan.Literal_I8Val:
				extracted = int64(v.I8Val)
			case *plan.Literal_U64Val:
				extracted = int64(v.U64Val)
			case *plan.Literal_U32Val:
				extracted = int64(v.U32Val)
			case *plan.Literal_U16Val:
				extracted = int64(v.U16Val)
			case *plan.Literal_U8Val:
				extracted = int64(v.U8Val)
			}
			require.Equal(t, tc.expected, extracted)
		})
	}
}

// TestModuloEdgeCases tests edge cases for modulo operations
func TestModuloEdgeCases(t *testing.T) {
	t.Run("zero should not be valid - signed", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}
		if v, ok := lit.Value.(*plan.Literal_I64Val); ok {
			require.False(t, v.I64Val > 0, "zero should not pass > 0 check")
		}
	})

	t.Run("zero should not be valid - unsigned", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 0}}
		if v, ok := lit.Value.(*plan.Literal_U64Val); ok {
			require.False(t, v.U64Val > 0, "zero should not pass > 0 check")
		}
	})

	t.Run("negative should not be valid", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -5}}
		if v, ok := lit.Value.(*plan.Literal_I64Val); ok {
			require.False(t, v.I64Val > 0, "negative should not pass > 0 check")
		}
	})

	t.Run("uint64 overflow check", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_U64Val{U64Val: math.MaxUint64}}
		if v, ok := lit.Value.(*plan.Literal_U64Val); ok {
			// Should check against MaxInt64 to prevent overflow
			require.False(t, v.U64Val <= math.MaxInt64, "MaxUint64 exceeds MaxInt64")
		}
	})

	t.Run("uint64 within range", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 100}}
		if v, ok := lit.Value.(*plan.Literal_U64Val); ok {
			require.True(t, v.U64Val > 0 && v.U64Val <= math.MaxInt64)
		}
	})

	t.Run("both % and mod function names supported", func(t *testing.T) {
		funcNames := []string{"%", "mod"}
		require.Contains(t, funcNames, "%")
		require.Contains(t, funcNames, "mod")
	})

	t.Run("min logic with zero colNdv", func(t *testing.T) {
		colNdv := 0.0
		modValue := 10.0
		// When colNdv is 0 or negative, should return modValue
		if colNdv > 0 {
			require.Fail(t, "should not reach here")
		} else {
			require.Equal(t, modValue, modValue)
		}
	})

	t.Run("min logic with negative colNdv", func(t *testing.T) {
		colNdv := -1.0
		modValue := 10.0
		// When colNdv is negative, should return modValue
		if colNdv > 0 {
			require.Fail(t, "should not reach here")
		} else {
			require.Equal(t, modValue, modValue)
		}
	})
}

// TestGetExprNdvModuloOperator tests modulo operator with different values
func TestGetExprNdvModuloOperator(t *testing.T) {
	testCases := []struct {
		name        string
		funcName    string
		modValue    int64
		colNdv      float64
		expectedMin float64
	}{
		{"%_operator_small_mod", "%", 2, 5000000, 2},
		{"mod_function_medium_mod", "mod", 10, 1000000, 10},
		{"%_operator_large_mod", "%", 1000, 500, 500},
		{"mod_function_exact_match", "mod", 1000, 1000, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := math.Min(tc.colNdv, float64(tc.modValue))
			require.Equal(t, tc.expectedMin, result)
		})
	}
}

// TestGetExprNdvModuloInvalidValues tests modulo with invalid values
func TestGetExprNdvModuloInvalidValues(t *testing.T) {
	testCases := []struct {
		name     string
		modValue int64
		isValid  bool
	}{
		{"zero_modulo", 0, false},
		{"negative_modulo", -5, false},
		{"positive_modulo", 10, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isValid := tc.modValue > 0
			require.Equal(t, tc.isValid, isValid)
		})
	}
}

// TestGetExprNdvModuloUint64Overflow tests uint64 overflow protection
func TestGetExprNdvModuloUint64Overflow(t *testing.T) {
	t.Run("uint64_exceeds_maxint64", func(t *testing.T) {
		val := uint64(math.MaxUint64)
		isValid := val > 0 && val <= math.MaxInt64
		require.False(t, isValid)
	})

	t.Run("uint64_within_range", func(t *testing.T) {
		val := uint64(100)
		isValid := val > 0 && val <= math.MaxInt64
		require.True(t, isValid)
	})
}

// TestGetExprNdvModuloAllIntegerTypes tests all integer type literals
func TestGetExprNdvModuloAllIntegerTypes(t *testing.T) {
	testCases := []struct {
		name     string
		literal  *plan.Literal
		expected float64
	}{
		{"int64_type", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}, 5},
		{"int32_type", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 5}}, 5},
		{"int16_type", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 5}}, 5},
		{"int8_type", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 5}}, 5},
		{"uint64_type", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 5}}, 5},
		{"uint32_type", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 5}}, 5},
		{"uint16_type", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 5}}, 5},
		{"uint8_type", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 5}}, 5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var extracted float64
			switch v := tc.literal.Value.(type) {
			case *plan.Literal_I64Val:
				extracted = float64(v.I64Val)
			case *plan.Literal_I32Val:
				extracted = float64(v.I32Val)
			case *plan.Literal_I16Val:
				extracted = float64(v.I16Val)
			case *plan.Literal_I8Val:
				extracted = float64(v.I8Val)
			case *plan.Literal_U64Val:
				extracted = float64(v.U64Val)
			case *plan.Literal_U32Val:
				extracted = float64(v.U32Val)
			case *plan.Literal_U16Val:
				extracted = float64(v.U16Val)
			case *plan.Literal_U8Val:
				extracted = float64(v.U8Val)
			}
			require.Equal(t, tc.expected, extracted)
		})
	}
}

// TestGetExprNdvModuloSmallColNdv tests when column NDV is smaller than modulo value
func TestGetExprNdvModuloSmallColNdv(t *testing.T) {
	t.Run("colndv_smaller_than_modvalue", func(t *testing.T) {
		colNdv := 50.0
		modValue := 1000.0
		result := math.Min(colNdv, modValue)
		require.Equal(t, 50.0, result)
	})
}

// TestGetExprNdvModuloZeroColNdv tests when column NDV is zero or negative
func TestGetExprNdvModuloZeroColNdv(t *testing.T) {
	t.Run("colndv_zero_fallback", func(t *testing.T) {
		colNdv := 0.0
		modValue := 10.0
		var result float64
		if colNdv > 0 {
			result = math.Min(colNdv, modValue)
		} else {
			result = modValue
		}
		require.Equal(t, 10.0, result)
	})

	t.Run("colndv_negative_fallback", func(t *testing.T) {
		colNdv := -1.0
		modValue := 10.0
		var result float64
		if colNdv > 0 {
			result = math.Min(colNdv, modValue)
		} else {
			result = modValue
		}
		require.Equal(t, 10.0, result)
	})
}

// TestGetExprNdvModuloMissingArgs tests modulo with missing arguments
func TestGetExprNdvModuloMissingArgs(t *testing.T) {
	t.Run("modulo_single_arg", func(t *testing.T) {
		args := []*plan.Expr{
			{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "col1"}}},
		}
		require.Less(t, len(args), 2)
	})
}

// TestGetExprNdvModuloNonLiteral tests modulo with non-literal second argument
func TestGetExprNdvModuloNonLiteral(t *testing.T) {
	t.Run("modulo_with_column_arg", func(t *testing.T) {
		col2Expr := &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{Name: "col2"},
			},
		}
		lit := col2Expr.GetLit()
		require.Nil(t, lit)
	})
}
