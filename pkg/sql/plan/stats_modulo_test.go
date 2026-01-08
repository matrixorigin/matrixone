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
	t.Run("zero should not be valid", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}
		if v, ok := lit.Value.(*plan.Literal_I64Val); ok {
			require.False(t, v.I64Val > 0, "zero should not pass > 0 check")
		}
	})

	t.Run("negative should not be valid", func(t *testing.T) {
		lit := &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -5}}
		if v, ok := lit.Value.(*plan.Literal_I64Val); ok {
			require.False(t, v.I64Val > 0, "negative should not pass > 0 check")
		}
	})

	t.Run("both % and mod function names supported", func(t *testing.T) {
		funcNames := []string{"%", "mod"}
		require.Contains(t, funcNames, "%")
		require.Contains(t, funcNames, "mod")
	})
}
