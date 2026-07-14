// Copyright 2021 - 2026 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestComparisonTypeCastRuleNumericString verifies that exact numeric values
// keep their original types for the mixed comparison kernel. Floating-point
// operands retain the existing DOUBLE conversion semantics.
func TestComparisonTypeCastRuleNumericString(t *testing.T) {
	double := types.T_float64.ToType()
	exactNumerics := []types.Type{
		types.T_int8.ToType(),
		types.T_int64.ToType(),
		types.T_uint64.ToType(),
		types.T_decimal64.ToType(),
		types.T_decimal128.ToType(),
	}
	strs := []types.Type{
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
	}
	for _, n := range exactNumerics {
		for _, s := range strs {
			has, t1, t2 := comparisonTypeCastRule(n, s)
			require.False(t, has, "%v = %v", n, s)
			require.Equal(t, n.Oid, t1.Oid, "%v = %v", n, s)
			require.Equal(t, s.Oid, t2.Oid, "%v = %v", n, s)

			has, t1, t2 = comparisonTypeCastRule(s, n)
			require.False(t, has, "%v = %v", s, n)
			require.Equal(t, s.Oid, t1.Oid, "%v = %v", s, n)
			require.Equal(t, n.Oid, t2.Oid, "%v = %v", s, n)
		}
	}
	for _, n := range []types.Type{types.T_float32.ToType(), double} {
		has, t1, t2 := comparisonTypeCastRule(n, types.T_varchar.ToType())
		require.True(t, has)
		require.Equal(t, double.Oid, t1.Oid)
		require.Equal(t, double.Oid, t2.Oid)
	}

	// Non-numeric operands keep following the fixed rule table.
	has, t1, t2 := comparisonTypeCastRule(types.T_date.ToType(), types.T_varchar.ToType())
	require.True(t, has)
	require.Equal(t, types.T_date, t1.Oid)
	require.Equal(t, types.T_date, t2.Oid)
}
