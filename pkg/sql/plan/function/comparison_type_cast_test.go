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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestComparisonTypeCastRuleUsesFloat64ForStringNumericComparison(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  types.Type
		right types.Type
	}{
		{name: "varchar int64", left: types.T_varchar.ToType(), right: types.T_int64.ToType()},
		{name: "int64 varchar", left: types.T_int64.ToType(), right: types.T_varchar.ToType()},
		{name: "char decimal", left: types.T_char.ToType(), right: types.T_decimal128.ToType()},
		{name: "float32 text", left: types.T_float32.ToType(), right: types.T_text.ToType()},
		{name: "bool varbinary", left: types.T_bool.ToType(), right: types.T_varbinary.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hasCast, left, right := comparisonTypeCastRule(tc.left, tc.right)
			require.True(t, hasCast)
			require.Equal(t, types.T_float64, left.Oid)
			require.Equal(t, types.T_float64, right.Oid)
		})
	}
}

func TestStringNumericComparisonOperatorsRequestFloat64Casts(t *testing.T) {
	ctx := context.Background()
	stringType := types.T_varchar.ToType()
	intType := types.T_int64.ToType()
	floatType := types.T_float64.ToType()

	for _, operator := range []string{"=", "<=>", "!=", ">", ">=", "<", "<="} {
		t.Run(operator, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, operator, []types.Type{stringType, intType})
			require.NoError(t, err)
			targets, shouldCast := get.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.Type{floatType, floatType}, targets)
		})
	}

	get, err := GetFunctionByName(ctx, "between", []types.Type{stringType, intType, types.T_decimal128.ToType()})
	require.NoError(t, err)
	targets, shouldCast := get.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{floatType, floatType, floatType}, targets)
}
