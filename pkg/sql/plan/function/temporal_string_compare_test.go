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

func TestComparisonTypeCastRuleUsesStringForTimeAndString(t *testing.T) {
	for _, tc := range []struct {
		name       string
		left       types.Type
		right      types.Type
		targetType types.T
	}{
		{name: "time varchar", left: types.T_time.ToTypeWithScale(6), right: types.T_varchar.ToType(), targetType: types.T_varchar},
		{name: "varchar time", left: types.T_varchar.ToType(), right: types.T_time.ToTypeWithScale(6), targetType: types.T_varchar},
		{name: "time char", left: types.T_time.ToTypeWithScale(6), right: types.T_char.ToType(), targetType: types.T_varchar},
		{name: "text time", left: types.T_text.ToType(), right: types.T_time.ToTypeWithScale(6), targetType: types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hasCast, left, right := comparisonTypeCastRule(tc.left, tc.right)
			require.True(t, hasCast)
			require.Equal(t, tc.targetType, left.Oid)
			require.Equal(t, tc.targetType, right.Oid)
		})
	}
}

func TestComparisonTypeCastRuleKeepsBinaryTimeCoercion(t *testing.T) {
	hasCast, left, right := comparisonTypeCastRule(types.T_time.ToTypeWithScale(6), types.T_varbinary.ToType())
	require.True(t, hasCast)
	require.Equal(t, types.T_time, left.Oid)
	require.Equal(t, types.T_time, right.Oid)
}

func TestComparisonTypeCastRuleDoesNotChangeTimeArithmeticCoercion(t *testing.T) {
	hasCast, left, right := fixedTypeCastRule1(types.T_time.ToTypeWithScale(6), types.T_varchar.ToType())
	require.True(t, hasCast)
	require.Equal(t, types.T_time, left.Oid)
	require.Equal(t, types.T_time, right.Oid)
}

func TestTimeStringComparisonOperatorsRequestStringCasts(t *testing.T) {
	ctx := context.Background()
	timeType := types.T_time.ToTypeWithScale(6)
	varcharType := types.T_varchar.ToType()

	for _, operator := range []string{"=", "<=>", "!=", ">", ">=", "<", "<="} {
		t.Run(operator, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, operator, []types.Type{timeType, varcharType})
			require.NoError(t, err)
			targets, shouldCast := get.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.Type{varcharType, varcharType}, targets)
		})
	}

	get, err := GetFunctionByName(ctx, "between", []types.Type{timeType, varcharType, varcharType})
	require.NoError(t, err)
	targets, shouldCast := get.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{varcharType, varcharType, varcharType}, targets)
}
