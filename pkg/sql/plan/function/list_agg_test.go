// Copyright 2026 Matrix Origin
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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMySQLNumericAggTypeCheck(t *testing.T) {
	for _, name := range []string{"var_pop", "var_samp", "stddev_pop", "stddev_samp"} {
		t.Run(name, func(t *testing.T) {
			for _, input := range []struct {
				typ  types.Type
				want types.Type
			}{
				{types.T_varchar.ToType(), types.T_float64.ToType()},
				{types.T_date.ToType(), types.T_decimal128.ToType()},
				{types.T_datetime.ToType(), types.T_decimal128.ToType()},
			} {
				got, err := GetFunctionByName(context.Background(), name, []types.Type{input.typ})
				require.NoError(t, err)
				castTypes, shouldCast := got.ShouldDoImplicitTypeCast()
				require.True(t, shouldCast)
				require.Equal(t, []types.Type{input.want}, castTypes)
			}
		})
	}
}

func TestMySQLNumericAggTypeCheckRejectsAndHandlesSpecialTypes(t *testing.T) {
	result := mysqlNumericAggTypeCheck(nil)
	require.Equal(t, failedAggParametersWrong, result.status)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{types.T_float64.ToType()}, result.finalType)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_int64.ToType()})
	require.Equal(t, succeedMatched, result.status)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_bool.ToType()})
	require.Equal(t, failedAggParametersWrong, result.status)
}
