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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestOrderedSetPercentileCheck(t *testing.T) {
	check := allSupportedFunctions[PERCENTILE_CONT].checkFn
	require.NotNil(t, check)

	for _, inputs := range [][]types.Type{
		nil,
		{types.T_int64.ToType()},
		{types.T_int64.ToType(), types.T_float64.ToType(), types.T_int64.ToType()},
	} {
		result := check(nil, inputs)
		require.Equal(t, failedAggParametersWrong, result.status, "inputs=%v", inputs)
	}

	result := check(nil, []types.Type{types.T_any.ToType(), types.T_float64.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 0, result.idx)
	require.Equal(t, []types.Type{types.T_float64.ToType(), types.T_float64.ToType()}, result.finalType)

	result = check(nil, []types.Type{types.T_int64.ToType(), types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{types.T_int64.ToType(), types.T_float64.ToType()}, result.finalType)

	result = check(nil, []types.Type{types.T_any.ToType(), types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{types.T_float64.ToType(), types.T_float64.ToType()}, result.finalType)

	for _, inputs := range [][]types.Type{
		{types.T_varchar.ToType(), types.T_float64.ToType()},
		{types.T_int64.ToType(), types.T_varchar.ToType()},
		{types.New(types.T_decimal256, 65, 2), types.T_float64.ToType()},
		{types.T_int64.ToType(), types.New(types.T_decimal256, 65, 2)},
	} {
		result = check(nil, inputs)
		require.Equal(t, failedAggParametersWrong, result.status, "inputs=%v", inputs)
	}

	for _, oid := range []types.T{
		types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64,
	} {
		result = check(nil, []types.Type{oid.ToType(), types.T_float64.ToType()})
		require.Equal(t, succeedMatched, result.status, "oid=%s", oid)
	}
	result = check(nil, []types.Type{types.New(types.T_decimal128, 37, 2), types.T_float64.ToType()})
	require.Equal(t, succeedMatched, result.status)

	result = allSupportedFunctions[PERCENTILE_CONT].checkFn(nil, []types.Type{
		types.New(types.T_decimal128, 38, 0), types.T_float64.ToType(),
	})
	require.Equal(t, failedAggParametersWrong, result.status)

	result = allSupportedFunctions[PERCENTILE_CONT].checkFn(nil, []types.Type{
		types.New(types.T_decimal128, 38, 38), types.T_float64.ToType(),
	})
	require.Equal(t, failedAggParametersWrong, result.status)

	result = allSupportedFunctions[PERCENTILE_CONT].checkFn(nil, []types.Type{
		types.New(types.T_decimal128, 37, 0), types.T_float64.ToType(),
	})
	require.Equal(t, succeedMatched, result.status)

	require.Equal(t, succeedMatched,
		allSupportedFunctions[PERCENTILE_DISC].checkFn(nil, []types.Type{
			types.T_int64.ToType(), types.T_float64.ToType(),
		}).status)
	require.Equal(t, succeedMatched,
		allSupportedFunctions[PERCENTILE_DISC].checkFn(nil, []types.Type{
			types.New(types.T_decimal128, 38, 0), types.T_float64.ToType(),
		}).status)
}
