// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func TestApproxPercentileCheckFn(t *testing.T) {
	fn := allSupportedFunctions[APPROX_PERCENTILE]
	require.NotNil(t, fn.checkFn)

	check := fn.checkFn

	// wrong number of arguments
	result := check(nil, []types.Type{})
	require.Equal(t, failedAggParametersWrong, result.status)

	result = check(nil, []types.Type{types.T_int64.ToType()})
	require.Equal(t, failedAggParametersWrong, result.status)

	// T_any in arg0 should request cast
	result = check(nil, []types.Type{types.T_any.ToType(), types.T_float64.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 0, result.idx)

	// unsupported type in arg0
	result = check(nil, []types.Type{types.T_varchar.ToType(), types.T_float64.ToType()})
	require.Equal(t, failedAggParametersWrong, result.status)

	// T_any in arg1 should request cast
	result = check(nil, []types.Type{types.T_int64.ToType(), types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)

	// unsupported type in arg1
	result = check(nil, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.Equal(t, failedAggParametersWrong, result.status)

	// valid types
	for _, oid := range []types.T{
		types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	} {
		result = check(nil, []types.Type{oid.ToType(), types.T_float64.ToType()})
		require.Equal(t, succeedMatched, result.status, "oid %d should succeed", oid)
	}

	// valid arg1 types
	for _, arg1Oid := range []types.T{types.T_int32, types.T_int64, types.T_float32, types.T_float64} {
		result = check(nil, []types.Type{types.T_int64.ToType(), arg1Oid.ToType()})
		require.Equal(t, succeedMatched, result.status, "arg1 oid %d should succeed", arg1Oid)
	}
}

func TestApproxPercentileIsRegistered(t *testing.T) {
	fn := allSupportedFunctions[APPROX_PERCENTILE]
	require.Equal(t, APPROX_PERCENTILE, fn.functionId)
	require.True(t, fn.isAggregate())
	require.Equal(t, 1, len(fn.Overloads))
	require.True(t, fn.Overloads[0].isAgg)
	require.Equal(t, "approx_percentile", fn.Overloads[0].aggFramework.str)
}
