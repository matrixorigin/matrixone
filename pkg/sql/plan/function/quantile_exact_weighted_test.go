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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

// TestQuantileExactWeightedRegistration checks the published SQL entry name,
// the aggregate overload id, and the value-typed result derivation.
func TestQuantileExactWeightedRegistration(t *testing.T) {
	intValue := types.T_int32.ToType()
	intWeight := types.T_int32.ToType()
	doubleLevel := types.T_float64.ToType()
	fn, err := GetFunctionByName(context.Background(),
		"quantileexactweighted",
		[]types.Type{intValue, intWeight, doubleLevel})
	require.NoError(t, err)
	fid, overload := DecodeOverloadID(fn.GetEncodedOverloadID())
	require.Equal(t, int32(QUANTILE_EXACT_WEIGHTED), fid)
	require.Zero(t, overload)
	require.Equal(t, int64(619<<32), aggexec.AggIdOfQuantileExactWeighted,
		"the executor aggregate id must track the catalog function id")
	require.Equal(t, intValue.Oid, fn.GetReturnType().Oid,
		"the exact weighted percentile returns the value argument's own type")
}

// TestQuantileExactWeightedCheckFnVerifiesTypedAdmission checks the exact
// admission lists of the registration checkFn: decision D4's weight set with
// the bind-time DECIMAL256 rejection (the DECIMAL(65,0) witness) and the
// exact ordered-percentile numeric value domain.
func TestQuantileExactWeightedCheckFnVerifiesTypedAdmission(t *testing.T) {
	check := allSupportedFunctions[QUANTILE_EXACT_WEIGHTED].checkFn
	require.NotNil(t, check)

	level := types.T_float64.ToType()
	for _, weightOID := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_bit, types.T_float32, types.T_float64,
	} {
		t.Run("weight-"+weightOID.String(), func(t *testing.T) {
			result := check(nil, []types.Type{
				types.T_int64.ToType(), weightOID.ToType(), level})
			require.Equal(t, succeedMatched, result.status, "weight %s", weightOID)
		})
	}
	for _, weight := range []types.Type{
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 20, 2),
	} {
		t.Run("weight-"+weight.String(), func(t *testing.T) {
			result := check(nil, []types.Type{
				types.T_int64.ToType(), weight, level})
			require.Equal(t, succeedMatched, result.status)
		})
	}

	// DECIMAL(65,0) is a numeric type (T_decimal256) that must be rejected as
	// a weight at bind time; an "any numeric type" admission would let it in.
	require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
		types.T_int64.ToType(), types.New(types.T_decimal256, 65, 0), level}).status)

	for _, weight := range []types.Type{
		types.T_varchar.ToType(),
		types.T_date.ToType(),
	} {
		require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
			types.T_int64.ToType(), weight, level}).status, "weight %s", weight.Oid)
	}

	for _, value := range []types.Type{
		types.T_varchar.ToType(),
		types.New(types.T_decimal256, 40, 0),
		// Width-38 decimals are excluded exactly as percentile_cont does.
		types.New(types.T_decimal128, 38, 10),
	} {
		require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
			value, types.T_uint64.ToType(), level}).status, "value %s", value.Oid)
	}

	// Non-numeric levels are rejected; the [0,1] range check stays with the
	// compile-time percentile codec.
	require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
		types.T_int64.ToType(), types.T_uint64.ToType(),
		types.T_varchar.ToType()}).status)
	require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
		types.T_int64.ToType(), types.T_uint64.ToType(),
		types.New(types.T_decimal256, 40, 0)}).status)

	// The plan arity is exactly three.
	require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
		types.T_int64.ToType(), types.T_uint64.ToType()}).status)
	require.Equal(t, failedAggParametersWrong, check(nil, []types.Type{
		types.T_int64.ToType(), types.T_uint64.ToType(), level, level}).status)

	// Untyped literals normalize to DOUBLE for every argument, and only they
	// produce a registration cast: a typed weight is never cast.
	result := check(nil, []types.Type{
		types.T_any.ToType(), types.T_any.ToType(), types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{
		types.T_float64.ToType(), types.T_float64.ToType(), types.T_float64.ToType(),
	}, result.finalType)

	// A typed weight keeps its own type through registration: the ordinary
	// numeric registration cast never touches it.
	decimalWeight := types.New(types.T_decimal128, 20, 2)
	result = check(nil, []types.Type{
		types.T_any.ToType(), decimalWeight, types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{
		types.T_float64.ToType(), decimalWeight, types.T_float64.ToType(),
	}, result.finalType)
}
