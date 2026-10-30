// Copyright 2021 - 2022 Matrix Origin
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

// An integer dividend with a DECIMAL256 divisor divides exactly in DECIMAL256,
// like the reversed operands and an integer over DECIMAL128 (issue #31637).
func TestIntegerDividedByDecimal256Resolves(t *testing.T) {
	dec := types.New(types.T_decimal256, 65, 30)
	for _, oid := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	} {
		for _, fid := range []int{DIV, INTEGER_DIV} {
			ok, left, right := fixedTypeCastRule2(oid.ToType(), dec)
			require.True(t, ok, "%s / DECIMAL256", oid)
			require.Equal(t, types.T_decimal256, left.Oid, "%s / DECIMAL256", oid)
			require.Equal(t, types.T_decimal256, right.Oid, "%s / DECIMAL256", oid)

			fn := allSupportedFunctions[fid]
			result := fn.checkFn(fn.Overloads, []types.Type{oid.ToType(), dec})
			require.NotEqual(t, failedFunctionParametersWrong, result.status, "%s op %d DECIMAL256", oid, fid)
			reversed := fn.checkFn(fn.Overloads, []types.Type{dec, oid.ToType()})
			require.NotEqual(t, failedFunctionParametersWrong, reversed.status)
		}
		require.Equal(t, types.T_decimal256, resolvedReturnType(t, DIV, []types.Type{oid.ToType(), dec}).Oid)
	}
}
