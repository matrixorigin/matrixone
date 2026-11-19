// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

// MySQL treats TRUE/FALSE as 1/0 in GREATEST/LEAST: GREATEST(2, TRUE) is 2,
// GREATEST(1e0, TRUE) is 1 and GREATEST('a', TRUE) compares 'a' with '1'.
func TestLeastGreatestMixedBoolResolution(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	for _, tc := range []struct {
		name string
		args []types.Type
		want types.T
	}{
		{"bigint", []types.Type{types.T_int64.ToType(), boolType}, types.T_int64},
		{"bool first", []types.Type{boolType, types.T_int64.ToType()}, types.T_int64},
		{"int32", []types.Type{types.T_int32.ToType(), boolType}, types.T_int32},
		{"uint64", []types.Type{types.T_uint64.ToType(), boolType}, types.T_uint64},
		{"uint64 and int", []types.Type{types.T_uint32.ToType(), boolType, types.T_int8.ToType()}, types.T_int64},
		{"double", []types.Type{types.T_float64.ToType(), boolType}, types.T_float64},
		{"float", []types.Type{boolType, types.T_float32.ToType()}, types.T_float64},
		{"varchar", []types.Type{types.T_varchar.ToType(), boolType}, types.T_varchar},
		{"with null", []types.Type{types.T_any.ToType(), boolType, types.T_int64.ToType()}, types.T_int64},
	} {
		for _, name := range []string{"greatest", "least"} {
			resolved, err := GetFunctionByName(ctx, name, tc.args)
			require.NoError(t, err, "%s %s", name, tc.name)
			require.Equal(t, tc.want, resolved.GetReturnType().Oid, "%s %s", name, tc.name)
			require.True(t, resolved.needCast, "%s %s", name, tc.name)
			for i, arg := range tc.args {
				if arg.Oid == types.T_bool {
					require.Equal(t, tc.want, resolved.targetTypes[i].Oid, "%s %s arg %d", name, tc.name, i)
				}
			}
		}
	}
}

// Only-BOOL arguments keep the existing BOOL resolution.
func TestLeastGreatestOnlyBoolUnchanged(t *testing.T) {
	boolType := types.T_bool.ToType()
	res := leastGreatestCheck(nil, []types.Type{boolType, boolType})
	require.Equal(t, succeedMatched, res.status)
	require.Equal(t, types.T_bool, leastGreatestReturnType([]types.Type{boolType, types.T_any.ToType()}).Oid)
}
