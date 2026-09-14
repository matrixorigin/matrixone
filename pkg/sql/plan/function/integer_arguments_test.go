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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntegerArgumentContracts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []types.Type
		positions []int
	}{
		{"substring_index", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), {}}, []int{2}},
		{"period_add", []types.Type{{}, {}}, []int{0, 1}},
		{"period_diff", []types.Type{{}, {}}, []int{0, 1}},
		{"hex", []types.Type{{}}, []int{0}},
	} {
		for _, source := range []types.Type{types.New(types.T_decimal64, 18, 1), types.New(types.T_decimal128, 38, 20), types.New(types.T_decimal256, 65, 30)} {
			t.Run(tc.name+source.String(), func(t *testing.T) {
				args := append([]types.Type(nil), tc.args...)
				for _, p := range tc.positions {
					args[p] = source
					require.True(t, HasIntegerArgument(tc.name, p))
				}
				before := append([]types.Type(nil), args...)
				result, err := GetFunctionByName(context.Background(), tc.name, args)
				require.NoError(t, err)
				require.True(t, result.needCast)
				for _, p := range tc.positions {
					require.Equal(t, types.T_int64, result.targetTypes[p].Oid)
				}
				speculative, ok := GetFunctionByNameWithoutError(tc.name, args)
				require.True(t, ok)
				require.Equal(t, result, speculative)
				modes := make([]StringDomainCheckMode, len(args))
				withModes, err := GetFunctionByNameWithStringDomainCheckModes(context.Background(), tc.name, args, modes)
				require.NoError(t, err)
				require.Equal(t, result, withModes)
				require.Equal(t, before, args, "resolution must not mutate source types")
			})
		}
	}
	require.False(t, HasIntegerArgument("not_a_function", 0))
	require.False(t, HasIntegerArgument("hex", -1))
	require.False(t, HasIntegerArgument("hex", 1))
	require.False(t, HasIntegerArgument("substring_index", 0))
	for _, name := range []string{"hex", "period_add", "substring_index"} {
		_, err := GetFunctionByName(context.Background(), name, nil)
		require.Error(t, err)
	}
}

func TestIntegerArgumentContractsPreserveOtherDomains(t *testing.T) {
	for _, oid := range []types.T{types.T_float64, types.T_uint64, types.T_int64, types.T_bit} {
		result, err := GetFunctionByName(context.Background(), "substring_index", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), oid.ToType()})
		require.NoError(t, err)
		_, index := DecodeOverloadID(result.GetEncodedOverloadID())
		want := int32(2)
		if oid == types.T_float64 {
			want = 0
		}
		if oid == types.T_uint64 || oid == types.T_bit {
			want = 1
		}
		require.Equal(t, want, index)
	}
	result, err := GetFunctionByName(context.Background(), "sin", []types.Type{types.New(types.T_decimal64, 4, 1)})
	require.NoError(t, err)
	require.True(t, result.needCast)
	require.Equal(t, types.T_float64, result.targetTypes[0].Oid)
	result, err = GetFunctionByName(context.Background(), "hex", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.False(t, result.needCast)
}
