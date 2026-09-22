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

func TestBitIntegerArgumentCanonicalBinding(t *testing.T) {
	for _, name := range []string{"hex", "char", "make_set", "export_set"} {
		for _, source := range []types.T{types.T_int8, types.T_int64, types.T_uint64, types.T_bit, types.T_enum, types.T_bool, types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float32, types.T_float64, types.T_varchar} {
			t.Run(name+"/"+source.String(), func(t *testing.T) {
				inputs := []types.Type{source.ToType()}
				if name == "make_set" || name == "export_set" {
					inputs = append(inputs, types.T_varchar.ToType(), types.T_varchar.ToType())
				}
				original := append([]types.Type(nil), inputs...)
				check := func(result FuncGetResult) {
					_, id := DecodeOverloadID(result.GetEncodedOverloadID())
					target := types.T_int64
					if source == types.T_uint64 || source == types.T_bit || source == types.T_varchar {
						target = types.T_uint64
					}
					if name == "hex" {
						if source == types.T_varchar {
							require.Equal(t, int32(0), id)
							return
						}
						wantID := int32(2)
						if target == types.T_uint64 {
							wantID = 3
						}
						require.Equal(t, wantID, id)
					} else {
						require.Equal(t, int32(0), id)
					}
					casts, needed := result.ShouldDoImplicitTypeCast()
					if source != target {
						require.True(t, needed)
						require.Equal(t, target, casts[0].Oid)
					}
				}
				result, err := GetFunctionByName(context.Background(), name, inputs)
				require.NoError(t, err)
				check(result)
				result, ok := GetFunctionByNameWithoutError(name, inputs)
				require.True(t, ok)
				check(result)
				result, err = GetFunctionByNameWithStringDomainCheckModes(context.Background(), name, inputs, make([]StringDomainCheckMode, len(inputs)))
				require.NoError(t, err)
				check(result)
				require.Equal(t, original, inputs)
			})
		}
	}
	for _, id := range []int32{4, 5, 8, 9, 10, 11, 12, 13, 14} {
		_, err := GetFunctionById(context.Background(), encodeOverloadID(HEX, id))
		require.NoError(t, err)
		_, err = GetFunctionByNameWithOverload(context.Background(), "hex", []types.Type{types.T_float64.ToType()}, id)
		require.ErrorContains(t, err, "legacy execution only")
	}
	for _, source := range []types.T{types.T_array_float32, types.T_array_float64} {
		resolved, err := GetFunctionByName(context.Background(), "hex", []types.Type{source.ToType()})
		require.NoError(t, err)
		_, id := DecodeOverloadID(resolved.GetEncodedOverloadID())
		want := int32(6)
		if source == types.T_array_float64 {
			want = 7
		}
		require.Equal(t, want, id)
	}
	_, err := GetFunctionByName(context.Background(), "export_set", []types.Type{
		types.T_json.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	})
	require.Error(t, err, "unsupported sources fail during binding")
	for _, name := range []string{"bin", "oct"} {
		_, exists := IntegerArgumentTargetForSource(name, 0, types.T_decimal128, false)
		require.False(t, exists)
	}
	for _, position := range []int{1, 2} {
		target, ok := IntegerArgumentTarget("conv", position)
		require.True(t, ok)
		require.Equal(t, types.T_int64, target)
	}
	_, ok := IntegerArgumentTarget("conv", 0)
	require.False(t, ok)
}
