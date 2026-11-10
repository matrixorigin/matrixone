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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDecimalModOriginalOperandPrecision(t *testing.T) {
	for _, tc := range []struct {
		name    string
		decimal types.Type
		integer types.Type
		width   int32
	}{
		{"signed tiny", types.New(types.T_decimal64, 2, 1), types.T_int8.ToType(), 3},
		{"unsigned tiny", types.New(types.T_decimal64, 2, 1), types.T_uint8.ToType(), 3},
		{"signed small", types.New(types.T_decimal64, 2, 1), types.T_int16.ToType(), 5},
		{"unsigned small", types.New(types.T_decimal64, 2, 1), types.T_uint16.ToType(), 5},
		{"signed medium", types.New(types.T_decimal64, 2, 1), types.New(types.T_int32, 24, -1), 7},
		{"unsigned medium", types.New(types.T_decimal64, 2, 1), types.New(types.T_uint32, 24, -1), 8},
		{"unsigned int", types.New(types.T_decimal64, 2, 1), types.T_uint32.ToType(), 10},
		{"int", types.New(types.T_decimal64, 10, 3), types.T_int32.ToType(), 10},
		{"signed bigint", types.New(types.T_decimal64, 18, 3), types.T_int64.ToType(), 19},
		{"unsigned bigint", types.New(types.T_decimal64, 18, 3), types.T_uint64.ToType(), 20},
		{"decimal128 control", types.New(types.T_decimal128, 38, 3), types.T_int64.ToType(), 38},
		{"decimal256 control", types.New(types.T_decimal256, 50, 3), types.T_uint64.ToType(), 50},
	} {
		for _, name := range []string{"mod", "%"} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/reverse=%t", tc.name, name, reverse), func(t *testing.T) {
					inputs := []types.Type{tc.decimal, tc.integer}
					if reverse {
						inputs[0], inputs[1] = inputs[1], inputs[0]
					}
					resolved, err := GetFunctionByName(context.Background(), name, inputs)
					require.NoError(t, err)
					got := resolved.GetReturnType()
					require.Equal(t, tc.width, got.Width)
					require.Equal(t, tc.decimal.Scale, got.Scale)
					cast, left, right := arithmeticTypeCastRule1(inputs[0], inputs[1])
					wantOID := inputs[0].Oid
					if cast {
						wantOID = left.Oid
					}
					require.Equal(t, wantOID, got.Oid, "result storage must still match the kernel")
					targets, needCast := resolved.ShouldDoImplicitTypeCast()
					require.Equal(t, cast, needCast)
					if cast {
						require.Equal(t, []types.Type{left, right}, targets)
					}
					speculative, ok := GetFunctionByNameWithoutError(name, inputs)
					require.True(t, ok)
					require.Equal(t, resolved.GetReturnType(), speculative.GetReturnType())
					selected, err := GetFunctionByNameWithOverload(context.Background(), name, inputs, 0)
					require.NoError(t, err)
					require.Equal(t, got, selected.GetReturnType())
					numericLeft, numericRight, numericResult, ok := ResolveNumericBinaryTypes(name, inputs[0], inputs[1], nil)
					require.True(t, ok)
					require.Equal(t, got, numericResult)
					if cast {
						require.Equal(t, left, numericLeft)
						require.Equal(t, right, numericRight)
					}
				})
			}
		}
	}
}

func TestDecimalModDecimalDomainsAndExclusions(t *testing.T) {
	for _, tc := range []struct {
		name              string
		left, right, want types.Type
	}{
		{"decimal precision and scale", types.New(types.T_decimal64, 10, 2), types.New(types.T_decimal64, 15, 4), types.New(types.T_decimal64, 15, 4)},
		{"reverse decimal precision and scale", types.New(types.T_decimal64, 15, 4), types.New(types.T_decimal64, 10, 2), types.New(types.T_decimal64, 15, 4)},
		{"default decimal width", types.New(types.T_decimal64, 0, 2), types.New(types.T_decimal64, 10, 3), types.New(types.T_decimal64, 18, 3)},
		{"extended decimal scale", types.New(types.T_decimal256, 65, 39), types.T_int32.ToType(), types.New(types.T_decimal256, 65, 39)},
		{"time exclusion", types.New(types.T_decimal64, 10, 3), types.New(types.T_time, 0, 6), types.New(types.T_decimal64, 10, 6)},
		{"reverse time exclusion", types.New(types.T_time, 0, 6), types.New(types.T_decimal64, 10, 3), types.New(types.T_decimal64, 18, 6)},
		{"null exclusion", types.New(types.T_decimal64, 10, 3), types.T_any.ToType(), types.New(types.T_decimal64, 10, 3)},
		{"float exclusion", types.New(types.T_decimal64, 10, 3), types.T_float64.ToType(), types.New(types.T_float64, 0, 3)},
		{"string exclusion", types.New(types.T_decimal64, 10, 3), types.T_varchar.ToType(), types.New(types.T_float64, 0, 3)},
		{"integer only", types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "mod", []types.Type{tc.left, tc.right})
			require.NoError(t, err)
			require.Equal(t, tc.want, resolved.GetReturnType())
			switch tc.name {
			case "time exclusion":
				casts, needed := resolved.ShouldDoImplicitTypeCast()
				require.True(t, needed)
				require.Equal(t, []types.Type{tc.left, types.New(types.T_decimal64, 18, 6)}, casts)
			case "reverse time exclusion":
				casts, needed := resolved.ShouldDoImplicitTypeCast()
				require.True(t, needed)
				require.Equal(t, []types.Type{types.New(types.T_decimal64, 18, 6), tc.right}, casts)
			}
		})
	}
}

func TestDecimalModUnknownNumericPeer(t *testing.T) {
	decimal := types.New(types.T_decimal64, 10, 3)
	for _, inputs := range [][2]types.Type{
		{decimal, types.T_any.ToType()}, {types.T_any.ToType(), decimal},
	} {
		left, right, result, ok := ResolveNumericBinaryTypes("mod", inputs[0], inputs[1], nil)
		require.True(t, ok)
		require.Equal(t, decimal, left)
		require.Equal(t, decimal, right)
		require.Equal(t, decimal, result)
	}
}
