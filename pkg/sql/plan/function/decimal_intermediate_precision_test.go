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

func TestDecimalIntermediatePrecision(t *testing.T) {
	decimal15 := types.New(types.T_decimal64, 15, 2)
	for _, tc := range []struct {
		name, op          string
		left, right, want types.Type
	}{
		{"price times discount", "*", decimal15, decimal15, types.New(types.T_decimal128, 30, 4)},
		{"integer minus discount", "-", types.T_int64.ToType(), decimal15, types.New(types.T_decimal128, 22, 2)},
		{"integer plus tax", "+", types.T_int64.ToType(), decimal15, types.New(types.T_decimal128, 22, 2)},
		{"small sum stays decimal64", "+", types.New(types.T_decimal64, 5, 2), types.New(types.T_decimal64, 5, 2), types.New(types.T_decimal64, 18, 2)},
		{"mixed integer multiplication", "*", types.T_int64.ToType(), decimal15, types.New(types.T_decimal128, 34, 2)},
		{"mixed unsigned multiplication", "*", types.T_uint64.ToType(), decimal15, types.New(types.T_decimal128, 35, 2)},
		{"actual wide multiplication", "*", types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal256, 40, 0)},
		{"actual wide addition", "+", types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal256, 39, 0)},
		{"legacy full coefficient square", "*", types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal128, 38, 0)},
		{"narrow decimal64 square", "*", types.New(types.T_decimal64, 17, 0), types.New(types.T_decimal64, 17, 0), types.New(types.T_decimal128, 34, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), tc.op, []types.Type{tc.left, tc.right})
			require.NoError(t, err)
			require.Equal(t, tc.want, resolved.retType)
		})
	}
}

func TestDecimalLegacyCoefficientDomain(t *testing.T) {
	for _, scale := range []int32{0, 2, 18} {
		for _, width := range []int32{0, 18, 19} {
			p, s, ok := decimalArithmeticDomain(types.New(types.T_decimal64, width, scale))
			require.True(t, ok)
			require.Equal(t, int32(19), p)
			require.Equal(t, scale, s)
		}
	}
	ctx := context.Background()
	legacy, err := GetFunctionByName(ctx, "+", []types.Type{types.New(types.T_time, 0, 0), types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_decimal64, 18, 0), legacy.retType)
	square, err := GetFunctionByName(ctx, "*", []types.Type{legacy.retType, legacy.retType})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), square.retType)
	product, err := GetFunctionByName(ctx, "*", []types.Type{square.retType, types.New(types.T_decimal64, 1, 0)})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_decimal256, 40, 0), product.retType)
}

func TestDecimalRevenueIntermediateStays128(t *testing.T) {
	ctx := context.Background()
	price := types.New(types.T_decimal64, 15, 2)
	discount, err := GetFunctionByName(ctx, "-", []types.Type{types.T_int64.ToType(), price})
	require.NoError(t, err)
	revenue, err := GetFunctionByName(ctx, "*", []types.Type{price, discount.retType})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_decimal128, 37, 4), revenue.retType)
	for _, arg := range revenue.targetTypes {
		require.NotEqual(t, types.T_decimal256, arg.Oid)
	}
}

func TestDecimalMultiplyRetainsBIGINTCastDomain(t *testing.T) {
	integer := types.T_int64.ToType()
	decimal := types.New(types.T_decimal64, 15, 2)
	for _, reverse := range []bool{false, true} {
		inputs := []types.Type{integer, decimal}
		want := []types.Type{types.New(types.T_decimal64, 19, 0), decimal}
		if reverse {
			inputs[0], inputs[1] = inputs[1], inputs[0]
			want[0], want[1] = want[1], want[0]
		}
		resolved, err := GetFunctionByName(context.Background(), "*", inputs)
		require.NoError(t, err)
		require.Equal(t, want, resolved.targetTypes)
		require.Equal(t, types.New(types.T_decimal128, 34, 2), resolved.retType)
	}
}

func TestDecimalReturnTypeUnknownDomainKeepsCarrierEnvelope(t *testing.T) {
	for _, tc := range []struct {
		carrier types.T
		width   int32
	}{
		{types.T_decimal128, 38},
		{types.T_decimal256, 65},
	} {
		inputs := []types.Type{types.New(tc.carrier, 20, 2), types.New(types.T_time, 0, 3)}
		require.Equal(t, types.New(tc.carrier, tc.width, 3), decimalAddSubReturnType(inputs))
		require.Equal(t, types.New(tc.carrier, tc.width, 5), decimalMultiplyReturnType(inputs))
	}
}

func TestDecimalMixedCarrierMultiplyKeepsCheckedRoute(t *testing.T) {
	for _, scale := range []int32{0, 2} {
		for _, width := range []int32{0, 18} {
			for _, reverse := range []bool{false, true} {
				args := []types.Type{types.New(types.T_decimal128, 20, scale), types.New(types.T_decimal64, width, scale)}
				if reverse {
					args[0], args[1] = args[1], args[0]
				}
				resolved, err := GetFunctionByName(t.Context(), "*", args)
				require.NoError(t, err)
				require.Equal(t, types.New(types.T_decimal128, 38, 2*scale), resolved.retType)
				for _, target := range resolved.targetTypes {
					require.Equal(t, types.T_decimal128, target.Oid)
				}
			}
		}
	}
	// An exact declared domain already beyond the D128 ceiling stays wide.
	for _, args := range [][]types.Type{
		{types.New(types.T_decimal128, 21, 0), types.New(types.T_decimal64, 18, 0)},
		{types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal128, 19, 0)},
	} {
		resolved, err := GetFunctionByName(t.Context(), "*", args)
		require.NoError(t, err)
		require.Equal(t, types.T_decimal256, resolved.retType.Oid)
	}
}

func TestDecimal128CarrierEnvelopeBoundsFollowingMultiply(t *testing.T) {
	for _, width := range []int32{0, 38} {
		resolved, err := GetFunctionByName(t.Context(), "*", []types.Type{types.New(types.T_decimal128, width, 0), types.New(types.T_decimal64, 17, 0)})
		require.NoError(t, err)
		require.Equal(t, types.New(types.T_decimal256, 56, 0), resolved.retType)
	}
}
