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
		{"carry promotes decimal64", "+", types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 19, 2)},
		{"subtraction promotes decimal64", "-", types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal128, 19, 0)},
		{"small sum stays decimal64", "+", types.New(types.T_decimal64, 5, 2), types.New(types.T_decimal64, 5, 2), types.New(types.T_decimal64, 6, 2)},
		{"mixed integer multiplication", "*", types.T_int64.ToType(), decimal15, types.New(types.T_decimal128, 34, 2)},
		{"mixed unsigned multiplication", "*", types.T_uint64.ToType(), decimal15, types.New(types.T_decimal128, 35, 2)},
		{"actual wide multiplication", "*", types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal256, 40, 0)},
		{"actual wide addition", "+", types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal256, 39, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), tc.op, []types.Type{tc.left, tc.right})
			require.NoError(t, err)
			require.Equal(t, tc.want, resolved.retType)
		})
	}
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
