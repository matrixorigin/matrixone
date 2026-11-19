// Copyright 2021 Matrix Origin
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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDecimalMul128By64Exact(t *testing.T) {
	cases := []struct {
		x    string
		y    int64
		want string
	}{
		{"0", math.MinInt64, "0"},
		{"-1", math.MinInt64, "9223372036854775808"},
		{"9223372036854775808", 2, "18446744073709551616"},
		{"18446744073709551615", 2, "36893488147419103230"},
		{"170141183460469231731687303715884105727", -1, "-170141183460469231731687303715884105727"},
		{"-170141183460469231731687303715884105728", -1, "170141183460469231731687303715884105728"},
		{"-170141183460469231731687303715884105728", math.MinInt64, "1569275433846670190958947355801916604025588861116008628224"},
	}
	for _, tc := range cases {
		t.Run(tc.x, func(t *testing.T) {
			// Parse through the wide carrier so signed-128 endpoints are independent
			// of the public 38-digit Decimal128 parser limit.
			wide, err := types.ParseDecimal256(tc.x, 76, 0)
			require.NoError(t, err)
			x := types.Decimal128{B0_63: wide.B0_63, B64_127: wide.B64_127}
			original := x
			got := decimalMul128By64(x, types.Decimal64(tc.y))
			require.Equal(t, tc.want, got.Format(0))
			require.Equal(t, original, x)
		})
	}
}

func TestDecimalAddSub128Exact(t *testing.T) {
	for _, tc := range []struct{ x, y, sum, difference string }{
		{"0", "0", "0", "0"},
		{"-1", "1", "0", "-2"},
		{"18446744073709551615", "1", "18446744073709551616", "18446744073709551614"},
		{"170141183460469231731687303715884105727", "1", "170141183460469231731687303715884105728", "170141183460469231731687303715884105726"},
		{"-170141183460469231731687303715884105728", "-1", "-170141183460469231731687303715884105729", "-170141183460469231731687303715884105727"},
		{"-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "-1", "-340282366920938463463374607431768211455"},
	} {
		parse := func(s string) types.Decimal128 {
			v, err := types.ParseDecimal256(s, 76, 0)
			require.NoError(t, err)
			return types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
		}
		x, y := parse(tc.x), parse(tc.y)
		sum, difference := decimalAdd128Exact(x, y), decimalSub128Exact(x, y)
		require.Equal(t, tc.sum, sum.Format(0))
		require.Equal(t, tc.difference, difference.Format(0))
	}
}

func TestDecimalArithmeticChunkWidthsAndScales(t *testing.T) {
	for _, a := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, b := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
			for _, r := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
				for _, op := range []int32{PLUS, MINUS, MULTI} {
					t.Run(fmt.Sprintf("%d/%d/%d/%d", a, b, r, op), func(t *testing.T) {
						mp := mpool.MustNewZero()
						t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
						makeVec := func(oid types.T, scale int32, coefficient int64) *vector.Vector {
							v := vector.NewVec(types.New(oid, 6, scale))
							t.Cleanup(func() { v.Free(mp) })
							switch oid {
							case types.T_decimal64:
								require.NoError(t, vector.AppendFixed(v, types.Decimal64(coefficient), false, mp))
							case types.T_decimal128:
								require.NoError(t, vector.AppendFixed(v, types.Decimal128{B0_63: uint64(coefficient)}, false, mp))
							default:
								require.NoError(t, vector.AppendFixed(v, types.Decimal256FromInt64(coefficient), false, mp))
							}
							return v
						}
						scale := int32(2)
						expected := "1.23"
						if op == MINUS {
							expected = "1.17"
						}
						if op == MULTI {
							scale = 3
							expected = "0.036"
						}
						x, y, out := makeVec(a, 1, 12), makeVec(b, 2, 3), makeVec(r, scale, 0)
						require.NoError(t, DecimalArithmeticChunk(op, x, y, out, 0, 0, 0, 1))
						var got string
						switch r {
						case types.T_decimal64:
							got = vector.GetFixedAtNoTypeCheck[types.Decimal64](out, 0).Format(scale)
						case types.T_decimal128:
							got = vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(scale)
						default:
							got = vector.GetFixedAtNoTypeCheck[types.Decimal256](out, 0).Format(scale)
						}
						require.Equal(t, expected, got)
					})
				}
			}
		}
	}
}
