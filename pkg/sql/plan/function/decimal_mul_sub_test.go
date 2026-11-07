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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"math"
	"math/big"
	"testing"
)

func TestDecimalMulSub64ExactDomain(t *testing.T) {
	values := []int64{0, 1, -1, 100, -100, math.MinInt32 - 1, math.MinInt32, math.MaxInt32, math.MaxInt32 + 1, math.MaxInt64, math.MinInt64}
	for _, c := range values {
		for _, x := range values {
			for _, y := range values {
				out := make([]types.Decimal128, 1)
				require.True(t, decimalMulSub64([]types.Decimal64{types.Decimal64(x)}, []types.Decimal64{types.Decimal64(y)}, types.Decimal64(c), out))
				want := new(big.Int).Sub(big.NewInt(c), big.NewInt(y))
				want.Mul(want, big.NewInt(x))
				require.Equal(t, want.String(), out[0].Format(0), "x=%d c=%d y=%d", x, c, y)
			}
		}
	}
	require.False(t, decimalMulSub64(nil, []types.Decimal64{1}, 0, nil))
}

func TestDecimalMulSubOneRowVectors(t *testing.T) {
	for shape := 0; shape < 3; shape++ {
		for _, xn := range []bool{false, true} {
			for _, yn := range []bool{false, true} {
				t.Run(fmt.Sprintf("shape%d/null%v/%v", shape, xn, yn), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
					input := func(value types.Decimal64, constant, isNull bool) *vector.Vector {
						typ := types.New(types.T_decimal64, 15, 2)
						if constant {
							if isNull {
								v := vector.NewConstNull(typ, 1, mp)
								t.Cleanup(func() { v.Free(mp) })
								return v
							}
							v, err := vector.NewConstFixed(typ, value, 1, mp)
							require.NoError(t, err)
							t.Cleanup(func() { v.Free(mp) })
							return v
						}
						v := vector.NewVec(typ)
						t.Cleanup(func() { v.Free(mp) })
						require.NoError(t, vector.AppendFixed(v, value, isNull, mp))
						return v
					}
					x, y := input(123456, shape == 1, xn), input(5, shape == 2, yn)
					out := vector.NewVec(types.New(types.T_decimal128, 38, 4))
					t.Cleanup(func() { out.Free(mp) })
					require.NoError(t, out.PreExtend(1, mp))
					out.SetLength(1)
					require.True(t, TryDecimalMulSub64(x, y, 100, out))
					require.Equal(t, xn || yn, out.IsNull(0))
					if !xn && !yn {
						require.Equal(t, "1172.8320", vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(4))
						for _, pair := range [][2]int64{{math.MaxInt64, -5}, {math.MinInt64, 5}} {
							vector.MustFixedColNoTypeCheck[types.Decimal64](y)[0] = types.Decimal64(pair[1])
							require.True(t, TryDecimalMulSub64(x, y, types.Decimal64(pair[0]), out))
							want := new(big.Int).Sub(big.NewInt(pair[0]), big.NewInt(pair[1]))
							want.Mul(want, big.NewInt(123456))
							require.Equal(t, want.String(), vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(0))
						}
					}
				})
			}
		}
	}
}

func BenchmarkDecimalMulSub64(b *testing.B) {
	for _, n := range []int{1, 256, 8192} {
		b.Run(fmtSize(n), func(b *testing.B) {
			x, y := make([]types.Decimal64, n), make([]types.Decimal64, n)
			out := make([]types.Decimal128, n)
			for i := range x {
				x[i] = 123456
				y[i] = 5
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				decimalMulSub64(x, y, 100, out)
			}
		})
	}
}
func fmtSize(n int) string {
	if n == 1 {
		return "one"
	}
	if n == 256 {
		return "caller256"
	}
	return "block8192"
}
