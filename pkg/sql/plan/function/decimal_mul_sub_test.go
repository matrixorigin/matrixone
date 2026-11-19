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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"math"
	"math/big"
	"testing"
)

func TestDecimalMulSub64ExactDomain(t *testing.T) {
	values := []int64{0, 1, -1, 100, -100, math.MaxInt64, math.MinInt64}
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
