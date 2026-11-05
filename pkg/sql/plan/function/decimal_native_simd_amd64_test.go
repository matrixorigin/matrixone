//go:build amd64 && go1.27 && goexperiment.simd

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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"math"
	"math/big"
	"simd/archsimd"
	"testing"
)

func TestDecimalNativePrefixes(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	bounds := []int64{math.MinInt32, math.MaxInt32, -1, 0, 1, 123456}
	for _, n := range []int{0, 1, 31, 32, 33, 64, 257} {
		t.Run(fmt.Sprintf("rows%d", n), func(t *testing.T) {
			a, b := make([]types.Decimal64, n), make([]types.Decimal64, n)
			for i := range a {
				a[i] = types.Decimal64(bounds[i%len(bounds)])
				b[i] = types.Decimal64(bounds[(i+1)%len(bounds)])
			}
			sentinel := types.Decimal128{B0_63: 17, B64_127: 29}
			fresh := func() []types.Decimal128 {
				v := make([]types.Decimal128, n)
				for i := range v {
					v[i] = sentinel
				}
				return v
			}
			wantPrefix := 0
			if n >= decimalSIMDMultiplyMinRows {
				wantPrefix = n &^ 3
			}
			for _, broadcast := range []bool{false, true} {
				x := a
				if broadcast && n > 0 {
					x = a[:1]
				}
				out := fresh()
				rows := decimal64MulSIMDPrefix(x, b, out, n)
				want := wantPrefix
				if n == 1 {
					want = 0
				}
				require.Equal(t, want, rows)
				for i := 0; i < rows; i++ {
					at := i
					if broadcast {
						at = 0
					}
					z := new(big.Int).Mul(big.NewInt(int64(x[at])), big.NewInt(int64(b[i])))
					require.Equal(t, z.String(), out[i].Format(0))
				}
				for _, v := range out[rows:] {
					require.Equal(t, sentinel, v)
				}
				if n > 0 {
					actual := fresh()
					require.NoError(t, d64Mul(x, b, actual, 2, 2, new(nulls.Nulls)))
					for i, v := range actual {
						at := i
						if broadcast {
							at = 0
						}
						z := new(big.Int).Mul(big.NewInt(int64(x[at])), big.NewInt(int64(b[i])))
						require.Equal(t, z.String(), v.Format(0))
					}
				}
			}
			fullBounds := []int64{math.MinInt64, math.MaxInt64, -1, 0, 1}
			for i := range a {
				a[i] = types.Decimal64(fullBounds[i%len(fullBounds)])
			}
			out := fresh()
			rows := decimal64WidenSIMDPrefix(a, out, n)
			want := 0
			if n >= decimalSIMDCastMinRows {
				want = n &^ 3
			}
			require.Equal(t, want, rows)
			for i := 0; i < rows; i++ {
				require.Equal(t, big.NewInt(int64(a[i])).String(), out[i].Format(0))
			}
			for _, v := range out[rows:] {
				require.Equal(t, sentinel, v)
			}
		})
	}
	for _, c := range []int64{100, math.MaxInt64, math.MinInt64} {
		for _, bad := range []int{-1, 0, 1, 2, 3, 4, 5, 6, 7, 32, 33, 34, 35} {
			t.Run(fmt.Sprintf("fused/c%d/bad%d", c, bad), func(t *testing.T) {
				const n = 37
				a, b := make([]types.Decimal64, n), make([]types.Decimal64, n)
				for i := range a {
					a[i] = 123456
					b[i] = types.Decimal64(c - 7)
				}
				if c == math.MinInt64 {
					for i := range b {
						b[i] = types.Decimal64(c + 7)
					}
				}
				if bad >= 0 {
					minimum := int64(math.MinInt64)
					a[bad] = types.Decimal64(minimum)
					b[bad] = types.Decimal64(math.MaxInt64)
				}
				sentinel := types.Decimal128{B0_63: 17, B64_127: 29}
				out := make([]types.Decimal128, n)
				for i := range out {
					out[i] = sentinel
				}
				rows := decimalMulSubSIMDPrefix(a, b, types.Decimal64(c), out)
				want := n &^ 3
				if bad >= 0 {
					want = bad &^ 3
				}
				require.Equal(t, want, rows)
				for _, v := range out[rows:] {
					require.Equal(t, sentinel, v)
				}
				require.True(t, decimalMulSub64(a, b, types.Decimal64(c), out))
				for i, v := range out {
					z := new(big.Int).Sub(big.NewInt(c), big.NewInt(int64(b[i])))
					z.Mul(z, big.NewInt(int64(a[i])))
					require.Equal(t, z.String(), v.Format(0))
				}
			})
		}
	}
	for _, pair := range [][2]int64{{math.MaxInt64, math.MinInt64}, {math.MinInt64, math.MaxInt64}} {
		for lane := 0; lane < 4; lane++ {
			a, b := make([]types.Decimal64, 37), make([]types.Decimal64, 37)
			for i := range a {
				a[i] = 3
				b[i] = types.Decimal64(pair[0])
			}
			b[4+lane] = types.Decimal64(pair[1])
			out := make([]types.Decimal128, 37)
			require.Equal(t, 4, decimalMulSubSIMDPrefix(a, b, types.Decimal64(pair[0]), out))
			require.True(t, decimalMulSub64(a, b, types.Decimal64(pair[0]), out))
			for i, v := range out {
				z := new(big.Int).Sub(big.NewInt(pair[0]), big.NewInt(int64(b[i])))
				z.Mul(z, big.NewInt(int64(a[i])))
				require.Equal(t, z.String(), v.Format(0))
			}
		}
	}

	// Every wide lane is refused before publication; the ordinary owner completes
	// the suffix with exact full-int64 multiplication.
	for lane := 0; lane < 4; lane++ {
		a, b := make([]types.Decimal64, 37), make([]types.Decimal64, 37)
		for i := range a {
			a[i] = 3
			b[i] = 7
		}
		a[4+lane] = types.Decimal64(math.MaxInt64)
		out := make([]types.Decimal128, len(a))
		require.Equal(t, 4, decimal64MulSIMDPrefix(a, b, out, len(a)))
		require.NoError(t, d64Mul(a, b, out, 0, 0, new(nulls.Nulls)))
		for i, v := range out {
			z := new(big.Int).Mul(big.NewInt(int64(a[i])), big.NewInt(int64(b[i])))
			require.Equal(t, z.String(), v.Format(0))
		}
	}
}
