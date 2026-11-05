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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"math/big"
	"simd/archsimd"
	"testing"
)

func TestDecimal128SIMDCheckedPublication(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	sentinel := types.Decimal128{B0_63: 17, B64_127: 29}
	max := types.Decimal128{B0_63: ^uint64(0), B64_127: uint64(1)<<63 - 1}
	min := types.Decimal128{B64_127: uint64(1) << 63}
	signed := func(v types.Decimal128) *big.Int {
		z := new(big.Int).SetUint64(v.B64_127)
		z.Lsh(z, 64)
		z.Add(z, new(big.Int).SetUint64(v.B0_63))
		if int64(v.B64_127) < 0 {
			z.Sub(z, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		return z
	}
	limit := new(big.Int).Lsh(big.NewInt(1), 127)
	modulus := new(big.Int).Lsh(big.NewInt(1), 128)
	for _, op := range []int32{PLUS, MINUS} {
		for _, n := range []int{256, 257, 263} {
			for _, shape := range []int{0, 1, 2} {
				for _, bad := range []int{-1, 0, 1, 2, 3, 255, 256} {
					if bad >= n {
						continue
					}
					a, b := make([]types.Decimal128, n), make([]types.Decimal128, n)
					for i := range a {
						a[i] = types.Decimal128{B0_63: uint64(i + 1)}
						b[i] = types.Decimal128{B0_63: 1}
					}
					if shape == 1 {
						a = a[:1]
					}
					if shape == 2 {
						b = b[:1]
					}
					am, bm := -1, -1
					if len(a) == 1 {
						am = 0
					}
					if len(b) == 1 {
						bm = 0
					}
					if bad >= 0 {
						a[bad&am] = max
						if op == MINUS {
							a[bad&am] = min
						}
					}
					want, got := make([]types.Decimal128, n), make([]types.Decimal128, n)
					for i := range got {
						got[i] = sentinel
						want[i] = sentinel
					}
					first := -1
					for i := 0; i < n; i++ {
						z := signed(a[i&am])
						if op == PLUS {
							z.Add(z, signed(b[i&bm]))
						} else {
							z.Sub(z, signed(b[i&bm]))
						}
						overflow := z.Cmp(limit) >= 0 || z.Cmp(new(big.Int).Neg(limit)) < 0
						raw := new(big.Int).Mod(z, modulus)
						want[i] = types.Decimal128{B0_63: raw.Uint64(), B64_127: new(big.Int).Rsh(raw, 64).Uint64()}
						if overflow {
							first = i
							break
						}
					}
					idx, ok := tryDecimal128SIMD(op, a, b, got)
					require.True(t, ok)
					require.Equal(t, first, idx)
					require.Equal(t, want, got)
					// Reuse the first operand as output, including a broadcast backed by that output.
					inplace := make([]types.Decimal128, n)
					copy(inplace, a)
					input := inplace
					if len(a) == 1 {
						input = inplace[:1]
					}
					original := append([]types.Decimal128(nil), inplace...)
					idx, ok = tryDecimal128SIMD(op, input, b, inplace)
					require.True(t, ok)
					require.Equal(t, first, idx)
					end := n
					if first >= 0 {
						end = first + 1
					}
					require.Equal(t, want[:end], inplace[:end])
					require.Equal(t, original[end:], inplace[end:])
					right := make([]types.Decimal128, n)
					copy(right, b)
					rightInput := right
					if len(b) == 1 {
						rightInput = right[:1]
					}
					untouched := append([]types.Decimal128(nil), right...)
					idx, ok = tryDecimal128SIMD(op, a, rightInput, right)
					require.True(t, ok)
					require.Equal(t, first, idx)
					require.Equal(t, want[:end], right[:end])
					require.Equal(t, untouched[end:], right[end:])

				}
			}
		}
	}
}

func TestDecimal128PreparedExact(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, op := range []int32{PLUS, MINUS} {
		for _, rt := range []types.T{types.T_decimal128, types.T_decimal256} {
			mp := mpool.MustNewZero()
			x, y := vector.NewVec(types.New(types.T_decimal128, 38, 0)), vector.NewVec(types.New(types.T_decimal128, 38, 0))
			got, want := vector.NewVec(types.New(rt, 65, 0)), vector.NewVec(types.New(rt, 65, 0))
			for i := 0; i < 263; i++ {
				a := types.Decimal128FromInt64(int64(i - 130))
				b := types.Decimal128FromInt64(int64(i%7 - 3))
				if rt == types.T_decimal256 {
					a = types.Decimal128{B0_63: ^uint64(0), B64_127: uint64(1)<<63 - 1}
					if i%2 != 0 {
						a = types.Decimal128{B64_127: uint64(1) << 63}
					}
					b = a
					if op == MINUS {
						if i%2 == 0 {
							b = types.Decimal128{B64_127: uint64(1) << 63}
						} else {
							b = types.Decimal128{B0_63: ^uint64(0), B64_127: uint64(1)<<63 - 1}
						}
					}
				}
				require.NoError(t, vector.AppendFixed(x, a, false, mp))
				require.NoError(t, vector.AppendFixed(y, b, false, mp))
			}
			require.NoError(t, got.PreExtend(263, mp))
			got.SetLength(263)
			require.NoError(t, want.PreExtend(263, mp))
			want.SetLength(263)
			require.NoError(t, DecimalArithmeticChunk(op, x, y, want, 1, 1, 1, 261))
			require.True(t, TryDecimalArithmeticSIMD(op, x, y, got, 1, 1, 1, 261))
			require.Equal(t, want.GetData()[rt.ToType().TypeSize():262*rt.ToType().TypeSize()], got.GetData()[rt.ToType().TypeSize():262*rt.ToType().TypeSize()])
			x.Free(mp)
			y.Free(mp)
			got.Free(mp)
			want.Free(mp)
			require.Zero(t, mp.CurrNB())
		}
	}
}
