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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"math/big"
	"simd/archsimd"
	"testing"
)

type decimal128NativeTestOp struct {
	name     string
	minimum  int
	subtract bool
	prefix   func([]types.Decimal128, []types.Decimal128, []types.Decimal128, int) int
	owner    func([]types.Decimal128, []types.Decimal128, []types.Decimal128, *nulls.Nulls) int
}

func decimal128NativeModel(v types.Decimal128) *big.Int {
	hi := new(big.Int).Lsh(big.NewInt(int64(v.B64_127)), 64)
	return hi.Add(hi, new(big.Int).SetUint64(v.B0_63))
}

func checkDecimal128Native(t *testing.T, op decimal128NativeTestOp, a, b []types.Decimal128, n int, alias string, masked []uint64) {
	t.Helper()
	limit := new(big.Int).Lsh(big.NewInt(1), 127)
	minimum := new(big.Int).Neg(new(big.Int).Set(limit))
	modulus := new(big.Int).Lsh(big.NewInt(1), 128)
	for _, prefixOnly := range []bool{false, true} {
		if prefixOnly && len(masked) != 0 {
			continue
		}
		x, y := make([]types.Decimal128, len(a)), make([]types.Decimal128, len(b))
		copy(x, a)
		copy(y, b)
		sentinel := types.Decimal128{B0_63: 17, B64_127: 29}
		storage := make([]types.Decimal128, n+2)
		for i := range storage {
			storage[i] = sentinel
		}
		out := storage[1 : n+1]
		if alias == "left" {
			copy(out, x)
			x = out[:len(x)]
		} else if alias == "right" {
			copy(out, y)
			y = out[:len(y)]
		}
		initial := append([]types.Decimal128(nil), out...)
		want := make([]types.Decimal128, n)
		copy(want, out)
		np := nulls.NewWithSize(n)
		for _, row := range masked {
			np.Add(row)
		}
		firstError := -1
		for i := 0; i < n; i++ {
			if np.Contains(uint64(i)) {
				continue
			}
			xi, yi := i, i
			if len(a) == 1 {
				xi = 0
			}
			if len(b) == 1 {
				yi = 0
			}
			av, bv := decimal128NativeModel(a[xi]), decimal128NativeModel(b[yi])
			z := new(big.Int).Add(av, bv)
			if op.subtract {
				z.Sub(av, bv)
			}
			overflow := z.Cmp(minimum) < 0 || z.Cmp(limit) >= 0
			u := new(big.Int).Mod(z, modulus)
			lo := u.Uint64()
			want[i] = types.Decimal128{B0_63: lo, B64_127: u.Rsh(u, 64).Uint64()}
			if overflow {
				firstError = i
				break
			}
		}
		info := fmt.Sprintf("op=%s n=%d lens=%d/%d alias=%s prefix=%t first-error=%d", op.name, n, len(a), len(b), alias, prefixOnly, firstError)
		if prefixOnly {
			count := op.prefix(x, y, out, n)
			wantCount := 0
			if n >= op.minimum {
				wantCount = n &^ 7
				if firstError >= 0 {
					wantCount = min(wantCount, firstError&^7)
				}
			}
			require.Equal(t, wantCount, count, info)
			copy(want[count:], initial[count:])
		} else {
			require.Equal(t, firstError, op.owner(x, y, out, np), info)
		}
		require.Equal(t, want, out, info)
		require.Equal(t, sentinel, storage[0], info)
		require.Equal(t, sentinel, storage[n+1], info)
		if alias != "left" {
			require.Equal(t, a, x, info)
		}
		if alias != "right" {
			require.Equal(t, b, y, info)
		}
	}
}

func TestDecimal128NativeChecked(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, op := range []decimal128NativeTestOp{
		{"add", decimalSIMDAdd128MinRows, false, decimal128AddSIMDPrefix, d128AddSameScale},
		{"sub", decimalSIMDSub128MinRows, true, decimal128SubSIMDPrefix, d128SubSameScale},
	} {
		t.Run(op.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 7, 8, op.minimum - 1, op.minimum, op.minimum + 1, 255, 256, 257} {
				a, b := make([]types.Decimal128, n), make([]types.Decimal128, n)
				for i := range a {
					a[i] = types.Decimal128{B0_63: ^uint64(i % 7), B64_127: uint64(int64(i%7-3) << 50)}
					b[i] = types.Decimal128{B0_63: uint64(i%5 + 1), B64_127: uint64(int64(i%5-2) << 49)}
				}
				for _, shape := range []string{"flat", "left_const", "right_const"} {
					x, y := a, b
					if n > 0 && shape == "left_const" {
						x = a[:1]
					} else if n > 0 && shape == "right_const" {
						y = b[:1]
					}
					for _, alias := range []string{"none", "left", "right"} {
						checkDecimal128Native(t, op, x, y, n, alias, nil)
					}
				}
			}
			for _, negative := range []bool{false, true} {
				for _, row := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 127, 128, 255, 256} {
					max := types.Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0) >> 1}
					min := types.Decimal128{B64_127: 1 << 63}
					av, bv := max, types.Decimal128FromInt64(1)
					if negative {
						av, bv = min, types.Decimal128FromInt64(-1)
					}
					if op.subtract {
						bv = types.Decimal128FromInt64(-int64(bv.B0_63))
					}
					for _, shape := range []string{"flat", "left_const", "right_const"} {
						a, b := make([]types.Decimal128, 257), make([]types.Decimal128, 257)
						a[row], b[row] = av, bv
						if shape == "left_const" {
							a = []types.Decimal128{av}
						} else if shape == "right_const" {
							b = []types.Decimal128{bv}
						}
						for _, alias := range []string{"none", "left", "right"} {
							checkDecimal128Native(t, op, a, b, 257, alias, nil)
							checkDecimal128Native(t, op, a, b, 257, alias, []uint64{uint64(row)})
						}
					}
				}
			}
		})
	}
}

func TestDecimal128PreparedExact(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, op := range []int32{PLUS, MINUS} {
		for _, rt := range []types.T{types.T_decimal128, types.T_decimal256} {
			t.Run(fmt.Sprintf("op%d/%s", op, rt.String()), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				x, y := vector.NewVec(types.New(types.T_decimal128, 38, 0)), vector.NewVec(types.New(types.T_decimal128, 38, 0))
				got, want := vector.NewVec(types.New(rt, 65, 0)), vector.NewVec(types.New(rt, 65, 0))
				for _, v := range []*vector.Vector{x, y, got, want} {
					t.Cleanup(func() { v.Free(mp) })
				}
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
			})
		}
	}
}
