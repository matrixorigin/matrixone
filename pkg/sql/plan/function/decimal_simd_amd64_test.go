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
	"math"
	"simd/archsimd"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestDecimalSIMDExactAndFallback(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, op := range []int32{PLUS, MINUS, MULTI} {
		for _, wide := range []bool{false, true} {
			for _, n := range []int{255, 256, 257, 263} {
				if wide && op != MULTI {
					continue
				}
				for _, rt := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
					if op == MULTI && rt == types.T_decimal64 || op != MULTI && rt == types.T_decimal256 {
						continue
					}
					t.Run(fmt.Sprintf("%d/%v/%d/%d", op, wide, n, rt), func(t *testing.T) {
						mp := mpool.MustNewZero()
						t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
						typ := types.New(types.T_decimal64, 18, 0)
						if wide {
							typ = types.New(types.T_decimal128, 38, 0)
						}
						x, y := vector.NewVec(typ), vector.NewVec(types.New(types.T_decimal64, 18, 0))
						got, want := vector.NewVec(types.New(rt, 65, 0)), vector.NewVec(types.New(rt, 65, 0))
						for _, v := range []*vector.Vector{x, y, got, want} {
							t.Cleanup(func() { v.Free(mp) })
						}
						for i := 0; i < n+2; i++ {
							a, b := int64(i%7-3), int64(i%5-2)
							if op == MULTI {
								bounds := []int64{math.MinInt32, math.MaxInt32, -1, 0, 1}
								a, b = bounds[i%len(bounds)], bounds[(i+1)%len(bounds)]
							}
							if op != MULTI && rt == types.T_decimal128 {
								bounds := []int64{math.MaxInt64, math.MaxInt64, math.MinInt64, math.MinInt64, math.MaxInt64, -1, 0, 1}
								a = bounds[i%len(bounds)]
								b = bounds[(i+1)%len(bounds)]
							}
							if wide {
								require.NoError(t, vector.AppendFixed(x, types.Decimal128{B0_63: uint64(a), B64_127: uint64(a >> 63)}, false, mp))
							} else {
								require.NoError(t, vector.AppendFixed(x, types.Decimal64(a), false, mp))
							}
							require.NoError(t, vector.AppendFixed(y, types.Decimal64(b), false, mp))
						}
						require.NoError(t, got.PreExtend(n+2, mp))
						got.SetLength(n + 2)
						require.NoError(t, want.PreExtend(n+2, mp))
						want.SetLength(n + 2)
						clear(got.GetData())
						clear(want.GetData())
						require.NoError(t, decimalArithmeticChunkScalar(op, x, y, want, 1, 1, 1, n))
						require.Equal(t, n >= 256, tryDecimalSIMD(op, x, y, got, 1, 1, 1, n))
						if n >= 256 {
							require.Equal(t, want.GetData(), got.GetData())
						}
						if op == MULTI && n >= 256 {
							// A later full-width value ends vectorization without losing the prefix.
							if wide {
								vector.MustFixedColNoTypeCheck[types.Decimal128](x)[5] = types.Decimal128{B0_63: 1, B64_127: 1}
							} else {
								vector.MustFixedColNoTypeCheck[types.Decimal64](x)[5] = types.Decimal64(math.MaxInt64)
							}
							require.NoError(t, decimalArithmeticChunkScalar(op, x, y, want, 1, 1, 1, n))
							require.True(t, tryDecimalSIMD(op, x, y, got, 1, 1, 1, n))
							require.Equal(t, want.GetData(), got.GetData())
							if wide {
								vector.MustFixedColNoTypeCheck[types.Decimal128](x)[1] = types.Decimal128{B0_63: 1, B64_127: 1}
							} else {
								vector.MustFixedColNoTypeCheck[types.Decimal64](x)[1] = types.Decimal64(math.MaxInt64)
							}
							before := append([]byte(nil), got.GetData()...)
							require.False(t, tryDecimalSIMD(op, x, y, got, 1, 1, 1, n))
							require.Equal(t, before, got.GetData())
						}
					})
				}
			}
		}
	}
}

func BenchmarkDecimalSIMDKernels(b *testing.B) {
	if !archsimd.X86.AVX2() {
		b.Skip("AVX2 unavailable")
	}
	for _, op := range []int32{PLUS, MINUS, MULTI} {
		for _, wide := range []bool{false, true} {
			if wide && op != MULTI {
				continue
			}
			for _, enabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("op%d/wide%v/simd%v", op, wide, enabled), func(b *testing.B) {
					mp := mpool.MustNewZero()
					typ := types.New(types.T_decimal64, 15, 0)
					if wide {
						typ = types.New(types.T_decimal128, 31, 0)
					}
					x, y, out := vector.NewVec(typ), vector.NewVec(types.New(types.T_decimal64, 15, 0)), vector.NewVec(types.New(types.T_decimal128, 38, 0))
					if wide {
						out = vector.NewVec(types.New(types.T_decimal256, 65, 0))
					}
					defer x.Free(mp)
					defer y.Free(mp)
					defer out.Free(mp)
					for i := 0; i < 256; i++ {
						if wide {
							if err := vector.AppendFixed(x, types.Decimal128{B0_63: 123456}, false, mp); err != nil {
								b.Fatal(err)
							}
						} else {
							if err := vector.AppendFixed(x, types.Decimal64(123456), false, mp); err != nil {
								b.Fatal(err)
							}
						}
						if err := vector.AppendFixed(y, types.Decimal64(95), false, mp); err != nil {
							b.Fatal(err)
						}
					}
					if err := out.PreExtend(256, mp); err != nil {
						b.Fatal(err)
					}
					out.SetLength(256)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if enabled {
							if !tryDecimalSIMD(op, x, y, out, 0, 0, 0, 256) {
								b.Fatal("declined")
							}
						} else {
							if err := decimalArithmeticChunkScalar(op, x, y, out, 0, 0, 0, 256); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

func TestDecimalSIMDBroadcastAndDecline(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	typ := types.New(types.T_decimal64, 15, 0)
	x, err := vector.NewConstFixed(typ, types.Decimal64(100), 257, mp)
	require.NoError(t, err)
	t.Cleanup(func() { x.Free(mp) })
	y := vector.NewVec(typ)
	t.Cleanup(func() { y.Free(mp) })
	out, want := vector.NewVec(types.New(types.T_decimal128, 38, 0)), vector.NewVec(types.New(types.T_decimal128, 38, 0))
	for _, v := range []*vector.Vector{out, want} {
		t.Cleanup(func() { v.Free(mp) })
		require.NoError(t, v.PreExtend(257, mp))
		v.SetLength(257)
		clear(v.GetData())
	}
	for i := 0; i < 257; i++ {
		value := int64(i%13 - 6)
		require.NoError(t, vector.AppendFixed(y, types.Decimal64(value), false, mp))
	}
	for _, op := range []int32{PLUS, MINUS, MULTI} {
		require.NoError(t, decimalArithmeticChunkScalar(op, x, y, want, 0, 0, 0, 257))
		require.True(t, tryDecimalSIMD(op, x, y, out, 0, 0, 0, 257))
		require.Equal(t, want.GetData(), out.GetData())
	}
	// Every lane of the first block must reject coefficients immediately
	// outside signed32, including modular-addition wraparound at signed64 limits.
	for _, value := range []int64{math.MinInt32 - 1, math.MaxInt32 + 1, math.MinInt64, math.MaxInt64} {
		for lane := 0; lane < 4; lane++ {
			t.Run(fmt.Sprintf("decline_%d_lane%d", value, lane), func(t *testing.T) {
				values := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
				old := values[lane]
				t.Cleanup(func() { values[lane] = old })
				values[lane] = types.Decimal64(value)
				before := append([]byte(nil), out.GetData()...)
				require.False(t, tryDecimalSIMD(MULTI, x, y, out, 0, 0, 0, 257))
				require.Equal(t, before, out.GetData())
			})
		}
	}
	before := append([]byte(nil), out.GetData()...)
	y.GetNulls().Add(0)
	require.False(t, tryDecimalSIMD(MULTI, x, y, out, 0, 0, 0, 257))
	require.Equal(t, before, out.GetData())
	y.GetNulls().Del(0)
	out.GetType().Scale = 1
	require.False(t, tryDecimalSIMD(MULTI, x, y, out, 0, 0, 0, 257))
	require.Equal(t, before, out.GetData())
}

func BenchmarkDecimalSIMDRefusal(b *testing.B) {
	for _, n := range []int{1, 255, 256, 8192} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows%d/simd%v", n, enabled), func(b *testing.B) {
				mp := mpool.MustNewZero()
				x := vector.NewVec(types.New(types.T_decimal128, 38, 0))
				y := vector.NewVec(types.New(types.T_decimal64, 18, 0))
				out := vector.NewVec(types.New(types.T_decimal256, 65, 0))
				defer x.Free(mp)
				defer y.Free(mp)
				defer out.Free(mp)
				for i := 0; i < n; i++ {
					if err := vector.AppendFixed(x, types.Decimal128{B0_63: 1, B64_127: 1}, false, mp); err != nil {
						b.Fatal(err)
					}
					if err := vector.AppendFixed(y, types.Decimal64(95), false, mp); err != nil {
						b.Fatal(err)
					}
				}
				if err := out.PreExtend(n, mp); err != nil {
					b.Fatal(err)
				}
				out.SetLength(n)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if enabled {
						if n >= 256 && TryDecimalArithmeticSIMD(MULTI, x, y, out, 0, 0, 0, n) {
							continue
						}
						if err := DecimalArithmeticChunk(MULTI, x, y, out, 0, 0, 0, n); err != nil {
							b.Fatal(err)
						}
					} else {
						if err := decimalArithmeticChunkScalar(MULTI, x, y, out, 0, 0, 0, n); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// Frozen pre-SIMD dispatch is a test-only reference; no duplicate production path.
func decimalArithmeticChunkScalar(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) error {
	if op == MULTI && (out.GetType().Oid == types.T_decimal256 || out.GetType().Oid == types.T_decimal128) {
		if x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal128 {
			x, y = y, x
			xi, yi = yi, xi
		}
		if x.GetType().Oid == types.T_decimal128 && y.GetType().Oid == types.T_decimal64 {
			a := vector.MustFixedColNoTypeCheck[types.Decimal128](x)
			b := vector.MustFixedColNoTypeCheck[types.Decimal64](y)
			xm, ym := -1, -1
			if x.IsConst() {
				xm = 0
			}
			if y.IsConst() {
				ym = 0
			}
			if out.GetType().Oid == types.T_decimal128 {
				dst := vector.MustFixedColNoTypeCheck[types.Decimal128](out)
				for i := 0; i < n; i++ {
					v := decimalMul128By64(a[(xi+i)&xm], b[(yi+i)&ym])
					dst[oi+i] = types.Decimal128{B0_63: v.B0_63, B64_127: v.B64_127}
				}
			} else {
				dst := vector.MustFixedColNoTypeCheck[types.Decimal256](out)
				for i := 0; i < n; i++ {
					dst[oi+i] = decimalMul128By64(a[(xi+i)&xm], b[(yi+i)&ym])
				}
			}
			return nil
		}
	}
	if x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal64 && (op == MULTI || (x.GetType().Scale == out.GetType().Scale && y.GetType().Scale == out.GetType().Scale)) {
		if decimalChunk64(op, x, y, out, xi, yi, oi, n) {
			return nil
		}
	}
	sx, sy := int32(0), int32(0)
	if op != MULTI {
		sx = out.GetType().Scale - x.GetType().Scale
		sy = out.GetType().Scale - y.GetType().Scale
	}
	switch x.GetType().Oid {
	case types.T_decimal64:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal64](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	case types.T_decimal128:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal128](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	default:
		return decimalChunkRight(op, vector.MustFixedColNoTypeCheck[types.Decimal256](x), y, out, xi, yi, oi, n, x.IsConst(), sx, sy)
	}
}
