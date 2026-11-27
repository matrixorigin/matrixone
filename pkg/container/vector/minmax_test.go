// Copyright 2026 Matrix Origin
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

package vector

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"

	"golang.org/x/exp/rand"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBoolMinMax(t *testing.T) {
	for _, tc := range []struct {
		name         string
		values       []bool
		nulls        []int
		ok, min, max bool
	}{
		{name: "empty"},
		{name: "false", values: []bool{false, false}, ok: true},
		{name: "true", values: []bool{true, true}, ok: true, min: true, max: true},
		{name: "mixed", values: []bool{false, true}, ok: true, max: true},
		{name: "mixed reversed", values: []bool{true, false}, ok: true, max: true},
		{name: "nullable mixed", values: []bool{true, false, true}, nulls: []int{0}, ok: true, max: true},
		{name: "null false", values: []bool{false, true}, nulls: []int{0}, ok: true, min: true, max: true},
		{name: "null true", values: []bool{false, true}, nulls: []int{1}, ok: true},
		{name: "all null", values: []bool{false, true}, nulls: []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_bool.ToType(), mp)
			defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
			for i, value := range tc.values {
				require.NoError(t, AppendFixed(v, value, slices.Contains(tc.nulls, i), mp))
			}
			ok, min, max := v.GetMinMaxValue()
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.min, types.DecodeBool(min))
				require.Equal(t, tc.max, types.DecodeBool(max))
			}
		})
	}
}

// minMaxFixture builds a bool or int64 vector with a NULL pattern ("none",
// "sparse", "dense", "all"), owned by a pool or a read-only reference.
func minMaxFixture[T types.FixedSizeT](tb testing.TB, typ types.T, values []T, pattern, backing string) *Vector {
	tb.Helper()
	mp := mpool.MustNewZero()
	tb.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(tb, mp.CurrNB())
		require.Zero(tb, mp.OnHeapCurrNB())
	})
	v := NewVec(typ.ToType(), mp)
	tb.Cleanup(func() { v.Free(mp) })
	if backing == "readonly" {
		// A read-only reference vector over memory the test keeps alive.
		v = NewVecWithData(typ.ToType(), len(values), types.EncodeSlice(values), nil, nil)
	} else {
		require.NoError(tb, AppendFixedList(v, values, nil, mp))
	}
	validity := bytes.Repeat([]byte{255}, (len(values)+10)/8)
	nulls := 0
	for i := range values {
		isNull := pattern == "all" || pattern == "sparse" && i%17 == 0 || pattern == "dense" && i%3 != 0
		if isNull {
			nulls++
			if backing == "readonly" {
				validity[(i+3)/8] &^= 1 << uint((i+3)%8)
			} else {
				v.SetNull(uint64(i))
			}
		}
	}
	if backing == "readonly" && nulls != 0 {
		// A read-only vector's NULLs reference an encoded bitmap, as a
		// cached block's do.
		var owned bitmap.Bitmap
		require.NoError(tb, owned.InitWithInvertedBits(validity, 3, len(values), mpool.MustNewZero()))
		encoded := owned.Marshal()
		owned.Free()
		require.NoError(tb, v.GetNulls().ReadNoCopy(encoded))
	}
	return v
}

// GetMinMaxValue over bool and int64 columns of every size around the int64
// kernel's 32-row threshold and word boundaries, with NULL masks and owned or
// read-only backing; it must not modify the vector.
func TestGetMinMaxValueFixed(t *testing.T) {
	for _, n := range []int{0, 1, 2, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65, 129} {
		for _, pattern := range []string{"false", "true", "early", "late", "alternating"} {
			for _, nulls := range []string{"none", "sparse", "dense", "all"} {
				for _, backing := range []string{"owned", "readonly"} {
					t.Run(fmt.Sprintf("%d/%s/%s/%s", n, pattern, nulls, backing), func(t *testing.T) {
						bs, ints := minMaxValues(n, pattern)
						for _, v := range []*Vector{minMaxFixture(t, types.T_bool, bs, nulls, backing), minMaxFixture(t, types.T_int64, ints, nulls, backing)} {
							before, err := v.MarshalBinary()
							require.NoError(t, err)
							var found, wantMinB, wantMaxB bool
							var wantMinI, wantMaxI int64
							for row := 0; row < n; row++ {
								if v.IsNull(uint64(row)) {
									continue
								}
								if !found {
									wantMinB, wantMaxB = bs[row], bs[row]
									wantMinI, wantMaxI = ints[row], ints[row]
								} else {
									wantMinB, wantMaxB = wantMinB && bs[row], wantMaxB || bs[row]
									if ints[row] < wantMinI {
										wantMinI = ints[row]
									}
									if ints[row] > wantMaxI {
										wantMaxI = ints[row]
									}
								}
								found = true
							}
							ok, lo, hi := v.GetMinMaxValue()
							require.Equal(t, found, ok)
							if found {
								if v.GetType().Oid == types.T_bool {
									require.Equal(t, wantMinB, types.DecodeBool(lo))
									require.Equal(t, wantMaxB, types.DecodeBool(hi))
								} else {
									require.Equal(t, wantMinI, types.DecodeInt64(lo))
									require.Equal(t, wantMaxI, types.DecodeInt64(hi))
								}
							}
							after, err := v.MarshalBinary()
							require.NoError(t, err)
							require.Equal(t, before, after)
							if backing == "readonly" && n != 0 {
								require.True(t, v.IsReadOnly())
							}
						}
					})
				}
			}
		}
	}
}

func TestGetMinMaxValueConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, n := range []int{1, 32, 2048} {
		for _, b := range []bool{false, true} {
			v, err := NewConstFixed(types.T_bool.ToType(), b, n, mp)
			require.NoError(t, err)
			func() {
				defer v.Free(mp)
				ok, lo, hi := v.GetMinMaxValue()
				require.True(t, ok)
				require.Equal(t, b, types.DecodeBool(lo))
				require.Equal(t, b, types.DecodeBool(hi))
			}()
		}
		if n == 1 {
			v, err := NewConstFixed(types.T_int64.ToType(), int64(math.MinInt64), n, mp)
			require.NoError(t, err)
			func() {
				defer v.Free(mp)
				ok, lo, hi := v.GetMinMaxValue()
				require.True(t, ok)
				require.Equal(t, int64(math.MinInt64), types.DecodeInt64(lo))
				require.Equal(t, int64(math.MinInt64), types.DecodeInt64(hi))
			}()
		}
	}
	require.Zero(t, mp.CurrNB())
}

// The int64 kernel over windows at an offset, for each value order.
func TestGetMinMaxValueInt64Windows(t *testing.T) {
	for _, n := range []int{31, 32, 33, 34, 35, 65} {
		for _, order := range []string{"ascending", "descending", "constant", "alternating"} {
			xs := make([]int64, n+6)
			for i := range xs {
				xs[i] = int64(i)
			}
			if order == "descending" {
				slices.Reverse(xs)
			}
			if order == "constant" {
				for i := range xs {
					xs[i] = math.MinInt64
				}
			}
			if order == "alternating" {
				for i := range xs {
					if i%2 == 0 {
						xs[i] = math.MaxInt64
					} else {
						xs[i] = math.MinInt64
					}
				}
			}
			for _, backing := range []string{"owned", "readonly"} {
				v := minMaxFixture(t, types.T_int64, xs, "none", backing)
				w, err := v.Window(3, n+3)
				require.NoError(t, err)
				t.Cleanup(func() { w.Free(w.MPool()) })
				want := slices.Clone(xs[3 : n+3])
				slices.Sort(want)
				ok, lo, hi := w.GetMinMaxValue()
				require.True(t, ok)
				require.Equal(t, want[0], types.DecodeInt64(lo))
				require.Equal(t, want[len(want)-1], types.DecodeInt64(hi))
			}
		}
	}
}

// The unrolled kernels agree with a plain loop for every integer-backed type
// and for float32/float64 (NaN skipped), at sizes around the unroll threshold
// and width, through GetMinMaxValue.
func TestGetMinMaxValueUnrolledKernels(t *testing.T) {
	sizes := []int{1, 4, 5, 31, 32, 33, 34, 35, 64, 65, 129}
	t.Run("integer", func(t *testing.T) {
		checkIntegerMinMax[int8](t, types.T_int8, sizes)
		checkIntegerMinMax[int16](t, types.T_int16, sizes)
		checkIntegerMinMax[int32](t, types.T_int32, sizes)
		checkIntegerMinMax[int64](t, types.T_int64, sizes)
		checkIntegerMinMax[uint8](t, types.T_uint8, sizes)
		checkIntegerMinMax[uint16](t, types.T_uint16, sizes)
		checkIntegerMinMax[uint32](t, types.T_uint32, sizes)
		checkIntegerMinMax[uint64](t, types.T_uint64, sizes)
		checkIntegerMinMax[uint64](t, types.T_bit, sizes)
		checkIntegerMinMax[types.Date](t, types.T_date, sizes)
		checkIntegerMinMax[types.MoYear](t, types.T_year, sizes)
		checkIntegerMinMax[types.Datetime](t, types.T_datetime, sizes)
		checkIntegerMinMax[types.Time](t, types.T_time, sizes)
		checkIntegerMinMax[types.Timestamp](t, types.T_timestamp, sizes)
		checkIntegerMinMax[types.Enum](t, types.T_enum, sizes)
	})
	t.Run("float", func(t *testing.T) {
		checkFloatMinMax[float32](t, types.T_float32, sizes)
		checkFloatMinMax[float64](t, types.T_float64, sizes)
	})
}

func checkIntegerMinMax[T integerT](t *testing.T, oid types.T, sizes []int) {
	t.Helper()
	r := rand.New(rand.NewSource(uint64(oid)))
	var zero T
	minT, maxT := zero, ^zero
	if maxT < minT { // signed: ^0 is -1; shift 1 up to the sign bit for the minimum
		for minT = 1; minT > 0; minT <<= 1 {
		}
		maxT = ^minT
	}
	for _, n := range sizes {
		for _, order := range []string{"ascending", "descending", "constant", "random", "extremes"} {
			t.Run(fmt.Sprintf("%s/%d/%s", oid, n, order), func(t *testing.T) {
				xs := make([]T, n)
				for i := range xs {
					switch order {
					case "ascending":
						xs[i] = T(i)
					case "descending":
						xs[i] = T(n - i)
					case "constant":
						xs[i] = minT
					case "random":
						xs[i] = T(r.Uint64())
					case "extremes":
						xs[i] = T(r.Uint64())
						if i == n/3 {
							xs[i] = minT
						}
						if i == n-1 {
							xs[i] = maxT
						}
					}
				}
				wantLo, wantHi := xs[0], xs[0]
				for _, x := range xs {
					wantLo, wantHi = min(wantLo, x), max(wantHi, x)
				}
				mp := mpool.MustNewZero()
				v := NewVec(oid.ToType(), mp)
				defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
				require.NoError(t, AppendFixedList(v, xs, nil, mp))
				ok, lo, hi := v.GetMinMaxValue()
				require.True(t, ok)
				require.Equal(t, wantLo, types.DecodeFixed[T](lo))
				require.Equal(t, wantHi, types.DecodeFixed[T](hi))
			})
		}
	}
}

func checkFloatMinMax[T ~float32 | ~float64](t *testing.T, oid types.T, sizes []int) {
	t.Helper()
	r := rand.New(rand.NewSource(uint64(oid)))
	nan, inf := T(math.NaN()), T(math.Inf(1))
	for _, n := range sizes {
		for _, shape := range []string{"random", "sparse-nan", "nan-first", "all-nan", "infinities", "signed-zero"} {
			t.Run(fmt.Sprintf("%s/%d/%s", oid, n, shape), func(t *testing.T) {
				xs := make([]T, n)
				for i := range xs {
					xs[i] = T(r.NormFloat64() * 1e6)
					switch shape {
					case "sparse-nan":
						if i%5 == 2 {
							xs[i] = nan
						}
					case "nan-first":
						if i < 4 {
							xs[i] = nan
						}
					case "all-nan":
						xs[i] = nan
					case "infinities":
						if i == n/2 {
							xs[i] = -inf
						}
						if i == n-1 {
							xs[i] = inf
						}
					case "signed-zero":
						xs[i] = T(math.Copysign(0, float64(1-2*(i%2))))
					}
				}
				var wantLo, wantHi T
				found := false
				for _, x := range xs {
					if x != x {
						continue
					}
					if !found {
						wantLo, wantHi, found = x, x, true
					}
					wantLo, wantHi = min(wantLo, x), max(wantHi, x)
				}
				mp := mpool.MustNewZero()
				v := NewVec(oid.ToType(), mp)
				defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
				require.NoError(t, AppendFixedList(v, xs, nil, mp))
				ok, lo, hi := v.GetMinMaxValue()
				require.Equal(t, found, ok)
				if found {
					// compare as values: +0 and -0 are equal bounds
					require.True(t, wantLo == types.DecodeFixed[T](lo), "min %v, want %v", types.DecodeFixed[T](lo), wantLo)
					require.True(t, wantHi == types.DecodeFixed[T](hi), "max %v, want %v", types.DecodeFixed[T](hi), wantHi)
				}
			})
		}
	}
}

func minMaxValues(n int, pattern string) ([]bool, []int64) {
	bs, ints := make([]bool, n), make([]int64, n)
	for i := range bs {
		switch pattern {
		case "true":
			bs[i] = true
		case "early":
			bs[i] = i != 0
		case "late":
			bs[i] = i == n-1
		case "alternating":
			bs[i] = i%2 == 0
		}
		ints[i] = int64(i*7919%65537) - 32768
	}
	if n > 0 {
		ints[n/2] = math.MinInt64
		ints[n-1] = math.MaxInt64
	}
	return bs, ints
}

var minMaxOK bool
var minMaxLo, minMaxHi []byte

func BenchmarkGetMinMaxValue(b *testing.B) {
	for _, n := range []int{1, 8, 31, 32, 128, 2048, 65536} {
		for _, pattern := range []string{"false", "true", "early", "late", "alternating"} {
			for _, mask := range []string{"none", "sparse", "dense", "all"} {
				b.Run(fmt.Sprintf("bool/%s/%s/%d", pattern, mask, n), func(b *testing.B) {
					xs, _ := minMaxValues(n, pattern)
					v := minMaxFixture(b, types.T_bool, xs, mask, "owned")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						minMaxOK, minMaxLo, minMaxHi = v.GetMinMaxValue()
					}
				})
			}
		}
		for _, mask := range []string{"none", "sparse", "dense", "all"} {
			b.Run(fmt.Sprintf("int64/%s/%d", mask, n), func(b *testing.B) {
				_, xs := minMaxValues(n, "alternating")
				v := minMaxFixture(b, types.T_int64, xs, mask, "owned")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					minMaxOK, minMaxLo, minMaxHi = v.GetMinMaxValue()
				}
			})
		}
	}
}

// BenchmarkGetMinMaxValueByType covers the unrolled kernels for each integer
// and float width on null-free random columns.
func BenchmarkGetMinMaxValueByType(b *testing.B) {
	for _, n := range []int{128, 2048, 65536} {
		benchMinMaxType[int8](b, types.T_int8, n)
		benchMinMaxType[int16](b, types.T_int16, n)
		benchMinMaxType[int32](b, types.T_int32, n)
		benchMinMaxType[int64](b, types.T_int64, n)
		benchMinMaxType[float32](b, types.T_float32, n)
		benchMinMaxType[float64](b, types.T_float64, n)
	}
}

func benchMinMaxType[T integerT | ~float32 | ~float64](b *testing.B, oid types.T, n int) {
	b.Run(fmt.Sprintf("%s/%d", oid, n), func(b *testing.B) {
		r := rand.New(rand.NewSource(1))
		xs := make([]T, n)
		for i := range xs {
			xs[i] = T(r.Int63n(1 << 20))
		}
		mp := mpool.MustNewZero()
		v := NewVec(oid.ToType(), mp)
		b.Cleanup(func() { v.Free(mp) })
		require.NoError(b, AppendFixedList(v, xs, nil, mp))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			minMaxOK, minMaxLo, minMaxHi = v.GetMinMaxValue()
		}
	})
}

func BenchmarkGetMinMaxValueInt64Distributions(b *testing.B) {
	for _, n := range []int{1, 8, 31, 32, 33, 128, 2048, 65536} {
		for _, pattern := range []string{"ascending", "descending", "constant", "alternating", "random"} {
			for _, mask := range []string{"none", "sparse"} {
				b.Run(fmt.Sprintf("%s/%s/%d", pattern, mask, n), func(b *testing.B) {
					_, xs := minMaxValues(n, "alternating")
					for i := range xs {
						switch pattern {
						case "ascending":
							xs[i] = int64(i) - int64(n/2)
						case "descending":
							xs[i] = int64(n/2) - int64(i)
						case "constant":
							xs[i] = math.MinInt64
						case "alternating":
							if i%2 == 0 {
								xs[i] = math.MinInt64
							} else {
								xs[i] = math.MaxInt64
							}
						}
					}
					v := minMaxFixture(b, types.T_int64, xs, mask, "owned")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						minMaxOK, minMaxLo, minMaxHi = v.GetMinMaxValue()
					}
				})
			}
		}
	}
}
