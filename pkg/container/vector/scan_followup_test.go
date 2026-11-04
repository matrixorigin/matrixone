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

package vector

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/stretchr/testify/require"
)

func scanFixture[T types.FixedSizeT](tb testing.TB, typ types.T, values []T, pattern, backing string) *Vector {
	tb.Helper()
	mp := mpool.MustNewZero()
	tb.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(tb, mp.CurrNB())
		require.Zero(tb, mp.OnHeapCurrNB())
	})
	v := NewVec(typ.ToType())
	tb.Cleanup(func() { v.Free(mp) })
	if backing == "borrowed" {
		lease, err := NewRefCountedBufferLease(nil, 0, nil)
		require.NoError(tb, err)
		tb.Cleanup(lease.Release)
		require.NoError(tb, v.InstallBorrowedData(types.EncodeSlice(values), lease))
		v.SetLength(len(values))
	} else {
		require.NoError(tb, AppendFixedList(v, values, nil, mp))
	}
	validity := bytes.Repeat([]byte{255}, (len(values)+10)/8)
	nulls := 0
	for i := range values {
		isNull := pattern == "all" || pattern == "sparse" && i%17 == 0 || pattern == "dense" && i%3 != 0
		if isNull {
			nulls++
			if backing == "borrowed" {
				validity[(i+3)/8] &^= 1 << uint((i+3)%8)
			} else {
				v.SetNull(uint64(i))
			}
		}
	}
	if backing == "borrowed" && nulls != 0 {
		lease, err := NewRefCountedBufferLease(nil, 0, nil)
		require.NoError(tb, err)
		tb.Cleanup(lease.Release)
		require.NoError(tb, v.GetNulls().InstallBorrowedValidity(validity, 3, len(values), nulls, lease))
	}
	return v
}

func TestScanFollowupMinMax(t *testing.T) {
	for _, n := range []int{0, 1, 2, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65, 129} {
		for _, pattern := range []string{"false", "true", "early", "late", "alternating"} {
			for _, nulls := range []string{"none", "sparse", "dense", "all"} {
				for _, backing := range []string{"owned", "borrowed"} {
					t.Run(fmt.Sprintf("%d/%s/%s/%s", n, pattern, nulls, backing), func(t *testing.T) {
						bs, ints := scanValues(n, pattern)
						for _, v := range []*Vector{scanFixture(t, types.T_bool, bs, nulls, backing), scanFixture(t, types.T_int64, ints, nulls, backing)} {
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
							if backing == "borrowed" && n != 0 {
								require.NotNil(t, v.dataLease)
								if v.HasNull() {
									require.True(t, v.GetNulls().HasBorrowedValidity())
								}
							}
						}
					})
				}
			}
		}
	}
}

func TestScanFollowupConst(t *testing.T) {
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

func TestScanFollowupInt64OrderAndWindows(t *testing.T) {
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
			for _, backing := range []string{"owned", "borrowed"} {
				v := scanFixture(t, types.T_int64, xs, "none", backing)
				w, err := v.Window(3, n+3)
				require.NoError(t, err)
				t.Cleanup(func() { w.Free(nil) })
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

func scanValues(n int, pattern string) ([]bool, []int64) {
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

var scanMin, scanMax []byte
var scanOK bool
var scanChecksum uint64

func BenchmarkScanBorrowed(b *testing.B) {
	for _, n := range []int{8, 32, 2048} {
		for _, pattern := range []string{"false", "early", "late"} {
			for _, mask := range []string{"none", "sparse", "dense"} {
				b.Run(fmt.Sprintf("bool/%s/%s/%d", pattern, mask, n), func(b *testing.B) {
					xs, _ := scanValues(n, pattern)
					v := scanFixture(b, types.T_bool, xs, mask, "borrowed")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						scanOK, scanMin, scanMax = v.GetMinMaxValue()
					}
				})
			}
		}
		for _, mask := range []string{"none", "sparse", "dense"} {
			b.Run(fmt.Sprintf("int64/%s/%d", mask, n), func(b *testing.B) {
				_, xs := scanValues(n, "alternating")
				v := scanFixture(b, types.T_int64, xs, mask, "borrowed")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scanOK, scanMin, scanMax = v.GetMinMaxValue()
				}
			})
		}
	}
}

func BenchmarkScanConstants(b *testing.B) {
	for _, value := range []bool{false, true} {
		b.Run(fmt.Sprint(value), func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(b, mp.CurrNB()) })
			v, err := NewConstFixed(types.T_bool.ToType(), value, 2048, mp)
			require.NoError(b, err)
			b.Cleanup(func() { v.Free(mp) })
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanOK, scanMin, scanMax = v.GetMinMaxValue()
			}
		})
	}
}

// These controls consume every physical element and must not be interpreted as
// a speedup in the unchanged raw checksum loop.
//
//go:noinline
func scanRawInt64(xs []int64) uint64 {
	var s uint64
	for _, v := range xs {
		s += uint64(v)
	}
	return s
}

//go:noinline
func scanRawBool(xs []bool) uint64 {
	var s uint64
	for _, v := range xs {
		if v {
			s++
		}
	}
	return s
}

//go:noinline
func scanRawBytes(xs []types.Varlena, area []byte) uint64 {
	var s uint64
	for i := range xs {
		for _, v := range xs[i].GetByteSlice(area) {
			s += uint64(v)
		}
	}
	return s
}

func BenchmarkScanFollowup(b *testing.B) {
	for _, n := range []int{1, 8, 31, 32, 128, 2048, 65536} {
		for _, pattern := range []string{"false", "true", "early", "late", "alternating"} {
			for _, mask := range []string{"none", "sparse", "dense", "all"} {
				b.Run(fmt.Sprintf("bool/%s/%s/%d", pattern, mask, n), func(b *testing.B) {
					xs, _ := scanValues(n, pattern)
					v := scanFixture(b, types.T_bool, xs, mask, "owned")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						scanOK, scanMin, scanMax = v.GetMinMaxValue()
					}
				})
			}
		}
		for _, mask := range []string{"none", "sparse", "dense", "all"} {
			b.Run(fmt.Sprintf("int64/%s/%d", mask, n), func(b *testing.B) {
				_, xs := scanValues(n, "alternating")
				v := scanFixture(b, types.T_int64, xs, mask, "owned")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scanOK, scanMin, scanMax = v.GetMinMaxValue()
				}
			})
		}
		b.Run(fmt.Sprintf("raw-int64/%d", n), func(b *testing.B) {
			_, xs := scanValues(n, "alternating")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanChecksum = scanRawInt64(xs)
			}
		})
		b.Run(fmt.Sprintf("raw-bool/%d", n), func(b *testing.B) {
			xs, _ := scanValues(n, "alternating")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanChecksum = scanRawBool(xs)
			}
		})
	}
	for _, width := range []int{8, 23, 24, 64, 1024} {
		for _, n := range []int{8, 128, 2048} {
			for _, mode := range []string{"raw-bytes", "character-count"} {
				b.Run(fmt.Sprintf("utf8-vector/%s/%d/%d", mode, width, n), func(b *testing.B) {
					mp := mpool.MustNewZero()
					b.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(b, mp.CurrNB()) })
					v := NewVec(types.T_varbinary.ToType())
					b.Cleanup(func() { v.Free(mp) })
					values := make([][]byte, n)
					for i := range values {
						values[i] = bytes.Repeat([]byte{'a'}, width)
					}
					require.NoError(b, AppendBytesList(v, values, nil, mp))
					col, area := MustVarlenaRawData(v)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if mode == "raw-bytes" {
							scanChecksum = scanRawBytes(col, area)
						} else {
							var sum uint64
							for j := range col {
								sum += lengthutf8.CountUTF8CodePoints(col[j].GetByteSlice(area))
							}
							scanChecksum = sum
						}
					}
				})
			}
		}
	}
}

func BenchmarkScanInt64Distributions(b *testing.B) {
	for _, n := range []int{1, 8, 31, 32, 33, 128, 2048, 65536} {
		for _, pattern := range []string{"ascending", "descending", "constant", "alternating", "random"} {
			for _, mask := range []string{"none", "sparse"} {
				b.Run(fmt.Sprintf("%s/%s/%d", pattern, mask, n), func(b *testing.B) {
					_, xs := scanValues(n, "alternating")
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
					v := scanFixture(b, types.T_int64, xs, mask, "owned")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						scanOK, scanMin, scanMax = v.GetMinMaxValue()
					}
				})
			}
		}
	}
}
