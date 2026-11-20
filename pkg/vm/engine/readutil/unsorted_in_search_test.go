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

package readutil

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
	"math"
	"slices"
	"strconv"
	"testing"
)

func TestUnsortedIntegerPKIN(t *testing.T) {
	t.Run("int8", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_int8, int8(math.MinInt8), int8(math.MaxInt8)) })
	t.Run("int16", func(t *testing.T) {
		checkUnsortedIntegerPKIN(t, types.T_int16, int16(math.MinInt16), int16(math.MaxInt16))
	})
	t.Run("int32", func(t *testing.T) {
		checkUnsortedIntegerPKIN(t, types.T_int32, int32(math.MinInt32), int32(math.MaxInt32))
	})
	t.Run("int64", func(t *testing.T) {
		checkUnsortedIntegerPKIN(t, types.T_int64, int64(math.MinInt64), int64(math.MaxInt64))
	})
	t.Run("uint8", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_uint8, uint8(0), uint8(math.MaxUint8)) })
	t.Run("uint16", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_uint16, uint16(0), uint16(math.MaxUint16)) })
	t.Run("uint32", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_uint32, uint32(0), uint32(math.MaxUint32)) })
	t.Run("uint64", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_uint64, uint64(0), uint64(math.MaxUint64)) })
	t.Run("bit", func(t *testing.T) { checkUnsortedIntegerPKIN(t, types.T_bit, uint64(0), uint64(math.MaxUint64)) })
}

func checkUnsortedIntegerPKIN[T types.Ints | types.UInts](t *testing.T, oid types.T, minimum, maximum T) {
	keys := []T{minimum, maximum}
	for i := 0; i < 32; i++ {
		keys = append(keys, T(i*2))
	}
	slices.Sort(keys)
	rows := []T{maximum, 9, 6, 50, minimum, 62, 63, 4, 2, 32, 0, maximum}
	for _, mode := range []string{"sorted", "duplicates", "unsorted", "stale_sorted_flag", "tiny"} {
		t.Run(mode, func(t *testing.T) {
			values := slices.Clone(keys)
			switch mode {
			case "duplicates":
				values = append(values, values...)
				slices.Sort(values)
			case "unsorted", "stale_sorted_flag":
				slices.Reverse(values)
			case "tiny":
				values = values[:3]
			}
			mp := mpool.MustNewZero()
			var owned []*vector.Vector
			t.Cleanup(func() {
				for _, v := range owned {
					v.Free(mp)
				}
				require.Zero(t, mp.CurrNB())
			})
			makeVec := func(data []T) *vector.Vector {
				v := vector.NewVec(oid.ToType())
				owned = append(owned, v)
				require.NoError(t, vector.AppendFixedList(v, data, nil, mp))
				return v
			}
			needles := makeVec(values)
			needles.SetSorted(mode != "unsorted")
			block := makeVec(rows)
			for _, fake := range []bool{false, true} {
				filter, err := ConstructBlockPKFilter(fake, BasePKFilter{Valid: true, Op: function.IN, Oid: oid, Vec: needles}, nil)
				require.NoError(t, err)
				if filter.Cleanup != nil {
					t.Cleanup(filter.Cleanup)
				}
				require.True(t, filter.Valid)
				require.Equal(t, fake, filter.HasFakePK)
				want := vector.OrderedLinearSearchOffsetByValFactory(values, nil)(block)
				got := filter.UnSortedSearchFunc(containers.Vectors{*block})
				require.Equal(t, want, got)
				require.Equal(t, want, filter.UnSortedSearchFunc(containers.Vectors{*block}))
			}

			require.Equal(t, values, vector.MustFixedColNoTypeCheck[T](needles))
			require.Equal(t, rows, vector.MustFixedColNoTypeCheck[T](block))
		})
	}
}

var unsortedPKSearchSink []int64

func BenchmarkUnsortedIntegerPKIN(b *testing.B) {
	for _, n := range []int{15, 16, 17, 100, 1000, 8192, 8193} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			keys := make([]uint64, n)
			for i := range keys {
				keys[i] = uint64(i * 7919)
			}
			for _, shape := range []string{"misses", "mixed", "early_match"} {
				b.Run(shape, func(b *testing.B) {
					data := make([]uint64, 8192)
					for i := range data {
						switch shape {
						case "misses":
							data[i] = uint64(i*7919 + 1)
						case "mixed":
							data[i] = uint64((i % 2000) * 7919)
						case "early_match":
							data[i] = keys[0]
						}
					}
					mp := mpool.MustNewZero()
					v := vector.NewVec(types.T_uint64.ToType())
					defer v.Free(mp)
					if err := vector.AppendFixedList(v, data, nil, mp); err != nil {
						b.Fatal(err)
					}
					for _, mode := range []string{"legacy", "bounded", "fake_pk"} {
						b.Run(mode, func(b *testing.B) {
							lookup := vector.OrderedLinearSearchOffsetByValFactory(keys, nil)
							if mode != "legacy" {
								lookup = unsortedIntegerINSearch(keys, mode == "fake_pk")
							}
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								unsortedPKSearchSink = lookup(v)
							}
						})
					}
				})
			}
		})
	}
}

var unsortedPKConstructorSink func(*vector.Vector) []int64

func BenchmarkUnsortedPKConstructor(b *testing.B) {
	for _, n := range []int{16, 100, 1000, 8192, 8193} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			keys := make([]uint64, n)
			for i := range keys {
				keys[i] = uint64(i * 7919)
			}
			for _, fake := range []bool{false, true} {
				b.Run(strconv.FormatBool(fake), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						unsortedPKConstructorSink = unsortedIntegerINSearch(keys, fake)
					}
				})
			}
		})
	}
}
