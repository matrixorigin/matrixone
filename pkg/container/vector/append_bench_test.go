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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// BenchmarkAppendValueRows measures the public append boundary used by CSV
// materialization and casts. Each operation reuses physical capacity for one
// object block; metadata controls retain the same value and row-count oracles.
// Performance acceptance compares matched binaries, without a timing threshold
// in ordinary unit tests.
func BenchmarkAppendValueRows(b *testing.B) {
	const rows = 8192
	for _, test := range []struct {
		name   string
		oid    types.T
		value  []byte
		isNull bool
		seed   bool
	}{
		{name: "ordinary_int64", oid: types.T_int64},
		{name: "null_int64", oid: types.T_int64, isNull: true},
		{name: "ordinary_inline_varchar", oid: types.T_varchar, value: []byte("ordinary column")},
		{name: "ordinary_area_varchar", oid: types.T_varchar, value: bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1)},
		{name: "null_varchar", oid: types.T_varchar, isNull: true},
		{name: "seeded_varchar", oid: types.T_varchar, value: []byte("ordinary column"), seed: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			vec := NewVec(test.oid.ToType())
			b.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				vec.Free(mp)
				require.Zero(b, mp.CurrNB())
			})
			require.NoError(b, vec.PreExtend(rows, mp))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				vec.ResetWithSameType()
				start := 0
				if test.seed {
					if test.oid == types.T_int64 {
						if err := AppendFixed(vec, int64(42), false, mp); err != nil {
							b.Fatal(err)
						}
					} else if err := AppendBytes(vec, test.value, false, mp); err != nil {
						b.Fatal(err)
					}
					start = 1
				}
				if test.oid == types.T_int64 {
					for row := start; row < rows; row++ {
						if err := AppendFixed(vec, int64(42), test.isNull, mp); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					for row := start; row < rows; row++ {
						if err := AppendBytes(vec, test.value, test.isNull, mp); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.StopTimer()
			require.Equal(b, rows, vec.Length())
			require.Equal(b, test.isNull, vec.IsNull(rows-1))
			if !test.isNull {
				if test.oid == types.T_int64 {
					require.Equal(b, int64(42), GetFixedAtNoTypeCheck[int64](vec, rows-1))
				} else {
					require.Equal(b, test.value, vec.GetBytesAt(rows-1))
				}
			}
		})
	}
}

// BenchmarkAppendFixedListReuse measures one public bulk append into a vector
// whose physical capacity is retained between iterations.
func BenchmarkAppendFixedListReuse(b *testing.B) {
	for _, size := range []int{1, 8, 15, 16, 128, 2048} {
		for _, mode := range []string{"flat", "flat_mask", "mixed"} {
			if mode == "mixed" && size < 128 {
				continue
			}
			b.Run(mode+"_"+strconv.Itoa(size), func(b *testing.B) {
				mp := mpool.MustNewZero()
				vec := NewVec(types.T_int64.ToType())
				b.Cleanup(func() {
					defer mpool.DeleteMPool(mp)
					vec.Free(mp)
					require.Zero(b, mp.CurrNB())
					require.Zero(b, mp.OnHeapCurrNB())
				})
				values := make([]int64, size)
				for i := range values {
					values[i] = int64(i)
				}
				var nulls []bool
				if mode != "flat" {
					nulls = make([]bool, size)
					if mode == "mixed" {
						for i := 0; i < size; i += 8 {
							nulls[i] = true
						}
					}
				}
				if err := vec.PreExtend(size, mp); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					vec.ResetWithSameType()
					if err := AppendFixedList(vec, values, nulls, mp); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				require.Equal(b, size, vec.Length())
				for i := range values {
					if mode == "mixed" && nulls[i] {
						require.True(b, vec.IsNull(uint64(i)))
					} else {
						require.False(b, vec.IsNull(uint64(i)))
						require.Equal(b, values[i], GetFixedAtNoTypeCheck[int64](vec, i))
					}
				}
			})
		}
	}
}

var appendVarlenBenchSink int

// BenchmarkAppendVarlenList measures public bulk appends for non-inline values.
// Cold makes a new vector per operation; reuse retains descriptor and area
// capacity. The source values are prepared outside the timer.
func BenchmarkAppendVarlenList(b *testing.B) {
	const payloadLength = 64
	for _, size := range []int{1, 8, 15, 16, 128, 2048} {
		payload := bytes.Repeat([]byte{'x'}, payloadLength)
		byteValues := make([][]byte, size)
		stringValues := make([]string, size)
		for i := range byteValues {
			byteValues[i] = payload
			stringValues[i] = string(payload)
		}
		for _, tc := range []struct {
			name string
			oid  types.T
			add  func(*Vector, *mpool.MPool) error
		}{
			{"bytes_varbinary", types.T_varbinary, func(v *Vector, mp *mpool.MPool) error {
				return AppendBytesList(v, byteValues, nil, mp)
			}},
			{"string_varchar", types.T_varchar, func(v *Vector, mp *mpool.MPool) error {
				return AppendStringList(v, stringValues, nil, mp)
			}},
		} {
			for _, reuse := range []bool{false, true} {
				mode := "cold"
				if reuse {
					mode = "reuse"
				}
				b.Run(tc.name+"_"+mode+"_"+strconv.Itoa(size), func(b *testing.B) {
					mp := mpool.MustNewZero()
					b.Cleanup(func() {
						defer mpool.DeleteMPool(mp)
						require.Zero(b, mp.CurrNB())
						require.Zero(b, mp.OnHeapCurrNB())
					})
					check := func(v *Vector) {
						require.Equal(b, size, v.Length())
						require.Equal(b, payload, v.GetBytesAt(0))
						require.Equal(b, payload, v.GetBytesAt(size-1))
					}
					if reuse {
						vec := NewVec(tc.oid.ToType())
						b.Cleanup(func() { vec.Free(mp) })
						require.NoError(b, tc.add(vec, mp))
						check(vec)
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							vec.ResetWithSameType()
							if err := tc.add(vec, mp); err != nil {
								b.Fatal(err)
							}
							appendVarlenBenchSink += len(vec.GetBytesAt(size - 1))
						}
						b.StopTimer()
						check(vec)
					} else {
						func() {
							vec := NewVec(tc.oid.ToType())
							defer vec.Free(mp)
							require.NoError(b, tc.add(vec, mp))
							check(vec)
						}()
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							vec := NewVec(tc.oid.ToType())
							if err := tc.add(vec, mp); err != nil {
								vec.Free(mp)
								b.Fatal(err)
							}
							appendVarlenBenchSink += len(vec.GetBytesAt(size - 1))
							vec.Free(mp)
						}
					}
				})
			}
		}
	}
}
