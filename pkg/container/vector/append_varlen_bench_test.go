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
