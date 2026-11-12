// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func BenchmarkUnionFixedSelection(b *testing.B) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_TS, types.T_decimal128, types.T_Blockid, types.T_Rowid, types.T_decimal256} {
		for _, rows := range []int{1, 256} {
			for _, shape := range []string{"flat", "nullable", "constant"} {
				b.Run(fmt.Sprintf("%s/rows=%d/%s", oid, rows, shape), func(b *testing.B) {
					mp := mpool.MustNewZero()
					src, dst := NewVec(oid.ToType()), NewVec(oid.ToType())
					b.Cleanup(func() { src.Free(mp); dst.Free(mp) })
					require.NoError(b, src.PreExtend(1024, mp))
					src.SetLength(1024)
					for i := range src.data {
						src.data[i] = byte(i)
					}
					sels := make([]int64, rows)
					sels32 := make([]int32, rows)
					for i := range sels {
						sels[i] = int64((i * 17) % 1024)
						sels32[i] = int32(sels[i])
					}
					if shape == "nullable" {
						src.GetNulls().Add(17)
					} else if shape == "constant" {
						src.SetClass(CONSTANT)
					}
					require.NoError(b, dst.Union(src, sels, mp))

					for _, index32 := range []bool{false, true} {
						b.Run(fmt.Sprintf("int32=%t", index32), func(b *testing.B) {
							b.ReportAllocs()
							for range b.N {
								dst.CleanOnlyData()
								var err error
								if index32 {
									err = dst.UnionInt32(src, sels32, mp)
								} else {
									err = dst.Union(src, sels, mp)
								}
								if err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		}
	}
}

func BenchmarkUnionFixedSelectionDensity(b *testing.B) {
	for _, width := range []int{4, 8} {
		for _, dense := range []bool{false, true} {
			shape := "sparse"
			if dense {
				shape = "dense"
			}
			b.Run(fmt.Sprintf("width%d/%s", width, shape), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(b, mp.CurrNB()) })
				typ := types.T_uint32.ToType()
				if width == 8 {
					typ = types.T_uint64.ToType()
				}
				src, dst := NewOffHeapVecWithType(typ), NewOffHeapVecWithType(typ)
				b.Cleanup(func() { src.Free(mp); dst.Free(mp) })
				require.NoError(b, src.PreExtend(8192, mp))
				src.SetLength(8192)
				for i := range src.data {
					src.data[i] = byte(i)
				}
				rows := make([]int64, 0, 8192)
				for row := range 8192 {
					if dense && row%20 != 0 || !dense && row%20 == 0 {
						rows = append(rows, int64(row))
					}
				}
				require.NoError(b, dst.Union(src, rows, mp))
				b.SetBytes(int64(len(rows) * width))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					dst.CleanOnlyData()
					if err := dst.Union(src, rows, mp); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
