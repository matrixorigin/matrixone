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

func TestUnionFixedSelection(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_TS, types.T_decimal128, types.T_uuid, types.T_Blockid, types.T_Rowid, types.T_decimal256} {
		for _, index32 := range []bool{false, true} {
			for _, nullable := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/int32=%t/nullable=%t", oid, index32, nullable), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					src, dst := NewVec(oid.ToType()), NewVec(oid.ToType())
					t.Cleanup(func() { src.Free(mp); dst.Free(mp) })
					require.NoError(t, src.PreExtend(4, mp))
					src.SetLength(4)
					width := src.typ.TypeSize()
					expected := make([][]byte, 4)
					for row := range expected {
						expected[row] = make([]byte, width)
						for column := range width {
							expected[row][column] = byte(row*32 + column + 1)
						}
						copy(src.data[row*width:(row+1)*width], expected[row])
					}
					src.GetGrouping().Add(2)
					if nullable {
						src.GetNulls().Add(1)
					}
					require.NoError(t, src.SetStringSource(types.StringSourceLiteral))
					view, err := src.Window(1, 4)
					require.NoError(t, err)
					t.Cleanup(func() { view.Free(mp) })
					for range 2 {
						dst.CleanOnlyData()
						// Preserve an existing prefix, then select from a borrowed
						// source with repetitions, a tail row and reverse order.
						require.NoError(t, dst.UnionOne(src, 0, mp))
						if index32 {
							require.NoError(t, dst.UnionInt32(view, nil, mp))
							require.Equal(t, 1, dst.Length())
							require.NoError(t, dst.UnionInt32(view, []int32{2, 1, 1, 0}, mp))
						} else {
							require.NoError(t, dst.Union(view, nil, mp))
							require.Equal(t, 1, dst.Length())
							require.NoError(t, dst.Union(view, []int64{2, 1, 1, 0}, mp))
						}
						require.Equal(t, 5, dst.Length())
						for row, sourceRow := range []int{0, 3, 2, 2, 1} {
							isNull := nullable && sourceRow == 1
							require.Equal(t, isNull, dst.IsNull(uint64(row)))
							require.Equal(t, sourceRow == 2, dst.GetGrouping().Contains(uint64(row)))
							require.Equal(t, types.StringSourceLiteral, dst.GetStringSourceAt(row))
							if !isNull {
								require.Equal(t, expected[sourceRow], dst.data[row*width:(row+1)*width])
							}
						}
					}
					// Self-append must read the original prefix after growth has
					// replaced the backing allocation.
					oldLength := dst.Length()
					copies := cap(dst.data)/width + 1
					if index32 {
						rows := make([]int32, copies)
						for i := range rows {
							rows[i] = 1
						}
						require.NoError(t, dst.UnionInt32(dst, rows, mp))
					} else {
						rows := make([]int64, copies)
						for i := range rows {
							rows[i] = 1
						}
						require.NoError(t, dst.Union(dst, rows, mp))
					}
					require.Equal(t, oldLength+copies, dst.Length())
					for row := oldLength; row < dst.Length(); row++ {
						require.Equal(t, expected[3], dst.data[row*width:(row+1)*width])
						require.False(t, dst.IsNull(uint64(row)))
					}
					// Results own their copied bytes and cannot change with the source.
					clear(src.data)
					require.Equal(t, expected[3], dst.data[width:2*width])
				})
			}
		}
	}
}
