// Copyright 2022 Matrix Origin
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

package mergeutil

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSortColumnsByIndex(t *testing.T) {
	mp := mpool.MustNewZero()

	const (
		vecNum = 3
		vecLen = 50
	)

	var vecs = make([]*vector.Vector, vecNum)
	for i := 0; i < vecNum; i++ {
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_int32.ToType())
		}

		for j := 0; j < vecLen; j++ {
			x := rand.Int32N(10000)
			err := vector.AppendFixed[int32](vecs[i], x, false, mp)
			require.NoError(t, err)
		}
	}

	for i := 0; i < vecNum; i++ {
		err := SortColumnsByIndex(vecs, i, mp)
		require.NoError(t, err)

		for j := 0; j < vecNum; j++ {
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[j])
			if j == i {
				require.True(t, slices.IsSorted(vals))
				require.True(t, vecs[j].GetSorted())
			} else {
				require.False(t, slices.IsSorted(vals))
				require.False(t, vecs[j].GetSorted())
			}
		}
	}
}

func TestSortColumnsByIndexWithBuf(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("basic reuse across rounds", func(t *testing.T) {
		const (
			vecNum = 3
			vecLen = 50
		)
		var idxBuf []int64
		var shuffleBuf []byte
		for round := 0; round < 3; round++ {
			vecs := make([]*vector.Vector, vecNum)
			for i := 0; i < vecNum; i++ {
				vecs[i] = vector.NewVec(types.T_int32.ToType())
				for j := 0; j < vecLen; j++ {
					require.NoError(t, vector.AppendFixed[int32](vecs[i], rand.Int32N(10000), false, mp))
				}
			}
			require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &idxBuf, &shuffleBuf))
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[0])
			require.True(t, slices.IsSorted(vals))
			require.True(t, vecs[0].GetSorted())
		}
		require.GreaterOrEqual(t, cap(idxBuf), 50)
	})

	t.Run("single element", func(t *testing.T) {
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		require.NoError(t, vector.AppendFixed[int32](vecs[0], int32(42), false, mp))
		var idxBuf []int64
		var shuffleBuf []byte
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &idxBuf, &shuffleBuf))
		require.Equal(t, int32(42), vector.MustFixedColNoTypeCheck[int32](vecs[0])[0])
		require.GreaterOrEqual(t, cap(idxBuf), 1)
	})

	t.Run("buffer grows when vector is larger", func(t *testing.T) {
		var idxBuf []int64 = make([]int64, 4) // deliberately small
		var shuffleBuf []byte
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		for i := 0; i < 100; i++ {
			require.NoError(t, vector.AppendFixed[int32](vecs[0], rand.Int32N(1000), false, mp))
		}
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &idxBuf, &shuffleBuf))
		require.GreaterOrEqual(t, cap(idxBuf), 100) // grew to fit
		require.True(t, slices.IsSorted(vector.MustFixedColNoTypeCheck[int32](vecs[0])))
	})

	t.Run("buffer not reallocated when capacity sufficient", func(t *testing.T) {
		const rows = 50
		var idxBuf []int64 = make([]int64, rows)
		var shuffleBuf []byte
		ptr0 := &idxBuf[0]
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType())}
		for i := 0; i < rows; i++ {
			require.NoError(t, vector.AppendFixed[int32](vecs[0], rand.Int32N(1000), false, mp))
		}
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &idxBuf, &shuffleBuf))
		require.Equal(t, ptr0, &idxBuf[0]) // same backing array
	})

	t.Run("geometry payload column", func(t *testing.T) {
		vecs := []*vector.Vector{
			vector.NewVec(types.T_int32.ToType()),
			vector.NewVec(types.T_geometry.ToType()),
		}
		for _, key := range []int32{3, 1, 2} {
			require.NoError(t, vector.AppendFixed[int32](vecs[0], key, false, mp))
		}
		require.NoError(t, vector.AppendBytesList(vecs[1], [][]byte{
			[]byte("POINT(3 3)"),
			[]byte("POINT(1 1)"),
			[]byte("POINT(2 2)"),
		}, nil, mp))

		var idxBuf []int64
		var shuffleBuf []byte
		require.NoError(t, SortColumnsByIndexWithBuf(vecs, 0, mp, &idxBuf, &shuffleBuf))
		require.Equal(t, []int32{1, 2, 3}, vector.MustFixedColNoTypeCheck[int32](vecs[0]))
		require.Equal(t, [][]byte{
			[]byte("POINT(1 1)"),
			[]byte("POINT(2 2)"),
			[]byte("POINT(3 3)"),
		}, vector.InefficientMustBytesCol(vecs[1]))
	})
}

// A sink may reuse backing or replace it. Both must invalidate the byte/data
// capacity frontier, including after an oversized singleton and bitmap growth.
func TestMergeSortByteBoundAcrossBufferReuse(t *testing.T) {
	for _, replace := range []bool{false, true} {
		for _, bits := range []bool{false, true} {
			t.Run(fmt.Sprintf("replace=%t/bitmaps=%t", replace, bits), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				newBuffer := func() *batch.Batch {
					b := batch.NewWithSize(3)
					b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
					b.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
					return b
				}
				inputs := []*batch.Batch{newBuffer(), newBuffer()}
				payloads := []string{"inline", strings.Repeat("x", 40), strings.Repeat("y", 300)}
				for index, src := range inputs {
					t.Cleanup(func() { src.Clean(mp) })
					for row, payload := range payloads {
						require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(row*2+index+1), false, mp))
						require.NoError(t, vector.AppendBytes(src.Vecs[1], []byte(payload), bits && row == 1, mp))
					}
					src.Vecs[2].Free(mp)
					var err error
					src.Vecs[2], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("c", 32)), 3, mp)
					require.NoError(t, err)
					if bits {
						src.Vecs[1].GetGrouping().Add(2)
					}
					src.SetRowCount(3)
				}
				out := newBuffer()
				t.Cleanup(func() { out.Clean(mp) })
				var got []int64
				limit := int64(4096)
				sink := func(buf *batch.Batch) (*batch.Batch, error) {
					require.Positive(t, buf.RowCount())
					if int64(buf.Allocated()) > limit {
						require.Equal(t, 1, buf.RowCount())
					}
					for row, key := range vector.MustFixedColWithTypeCheck[int64](buf.Vecs[0]) {
						got = append(got, key)
						pos := int((key - 1) / 2)
						require.Equal(t, bits && pos == 1, buf.Vecs[1].IsNull(uint64(row)))
						if !buf.Vecs[1].IsNull(uint64(row)) {
							require.Equal(t, payloads[pos], buf.Vecs[1].GetStringAt(row))
						}
						require.Equal(t, bits && pos == 2, buf.Vecs[1].GetGrouping().Contains(uint64(row)))
						require.Equal(t, strings.Repeat("c", 32), buf.Vecs[2].GetStringAt(row))
					}
					if replace {
						buf.Clean(mp)
						return newBuffer(), nil
					}
					buf.CleanOnlyData()
					return buf, nil
				}
				var err error
				for _, limit = range []int64{4096, 256, 4096} {
					got = nil
					out, err = MergeSortBatches(inputs, 0, out, sink, mp, nil, limit)
					require.NoError(t, err)
					require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, got)
				}
			})
		}
	}
}
