// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func buildTestBitmapVecs(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector) {
	nulls := []bool{false, false, false, false, true, false, false, false, false, true}
	uint64s := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	vec1 := testutil.NewUInt64Vector(10, types.T_uint64.ToType(), mp, false, nil, uint64s[:10])
	vec2 := testutil.NewUInt64Vector(10, types.T_uint64.ToType(), mp, false, nulls, uint64s[2:])
	return vec1, vec2
}

func checkBitmap(t *testing.T, vec *vector.Vector, idx int, expected []uint32) {
	bitmap := roaring.NewBitmap()
	bs := vec.GetBytesAt(idx)
	require.NoError(t, bitmap.UnmarshalBinary(bs))
	require.Equal(t, expected, bitmap.ToArray())
}

func TestBitmapConstructExec(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1, vec2 := buildTestBitmapVecs(t, mp)

	t.Run("BulkFill", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		exec.GetOptResult().modifyChunkSize(1)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec1}))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec2}))
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("BatchFill", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec1}))
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec2}))
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("Merge", func(t *testing.T) {
		curNB := mp.CurrNB()
		execa1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa1.GetOptResult().modifyChunkSize(1)
		execa2.GetOptResult().modifyChunkSize(1)
		require.NoError(t, execa1.GroupGrow(1))
		require.NoError(t, execa2.GroupGrow(1))

		execb1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb1.GetOptResult().modifyChunkSize(1)
		execb1.GroupGrow(1)
		execb2.GetOptResult().modifyChunkSize(1)
		execb2.GroupGrow(1)

		require.NoError(t, execa1.BulkFill(0, []*vector.Vector{vec1}))
		require.NoError(t, execa2.BulkFill(0, []*vector.Vector{vec2}))

		buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
		buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

		err := execa1.SaveIntermediateResultOfChunk(0, buf1)
		require.NoError(t, err)
		err = execa2.SaveIntermediateResultOfChunk(0, buf2)
		require.NoError(t, err)

		r1 := bytes.NewReader(buf1.Bytes())
		r2 := bytes.NewReader(buf2.Bytes())

		err = execb1.UnmarshalFromReader(r1, mp)
		require.NoError(t, err)
		err = execb2.UnmarshalFromReader(r2, mp)
		require.NoError(t, err)

		execb1.Merge(execb2, 0, 0)
		results, err := execb1.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		execa1.Free()
		execa2.Free()
		execb1.Free()
		execb2.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("BatchMerge", func(t *testing.T) {
		curNB := mp.CurrNB()
		execa1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, execa1.GroupGrow(1))
		require.NoError(t, execa2.GroupGrow(1))
		require.NoError(t, execa1.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec1}))
		require.NoError(t, execa2.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec2}))

		buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
		buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

		err := execa1.SaveIntermediateResult(1, [][]uint8{{1}}, buf1)
		require.NoError(t, err)
		err = execa2.SaveIntermediateResult(1, [][]uint8{{1}}, buf2)
		require.NoError(t, err)

		execb1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())

		r1 := bytes.NewReader(buf1.Bytes())
		r2 := bytes.NewReader(buf2.Bytes())

		err = execb1.UnmarshalFromReader(r1, mp)
		require.NoError(t, err)
		err = execb2.UnmarshalFromReader(r2, mp)
		require.NoError(t, err)

		execb1.BatchMerge(execb2, 0, []uint64{1})
		results, err := execb1.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		execa1.Free()
		execa2.Free()
		execb1.Free()
		execb2.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})
}
