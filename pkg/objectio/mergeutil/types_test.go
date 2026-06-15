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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

func TestMergeSortBatchesDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	decimalTyp := types.New(types.T_decimal256, 39, 4)
	batches := []*batch.Batch{
		newDecimal256MergeBatch(t, mp, decimalTyp, []string{
			"-2.0000",
			"1234567890123456789012345678901234.5678",
			"9999999999999999999999999999999999.9999",
		}, []int32{10, 30, 50}),
		newDecimal256MergeBatch(t, mp, decimalTyp, []string{
			"0.0001",
			"1234567890123456789012345678901234.5677",
		}, []int32{20, 40}),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}

	newBuffer := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{decimalTyp, types.T_int32.ToType()})
	}
	buffer := newBuffer()

	var gotKeys []string
	var gotPayloads []int32
	buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		keys := vector.MustFixedColNoTypeCheck[types.Decimal256](out.Vecs[0])
		payloads := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])
		for i := 0; i < out.RowCount(); i++ {
			gotKeys = append(gotKeys, keys[i].Format(decimalTyp.Scale))
			gotPayloads = append(gotPayloads, payloads[i])
		}
		out.Clean(mp)
		return newBuffer(), nil
	}, mp, nil)
	require.NoError(t, err)
	defer buffer.Clean(mp)
	require.Equal(t, []string{
		"-2.0000",
		"0.0001",
		"1234567890123456789012345678901234.5677",
		"1234567890123456789012345678901234.5678",
		"9999999999999999999999999999999999.9999",
	}, gotKeys)
	require.Equal(t, []int32{10, 20, 40, 30, 50}, gotPayloads)
}

func newDecimal256MergeBatch(
	t *testing.T,
	mp *mpool.MPool,
	decimalTyp types.Type,
	keys []string,
	payloads []int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, payloads, len(keys))

	bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{decimalTyp, types.T_int32.ToType()})
	for i, key := range keys {
		value, err := types.ParseDecimal256(key, decimalTyp.Width, decimalTyp.Scale)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], payloads[i], false, mp))
	}
	bat.SetRowCount(len(keys))
	return bat
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
