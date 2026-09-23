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
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sort"
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

func TestMergeSortBatchesYear(t *testing.T) {
	mp := mpool.MustNewZero()
	batches := []*batch.Batch{
		newYearMergeBatch(t, mp, []types.MoYear{0, 2001, 2024}, []int32{10, 30, 50}),
		newYearMergeBatch(t, mp, []types.MoYear{1901, 2010}, []int32{20, 40}),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}

	newBuffer := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_year.ToType(), types.T_int32.ToType()})
	}
	buffer := newBuffer()

	var gotKeys []types.MoYear
	var gotPayloads []int32
	buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		keys := vector.MustFixedColNoTypeCheck[types.MoYear](out.Vecs[0])
		payloads := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])
		for i := 0; i < out.RowCount(); i++ {
			gotKeys = append(gotKeys, keys[i])
			gotPayloads = append(gotPayloads, payloads[i])
		}
		out.Clean(mp)
		return newBuffer(), nil
	}, mp, nil)
	require.NoError(t, err)
	defer buffer.Clean(mp)
	require.Equal(t, []types.MoYear{0, 1901, 2001, 2010, 2024}, gotKeys)
	require.Equal(t, []int32{10, 20, 30, 40, 50}, gotPayloads)
}

func TestMergeSortBatchesDisjointRanges(t *testing.T) {
	mp := mpool.MustNewZero()
	rows := objectio.BlockMaxRows + 7
	lowRows := objectio.BlockMaxRows / 2
	batches := []*batch.Batch{
		newInt64MergeBatch(t, mp, lowRows, rows),
		newInt64MergeBatch(t, mp, 0, lowRows),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}

	merge := newMerge(
		sort.GenericLess[int64],
		&fixedDataSlice[int64]{getFixedCols[int64](batches, 0)},
		[]*nulls.Nulls{batches[0].Vecs[0].GetNulls(), batches[1].Vecs[0].GetNulls()},
	)
	require.Equal(t, []int{1, 0}, merge.disjointBatchOrder())

	newBuffer := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	}
	buffer := newBuffer()
	var gotKeys, gotPayloads []int64
	var outputRows []int
	var putBackOrder []int
	buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		gotKeys = append(gotKeys, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0])...)
		gotPayloads = append(gotPayloads, vector.MustFixedColNoTypeCheck[int64](out.Vecs[1])...)
		outputRows = append(outputRows, out.RowCount())
		out.Clean(mp)
		return newBuffer(), nil
	}, mp, func(index int) {
		putBackOrder = append(putBackOrder, index)
	})
	require.NoError(t, err)
	defer buffer.Clean(mp)
	require.Equal(t, []int{objectio.BlockMaxRows, 7}, outputRows)
	require.Equal(t, []int{1, 0}, putBackOrder)
	require.Len(t, gotKeys, rows)
	for i := range rows {
		require.Equal(t, int64(i), gotKeys[i])
		require.Equal(t, -int64(i), gotPayloads[i])
	}
}

func TestDisjointBatchOrderFallsBackForOverlappingRanges(t *testing.T) {
	ds := &fixedDataSlice[int64]{cols: [][]int64{{1, 4}, {2, 3}}}
	merge := newMerge(sort.GenericLess[int64], ds, []*nulls.Nulls{
		nulls.NewWithSize(2),
		nulls.NewWithSize(2),
	})
	require.Nil(t, merge.disjointBatchOrder())
}

func TestDisjointBatchOrderFallsBackForEqualBoundaries(t *testing.T) {
	ds := &fixedDataSlice[int64]{cols: [][]int64{{1, 2}, {2, 3}}}
	merge := newMerge(sort.GenericLess[int64], ds, []*nulls.Nulls{
		nulls.NewWithSize(2),
		nulls.NewWithSize(2),
	})
	require.Nil(t, merge.disjointBatchOrder())
}

func TestDisjointBatchOrderHandlesNullBoundariesConservatively(t *testing.T) {
	leftNulls := nulls.NewWithSize(3)
	leftNulls.Add(0)
	rightNulls := nulls.NewWithSize(3)
	rightNulls.Add(0)
	ds := &fixedDataSlice[int64]{cols: [][]int64{{0, 1, 2}, {0, 3, 4}}}
	merge := newMerge(sort.GenericLess[int64], ds, []*nulls.Nulls{leftNulls, rightNulls})
	require.Nil(t, merge.disjointBatchOrder(), "mixed null/non-null ranges overlap")

	allNulls := nulls.NewWithSize(2)
	allNulls.Add(0, 1)
	ds = &fixedDataSlice[int64]{cols: [][]int64{{0, 0}, {1, 2}}}
	merge = newMerge(sort.GenericLess[int64], ds, []*nulls.Nulls{allNulls, nulls.NewWithSize(2)})
	require.Equal(t, []int{0, 1}, merge.disjointBatchOrder())
}

func TestMergeSortBatchesDisjointSinkError(t *testing.T) {
	mp := mpool.MustNewZero()
	batches := []*batch.Batch{
		newInt64MergeBatch(t, mp, 2, 4),
		newInt64MergeBatch(t, mp, 0, 2),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}
	buffer := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	sinkErr := errors.New("sink failed")
	returned, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		return out, sinkErr
	}, mp, nil)
	require.ErrorIs(t, err, sinkErr)
	require.Same(t, buffer, returned)
	buffer.Clean(mp)
}

func TestMergeSortBatchesEmptyFloatBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	batches := []*batch.Batch{
		batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_float64.ToType()}),
		batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_float64.ToType()}),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}
	buffer := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_float64.ToType()})
	sinkCalls := 0
	buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		sinkCalls++
		return out, nil
	}, mp, nil)
	require.NoError(t, err)
	defer buffer.Clean(mp)
	require.Zero(t, sinkCalls)
	require.Zero(t, buffer.RowCount())
}

func TestMergeSortBatchesDisjointVarlenaRanges(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_varchar.ToType()
	batches := []*batch.Batch{
		newVarlenaMergeBatch(t, mp, typ, [][]byte{[]byte("middle"), []byte("z")}, []int32{20, 30}),
		newVarlenaMergeBatch(t, mp, typ, [][]byte{[]byte("a"), []byte("begin")}, []int32{1, 10}),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}

	newBuffer := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{typ, types.T_int32.ToType()})
	}
	buffer := newBuffer()
	var gotKeys []string
	var gotPayloads []int32
	buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
		for i := 0; i < out.RowCount(); i++ {
			gotKeys = append(gotKeys, string(out.Vecs[0].GetBytesAt(i)))
		}
		gotPayloads = append(gotPayloads, vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])...)
		out.Clean(mp)
		return newBuffer(), nil
	}, mp, nil)
	require.NoError(t, err)
	defer buffer.Clean(mp)
	require.Equal(t, []string{"a", "begin", "middle", "z"}, gotKeys)
	require.Equal(t, []int32{1, 10, 20, 30}, gotPayloads)
}

func BenchmarkMergeSortBatchesDisjointRanges(b *testing.B) {
	mp := mpool.MustNewZero()
	const batchCount = 16
	batches := make([]*batch.Batch, 0, batchCount)
	for i := batchCount - 1; i >= 0; i-- {
		batches = append(batches, newInt64MergeBatch(b, mp,
			i*objectio.BlockMaxRows, (i+1)*objectio.BlockMaxRows))
	}
	defer func() {
		for _, bat := range batches {
			bat.Clean(mp)
		}
	}()
	buffer := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	defer buffer.Clean(mp)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		buffer, err = MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
			out.CleanOnlyData()
			return out, nil
		}, mp, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMergeSortBatchesOverlappingRanges(b *testing.B) {
	mp := mpool.MustNewZero()
	const batchCount = 16
	batches := make([]*batch.Batch, 0, batchCount)
	for i := 0; i < batchCount; i++ {
		bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		for row := 0; row < objectio.BlockMaxRows; row++ {
			key := int64(row*batchCount + i)
			require.NoError(b, vector.AppendFixed(bat.Vecs[0], key, false, mp))
			require.NoError(b, vector.AppendFixed(bat.Vecs[1], -key, false, mp))
		}
		bat.SetRowCount(objectio.BlockMaxRows)
		batches = append(batches, bat)
	}
	defer func() {
		for _, bat := range batches {
			bat.Clean(mp)
		}
	}()
	buffer := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	defer buffer.Clean(mp)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		buffer, err = MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
			out.CleanOnlyData()
			return out, nil
		}, mp, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestMergeSortBatchesBinaryTypes(t *testing.T) {
	testCases := []struct {
		name string
		typ  types.Type
	}{
		{name: "binary", typ: types.New(types.T_binary, 4, 0)},
		{name: "varbinary", typ: types.New(types.T_varbinary, 8, 0)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			batches := []*batch.Batch{
				newVarlenaMergeBatch(t, mp, tc.typ,
					[][]byte{{0x00}, {'b'}, {0xff}}, []int32{10, 30, 50}),
				newVarlenaMergeBatch(t, mp, tc.typ,
					[][]byte{{0x00, 0x01}, {'a'}, {'b', 0x00}}, []int32{20, 40, 60}),
			}
			for _, bat := range batches {
				defer bat.Clean(mp)
			}

			newBuffer := func() *batch.Batch {
				return batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{tc.typ, types.T_int32.ToType()})
			}
			buffer := newBuffer()

			var gotKeys [][]byte
			var gotPayloads []int32
			buffer, err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
				payloads := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])
				for i := 0; i < out.RowCount(); i++ {
					gotKeys = append(gotKeys, append([]byte(nil), out.Vecs[0].GetBytesAt(i)...))
					gotPayloads = append(gotPayloads, payloads[i])
				}
				out.Clean(mp)
				return newBuffer(), nil
			}, mp, nil)
			require.NoError(t, err)
			defer buffer.Clean(mp)
			require.Equal(t, [][]byte{{0x00}, {0x00, 0x01}, {'a'}, {'b'}, {'b', 0x00}, {0xff}}, gotKeys)
			require.Equal(t, []int32{10, 20, 40, 30, 60, 50}, gotPayloads)
		})
	}
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

func newYearMergeBatch(
	t *testing.T,
	mp *mpool.MPool,
	keys []types.MoYear,
	payloads []int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, payloads, len(keys))

	bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_year.ToType(), types.T_int32.ToType()})
	for i, key := range keys {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], payloads[i], false, mp))
	}
	bat.SetRowCount(len(keys))
	return bat
}

func newInt64MergeBatch(t testing.TB, mp *mpool.MPool, start, end int) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	for i := start; i < end; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], -int64(i), false, mp))
	}
	bat.SetRowCount(end - start)
	return bat
}

func newVarlenaMergeBatch(
	t *testing.T,
	mp *mpool.MPool,
	typ types.Type,
	keys [][]byte,
	payloads []int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, payloads, len(keys))

	bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{typ, types.T_int32.ToType()})
	for i, key := range keys {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], key, false, mp))
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

	t.Run("JSON cluster key keeps physical byte order", func(t *testing.T) {
		jsonVector := vector.NewVec(types.T_json.ToType())
		payloadVector := vector.NewVec(types.T_int32.ToType())
		defer jsonVector.Free(mp)
		defer payloadVector.Free(mp)
		encodeJSON := func(text string) []byte {
			value, err := bytejson.ParseFromString(text)
			require.NoError(t, err)
			encoded, err := value.Marshal()
			require.NoError(t, err)
			return encoded
		}

		for i, value := range []string{"0", "1", "false", "true"} {
			require.NoError(t, vector.AppendBytes(jsonVector, encodeJSON(value), false, mp))
			require.NoError(t, vector.AppendFixed(payloadVector, int32(i), false, mp))
		}

		var idxBuf []int64
		var shuffleBuf []byte
		require.NoError(t, SortColumnsByIndexWithBuf(
			[]*vector.Vector{jsonVector, payloadVector}, 0, mp, &idxBuf, &shuffleBuf))
		require.Equal(t,
			[][]byte{encodeJSON("true"), encodeJSON("false"), encodeJSON("0"), encodeJSON("1")},
			vector.InefficientMustBytesCol(jsonVector))
		require.Equal(t, []int32{3, 2, 0, 1}, vector.MustFixedColNoTypeCheck[int32](payloadVector))
	})
}
