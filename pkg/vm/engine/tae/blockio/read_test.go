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

package blockio

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestFillOutputBatchBySelectedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	// Test case 1: Basic case without phyAddr and without orderByLimit
	t.Run("basic_no_phyaddr_no_orderbylimit", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_varchar.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, []byte("test"), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		selectRows := []int64{1, 3, 5}
		columns := []uint16{0, 1}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 3, outputBat.Vecs[0].Length())
		require.Equal(t, 3, outputBat.Vecs[1].Length())
	})

	// Test case 2: With orderByLimit (distVec needs to be appended)
	t.Run("with_orderbylimit", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_array_float32.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())

		selectRows := []int64{1, 3}
		columns := []uint16{0, 1}
		dists := []float64{0.5, 0.8}

		orderByLimit := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, orderByLimit, dists, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 2, outputBat.Vecs[0].Length())
		require.Equal(t, 3, len(outputBat.Vecs))
		require.Equal(t, 2, outputBat.Vecs[2].Length())
	})

	// Test case 3: Empty selectRows with phyAddr column
	t.Run("empty_selectrows_with_phyaddr", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
		}

		cacheVectors := make(containers.Vectors, 1)
		cacheVectors[0] = *vec0

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		selectRows := []int64{}
		columns := []uint16{0, 1}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, 0, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 0, outputBat.Vecs[0].Length())
	})

	// Test case 4: With orderByLimit and distVec already exists
	t.Run("with_orderbylimit_distvec_exists", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_array_float32.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(3)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())
		outputBat.Vecs[2] = vector.NewVec(types.T_float64.ToType())

		selectRows := []int64{1, 3}
		columns := []uint16{0, 1}
		dists := []float64{0.5, 0.8}

		orderByLimit := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, orderByLimit, dists, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 2, outputBat.Vecs[0].Length())
		require.Equal(t, 3, len(outputBat.Vecs))
		require.Equal(t, 2, outputBat.Vecs[2].Length())
	})

	// Test case 5: With phyAddr column and non-empty selectRows
	t.Run("with_phyaddr_and_selectrows", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i*10), false, mp)
		}

		cacheVectors := make(containers.Vectors, 1)
		cacheVectors[0] = *vec0

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		selectRows := []int64{2, 5, 7}
		columns := []uint16{0, 1}

		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, 0, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 3, outputBat.Vecs[0].Length())
		require.Equal(t, 3, outputBat.Vecs[1].Length())
	})
}

func TestHandleOrderByLimitOnSelectRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	vec0 := vector.NewVec(types.T_int32.ToType())
	vec1 := vector.NewVec(types.T_array_float32.ToType())

	for i := 0; i < 3; i++ {
		vector.AppendFixed(vec0, int32(i), false, mp)
	}

	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{1.0, 1.0}), false, mp) // dist: 2
	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp) // dist: 0.05
	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.5, 0.5}), false, mp) // dist: 0.5

	cacheVectors := make(containers.Vectors, 2)
	cacheVectors[0] = *vec0
	cacheVectors[1] = *vec1

	selectRows := []int64{0, 1, 2}

	orderByLimit := &objectio.IndexReaderTopOp{
		ColPos:     1,
		Limit:      2,
		Typ:        types.T_array_float32,
		NumVec:     types.ArrayToBytes[float32]([]float32{0.0, 0.0}),
		MetricType: metric.Metric_L2Distance,
		DistHeap:   make(objectio.Float64Heap, 0, 2),
	}

	resSels, resDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, orderByLimit, &objectio.BlockInfo{}, -1, cacheVectors)
	require.NoError(t, err)
	require.Equal(t, 2, len(resSels))
	require.Equal(t, 2, len(resDists))

	// Closest should be index 1 (0.1, 0.2), then index 2 (0.5, 0.5)
	require.Equal(t, int64(1), resSels[0])
	require.Equal(t, int64(2), resSels[1])
}

// TestTopInputRowsConstruction tests the code section at lines 674-684
// This tests the construction of topInputRows by filtering out deleted rows
func TestBuildTopInputRows(t *testing.T) {
	// empty deleteMask returns nil
	t.Run("empty_mask", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		require.Nil(t, buildTopInputRows(100, mask))
	})

	// all rows deleted returns empty slice
	t.Run("all_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 10; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(10, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
		require.Equal(t, 0, cap(rows))
	})

	// some rows deleted
	t.Run("some_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(1)
		mask.Add(3)
		mask.Add(5)
		mask.Add(7)
		rows := buildTopInputRows(10, mask)
		require.Equal(t, []int64{0, 2, 4, 6, 8, 9}, rows)
		require.Equal(t, 6, cap(rows))
	})

	// capHint clamped to 0 when all deleted
	t.Run("caphint_clamped", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 5; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(5, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
	})

	// single row not deleted -> empty mask -> nil
	t.Run("single_row_no_delete", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		require.Nil(t, buildTopInputRows(1, mask))
	})

	// single row deleted
	t.Run("single_row_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(0)
		rows := buildTopInputRows(1, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
	})

	// large sparse deletes
	t.Run("large_sparse", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 1000; i += 10 {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(1000, mask)
		require.Equal(t, 900, len(rows))
		require.Equal(t, 900, cap(rows))
		for _, r := range rows {
			require.False(t, mask.Contains(uint64(r)))
		}
	})

	// deletes at start
	t.Run("deletes_at_start", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 5; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 15, len(rows))
		require.Equal(t, int64(5), rows[0])
	})

	// deletes at end
	t.Run("deletes_at_end", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 15; i < 20; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 15, len(rows))
		require.Equal(t, int64(14), rows[14])
	})

	// alternating deletes
	t.Run("alternating", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 20; i += 2 {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 10, len(rows))
		for _, r := range rows {
			require.Equal(t, int64(1), r%2)
		}
	})

	// length 0
	t.Run("zero_length", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(0)
		rows := buildTopInputRows(0, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
	})
}

func TestHandleOrderByLimitOnSelectRowsForOrderedLimit(t *testing.T) {
	ctx := context.Background()
	selectRows := []int64{2, 4, 6, 8}
	info := &objectio.BlockInfo{ObjectFlags: objectio.ObjectFlag_Sorted}

	descLimit := &objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true, Desc: true}
	descRows, descDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, descLimit, info, -1, nil)
	require.NoError(t, err)
	require.Nil(t, descDists)
	require.Equal(t, []int64{6, 8}, descRows)

	ascLimit := &objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true}
	ascRows, ascDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, ascLimit, info, -1, nil)
	require.NoError(t, err)
	require.Nil(t, ascDists)
	require.Equal(t, []int64{2, 4}, ascRows)
}

// TestHandleOrderByLimitAllNullVectors verifies that HandleOrderByLimitOnIVFFlatIndex
// returns empty sels/dists when all vector rows are NULL.
// This is the root cause of the IVF-Flat entries table panic: when the InMem
// path gets all-NULL vectors, empty sels caused Shuffle to be a no-op, leaving
// the batch with a stale row count while the distance vector had 0 elements.
func TestHandleOrderByLimitAllNullVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	// Create a vector column where ALL rows are NULL.
	vecCol := vector.NewVec(types.T_array_float32.ToType())
	for i := 0; i < 5; i++ {
		vector.AppendBytes(vecCol, nil, true, mp) // null = true
	}

	orderByLimit := &objectio.IndexReaderTopOp{
		ColPos:     0,
		Limit:      2,
		Typ:        types.T_array_float32,
		NumVec:     types.ArrayToBytes[float32]([]float32{0.0, 0.0}),
		MetricType: metric.Metric_L2Distance,
	}

	sels, dists, err := HandleOrderByLimitOnIVFFlatIndex(ctx, nil, vecCol, orderByLimit)
	require.NoError(t, err)
	require.Empty(t, sels, "sels should be empty when all vectors are NULL")
	require.Empty(t, dists, "dists should be empty when all vectors are NULL")

	// Verify that Shuffle with empty sels is a no-op (the bug scenario).
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 5; i++ {
		vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
	}
	bat.SetRowCount(5)

	// Shuffle with empty sels does nothing — batch retains row count.
	err = bat.Shuffle(sels, mp)
	require.NoError(t, err)
	require.Equal(t, 5, bat.RowCount(), "Shuffle with empty sels should NOT reset row count")

	// The fix: caller must explicitly set row count to 0 when sels is empty.
	if len(sels) == 0 {
		bat.SetRowCount(0)
	}
	require.Equal(t, 0, bat.RowCount())
}
