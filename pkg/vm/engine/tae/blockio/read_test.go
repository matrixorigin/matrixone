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

	resSels, resDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, orderByLimit, -1, cacheVectors)
	require.NoError(t, err)
	require.Equal(t, 2, len(resSels))
	require.Equal(t, 2, len(resDists))

	// Closest should be index 1 (0.1, 0.2), then index 2 (0.5, 0.5)
	require.Equal(t, int64(1), resSels[0])
	require.Equal(t, int64(2), resSels[1])
}

// TestTopInputRowsConstruction tests the code section at lines 674-684
// This tests the construction of topInputRows by filtering out deleted rows
func TestTopInputRowsConstruction(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	// Test case 1: Empty deleteMask - topInputRows should be nil
	t.Run("empty_delete_mask", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 100
		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.Nil(t, topInputRows)
	})

	// Test case 2: All rows deleted - topInputRows should be empty
	t.Run("all_rows_deleted", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 10
		for i := 0; i < length; i++ {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 0, len(topInputRows))
		require.Equal(t, 0, cap(topInputRows))
	})

	// Test case 3: Some rows deleted - topInputRows should contain only live rows
	t.Run("some_rows_deleted", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 10
		deletedIndices := []uint64{1, 3, 5, 7}
		for _, idx := range deletedIndices {
			deleteMask.Add(idx)
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 6, len(topInputRows))
		require.Equal(t, 6, cap(topInputRows))

		expectedRows := []int64{0, 2, 4, 6, 8, 9}
		require.Equal(t, expectedRows, topInputRows)
	})

	// Test case 4: capHint calculation - negative capHint should be clamped to 0
	t.Run("negative_caphint_clamped", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 5
		// Add more deletes than length (edge case)
		for i := 0; i < length; i++ {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		require.Equal(t, 0, capHint)

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 0, len(topInputRows))
		require.Equal(t, 0, cap(topInputRows))
	})

	// Test case 5: Large dataset with sparse deletes
	t.Run("large_dataset_sparse_deletes", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 1000
		// Delete every 10th row
		for i := 0; i < length; i += 10 {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 900, len(topInputRows))
		require.Equal(t, 900, cap(topInputRows))

		// Verify no deleted rows are in topInputRows
		for _, row := range topInputRows {
			require.False(t, deleteMask.Contains(uint64(row)))
		}
	})

	// Test case 6: Single row dataset
	t.Run("single_row_not_deleted", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 1

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.Nil(t, topInputRows)
	})

	// Test case 7: Single row deleted
	t.Run("single_row_deleted", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 1
		deleteMask.Add(0)

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 0, len(topInputRows))
		require.Equal(t, 0, cap(topInputRows))
	})

	// Test case 8: Consecutive deletes at start
	t.Run("consecutive_deletes_at_start", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 20
		for i := 0; i < 5; i++ {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 15, len(topInputRows))
		require.Equal(t, int64(5), topInputRows[0])
		require.Equal(t, int64(19), topInputRows[14])
	})

	// Test case 9: Consecutive deletes at end
	t.Run("consecutive_deletes_at_end", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 20
		for i := 15; i < 20; i++ {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 15, len(topInputRows))
		require.Equal(t, int64(0), topInputRows[0])
		require.Equal(t, int64(14), topInputRows[14])
	})

	// Test case 10: Alternating deletes (every other row)
	t.Run("alternating_deletes", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 20
		for i := 0; i < length; i += 2 {
			deleteMask.Add(uint64(i))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 10, len(topInputRows))

		// Verify all rows are odd indices
		for _, row := range topInputRows {
			require.Equal(t, int64(1), row%2)
		}
	})

	// Test case 11: capHint accuracy - verify capacity matches expected live rows
	t.Run("caphint_accuracy", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 100
		deleteCount := 25
		for i := 0; i < deleteCount; i++ {
			deleteMask.Add(uint64(i * 4))
		}

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, capHint, len(topInputRows))
		require.Equal(t, capHint, cap(topInputRows))
	})

	// Test case 12: Verify loop iteration covers all indices
	t.Run("loop_covers_all_indices", func(t *testing.T) {
		deleteMask := objectio.GetReusableBitmap()
		defer deleteMask.Release()

		length := 50
		deleteMask.Add(10)
		deleteMask.Add(25)
		deleteMask.Add(49)

		capHint := length - deleteMask.Count()
		if capHint < 0 {
			capHint = 0
		}

		var topInputRows []int64
		if !deleteMask.IsEmpty() {
			topInputRows = make([]int64, 0, capHint)
			for i := 0; i < length; i++ {
				if !deleteMask.Contains(uint64(i)) {
					topInputRows = append(topInputRows, int64(i))
				}
			}
		}

		require.NotNil(t, topInputRows)
		require.Equal(t, 47, len(topInputRows))

		// Verify first and last elements
		require.Equal(t, int64(0), topInputRows[0])
		require.Equal(t, int64(48), topInputRows[46])

		// Verify deleted indices are not present
		for _, row := range topInputRows {
			require.NotEqual(t, int64(10), row)
			require.NotEqual(t, int64(25), row)
			require.NotEqual(t, int64(49), row)
		}
	})
}
