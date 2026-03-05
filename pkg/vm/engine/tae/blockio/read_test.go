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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
