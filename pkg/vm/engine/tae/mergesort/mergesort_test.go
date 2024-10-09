// Copyright 2022 Matrix Origin
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

package mergesort

import (
	"context"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

func TestSort1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	pool := mocks.GetTestVectorPool()

	// sort not null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, nil)
		vec2 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			vec2.Append(vec.Get(i), vec.IsNull(i))
		}
		vecs := []containers.Vector{vec, vec2}
		_, _ = SortBlockColumns(vecs, 0, pool)
		require.True(t, vecs[0].Equals(vecs[1]), vecType)
		vec.Close()
		vec2.Close()
	}

	// sort null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, nil)
		vec.Update(rand.Intn(10), nil, true)
		vec.Update(rand.Intn(10), nil, true)
		vec2 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			vec2.Append(vec.Get(i), vec.IsNull(i))
		}
		vecs := []containers.Vector{vec, vec2}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		require.True(t, vecs[0].Equals(vecs[1]), vecType)
		vec.Close()
		vec2.Close()
	}
}
func TestSort2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10000, false, nil)
		t0 := time.Now()
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		t.Logf("%-20v takes %v", vecType.String(), time.Since(t0))
		vec.Close()
	}
}

func TestSortBlockColumns(t *testing.T) {
	vec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	vec.Append([]byte("b"), false)
	vec.Append([]byte("c"), false)
	vec.Append([]byte("a"), false)
	vecSlice := []containers.Vector{vec}

	columns, err := SortBlockColumns(vecSlice, 0, mocks.GetTestVectorPool())
	require.NoError(t, err)
	require.Equal(t, []int64{2, 0, 1}, columns)
	require.Equal(t, []string{"a", "b", "c"}, vector.InefficientMustStrCol(vec.GetDownstreamVector()))

	vecWithNull := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	vecWithNull.Append(int32(1), false)
	vecWithNull.Append(int32(2), true)
	vecWithNull.Append(int32(3), false)

	vecSlice = []containers.Vector{vecWithNull}
	columns, err = SortBlockColumns(vecSlice, 0, mocks.GetTestVectorPool())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 0, 2}, columns)
	require.Equal(t, []int32{0, 1, 3}, vector.MustFixedColWithTypeCheck[int32](vecWithNull.GetDownstreamVector()))
}

func BenchmarkSortBlockColumns(b *testing.B) {
	vecTypes := types.MockColTypes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var vecs []containers.Vector
		for _, vecType := range vecTypes {
			vec := containers.MockVector(vecType, 10000, false, nil)
			vecs = []containers.Vector{vec}
		}
		b.StartTimer()
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
	}
}

func TestAObjMerge(t *testing.T) {
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	vecType := types.T_int32
	batCnt := 4
	vecCnt := 2
	rowCnt := 100
	toLayout := []uint32{100, 100, 100, 100}
	batches := make([]*containers.Batch, batCnt)

	for i := 0; i < batCnt; i++ {
		batches[i] = containers.NewBatch()
		for j := 0; j < vecCnt; j++ {
			vec := containers.NewVector(vecType.ToType())
			for k := 0; k < rowCnt; k++ {
				vec.Append(int32(i*rowCnt+k), false)
			}
			batches[i].AddVector(strconv.Itoa(j), vec)
		}
	}
	ret, releaseF, mapping, err := MergeAObj(context.Background(), testPool, batches, 0, toLayout)
	require.NoError(t, err)
	for i := 0; i < batCnt; i++ {
		for j := 0; j < vecCnt; j++ {
			t.Log(vector.MustFixedColWithTypeCheck[int32](ret[i].Vecs[j]))
			require.True(t, slices.IsSorted(vector.MustFixedColWithTypeCheck[int32](ret[i].Vecs[j])))
		}
	}
	t.Log(mapping)
	releaseF()
}

func TestAObjMergeContainsNull(t *testing.T) {
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	vecType := types.T_int32
	batCnt := 4
	vecCnt := 2
	rowCnt := 100
	toLayout := []uint32{100, 100, 100, 100}
	batches := make([]*containers.Batch, batCnt)

	for i := 0; i < batCnt; i++ {
		batches[i] = containers.NewBatch()
		for j := 0; j < vecCnt; j++ {
			vec := containers.NewVector(vecType.ToType())
			for k := 0; k < rowCnt; k++ {
				if k < 10 {
					vec.Append(0, true)
				} else {
					vec.Append(int32(i*rowCnt+k), false)
				}
			}
			batches[i].AddVector(strconv.Itoa(j), vec)
		}
	}

	ret, releaseF, mapping, err := MergeAObj(context.Background(), testPool, batches, 0, toLayout)
	require.NoError(t, err)
	for _, bat := range ret {
		for _, vec := range bat.Vecs {
			s := vector.MustFixedColWithTypeCheck[int32](vec)
			for i := range s {
				if vec.IsNull(uint64(i)) {
					s[i] = 0
				}
			}
			t.Log(s)
			require.True(t, slices.IsSorted(s))
		}
	}
	t.Log(mapping)
	releaseF()
}

func TestAObjMergeAllTypes(t *testing.T) {
	vecTypes := types.MockColTypes()
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		t0 := time.Now()
		batches := make([]*containers.Batch, 2)
		batches[0] = containers.NewBatch()
		batches[0].AddVector("", vec)
		batches[1] = containers.NewBatch()
		batches[1].AddVector("", vec2)
		_, releaseF, _, err := MergeAObj(context.Background(), testPool, batches, 0, []uint32{50000, 50000})
		require.NoError(t, err)
		t.Logf("%-20v takes %v", vecType, time.Since(t0))
		releaseF()
	}
}

func TestReshapeBatches1(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 4, false, nil)
		vec.Update(rand.Intn(4), nil, true)
		vec2 := containers.MockVector(vecType, 6, false, nil)
		vec2.Update(rand.Intn(6), nil, true)
		t.Log(vec)
		t.Log(vec2)
		vecs := []containers.Vector{vec, vec2}

		inputBatches := make([]*containers.Batch, 2)
		for i := range inputBatches {
			bat := containers.NewBatch()
			bat.Vecs = append(bat.Vecs, vecs[i])
			bat.Attrs = append(bat.Attrs, "")
			inputBatches[i] = bat
		}
		retBatches, releaseF, _, err := ReshapeBatches(inputBatches, []uint32{5, 5}, pool)
		require.NoError(t, err)
		t.Log(retBatches)
		for i := range retBatches {
			require.Equal(t, 5, retBatches[i].RowCount())
		}
		releaseF()
	}
}

func TestReshapeBatches3(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		vecs := []containers.Vector{vec, vec2}
		inputBatches := make([]*containers.Batch, 2)
		for i := range inputBatches {
			bat := containers.NewBatch()
			bat.Vecs = append(bat.Vecs, vecs[i])
			bat.Attrs = append(bat.Attrs, "")
			inputBatches[i] = bat
		}
		t0 := time.Now()
		_, releaseF, _, err := ReshapeBatches(inputBatches, []uint32{50000, 50000}, pool)
		require.NoError(t, err)
		t.Logf("%v takes %v", vecType, time.Since(t0))
		releaseF()
	}
}
