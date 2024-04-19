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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"math/rand"
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
	require.Equal(t, []string{"a", "b", "c"}, vector.MustStrCol(vec.GetDownstreamVector()))

	vecWithNull := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	vecWithNull.Append(int32(1), false)
	vecWithNull.Append(int32(2), true)
	vecWithNull.Append(int32(3), false)

	vecSlice = []containers.Vector{vecWithNull}
	columns, err = SortBlockColumns(vecSlice, 0, mocks.GetTestVectorPool())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 0, 2}, columns)
	require.Equal(t, []int32{0, 1, 3}, vector.MustFixedCol[int32](vecWithNull.GetDownstreamVector()))
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

func TestMerge1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	// mrege not null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 5, false, nil)
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		vec2 := containers.MockVector(vecType, 5, false, nil)
		vecs = []containers.Vector{vec2}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		vec3 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 5; i++ {
			vec3.Append(vec.Get(i), vec.IsNull(i))
		}
		vec4 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 5; i++ {
			vec4.Append(vec2.Get(i), vec2.IsNull(i))
		}
		sortedIdx := make([]uint32, 10)
		mapping := make([]uint32, 10)
		ret := MergeColumn(
			[]containers.Vector{vec3, vec4}, sortedIdx, mapping, []uint32{5, 5}, []uint32{5, 5}, mocks.GetTestVectorPool(),
		)
		ret2 := ShuffleColumn(
			[]containers.Vector{vec, vec2}, sortedIdx, []uint32{5, 5}, []uint32{5, 5}, mocks.GetTestVectorPool(),
		)
		t.Log(mapping)
		for i := range ret {
			require.True(t, ret[i].Equals(ret2[i]), vecType, i)
		}
		for i := range ret {
			ret[i].Close()
			ret2[i].Close()
		}
	}

	// merge null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 5, false, nil)
		vec.Update(rand.Intn(5), nil, true)
		vec.Update(rand.Intn(5), nil, true)
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		vec2 := containers.MockVector(vecType, 5, false, nil)
		vec2.Update(rand.Intn(5), nil, true)
		vecs = []containers.Vector{vec2}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())

		vec3 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 5; i++ {
			vec3.Append(vec.Get(i), vec.IsNull(i))
		}
		vec4 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 5; i++ {
			vec4.Append(vec2.Get(i), vec2.IsNull(i))
		}
		sortedIdx := make([]uint32, 10)
		mapping := make([]uint32, 10)
		ret := MergeColumn(
			[]containers.Vector{vec3, vec4}, sortedIdx, mapping, []uint32{5, 5}, []uint32{5, 5}, mocks.GetTestVectorPool(),
		)
		ret2 := ShuffleColumn(
			[]containers.Vector{vec, vec2}, sortedIdx, []uint32{5, 5}, []uint32{5, 5}, mocks.GetTestVectorPool(),
		)
		t.Log(mapping)
		for i := range ret {
			require.True(t, ret[i].Equals(ret2[i]), vecType, i)
		}
		for i := range ret {
			ret[i].Close()
			ret2[i].Close()
		}
	}
}

func TestMerge2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		sortedIdx := make([]uint32, 100000)
		mapping := make([]uint32, 100000)
		t0 := time.Now()
		ret := MergeColumn(
			[]containers.Vector{vec, vec2}, sortedIdx, mapping, []uint32{50000, 50000}, []uint32{50000, 50000}, mocks.GetTestVectorPool(),
		)
		t.Logf("%-20v takes %v", vecType, time.Since(t0))
		for _, v := range ret {
			v.Close()
		}
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

		inputBatches := make([]*batch.Batch, 2)
		for i := range inputBatches {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vecs[i].GetDownstreamVector()
			inputBatches[i] = bat
		}
		retBatches, releaseF := ReshapeBatches(inputBatches, []uint32{4, 6}, []uint32{5, 5}, pool)
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
		inputBatches := make([]*batch.Batch, 2)
		for i := range inputBatches {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vecs[i].GetDownstreamVector()
			inputBatches[i] = bat
		}
		t0 := time.Now()
		_, releaseF := ReshapeBatches(inputBatches, []uint32{50000, 50000}, []uint32{50000, 50000}, pool)
		t.Logf("%v takes %v", vecType, time.Since(t0))
		releaseF()
	}
}
