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

package mergesort

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomStrBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func TestReshape(t *testing.T) {
	pool := mocks.GetTestVectorPool()
	fromLayout := []uint32{2, 4}
	toLayout := []uint32{3, 3}

	vec1 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[0]; i++ {
		vec1.Append(int32(i), false)
	}

	vec2 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := fromLayout[0]; i < fromLayout[0]+fromLayout[1]; i++ {
		vec2.Append(int32(i), false)
	}

	retvec := make([]*vector.Vector, len(toLayout))
	ret := make([]containers.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(vec1.GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	vecs := []*vector.Vector{vec1.GetDownstreamVector(), vec2.GetDownstreamVector()}
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	Reshape(vecs, retvec, fromLayout, toLayout, pool.GetMPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	assertLayoutEqual(t, retvec, toLayout)
	require.Equal(t, []int32{0, 1, 2}, vector.MustFixedCol[int32](retvec[0]))
	require.Equal(t, []int32{3, 4, 5}, vector.MustFixedCol[int32](retvec[1]))
	for _, v := range ret {
		v.Close()
	}
}

func TestReshapeVarLen(t *testing.T) {
	pool := mocks.GetTestVectorPool()
	fromLayout := []uint32{2000, 4000, 6000}
	toLayout := []uint32{8192, 3808}
	size := 100

	vec1 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[0]; i++ {
		vec1.Append(randomStrBytes(size), false)
	}

	vec2 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[1]; i++ {
		vec2.Append(randomStrBytes(size), false)
	}

	vec3 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[2]; i++ {
		vec3.Append(randomStrBytes(size), false)
	}

	retvec := make([]*vector.Vector, len(toLayout))
	ret := make([]containers.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(vec1.GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	vecs := []*vector.Vector{vec1.GetDownstreamVector(), vec2.GetDownstreamVector(), vec3.GetDownstreamVector()}
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	Reshape(vecs, retvec, fromLayout, toLayout, pool.GetMPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	assertLayoutEqual(t, retvec, toLayout)

	for _, v := range ret {
		v.Close()
	}
}

func TestReshapeLargeInt(t *testing.T) {
	pool := mocks.GetTestVectorPool()
	fromLayout := []uint32{2000, 4000, 6000}
	toLayout := []uint32{8192, 3808}

	vec1 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[0]; i++ {
		vec1.Append(int32(i), false)
	}

	vec2 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[1]; i++ {
		vec2.Append(int32(i), false)
	}

	vec3 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[2]; i++ {
		vec3.Append(int32(i), false)
	}

	retvec := make([]*vector.Vector, len(toLayout))
	ret := make([]containers.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(vec1.GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	vecs := []*vector.Vector{vec1.GetDownstreamVector(), vec2.GetDownstreamVector(), vec3.GetDownstreamVector()}
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	Reshape(vecs, retvec, fromLayout, toLayout, pool.GetMPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	assertLayoutEqual(t, retvec, toLayout)

	for _, v := range ret {
		v.Close()
	}
}

type testPool struct {
	pool *containers.VectorPool
}

func (p *testPool) GetVector(typ *types.Type) (ret *vector.Vector, release func()) {
	v := p.pool.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (p *testPool) GetMPool() *mpool.MPool {
	return p.pool.GetMPool()
}

func TestReshapeBatches(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	batchSize := 5
	fromLayout := []uint32{2000, 4000, 6000}
	toLayout := []uint32{8192, 3808}

	batches := make([]*batch.Batch, len(fromLayout))
	for i := range fromLayout {
		batches[i] = batch.NewWithSize(batchSize)
		for j := 0; j < batchSize; j++ {
			vec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
			for k := uint32(0); k < fromLayout[i]; k++ {
				vec.Append(int32(k), false)
			}
			batches[i].Vecs[j] = vec.GetDownstreamVector()
			batches[i].SetRowCount(vec.Length())
		}
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	retBatch := make([]*batch.Batch, len(toLayout))
	rfs := make([]func(), len(toLayout))
	defer func() {
		for _, f := range rfs {
			f()
		}
	}()
	for i := range toLayout {
		var f func()
		retBatch[i], f = getSimilarBatch(batches[0], int(toLayout[i]), pool)
		rfs[i] = f
	}

	ReshapeBatches(batches, retBatch, fromLayout, toLayout, pool.GetMPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	for i, v := range retBatch {
		require.Equal(t, toLayout[i], uint32(v.RowCount()))
	}
}

func TestReshapeBatchesInOriginalWay(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	batchSize := 5
	fromLayout := []uint32{2000, 4000, 6000}
	toLayout := []uint32{8192, 3808}

	batches := make([]*batch.Batch, len(fromLayout))
	for i := range fromLayout {
		batches[i] = batch.NewWithSize(batchSize)
		for j := 0; j < batchSize; j++ {
			vec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
			for k := uint32(0); k < fromLayout[i]; k++ {
				vec.Append(int32(k), false)
			}
			batches[i].Vecs[j] = vec.GetDownstreamVector()
			batches[i].SetRowCount(vec.Length())
		}
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	attrs := []string{"a", "b", "c", "d", "e"}
	toSortVecs := make([]*vector.Vector, 0, len(batches))
	for i := range batches {
		toSortVecs = append(toSortVecs, batches[i].GetVector(int32(0)))
	}
	sortedVecs, releaseF := getRetVecs(len(toLayout), toSortVecs[0].GetType(), pool)
	defer releaseF()

	Reshape(toSortVecs, sortedVecs, fromLayout, toLayout, pool.GetMPool())
	retBatches := make([]*batch.Batch, 0, len(sortedVecs))

	for _, vec := range sortedVecs {
		b := batch.New(true, attrs)
		b.SetRowCount(vec.Length())
		retBatches = append(retBatches, b)
	}

	// arrange the other columns according to sortedidx, or, just reshape
	tempVecs := make([]*vector.Vector, 0, len(batches))
	for i := range attrs {
		// just put the sorted column to the write batch
		if i == 0 {
			for j, vec := range sortedVecs {
				retBatches[j].Vecs[i] = vec
			}
			continue
		}
		tempVecs = tempVecs[:0]

		for _, bat := range batches {
			if bat.RowCount() == 0 {
				continue
			}
			tempVecs = append(tempVecs, bat.Vecs[i])
		}
		require.Equal(t, len(toSortVecs), len(tempVecs))
		outvecs, release := getRetVecs(len(toLayout), tempVecs[0].GetType(), pool)
		defer release()

		require.Equal(t, len(sortedVecs), len(outvecs))

		Reshape(tempVecs, outvecs, fromLayout, toLayout, pool.GetMPool())

		for j, vec := range outvecs {
			retBatches[j].Vecs[i] = vec
		}
	}
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	for i, v := range retBatches {
		require.Equal(t, toLayout[i], uint32(v.RowCount()))
	}
}

// get vector from pool, and return a release function
func getRetVecs(count int, t *types.Type, vpool DisposableVecPool) (ret []*vector.Vector, releaseAll func()) {
	var fs []func()
	for i := 0; i < count; i++ {
		vec, release := vpool.GetVector(t)
		ret = append(ret, vec)
		fs = append(fs, release)
	}
	releaseAll = func() {
		for i := 0; i < count; i++ {
			fs[i]()
		}
	}
	return
}

func assertLayoutEqual(t *testing.T, vectors []*vector.Vector, layout []uint32) {
	for i, v := range vectors {
		require.Equal(t, layout[i], uint32(v.Length()))
	}
}
