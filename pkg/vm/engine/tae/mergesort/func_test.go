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
	Reshape(vecs, retvec, fromLayout, toLayout, pool.MPool())
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
	Reshape(vecs, retvec, fromLayout, toLayout, pool.MPool())
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
	Reshape(vecs, retvec, fromLayout, toLayout, pool.MPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	assertLayoutEqual(t, retvec, toLayout)

	for _, v := range ret {
		v.Close()
	}
}

func assertLayoutEqual(t *testing.T, vectors []*vector.Vector, layout []uint32) {
	for i, v := range vectors {
		require.Equal(t, layout[i], uint32(v.Length()))
	}
}
