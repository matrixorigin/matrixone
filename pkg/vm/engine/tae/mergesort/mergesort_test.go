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
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

func TestSort1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	pool := mocks.GetTestVectorPool()

	// sort not null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, nil)
		vec2 := containers.MakeVector(vecType)
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
		vec2 := containers.MakeVector(vecType)
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
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10000, false, nil)
		t0 := time.Now()
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		t.Logf("%-20v takes %v", vecType.String(), time.Since(t0))
		vec.Close()
	}
}
func TestMerge1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	// mrege not null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 5, false, nil)
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		vec2 := containers.MockVector(vecType, 5, false, nil)
		vecs = []containers.Vector{vec2}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		vec3 := containers.MakeVector(vecType)
		for i := 0; i < 5; i++ {
			vec3.Append(vec.Get(i), vec.IsNull(i))
		}
		vec4 := containers.MakeVector(vecType)
		for i := 0; i < 5; i++ {
			vec4.Append(vec2.Get(i), vec2.IsNull(i))
		}
		sortedIdx := make([]uint32, 10)
		ret, mapping := MergeSortedColumn([]containers.Vector{vec3, vec4}, &sortedIdx, []uint32{5, 5}, []uint32{5, 5})
		ret2 := ShuffleColumn([]containers.Vector{vec, vec2}, sortedIdx, []uint32{5, 5}, []uint32{5, 5})
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

		vec3 := containers.MakeVector(vecType)
		for i := 0; i < 5; i++ {
			vec3.Append(vec.Get(i), vec.IsNull(i))
		}
		vec4 := containers.MakeVector(vecType)
		for i := 0; i < 5; i++ {
			vec4.Append(vec2.Get(i), vec2.IsNull(i))
		}
		sortedIdx := make([]uint32, 10)
		ret, mapping := MergeSortedColumn([]containers.Vector{vec3, vec4}, &sortedIdx, []uint32{5, 5}, []uint32{5, 5})
		ret2 := ShuffleColumn([]containers.Vector{vec, vec2}, sortedIdx, []uint32{5, 5}, []uint32{5, 5})
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
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		sortedIdx := make([]uint32, 100000)
		t0 := time.Now()
		ret, _ := MergeSortedColumn([]containers.Vector{vec, vec2}, &sortedIdx, []uint32{50000, 50000}, []uint32{50000, 50000})
		t.Logf("%-20v takes %v", vecType, time.Since(t0))
		for _, v := range ret {
			v.Close()
		}
	}
}
func TestReshape1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes(18)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 4, false, nil)
		vec.Update(rand.Intn(4), nil, true)
		vec2 := containers.MockVector(vecType, 6, false, nil)
		vec2.Update(rand.Intn(6), nil, true)
		t.Log(vec)
		t.Log(vec2)
		ret := Reshape([]containers.Vector{vec, vec2}, []uint32{4, 6}, []uint32{5, 5})
		t.Log(ret)
		for _, v := range ret {
			v.Close()
		}
	}
}
func TestReshape2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes(18)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		t0 := time.Now()
		ret := Reshape([]containers.Vector{vec, vec2}, []uint32{50000, 50000}, []uint32{50000, 50000})
		t.Logf("%v takes %v", vecType, time.Since(t0))
		for _, v := range ret {
			v.Close()
		}
	}
}
