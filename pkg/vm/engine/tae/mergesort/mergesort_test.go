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

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func TestSort1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, false, nil)
		vec2 := containers.MockVector(vecType, 10, false, true, nil)
		vec2.Update(rand.Intn(10), types.Null{})
		vecs := []containers.Vector{vec, vec2}
		t.Log(vec)
		t.Log(vec2)
		_, _ = SortBlockColumns(vecs, 0)
		t.Log(vec)
		t.Log(vec2)
		vec.Close()
		vec2.Close()
	}
}
func TestSort2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10000, false, false, nil)
		t0 := time.Now()
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0)
		t.Logf("%v takes %v", vecType.String(), time.Since(t0))
		vec.Close()
	}
}
func TestMerge1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 5, false, false, nil)
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0)
		vec2 := containers.MockVector(vecType, 5, false, false, nil)
		vecs = []containers.Vector{vec2}
		_, _ = SortBlockColumns(vecs, 0)
		vec3 := containers.MockVector(vecType, 5, false, true, nil)
		vec3.Update(rand.Intn(5), types.Null{})
		vec4 := containers.MockVector(vecType, 5, false, true, nil)
		vec4.Update(rand.Intn(5), types.Null{})
		t.Log(vec)
		t.Log(vec2)
		t.Log(vec3)
		t.Log(vec4)
		sortedIdx := make([]uint32, 10)
		ret, mapping := MergeSortedColumn([]containers.Vector{vec, vec2}, &sortedIdx, []uint32{5, 5}, []uint32{5, 5})
		ret2 := ShuffleColumn([]containers.Vector{vec3, vec4}, sortedIdx, []uint32{5, 5}, []uint32{5, 5})
		t.Log(ret)
		t.Log(mapping)
		t.Log(ret2)
		for _, v := range ret {
			v.Close()
		}
		for _, v := range ret2 {
			v.Close()
		}
	}
}
func TestMerge2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, false, nil)
		sortedIdx := make([]uint32, 100000)
		t0 := time.Now()
		ret, _ := MergeSortedColumn([]containers.Vector{vec, vec2}, &sortedIdx, []uint32{50000, 50000}, []uint32{50000, 50000})
		t.Logf("%v takes %v", vecType, time.Since(t0))
		for _, v := range ret {
			v.Close()
		}

	}
}
func TestReshape1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(18)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 4, false, true, nil)
		vec.Update(rand.Intn(4), types.Null{})
		vec2 := containers.MockVector(vecType, 6, false, true, nil)
		vec2.Update(rand.Intn(6), types.Null{})
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
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(18)
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, true, nil)
		vec2 := containers.MockVector(vecType, 50000, false, true, nil)
		t0 := time.Now()
		ret := Reshape([]containers.Vector{vec, vec2}, []uint32{50000, 50000}, []uint32{50000, 50000})
		t.Logf("%v takes %v", vecType, time.Since(t0))
		for _, v := range ret {
			v.Close()
		}
	}
}
