//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

import (
	"testing"
)

func testIvfFlatGetCenters[T VectorType](t *testing.T, name string) {
	t.Run(name, func(t *testing.T) {
		dimension := uint32(16)
		n_vectors := uint64(1000)
		dataset := make([]T, n_vectors*uint64(dimension))
		// Fill some data
		for i := range dataset {
			dataset[i] = T(i % 127)
		}

		devices := []int{0}
		bp := DefaultIvfFlatBuildParams()
		bp.NLists = 16
		index, err := NewGpuIvfFlat[T](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
		if err != nil {
			t.Fatalf("Failed to create GpuIvfFlat: %v", err)
		}
		defer index.Destroy()

		if err := index.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		if err := index.Build(); err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		nLists := index.GetNList()
		centers, err := index.GetCenters(nLists)
		if err != nil {
			t.Fatalf("GetCenters failed: %v", err)
		}

		expectedLen := int(nLists * dimension)
		if len(centers) != expectedLen {
			t.Errorf("Expected centers length %d, got %d", expectedLen, len(centers))
		}
		
		// Check that centers are not all zeros (simple sanity check)
		allZeros := true
		for _, v := range centers {
			if v != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			t.Errorf("Centers are all zeros")
		}
	})
}

func TestIvfFlatGetCentersAllTypes(t *testing.T) {
	testIvfFlatGetCenters[float32](t, "float32")
	testIvfFlatGetCenters[Float16](t, "Float16")
	// testIvfFlatGetCenters[int8](t, "int8")
	// testIvfFlatGetCenters[uint8](t, "uint8")
}

func testIvfPqGetCenters[T VectorType](t *testing.T, name string) {
	t.Run(name, func(t *testing.T) {
		dimension := uint32(16)
		n_vectors := uint64(1000)
		dataset := make([]T, n_vectors*uint64(dimension))
		for i := range dataset {
			dataset[i] = T(i % 127)
		}

		devices := []int{0}
		bp := DefaultIvfPqBuildParams()
		bp.NLists = 16
		bp.M = 8
		index, err := NewGpuIvfPq[T](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
		if err != nil {
			t.Fatalf("Failed to create GpuIvfPq: %v", err)
		}
		defer index.Destroy()

		if err := index.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		if err := index.Build(); err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		centers, err := index.GetCenters()
		if err != nil {
			t.Fatalf("GetCenters failed: %v", err)
		}

		nLists := index.GetNList()
		rotDim := index.GetRotDim()
		expectedLen := int(nLists * rotDim)
		if len(centers) != expectedLen {
			t.Errorf("Expected centers length %d, got %d", expectedLen, len(centers))
		}

		allZeros := true
		for _, v := range centers {
			if v != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			t.Errorf("Centers are all zeros")
		}
	})
}

func TestIvfPqGetCentersAllTypes(t *testing.T) {
	testIvfPqGetCenters[float32](t, "float32")
	testIvfPqGetCenters[Float16](t, "Float16")
	// testIvfPqGetCenters[int8](t, "int8")
	// testIvfPqGetCenters[uint8](t, "uint8")
}
