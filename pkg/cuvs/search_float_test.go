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

func TestGpuSearchFloatAll(t *testing.T) {
	dimension := uint32(8)
	n_vectors := uint64(100)
	deviceID := 0

	// 1. Test IVF-PQ SearchFloat (with int8 quantization)
	t.Run("IVF-PQ", func(t *testing.T) {
		dataset := make([]float32, n_vectors*uint64(dimension))
		for i := range dataset {
			dataset[i] = float32(i % 10)
		}
		bp := IvfPqBuildParams{NLists: 10, M: 4, BitsPerCode: 8, AddDataOnBuild: true}
		// Create empty index
		index, err := NewGpuIvfPqEmpty[int8](n_vectors, dimension, L2Expanded, bp, []int{deviceID}, 1, SingleGpu)
		if err != nil {
			t.Fatalf("Failed to create IVF-PQ: %v", err)
		}
		defer index.Destroy()
		index.Start()
		
		// Explicitly train quantizer before adding data
		err = index.TrainQuantizer(dataset[:dimension*10], 10)
		if err != nil {
			t.Fatalf("TrainQuantizer failed: %v", err)
		}

		err = index.AddChunkFloat(dataset, n_vectors)
		if err != nil {
			t.Fatalf("AddChunkFloat failed: %v", err)
		}
		index.Build()

		queries := make([]float32, 2*uint64(dimension))
		for i := range queries {
			queries[i] = float32(i % 10)
		}
		res, err := index.SearchFloat(queries, 2, dimension, 1, IvfPqSearchParams{NProbes: 1})
		if err != nil {
			t.Fatalf("SearchFloat failed: %v", err)
		}
		if len(res.Neighbors) != 2 {
			t.Errorf("Expected 2 neighbors, got %d", len(res.Neighbors))
		}
	})

	// 2. Test IVF-Flat SearchFloat (with half quantization)
	t.Run("IVF-Flat", func(t *testing.T) {
		dataset := make([]Float16, n_vectors*uint64(dimension))
		bp := IvfFlatBuildParams{NLists: 10, AddDataOnBuild: true}
		index, err := NewGpuIvfFlat[Float16](dataset, n_vectors, dimension, L2Expanded, bp, []int{deviceID}, 1, SingleGpu)
		if err != nil {
			t.Fatalf("Failed to create IVF-Flat: %v", err)
		}
		defer index.Destroy()
		index.Start()
		index.Build()

		queries := make([]float32, uint64(dimension))
		res, err := index.SearchFloat(queries, 1, dimension, 1, IvfFlatSearchParams{NProbes: 1})
		if err != nil {
			t.Fatalf("SearchFloat failed: %v", err)
		}
		if len(res.Neighbors) != 1 {
			t.Errorf("Expected 1 neighbor, got %d", len(res.Neighbors))
		}
	})

	// 3. Test CAGRA SearchFloat (with float32)
	t.Run("CAGRA", func(t *testing.T) {
		dataset := make([]float32, n_vectors*uint64(dimension))
		bp := CagraBuildParams{IntermediateGraphDegree: 64, GraphDegree: 32, AttachDatasetOnBuild: true}
		index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, []int{deviceID}, 1, SingleGpu)
		if err != nil {
			t.Fatalf("Failed to create CAGRA: %v", err)
		}
		defer index.Destroy()
		index.Start()
		index.Build()

		queries := make([]float32, uint64(dimension))
		res, err := index.SearchFloat(queries, 1, dimension, 1, CagraSearchParams{ItopkSize: 64, SearchWidth: 1})
		if err != nil {
			t.Fatalf("SearchFloat failed: %v", err)
		}
		if len(res.Neighbors) != 1 {
			t.Errorf("Expected 1 neighbor, got %d", len(res.Neighbors))
		}
	})

	// 4. Test Brute-Force SearchFloat (with half)
	t.Run("Brute-Force", func(t *testing.T) {
		dataset := make([]Float16, n_vectors*uint64(dimension))
		index, err := NewGpuBruteForce[Float16](dataset, n_vectors, dimension, L2Expanded, 1, deviceID)
		if err != nil {
			t.Fatalf("Failed to create Brute-Force: %v", err)
		}
		defer index.Destroy()
		index.Start()
		index.Build()

		queries := make([]float32, uint64(dimension))
		neighbors, _, err := index.SearchFloat(queries, 1, dimension, 1)
		if err != nil {
			t.Fatalf("SearchFloat failed: %v", err)
		}
		if len(neighbors) != 1 {
			t.Errorf("Expected 1 neighbor, got %d", len(neighbors))
		}
	})

	// 5. Test KMeans PredictFloat (with uint8)
	t.Run("KMeans", func(t *testing.T) {
		nClusters := uint32(5)
		km, err := NewGpuKMeans[uint8](nClusters, dimension, L2Expanded, 20, deviceID, 1)
		if err != nil {
			t.Fatalf("Failed to create KMeans: %v", err)
		}
		defer km.Destroy()
		km.Start()

		dataset := make([]float32, n_vectors*uint64(dimension))
		for i := range dataset {
			dataset[i] = float32(i % 10)
		}
		
		// Explicitly train quantizer
		err = km.TrainQuantizer(dataset[:dimension*10], 10)
		if err != nil {
			t.Fatalf("TrainQuantizer failed: %v", err)
		}

		// FitPredictFloat
		labels, _, _, err := km.FitPredictFloat(dataset, n_vectors)
		if err != nil {
			t.Fatalf("FitPredictFloat failed: %v", err)
		}

		queries := make([]float32, 2*uint64(dimension))
		labels, _, err = km.PredictFloat(queries, 2)
		if err != nil {
			t.Fatalf("PredictFloat failed: %v", err)
		}
		if len(labels) != 2 {
			t.Errorf("Expected 2 labels, got %d", len(labels))
		}
	})
}
