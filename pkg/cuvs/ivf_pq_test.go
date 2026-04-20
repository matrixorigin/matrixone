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
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func TestGpuIvfPq(t *testing.T) {
	dimension := uint32(16)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8 // dimension 16 is divisible by 8
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	defer index.Destroy()

	err = index.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load/build GpuIvfPq: %v", err)
	}

	centers, err := index.GetCenters()
	if err != nil {
		t.Fatalf("GetCenters failed: %v", err)
	}
	t.Logf("Centers count: %d, dim_ext: %d", len(centers)/int(index.GetDimExt()), index.GetDimExt())

	query := make([]float32, dimension)
	for i := uint32(0); i < dimension; i++ {
		query[i] = 10.0 // Matches vector 1 exactly (1 * 10)
	}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10
	result, err := index.Search(query, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	t.Logf("Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
	if result.Neighbors[0] != 1 {
		t.Errorf("Expected neighbor 1, got %d", result.Neighbors[0])
	}
}

func TestGpuIvfPqSaveLoad(t *testing.T) {
	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	index.Start()
	index.Build()

	filename := "test_ivf_pq.idx"
	err = index.Save(filename)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	defer os.Remove(filename)
	index.Destroy()

	index2, err := NewGpuIvfPqFromFile[float32](filename, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq from file: %v", err)
	}
	defer index2.Destroy()

	err = index2.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err = index2.Build()
	if err != nil {
		t.Fatalf("Load from file failed: %v", err)
	}

	query := make([]float32, dimension) // all zeros
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10
	result, err := index2.Search(query, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 0 {
		t.Errorf("Expected 0, got %d", result.Neighbors[0])
	}
}

func TestGpuIvfPqPackUnpack(t *testing.T) {
	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	index.Start()
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for _, filename := range []string{"test_ivf_pq_pack.tar", "test_ivf_pq_pack.tar.gz"} {
		t.Run(filename, func(t *testing.T) {
			if err := index.Pack(filename); err != nil {
				t.Fatalf("Pack failed: %v", err)
			}
			defer os.Remove(filename)

			index2, err := NewGpuIvfPqEmpty[float32](0, dimension, L2Expanded, bp, devices, 1, SingleGpu)
			if err != nil {
				t.Fatalf("NewGpuIvfPqEmpty failed: %v", err)
			}
			defer index2.Destroy()
			if err := index2.Start(); err != nil {
				t.Fatalf("index2 Start failed: %v", err)
			}
			if err := index2.Unpack(filename); err != nil {
				t.Fatalf("Unpack failed: %v", err)
			}

			query := make([]float32, dimension)
			sp := DefaultIvfPqSearchParams()
			sp.NProbes = 10
			result, err := index2.Search(query, 1, dimension, 1, sp)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			if result.Neighbors[0] != 0 {
				t.Errorf("Expected neighbor 0, got %d", result.Neighbors[0])
			}
		})
	}
	index.Destroy()
}

func TestGpuIvfPqFromDataDirectory(t *testing.T) {
	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	index.Start()
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	tarFile := "test_ivf_pq_dir.tar"
	if err := index.Pack(tarFile); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}
	defer os.Remove(tarFile)
	index.Destroy()

	tmpDir, err := os.MkdirTemp("", "ivf-pq-dir-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if _, err := Unpack(tarFile, tmpDir); err != nil {
		t.Fatalf("Unpack to dir failed: %v", err)
	}

	index2, err := NewGpuIvfPqFromDataDirectory[float32](tmpDir, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("NewGpuIvfPqFromDataDirectory failed: %v", err)
	}
	defer index2.Destroy()

	query := make([]float32, dimension)
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10
	result, err := index2.Search(query, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 0 {
		t.Errorf("Expected neighbor 0, got %d", result.Neighbors[0])
	}
}

func TestGpuIvfPqChunked(t *testing.T) {
	dimension := uint32(8)
	totalCount := uint64(100)
	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 4

	// Create empty index (target type int8)
	index, err := NewGpuIvfPqEmpty[int8](totalCount, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPqEmpty: %v", err)
	}
	defer index.Destroy()

	err = index.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Add data in chunks (from float32, triggers on-the-fly quantization)
	chunkSize := uint64(50)
	for i := uint64(0); i < totalCount; i += chunkSize {
		chunk := make([]float32, chunkSize*uint64(dimension))
		val := float32(i/chunkSize*100 + 1) // 1.0 for first chunk, 101.0 for second
		for j := range chunk {
			chunk[j] = val
		}
		err = index.AddChunkFloat(chunk, chunkSize, nil)
		if err != nil {
			t.Fatalf("AddChunkFloat failed at offset %d: %v", i, err)
		}
	}

	// Debug: check dataset
	ds := index.GetDataset(totalCount * uint64(dimension))
	t.Logf("Dataset[0]: %v, Dataset[50*dim]: %v", ds[0], ds[50*uint64(dimension)])

	// Build index
	err = index.Build()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Search for first chunk
	query1 := make([]int8, dimension)
	for i := range query1 {
		query1[i] = -128 // matches first chunk (1.0)
	}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3
	result1, err := index.Search(query1, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 1 failed: %v", err)
	}
	if result1.Neighbors[0] < 0 || result1.Neighbors[0] >= 50 {
		t.Errorf("Expected neighbor from first chunk (0-49), got %d", result1.Neighbors[0])
	}

	// Search for second chunk
	query2 := make([]int8, dimension)
	for i := range query2 {
		query2[i] = 127 // matches second chunk (101.0)
	}
	result2, err := index.Search(query2, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 2 failed: %v", err)
	}
	if result2.Neighbors[0] < 50 || result2.Neighbors[0] >= 100 {
		t.Errorf("Expected neighbor from second chunk (50-99), got %d", result2.Neighbors[0])
	}
}

func TestGpuShardedIvfPq(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for sharded IVF-PQ test")
	}

	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded, nil)
	if err != nil {
		t.Fatalf("Failed to create sharded IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load sharded failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.1, 0.1, 10.1, 10.1, 10.1, 10.1}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 5
	result, err := index.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search sharded failed: %v", err)
	}
	t.Logf("Sharded Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func TestGpuReplicatedIvfPq(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for replicated IVF-PQ test")
	}

	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Replicated, nil)
	if err != nil {
		t.Fatalf("Failed to create replicated IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load replicated failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.1, 0.1, 10.1, 10.1, 10.1, 10.1}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 5
	result, err := index.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search replicated failed: %v", err)
	}
	t.Logf("Replicated Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func TestGpuIvfPqExtend(t *testing.T) {
	dimension := uint32(16)
	nBase := uint64(100)
	dataset := make([]float32, nBase*uint64(dimension))
	for i := uint64(0); i < nBase; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8
	index, err := NewGpuIvfPq[float32](dataset, nBase, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	defer index.Destroy()

	index.Start()
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	nExt := uint64(50)
	ext := make([]float32, nExt*uint64(dimension))
	extIDs := make([]int64, nExt)
	for i := uint64(0); i < nExt; i++ {
		extIDs[i] = int64(2000 + i)
		for j := uint32(0); j < dimension; j++ {
			ext[i*uint64(dimension)+uint64(j)] = 500.5
		}
	}
	if err := index.Extend(ext, nExt, extIDs); err != nil {
		t.Fatalf("Extend failed: %v", err)
	}

	if got := index.Len(); got != uint64(nBase+nExt) {
		t.Errorf("Len() = %d, want %d", got, nBase+nExt)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	// Query near base set: expect ID 0
	q0 := make([]float32, dimension)
	r, err := index.Search(q0, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if r.Neighbors[0] != 0 {
		t.Errorf("expected neighbor 0, got %d", r.Neighbors[0])
	}

	// Query exactly at extended cluster; expect ID in [2000, 2050)
	q500 := make([]float32, dimension)
	for j := range q500 {
		q500[j] = 500.5
	}
	r, err = index.Search(q500, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if r.Neighbors[0] < 2000 || r.Neighbors[0] >= 2050 {
		t.Errorf("expected neighbor in [2000, 2050), got %d", r.Neighbors[0])
	}
}

func TestGpuIvfPqExtendFloat(t *testing.T) {
	dimension := uint32(16)
	nBase := uint64(100)
	datasetF32 := make([]float32, nBase*uint64(dimension))
	for i := uint64(0); i < nBase; i++ {
		for j := uint32(0); j < dimension; j++ {
			datasetF32[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}
	dataset := make([]Float16, len(datasetF32))
	if err := GpuConvertF32ToF16(datasetF32, dataset, 0); err != nil {
		t.Fatalf("GpuConvertF32ToF16 failed: %v", err)
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8
	// Use Float16 so ExtendFloat exercises quantization
	index, err := NewGpuIvfPq[Float16](dataset, nBase, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq[Float16]: %v", err)
	}
	defer index.Destroy()

	index.Start()
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	nExt := uint64(50)
	ext := make([]float32, nExt*uint64(dimension))
	extIDs := make([]int64, nExt)
	for i := uint64(0); i < nExt; i++ {
		extIDs[i] = int64(3000 + i)
		for j := uint32(0); j < dimension; j++ {
			ext[i*uint64(dimension)+uint64(j)] = 500.5
		}
	}
	if err := index.ExtendFloat(ext, nExt, extIDs); err != nil {
		t.Fatalf("ExtendFloat failed: %v", err)
	}

	if got := index.Len(); got != uint64(nBase+nExt) {
		t.Errorf("Len() = %d, want %d", got, nBase+nExt)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	// Query exactly at extended cluster; expect ID in [3000, 3050)
	q500 := make([]float32, dimension)
	for j := range q500 {
		q500[j] = 500.5
	}
	r, err := index.SearchFloat(q500, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("SearchFloat failed: %v", err)
	}
	if r.Neighbors[0] < 3000 || r.Neighbors[0] >= 3050 {
		t.Errorf("expected neighbor in [3000, 3050), got %d", r.Neighbors[0])
	}
}

func TestGpuIvfPqDeleteId(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for deletion test")
	}

	dimension := uint32(16)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	defer index.Destroy()

	index.Start()
	index.Build()

	// Query exactly at vector 50
	q50 := make([]float32, dimension)
	for i := range q50 {
		q50[i] = 500.0 // 50 * 10
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	// 1. Test Search
	r, err := index.Search(q50, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	// IVF-PQ is approximate, but at query=data it should be very close to ID 50
	if r.Neighbors[0] != 50 {
		t.Logf("Warning: Search neighbor was %d, expected 50 (approximate search)", r.Neighbors[0])
	}

	// Delete ID 50
	if err := index.DeleteId(50); err != nil {
		t.Fatalf("DeleteId failed: %v", err)
	}

	// Search again
	r, err = index.Search(q50, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if r.Neighbors[0] == 50 {
		t.Errorf("Search: Neighbor 50 was deleted but still returned")
	}

	// 2. Test SearchFloat (this verifies the fix in search_float_internal)
	r, err = index.SearchFloat(q50, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("SearchFloat failed: %v", err)
	}
	if r.Neighbors[0] == 50 {
		t.Errorf("SearchFloat: Neighbor 50 was deleted but still returned")
	}
}

func BenchmarkGpuShardedIvfPq(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for sharded IVF-PQ benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 1000
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Sharded, nil)
	if err != nil {
		b.Fatalf("Failed to create sharded IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3

	for _, windowUs := range []int64{0, 100} {
		b.Run(fmt.Sprintf("BatchWindow%d", windowUs), func(b *testing.B) {
			index.SetBatchWindow(windowUs)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuSingleIvfPq(b *testing.B) {
	devices := []int{0}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 1000
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, SingleGpu, nil)
	if err != nil {
		b.Fatalf("Failed to create single IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3

	for _, windowUs := range []int64{0, 100} {
		b.Run(fmt.Sprintf("BatchWindow%d", windowUs), func(b *testing.B) {
			index.SetBatchWindow(windowUs)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuReplicatedIvfPq(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for replicated IVF-PQ benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 1000
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Replicated, nil)
	if err != nil {
		b.Fatalf("Failed to create replicated IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3

	for _, windowUs := range []int64{0, 100} {
		b.Run(fmt.Sprintf("BatchWindow%d", windowUs), func(b *testing.B) {
			index.SetBatchWindow(windowUs)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuAddChunkAndSearchIvfPqF16(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 1000
	// Use Float16 as internal type
	index, err := NewGpuIvfPqEmpty[Float16](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Add data in chunks using AddChunkFloat
	for i := 0; i < totalCount; i += chunkSize {
		chunk := dataset[i*dimension : (i+chunkSize)*dimension]
		if err := index.AddChunkFloat(chunk, uint64(chunkSize), nil); err != nil {
			b.Fatalf("AddChunkFloat failed at %d: %v", i, err)
		}
	}

	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})
}

func BenchmarkGpuAddChunkAndSearchIvfPqInt8(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 1000
	// Use int8 as internal type
	index, err := NewGpuIvfPqEmpty[int8](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Add data in chunks using AddChunkFloat
	for i := 0; i < totalCount; i += chunkSize {
		chunk := dataset[i*dimension : (i+chunkSize)*dimension]
		if err := index.AddChunkFloat(chunk, uint64(chunkSize), nil); err != nil {
			b.Fatalf("AddChunkFloat failed at %d: %v", i, err)
		}
	}

	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 3

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})
}
