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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// f16 converts a float32 to the cuVS half (Float16) bit pattern. types.Float16
// and cuvs.Float16 are both uint16 IEEE-754 halfs, so the bits transfer directly.
func f16(f float32) Float16 { return Float16(types.Float16FromFloat32(f)) }

// makeF16Dataset builds a deterministic, distinct-per-row Float16 dataset.
func makeF16Dataset(n uint64, dim uint32) []Float16 {
	ds := make([]Float16, n*uint64(dim))
	for i := uint64(0); i < n; i++ {
		for j := uint32(0); j < dim; j++ {
			v := float32((i*7+uint64(j)*13)%97) / 97.0
			ds[i*uint64(dim)+uint64(j)] = f16(v)
		}
	}
	return ds
}

// TestGpuF16QuantizeAll covers the vecf16-base -> int8/uint8 quantization path
// (the native half-source quantizer) for IVF-PQ and CAGRA: build via
// AddChunkQuantize from native Float16 input, then SearchQuantize with a Float16
// query. This is the f16->int8 / f16->uint8 combination that the float32-base
// info_test matrix and search_float_test do not exercise.
func TestGpuF16QuantizeAll(t *testing.T) {
	const (
		dimension = uint32(16)
		nVectors  = uint64(2000)
		k         = uint32(5)
	)
	deviceID := 0
	ds := makeF16Dataset(nVectors, dimension)
	// Self-query: the first base vector. This test verifies the f16->int8/uint8
	// build+search PATH returns valid results; exact-match recall under
	// quantization is covered separately by the C++ Int8VsUint8SignedDataHalf test.
	query := append([]Float16(nil), ds[:dimension]...)

	checkResult := func(t *testing.T, neighbors []int64) {
		if uint32(len(neighbors)) != k {
			t.Fatalf("expected %d neighbors, got %d", k, len(neighbors))
		}
		for _, n := range neighbors {
			if n < 0 || n >= int64(nVectors) {
				t.Fatalf("neighbor id %d out of range [0,%d)", n, nVectors)
			}
		}
	}

	// ---- IVF-PQ: f16 -> {int8, uint8} ----
	t.Run("IVF-PQ/f16-int8", func(t *testing.T) {
		runIvfPqF16Quant[int8](t, ds, query, nVectors, dimension, k, deviceID, checkResult)
	})
	t.Run("IVF-PQ/f16-uint8", func(t *testing.T) {
		runIvfPqF16Quant[uint8](t, ds, query, nVectors, dimension, k, deviceID, checkResult)
	})

	// ---- CAGRA: f16 -> {int8, uint8} ----
	t.Run("CAGRA/f16-int8", func(t *testing.T) {
		runCagraF16Quant[int8](t, ds, query, nVectors, dimension, k, deviceID, checkResult)
	})
	t.Run("CAGRA/f16-uint8", func(t *testing.T) {
		runCagraF16Quant[uint8](t, ds, query, nVectors, dimension, k, deviceID, checkResult)
	})
}

func runIvfPqF16Quant[Q VectorType](t *testing.T, ds, query []Float16, n uint64, dim, k uint32, dev int, check func(*testing.T, []int64)) {
	bp := IvfPqBuildParams{NLists: 50, M: 4, BitsPerCode: 8, AddDataOnBuild: true}
	index, err := NewGpuIvfPqEmpty[Float16, Q](n, dim, L2Expanded, bp, []int{dev}, 1, SingleGpu)
	if err != nil {
		t.Fatalf("NewGpuIvfPqEmpty[Float16,Q]: %v", err)
	}
	defer index.Destroy()
	index.Start()
	if err = index.TrainQuantizer(ds, n); err != nil {
		t.Fatalf("TrainQuantizer: %v", err)
	}
	if err = index.AddChunkQuantize(ds, n, nil); err != nil {
		t.Fatalf("AddChunkQuantize: %v", err)
	}
	if err = index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	res, err := index.SearchQuantize(query, 1, dim, k, IvfPqSearchParams{NProbes: 50})
	if err != nil {
		t.Fatalf("SearchQuantize: %v", err)
	}
	check(t, res.Neighbors)
}

func runCagraF16Quant[Q VectorType](t *testing.T, ds, query []Float16, n uint64, dim, k uint32, dev int, check func(*testing.T, []int64)) {
	bp := CagraBuildParams{IntermediateGraphDegree: 128, GraphDegree: 64, AttachDatasetOnBuild: true}
	index, err := NewGpuCagraEmpty[Float16, Q](n, dim, L2Expanded, bp, []int{dev}, 1, SingleGpu)
	if err != nil {
		t.Fatalf("NewGpuCagraEmpty[Float16,Q]: %v", err)
	}
	defer index.Destroy()
	index.Start()
	if err = index.TrainQuantizer(ds, n); err != nil {
		t.Fatalf("TrainQuantizer: %v", err)
	}
	if err = index.AddChunkQuantize(ds, n, nil); err != nil {
		t.Fatalf("AddChunkQuantize: %v", err)
	}
	if err = index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	res, err := index.SearchQuantize(query, 1, dim, k, CagraSearchParams{ItopkSize: 64})
	if err != nil {
		t.Fatalf("SearchQuantize: %v", err)
	}
	check(t, res.Neighbors)
}
