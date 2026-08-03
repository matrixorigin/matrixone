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
	"golang.org/x/exp/rand"
)

// f16 converts a float32 to the cuVS half (Float16) bit pattern. types.Float16
// and cuvs.Float16 are both uint16 IEEE-754 halfs, so the bits transfer directly.
func f16(f float32) Float16 { return Float16(types.Float16FromFloat32(f)) }

// makeF16Dataset builds a deterministic random half dataset in [0,1).
func makeF16Dataset(n uint64, dim uint32) []Float16 {
	r := rand.New(rand.NewSource(1))
	ds := make([]Float16, n*uint64(dim))
	for i := range ds {
		ds[i] = f16(r.Float32())
	}
	return ds
}

// TestGpuF16QuantizeAll covers the vecf16-base -> int8/uint8 quantization path
// (the native half-source quantizer) for IVF-PQ and CAGRA: build from native
// Float16 input via AddChunkQuantize, then SearchQuantize with a Float16 query.
// This is the f16->int8 / f16->uint8 combination the float32-base info_test and
// search_float_test do not exercise.
//
// Correctness is graded as self-match RECALL: each probe query is an exact copy
// of a stored row, so a working quantized search must return that row's id in the
// top-k for the large majority of probes. (Exact top-1 is not asserted — int8/
// uint8 + product quantization are lossy by design; recall is the right metric,
// matching the C++ Int8VsUint8SignedDataHalf test.) A broken search would score
// ~0, so the 0.8 floor is a real correctness check, not a shape check.
func TestGpuF16QuantizeAll(t *testing.T) {
	const (
		dimension = uint32(16)
		nVectors  = uint64(2000)
		k         = uint32(10)
		minRecall = 0.8
	)
	deviceID := 0
	ds := makeF16Dataset(nVectors, dimension)
	probes := []int64{0, 1, 7, 100, 500, 999, 1500, 1999} // rows to self-query

	t.Run("IVF-PQ/f16-int8", func(t *testing.T) {
		runIvfPqF16Quant[int8](t, ds, probes, nVectors, dimension, k, deviceID, minRecall)
	})
	t.Run("IVF-PQ/f16-uint8", func(t *testing.T) {
		runIvfPqF16Quant[uint8](t, ds, probes, nVectors, dimension, k, deviceID, minRecall)
	})
	t.Run("CAGRA/f16-int8", func(t *testing.T) {
		runCagraF16Quant[int8](t, ds, probes, nVectors, dimension, k, deviceID, minRecall)
	})
	t.Run("CAGRA/f16-uint8", func(t *testing.T) {
		runCagraF16Quant[uint8](t, ds, probes, nVectors, dimension, k, deviceID, minRecall)
	})
}

// rowVec returns a copy of row id from ds (an exact self-query).
func rowVec(ds []Float16, id int64, dim uint32) []Float16 {
	off := uint64(id) * uint64(dim)
	return append([]Float16(nil), ds[off:off+uint64(dim)]...)
}

func contains(xs []int64, want int64) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// gradeSelfMatch runs each probe as a self-query and returns the fraction whose
// own id appears in the top-k.
func gradeSelfMatch(t *testing.T, probes []int64, k uint32, search func(int64) ([]int64, error)) float64 {
	t.Helper()
	hits := 0
	for _, id := range probes {
		neighbors, err := search(id)
		if err != nil {
			t.Fatalf("SearchQuantize(row %d): %v", id, err)
		}
		if uint32(len(neighbors)) != k {
			t.Fatalf("row %d: expected %d neighbors, got %d", id, k, len(neighbors))
		}
		if contains(neighbors, id) {
			hits++
		}
	}
	return float64(hits) / float64(len(probes))
}

func runIvfPqF16Quant[Q VectorType](t *testing.T, ds []Float16, probes []int64, n uint64, dim, k uint32, dev int, minRecall float64) {
	// Start from the defaults (which set KmeansTrainsetFraction etc.) and override
	// only NLists — the default 1024 lists would be near-empty for 2000 vectors.
	// A struct literal would zero-default the omitted fields, e.g.
	// KmeansTrainsetFraction=0 => no kmeans training => near-zero recall.
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 50
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
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = bp.NLists // probe every list for a deterministic exhaustive search
	recall := gradeSelfMatch(t, probes, k, func(id int64) ([]int64, error) {
		res, err := index.SearchQuantize(rowVec(ds, id, dim), 1, dim, k, sp)
		return res.Neighbors, err
	})
	if recall < minRecall {
		t.Errorf("IVF-PQ f16-quant self-match recall %.2f < %.2f", recall, minRecall)
	}
}

func runCagraF16Quant[Q VectorType](t *testing.T, ds []Float16, probes []int64, n uint64, dim, k uint32, dev int, minRecall float64) {
	bp := DefaultCagraBuildParams()
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
	sp := DefaultCagraSearchParams()
	recall := gradeSelfMatch(t, probes, k, func(id int64) ([]int64, error) {
		res, err := index.SearchQuantize(rowVec(ds, id, dim), 1, dim, k, sp)
		return res.Neighbors, err
	})
	if recall < minRecall {
		t.Errorf("CAGRA f16-quant self-match recall %.2f < %.2f", recall, minRecall)
	}
}
