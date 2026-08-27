// Copyright 2026 Matrix Origin
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

//go:build gpu

package cuvs

import (
	"strings"
	"testing"
)

// A CAGRA build costs dataset + intermediate kNN graph, and capacity is planned
// against that sum (cagra_cost, index_cost.hpp). The graph is the larger half at
// low dimension -- igd*8 is 1024 B/row at the default degree, against dim*4 for
// an f32 dataset -- and it is TRANSIENT: optimize() frees it before build()
// returns, so a live cudaMemGetInfo never shows it to a concurrent admission.
//
// Claiming only the upload therefore left that half admitted nowhere: the small
// dataset fits, build() then allocates a graph several times its size, and a
// plan that was valid when it was made OOMs because something else took the
// memory in between. This pins that the graph is admitted on its own.
func TestCagraBuildRefusesWhenGraphDoesNotFit(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU")
	}
	const dev = 0

	const (
		// Graph-dominant on purpose: 8 f32 = 32 B/row of dataset against
		// 128*8 = 1024 B/row of graph, so the upload is ~3% of the demand and
		// only the graph term can be what refuses the build.
		dimension = uint32(8)
		nVectors  = uint64(400_000)
		igd       = uint64(128)
	)
	datasetBytes := nVectors * uint64(dimension) * 4
	graphBytes := nVectors * igd * 8

	// The governor admits against 60% of FREE, so ask it what free is rather
	// than assuming: perRow=1 makes rows_fitting_gpu_mem report the byte budget.
	budget, freeBytes, err := RowsFittingFreeMem(dev, 1)
	if err != nil {
		t.Skipf("cannot measure device %d: %v", dev, err)
	}
	if uint64(budget) <= graphBytes+datasetBytes {
		t.Skipf("device has only %d MB of budget; need more than %d MB to set this up",
			budget>>20, (graphBytes+datasetBytes)>>20)
	}

	// Squeeze the budget so the dataset still fits and the graph cannot: hold
	// everything except a margin between the two sizes.
	margin := datasetBytes + (graphBytes-datasetBytes)/2
	hold := uint64(budget) - margin
	held, err := ReserveDeviceMemory(dev, hold)
	if err != nil {
		t.Skipf("could not hold %d MB (free=%d MB): %v", hold>>20, freeBytes>>20, err)
	}
	defer held.Release()

	dataset := make([]float32, nVectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = float32(i % 1000)
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = igd
	index, err := NewGpuCagra[float32, float32](
		dataset, nVectors, dimension, L2Expanded, bp, []int{dev}, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("NewGpuCagra: %v", err)
	}
	defer index.Destroy()
	if err := index.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The upload (~12 MB) fits inside the margin; the graph (~390 MB) cannot.
	err = index.Build()
	if err == nil {
		t.Fatalf("build was admitted with only %d MB of budget left for a %d MB graph",
			margin>>20, graphBytes>>20)
	}
	// It must be the GRAPH claim that refused, not the upload: a refusal from the
	// upload would mean the margin was mis-sized and the test proves nothing.
	if !strings.Contains(err.Error(), "cagra::build graph") {
		t.Fatalf("expected the graph claim to refuse, got: %v", err)
	}
	t.Logf("refused as intended: %v", err)
}
