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
	"testing"

	"github.com/stretchr/testify/require"
)

// Scalar-quantizer training uploads an n_rows x dimension device matrix and was
// the one large allocation on this path taking NO governor claim.
// cap_train_rows_to_gpu_mem only samples free VRAM, and a snapshot cannot
// serialise anything: two int8 builds could observe the same free memory and
// upload concurrently, breaking the invariant that every large GPU allocation is
// governed.
//
// Driven by SQUEEZING the ledger rather than by racing two builds. A concurrency
// test would have to win a timing window to prove anything, while the property
// that actually changed is simply whether this site consults the ledger at all --
// and the ledger's own CAS is already covered by the device-memory tests. With
// the budget claimed out from under it, a training upload that takes a claim MUST
// be refused; one that takes none sails through, which is the unfixed behaviour.
func TestQuantizerTrainingIsGoverned(t *testing.T) {
	devices, err := GetGpuDeviceList()
	require.NoError(t, err)
	if len(devices) == 0 {
		t.Skip("no GPU devices")
	}
	dev := devices[0]
	require.Zero(t, ReservedDeviceMemory(dev), "ledger must start empty")

	const (
		dim = uint32(64)
		n   = uint64(4096)
	)
	ds := make([]float32, n*uint64(dim))
	for i := range ds {
		ds[i] = float32(i%1000) / 1000.0
	}

	// Claim all but a sliver of what any admission could grant, so the training
	// upload cannot fit. Sized from the same budget the claim will be checked
	// against, so this does not depend on the card's absolute capacity.
	budget, _, err := RowsFittingFreeMem(dev, 1)
	require.NoError(t, err)
	require.Positive(t, budget)
	squeeze := uint64(budget) - 4096

	hog, err := ReserveDeviceMemory(dev, squeeze)
	require.NoError(t, err, "could not squeeze the ledger")
	require.Equal(t, squeeze, ReservedDeviceMemory(dev))

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfFlatEmpty[float32, int8](n, dim, L2Expanded, bp, []int{dev}, 1, SingleGpu)
	require.NoError(t, err)
	defer index.Destroy()
	require.NoError(t, index.Start())

	// The float -> int8 path trains the quantizer from the staged rows. Staging
	// itself is host-side, so the refusal lands wherever the staged arena is
	// flushed and uploaded -- AddChunkQuantize if it flushes inline, otherwise
	// Build. Accept either, but insist the error names the TRAINING claim rather
	// than some later build allocation that happened to hit the same squeeze.
	addErr := index.AddChunkQuantize(ds, n, nil)
	buildErr := error(nil)
	if addErr == nil {
		buildErr = index.Build()
	}
	err = addErr
	if err == nil {
		err = buildErr
	}
	require.Error(t, err, "quantizer training must be refused when the ledger is exhausted")
	require.Contains(t, err.Error(), "quantizer::train upload",
		"the refusal must come from the training claim, not from some later allocation")

	// Releasing the squeeze restores the ledger exactly: the refused claim must
	// not have leaked a reservation on its way out.
	hog.Release()
	require.Zero(t, ReservedDeviceMemory(dev),
		"a refused training claim must leave nothing on the ledger")
}
