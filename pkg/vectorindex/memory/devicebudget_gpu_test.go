// Copyright 2021 Matrix Origin
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

package memory

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestDeviceLoadFitsOnRealDevice runs the aggregate gate against actual VRAM
// through cuvs.RowsFittingFreeMem, rather than the fake callback the CPU tests
// use. It is the piece the CPU suite cannot check: that the real callback's
// signature, its 60%-of-free rule, and the error path all line up with what
// DeviceLoadFits expects.
func TestDeviceLoadFitsOnRealDevice(t *testing.T) {
	devices, err := cuvs.GetGpuDeviceList()
	require.NoError(t, err)
	if len(devices) == 0 {
		t.Skip("no GPU devices")
	}

	// A megabyte fits on any card this code supports.
	require.NoError(t, DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 1<<20, cuvs.RowsFittingFreeMem),
		"1 MiB must be admitted")

	// 256 TiB fits on nothing, so this exercises the refusal against a real
	// cudaMemGetInfo reading rather than a stub.
	err = DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 1<<48, cuvs.RowsFittingFreeMem)
	require.Error(t, err, "an impossible aggregate must be refused")
	require.Contains(t, err.Error(), "resident on device")
	require.Contains(t, err.Error(), "built successfully")

	// REPLICATED charges every device in full; the impossible total must still be
	// refused, and the message must name a real device id.
	err = DeviceLoadFits(
		vectorindex.DistributionMode_REPLICATED, devices, 1<<48, cuvs.RowsFittingFreeMem)
	require.Error(t, err)

	// The gate must not disturb the governor's ledger: it is a pre-flight check,
	// not a claim. A leaked claim here would refuse every later load.
	for _, d := range DeviceDistinct(devices) {
		require.Zero(t, cuvs.ReservedDeviceMemory(d),
			"DeviceLoadFits must not reserve anything on device %d", d)
	}
}
