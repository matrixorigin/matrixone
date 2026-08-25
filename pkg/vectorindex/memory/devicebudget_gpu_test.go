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
	"github.com/stretchr/testify/require"
)

// TestDeviceAggregateOnRealDevice runs the CREATE gate against actual VRAM
// through cuvs.DeviceMaxAdmissible, rather than the fake callback the CPU tests
// use. It is the piece the CPU suite cannot check: that the real callback's
// signature, its budget-fraction-of-total rule, and its error path line up with
// what DeviceAggregateFitsHardware expects.
func TestDeviceAggregateOnRealDevice(t *testing.T) {
	devices, err := cuvs.GetGpuDeviceList()
	require.NoError(t, err)
	if len(devices) == 0 {
		t.Skip("no GPU devices")
	}

	// A megabyte fits on any card this code supports.
	require.NoError(t, DeviceAggregateFitsHardware(devices, 1<<20, cuvs.DeviceMaxAdmissible),
		"1 MiB must be admitted")

	// 256 TiB fits on nothing, so this exercises the refusal against a real
	// cudaMemGetInfo reading rather than a stub.
	err = DeviceAggregateFitsHardware(devices, 1<<48, cuvs.DeviceMaxAdmissible)
	require.Error(t, err, "an impossible demand must be refused")
	require.Contains(t, err.Error(), "even when completely idle")
}
