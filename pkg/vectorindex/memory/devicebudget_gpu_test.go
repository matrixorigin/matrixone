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

// TestDeviceAggregateOnRealDevice runs BOTH gates against actual VRAM through
// cuvs.BudgetFor, rather than the fake callbacks the CPU tests use. It is the
// piece the CPU suite cannot check: that the real closures' signatures, their
// fraction-of-total and fraction-of-free rules, and their error paths line up
// with what the gates expect.
func TestDeviceAggregateOnRealDevice(t *testing.T) {
	devices, err := cuvs.GetGpuDeviceList()
	require.NoError(t, err)
	if len(devices) == 0 {
		t.Skip("no GPU devices")
	}

	// One value, both gates -- the pairing production uses.
	budget := cuvs.BudgetFor("CAGRA")
	require.NotNil(t, budget.MaxAdmissible)
	require.NotNil(t, budget.RowsFitting)

	// A megabyte fits on any card this code supports.
	require.NoError(t, DeviceAggregateFitsHardware(uniform(devices, 1<<20), 1, true, budget),
		"1 MiB must be admitted by the permanent gate")
	require.NoError(t, DeviceAggregateFitsFree(uniform(devices, 1<<20), 1, 1, budget),
		"1 MiB must be admitted by the situational gate")

	// 256 TiB fits on nothing, so this exercises both refusals against real
	// cudaMemGetInfo readings rather than a stub.
	err = DeviceAggregateFitsHardware(uniform(devices, 1<<48), 1, true, budget)
	require.Error(t, err, "an impossible demand must be refused")
	require.Contains(t, err.Error(), "even when completely idle")

	err = DeviceAggregateFitsFree(uniform(devices, 1<<48), 1, 1, budget)
	require.Error(t, err, "an impossible demand must be refused right now too")
	require.Contains(t, err.Error(), "right now")

	// The pairing's whole point: both gates read ONE fraction. IVF-PQ's is lower
	// than CAGRA's, so its permanent ceiling must be strictly smaller on the same
	// card -- if these ever match, the per-index fraction stopped being plumbed
	// and the 65-vs-75 mismatch is back.
	pqCeil, err := cuvs.BudgetFor("IVFPQ").MaxAdmissible(devices[0])
	require.NoError(t, err)
	cagraCeil, err := budget.MaxAdmissible(devices[0])
	require.NoError(t, err)
	require.Less(t, pqCeil, cagraCeil,
		"IVF-PQ reserves more headroom than CAGRA, so its ceiling must be lower")
}
