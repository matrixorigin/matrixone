//go:build gpu

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

package cuvs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The ledger is process-global C++ state, so every test must leave device 0 at
// zero or it silently shrinks the budget for whatever runs next.
func requireLedgerEmpty(t *testing.T, where string) {
	t.Helper()
	require.Equal(t, uint64(0), ReservedDeviceMemory(0), "ledger not empty after %s", where)
}

func TestReserveDeviceMemoryRoundTrip(t *testing.T) {
	requireLedgerEmpty(t, "start")

	const want = 64 << 20 // 64 MiB, small enough to fit any test GPU
	r, err := ReserveDeviceMemory(0, want)
	require.NoError(t, err)
	t.Cleanup(r.Release) // survives a failed assertion below
	require.Equal(t, uint64(want), ReservedDeviceMemory(0),
		"a Go-side claim must be visible in the same ledger C++ loads use")

	r.Release()
	requireLedgerEmpty(t, "release")
	r.Release() // idempotent alongside the deferred cleanup
	requireLedgerEmpty(t, "double release")
}

func TestReserveDeviceMemoryRefusesZero(t *testing.T) {
	// 0 must not be read as "unknown demand, admit anyway": that is exactly the
	// escape hatch a caller that failed to size itself would fall through.
	_, err := ReserveDeviceMemory(0, 0)
	require.Error(t, err)
	requireLedgerEmpty(t, "refused zero claim")
}

func TestReserveDeviceMemoryRefusesImpossible(t *testing.T) {
	_, err := ReserveDeviceMemory(0, 1<<62)
	require.Error(t, err)
	requireLedgerEmpty(t, "refused impossible claim")
}
