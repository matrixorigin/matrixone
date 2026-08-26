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

package cagra

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
)

// The build claim must cover the quantizer staging arena, because
// prereserve_staging_arena() allocates it inside start() -- which runs inside
// InitEmpty, inside the window this claim is held.
//
// Claiming only rows*hostBytesPerRow while start() allocates that PLUS the arena
// means the claim understates what it covers, and the shortfall is counted
// nowhere until the pages land. That is worse than the original defect, where the
// arena was at least allocated outside any claim and showed up in availability on
// its own.
//
// Asserted against the ledger rather than the return value: what matters is what a
// concurrent build would see.
func TestBuildClaimIncludesStagingArena(t *testing.T) {
	b := &CagraBuild[float32, float32]{}
	b.SetHostBytesPerRow(100)

	t.Run("without a staging arena the claim is the per-row cost", func(t *testing.T) {
		b.SetStagingBytes(0)
		c, err := b.reserveBuildHost(10)
		require.NoError(t, err)
		// Settle via Cleanup so a failed assertion below cannot strand the claim
		// and cascade into the next sub-test -- these share one process ledger.
		t.Cleanup(c.Settle)
		require.Equal(t, uint64(1000), memory.HostReservedBytes())
	})

	t.Run("the arena is added to the same claim", func(t *testing.T) {
		b.SetStagingBytes(4096)
		c, err := b.reserveBuildHost(10)
		require.NoError(t, err)
		t.Cleanup(c.Settle)
		require.Equal(t, uint64(1000+4096), memory.HostReservedBytes(),
			"start() allocates the arena inside this claim's window, so the claim must cover it")
	})

	t.Run("no per-row cost means no claim at all", func(t *testing.T) {
		// Direct API users and tests construct builders without the TVF; they take
		// no claim and must not start taking one just because staging is non-zero.
		nb := &CagraBuild[float32, float32]{}
		nb.SetStagingBytes(4096)
		c, err := nb.reserveBuildHost(10)
		require.NoError(t, err)
		require.Nil(t, c)
		require.Zero(t, memory.HostReservedBytes())
	})
}
