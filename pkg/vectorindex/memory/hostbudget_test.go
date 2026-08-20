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

package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// HostRowsFitting reads real memory, so assert the contract rather than a figure.
func TestHostRowsFitting(t *testing.T) {
	rows, avail, err := HostRowsFitting(1536) // dim 768 f16
	require.NoError(t, err)
	require.Positive(t, avail)
	require.Positive(t, rows)
	require.Less(t, uint64(rows)*1536, avail, "must leave headroom, not spend all of it")

	// A wider row must not fit more of itself.
	wide, _, err := HostRowsFitting(3072)
	require.NoError(t, err)
	require.LessOrEqual(t, wide, rows)

	// Zero is inert rather than a divide-by-zero.
	z, _, err := HostRowsFitting(0)
	require.NoError(t, err)
	require.Zero(t, z)

	// A per-row cost larger than the whole 75% budget is a hard error, not a
	// silent zero. Silently returning (0, avail, nil) here would let the caller
	// treat the bound as "unmeasured" and disable it — exactly the OOM the
	// bound exists to prevent.
	huge, availHuge, err := HostRowsFitting(avail) // one row costs the entire node
	require.Error(t, err)
	require.Zero(t, huge)
	require.Positive(t, availHuge, "err path must still report the measured avail")
	require.Contains(t, err.Error(), "cannot hold one row",
		"the message must explain WHY the sizing failed")
}

// TestHostIDBytesPerRowIsCharged proves the ID bookkeeping actually MOVES the
// capacity, not merely that a constant exists. host_ids plus the id_to_index_
// entry are compulsory capacity-sized native allocations, so for a narrow vector
// they dominate the row and a model that omits them overstates how many rows the
// host can hold.
func TestHostIDBytesPerRowIsCharged(t *testing.T) {
	require.Positive(t, HostIDBytesPerRow)

	const narrowVector = 8 // int8 x dim 8

	vectorOnly, avail, err := HostRowsFitting(narrowVector)
	require.NoError(t, err)
	require.Positive(t, avail)
	withIDs, _, err := HostRowsFitting(narrowVector + HostIDBytesPerRow)
	require.NoError(t, err)

	require.Less(t, withIDs, vectorOnly,
		"charging IDs must reduce the admitted row count")
	// 8 B of vector vs 8+48 B charged: the honest capacity is ~7x smaller, so
	// omitting IDs is an overstatement of the budget, not a rounding error.
	require.LessOrEqual(t, withIDs*6, vectorOnly,
		"for a narrow row the ID term dominates; omitting it overstates capacity several-fold")
}
