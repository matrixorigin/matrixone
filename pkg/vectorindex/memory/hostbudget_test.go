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

// withHostAvail pins the availability source for one test and restores it
// afterwards. Registered with t.Cleanup so it runs even when a require.*
// assertion aborts the goroutine.
func withHostAvail(t *testing.T, avail uint64, measured bool) {
	t.Helper()
	prev := hostAvailFn
	t.Cleanup(func() { hostAvailFn = prev })
	hostAvailFn = func() (uint64, bool) { return avail, measured }
}

// HostRowsFitting reads real memory, so assert the contract rather than a figure.
func TestHostRowsFitting(t *testing.T) {
	rows, avail, err := HostRowsFitting(1536, 0) // dim 768 f16
	require.NoError(t, err)
	require.Positive(t, avail)
	require.Positive(t, rows)
	require.Less(t, uint64(rows)*1536, avail, "must leave headroom, not spend all of it")

	// A wider row must not fit more of itself.
	wide, _, err := HostRowsFitting(3072, 0)
	require.NoError(t, err)
	require.LessOrEqual(t, wide, rows)

	// Zero is inert rather than a divide-by-zero.
	z, _, err := HostRowsFitting(0, 0)
	require.NoError(t, err)
	require.Zero(t, z)

	// A per-row cost larger than the whole 75% budget is a hard error, not a
	// silent zero. Silently returning (0, avail, nil) here would let the caller
	// treat the bound as "unmeasured" and disable it — exactly the OOM the
	// bound exists to prevent.
	huge, availHuge, err := HostRowsFitting(avail, 0) // one row costs the entire node
	require.Error(t, err)
	require.Zero(t, huge)
	require.Positive(t, availHuge, "err path must still report the measured avail")
	require.Contains(t, err.Error(), "cannot hold one row",
		"the message must explain WHY the sizing failed")
}

// TestHostIDBytesPerRowIsCharged proves the ID bookkeeping actually MOVES the
// capacity, not merely that a constant exists. host_ids is a compulsory
// capacity-sized native allocation (host_ids.reserve in both chunked constructors),
// so for a narrow vector it is a real fraction of the row and a model that omits it
// overstates how many rows the host can hold.
//
// It is deliberately host_ids ALONE: id_to_index_ is built on demand and is never
// allocated during a build, so charging it would reserve memory against a structure
// that does not exist yet. See the constant's doc for what must change together if
// that ever stops being true.
func TestHostIDBytesPerRowIsCharged(t *testing.T) {
	require.Positive(t, HostIDBytesPerRow)

	// Pin availability rather than reading the live host. Measuring for real made
	// this test fail under -count=N: a cgroup momentarily at its limit reports
	// (0, measured), HostRowsFitting then refuses to hold one row, and the test
	// fails on an environment condition that has nothing to do with the ID term
	// it exists to prove. Every other test in this package pins it for the same
	// reason.
	withHostAvail(t, 1<<30, true)

	const narrowVector = 8 // int8 x dim 8

	vectorOnly, avail, err := HostRowsFitting(narrowVector, 0)
	require.NoError(t, err)
	require.Positive(t, avail)
	withIDs, _, err := HostRowsFitting(narrowVector+HostIDBytesPerRow, 0)
	require.NoError(t, err)

	require.Less(t, withIDs, vectorOnly,
		"charging IDs must reduce the admitted row count")
	// 8 B of vector vs 8+8 B charged: the honest capacity is half, so omitting the
	// ID term is an overstatement of the budget, not a rounding error.
	require.LessOrEqual(t, withIDs*2, vectorOnly,
		"for a narrow row the ID term is a full share; omitting it overstates capacity")
}

// The int8/uint8 staging arena is live at the SAME TIME as the capacity
// allocation, so it is subtracted from the budget BEFORE capacity is derived.
// Clamping the arena against the budget instead would let both be promised the
// same bytes -- the arena is not per-row, so it cannot be folded into perRow,
// and it does not shrink just because capacity grew.
func TestHostRowsFittingReservesBeforeCapacity(t *testing.T) {
	const avail = 4000
	withHostAvail(t, avail, true)
	budget := uint64(avail) / hostBudgetDenominator * hostBudgetNumerator // 3000

	base, _, err := HostRowsFitting(100, 0)
	require.NoError(t, err)
	require.Equal(t, int64(budget/100), base)

	// A reservation takes its bytes off the top: the rows left are what the
	// REMAINDER holds, not what the whole budget would have.
	withRes, _, err := HostRowsFitting(100, 1000)
	require.NoError(t, err)
	require.Equal(t, int64((budget-1000)/100), withRes)
	require.Less(t, withRes, base, "reserving must cost capacity, not be free")

	// A sample that eats the whole budget is a configuration to reject, not to
	// round down to zero rows and carry on.
	_, _, err = HostRowsFitting(100, budget)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quantizer_train_limit")
	// Both escapes must be named. The sample is capped by a sub-index's capacity,
	// so lowering max_index_capacity is a real remedy and an operator told only
	// about the train limit would not find it.
	require.Contains(t, err.Error(), "max_index_capacity")

	_, _, err = HostRowsFitting(100, budget+1)
	require.Error(t, err)

	// Unmeasured stays unmeasured: a reservation must not turn "cannot tell" into
	// a hard failure, or an unmeasurable host would stop building entirely.
	withHostAvail(t, 0, false)
	rows, availOut, err := HostRowsFitting(100, 1<<40)
	require.NoError(t, err)
	require.Zero(t, rows)
	require.Zero(t, availOut)
}
