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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// withHostAvail pins the availability source for one test and restores it
// afterwards. Registered with t.Cleanup so it runs even when a require.*
// assertion aborts the goroutine.
func withHostAvail(t *testing.T, avail uint64, measured bool) {
	t.Helper()
	prev := hostAvailFn
	t.Cleanup(func() {
		hostAvailFn = prev
		hostReserved.Store(0)
	})
	hostAvailFn = func() (uint64, bool) { return avail, measured }
	hostReserved.Store(0)
}

func TestReserveHostMemory_RefusesZero(t *testing.T) {
	withHostAvail(t, 1<<30, true)
	// A zero demand means the caller could not size its allocation. Admitting it
	// would let an unsized build through the governor entirely.
	r, err := ReserveHostMemory(0, "test")
	require.Error(t, err)
	require.Nil(t, r)
	require.Zero(t, HostReservedBytes())
}

func TestReserveHostMemory_BudgetIs75Percent(t *testing.T) {
	const avail = 1000
	withHostAvail(t, avail, true)
	budget := uint64(avail) / hostBudgetDenominator * hostBudgetNumerator // 750

	// Exactly the budget is admitted.
	r, err := ReserveHostMemory(budget, "fits")
	require.NoError(t, err)
	require.Equal(t, budget, HostReservedBytes())
	r.Release()
	require.Zero(t, HostReservedBytes())

	// One byte over is refused, and refusal must not leave anything on the ledger.
	_, err = ReserveHostMemory(budget+1, "toobig")
	require.Error(t, err)
	require.Zero(t, HostReservedBytes())
}

func TestReserveHostMemory_SecondClaimSeesTheFirst(t *testing.T) {
	withHostAvail(t, 1000, true) // budget 750
	first, err := ReserveHostMemory(500, "first")
	require.NoError(t, err)

	// This is the defect the governor exists to fix: without a ledger the second
	// build snapshots the same headroom and also concludes it fits.
	_, err = ReserveHostMemory(500, "second")
	require.Error(t, err, "second claim must see the first one's 500 bytes")

	// It fits again once the first releases.
	first.Release()
	second, err := ReserveHostMemory(500, "second")
	require.NoError(t, err)
	second.Release()
	require.Zero(t, HostReservedBytes())
}

func TestHostReservation_ReleaseIsIdempotent(t *testing.T) {
	withHostAvail(t, 1000, true)
	r, err := ReserveHostMemory(300, "once")
	require.NoError(t, err)
	r.Release()
	r.Release()
	r.Release()
	// A double release would underflow the unsigned ledger to a huge value and
	// permanently refuse every later claim.
	require.Zero(t, HostReservedBytes())

	var nilRes *HostReservation
	require.NotPanics(t, func() { nilRes.Release() })
	require.Zero(t, nilRes.Bytes())
}

func TestReserveHostMemory_UnmeasuredIsNoOp(t *testing.T) {
	withHostAvail(t, 0, false)
	// An unmeasurable host keeps today's behaviour (bounded by device memory
	// only) instead of failing builds that currently work.
	r, err := ReserveHostMemory(1<<40, "unmeasured")
	require.NoError(t, err)
	require.Zero(t, r.Bytes())
	require.Zero(t, HostReservedBytes())
	r.Release()
	require.Zero(t, HostReservedBytes())
}

// TestReserveHostMemory_ConcurrentAdmissionIsExclusive is the counterexample
// from the review: two CREATE INDEX statements that each fit the snapshot, run
// so they overlap. Exactly one must win.
func TestReserveHostMemory_ConcurrentAdmissionIsExclusive(t *testing.T) {
	const avail = 1000 // budget 750
	const each = 500   // each fits alone; together they do not
	withHostAvail(t, avail, true)

	const goroutines = 2
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var granted []*HostReservation
	var refusals int

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // barrier: both are inside the admission at the same time
			r, err := ReserveHostMemory(each, "concurrent")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				refusals++
				return
			}
			granted = append(granted, r)
		}()
	}
	close(start)
	wg.Wait()

	require.Len(t, granted, 1, "exactly one build may hold the headroom")
	require.Equal(t, 1, refusals)
	require.Equal(t, uint64(each), HostReservedBytes())
	for _, r := range granted {
		r.Release()
	}
	require.Zero(t, HostReservedBytes())
}

// TestReserveHostMemory_LedgerExactUnderChurn runs many concurrent
// claim/release cycles; the ledger must return to exactly zero. An off-by-one
// in the CAS loop or a non-idempotent release shows up here as a nonzero total.
func TestReserveHostMemory_LedgerExactUnderChurn(t *testing.T) {
	withHostAvail(t, 1<<40, true)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := ReserveHostMemory(uint64(n+1)*1024, "churn")
				if err != nil {
					continue
				}
				r.Release()
			}
		}(i)
	}
	wg.Wait()
	require.Zero(t, HostReservedBytes())
}

// TestReserveHostMemory_HoldingPastAllocationDoubleCounts pins the lifetime
// contract by measuring what breaking it costs. A claim held after its memory is
// allocated is counted twice -- once in the ledger, once in the availability it
// already lowered -- and the headroom lost is the whole claim.
//
// This is why the builders release as soon as InitEmpty returns instead of at
// the end of the build. If someone lengthens that lifetime again, this test
// documents the bill.
func TestReserveHostMemory_HoldingPastAllocationDoubleCounts(t *testing.T) {
	const total = 512
	const claim = 100

	// Phase 1: the build is admitted while nothing is allocated yet.
	withHostAvail(t, total, true)
	held, err := ReserveHostMemory(claim, "build1")
	require.NoError(t, err)

	// Phase 2: the build has now allocated its 100, so availability reflects it.
	hostAvailFn = func() (uint64, bool) { return total - claim, true }
	budget := uint64(total-claim) / hostBudgetDenominator * hostBudgetNumerator // 309

	// Still holding: the same 100 is charged again, so a second build that fits
	// the real budget is refused.
	_, err = ReserveHostMemory(budget, "build2")
	require.Error(t, err, "claim held past allocation double counts")

	// Committing at the allocation -- what the builders now do -- makes the same
	// request fit, because availability alone already accounts for the 100.
	held.Release()
	second, err := ReserveHostMemory(budget, "build2")
	require.NoError(t, err, "after release the second build sees the true budget")
	second.Release()
	require.Zero(t, HostReservedBytes())
}

// TestHostReservation_DeferredAndExplicitReleasePair checks the shape every call
// site uses: a deferred Release covering the paths that never allocate, plus an
// explicit one right after the allocation. Whichever runs first wins and the
// other is a no-op, so no flag is needed to tell success from error.
func TestHostReservation_DeferredAndExplicitReleasePair(t *testing.T) {
	withHostAvail(t, 1000, true)

	// Success path: Commit, then the deferred Release must not double-subtract.
	r, err := ReserveHostMemory(300, "ok")
	require.NoError(t, err)
	r.Release() // explicit, right after the "allocation"
	require.Zero(t, HostReservedBytes())
	r.Release() // the deferred one
	require.Zero(t, HostReservedBytes(), "the second Release must be a no-op")

	// Error path: only the deferred Release runs, and a later Commit is inert.
	r2, err := ReserveHostMemory(300, "failed")
	require.NoError(t, err)
	r2.Release() // only the deferred one runs
	require.Zero(t, HostReservedBytes())
	r2.Release()
	require.Zero(t, HostReservedBytes())

	// Both orders leave the ledger exactly empty, so the next build sees the
	// full budget rather than a stranded claim.
	full, err := ReserveHostMemory(750, "next")
	require.NoError(t, err)
	full.Release()
	require.Zero(t, HostReservedBytes())
}
