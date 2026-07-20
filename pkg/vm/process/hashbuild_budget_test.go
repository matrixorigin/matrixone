// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHashBuildBudgetExactLimitAndOverflow(t *testing.T) {
	b, err := NewHashBuildBudget(math.MaxUint64, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	g, err := b.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := g.Reserve(math.MaxUint64)
	if err != nil {
		t.Fatalf("exact limit rejected: %v", err)
	}
	if got := b.AggregateUsed(); got != math.MaxUint64 {
		t.Fatalf("aggregate used = %d, want max uint64", got)
	}
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("limit+1 error = %v, want admission rejection", err)
	}
	if got := b.AggregateUsed(); got != math.MaxUint64 {
		t.Fatalf("failed query reservation changed aggregate: %d", got)
	}
	if got := g.Used(); got != math.MaxUint64 {
		t.Fatalf("failed query reservation changed generation: %d", got)
	}
	if !tok.Release() || tok.Release() {
		t.Fatal("release must transition exactly once")
	}
	if b.AggregateUsed() != 0 || g.Used() != 0 {
		t.Fatalf("released reservation remains: cn=%d query=%d", b.AggregateUsed(), g.Used())
	}
}

func TestHashBuildBudgetQueryRejectRollsBackCN(t *testing.T) {
	b := MustNewHashBuildBudget(10, 4)
	g1, _ := b.OpenGeneration(1)
	g2, _ := b.OpenGeneration(2)
	first, err := g1.Reserve(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g2.Reserve(7); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("error = %v, want admission rejection", err)
	}
	if b.AggregateUsed() != 4 || g2.Used() != 0 {
		t.Fatalf("query rejection did not roll back CN: cn=%d g2=%d", b.AggregateUsed(), g2.Used())
	}
	first.Release()
}

func TestHashBuildBudgetConcurrentReserveRelease(t *testing.T) {
	const workers = 32
	b := MustNewHashBuildBudget(workers, workers)
	gens := make([]*HashBuildBudgetGeneration, workers)
	for i := range gens {
		gens[i], _ = b.OpenGeneration(uint64(i + 1))
	}
	start := make(chan struct{})
	acquired := make(chan *HashBuildReservation, workers)
	var wg sync.WaitGroup
	for i := range gens {
		wg.Add(1)
		go func(g *HashBuildBudgetGeneration) {
			defer wg.Done()
			<-start
			tok, err := g.Reserve(1)
			if err == nil {
				acquired <- tok
			}
		}(gens[i])
	}
	close(start)
	wg.Wait()
	if len(acquired) != workers {
		t.Fatalf("acquired %d reservations, want %d", len(acquired), workers)
	}
	if b.AggregateUsed() != workers {
		t.Fatalf("aggregate used = %d, want %d", b.AggregateUsed(), workers)
	}
	for i := 0; i < workers; i++ {
		(<-acquired).Release()
	}
	if b.AggregateUsed() != 0 {
		t.Fatalf("aggregate used after release = %d", b.AggregateUsed())
	}
}

func TestHashBuildBudgetTransferAndClose(t *testing.T) {
	b := MustNewHashBuildBudget(8, 8)
	g, _ := b.OpenGeneration(7)
	tok, err := g.Reserve(3)
	if err != nil {
		t.Fatal(err)
	}
	moved := tok.Transfer()
	if moved == nil || tok.Release() {
		t.Fatal("transfer must make original token inert")
	}
	if b.AggregateUsed() != 3 {
		t.Fatalf("transfer changed charge: %d", b.AggregateUsed())
	}
	g.Close()
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed generation error = %v", err)
	}
	if !moved.Release() || moved.Release() {
		t.Fatal("transferred token release must be exactly once")
	}
	if b.AggregateUsed() != 0 {
		t.Fatalf("live token release after close leaked: %d", b.AggregateUsed())
	}
	b.Close()
	if _, err = b.OpenGeneration(8); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed budget error = %v", err)
	}
}

func TestHashBuildBudgetGenerationIsolation(t *testing.T) {
	b := MustNewHashBuildBudget(8, 8)
	old, _ := b.OpenGeneration(1)
	oldToken, _ := old.Reserve(6)
	old.Close()
	newGeneration, _ := b.OpenGeneration(2)
	newToken, err := newGeneration.Reserve(2)
	if err != nil {
		t.Fatal(err)
	}
	oldToken.Release()
	if newGeneration.Used() != 2 || b.AggregateUsed() != 2 {
		t.Fatalf("old release affected new generation: new=%d aggregate=%d", newGeneration.Used(), b.AggregateUsed())
	}
	newToken.Release()
}

func TestHashBuildBudgetCapReductionFailsClosedUntilRelease(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	g1, _ := b.OpenGenerationWithCap(1, 10)
	owned, err := g1.Reserve(8)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.UpdateAggregateCap(6); err != nil {
		t.Fatal(err)
	}
	g2, _ := b.OpenGenerationWithCap(2, 6)
	if _, err = g2.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("cap shrink did not fail closed: %v", err)
	}
	owned.Release()
	newToken, err := g2.Reserve(6)
	if err != nil {
		t.Fatalf("reservation after release failed: %v", err)
	}
	newToken.Release()
}

func TestHashBuildReservationReconcileCopyAlias(t *testing.T) {
	b := MustNewHashBuildBudget(20, 20)
	g, _ := b.OpenGeneration(1)
	tok, err := g.Reserve(10)
	if err != nil {
		t.Fatal(err)
	}
	alias := *tok
	if err := tok.Grow(2); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if tok.Size() != 12 || alias.Size() != 12 || g.Used() != 12 {
		t.Fatalf("alias grow diverged: token=%d alias=%d used=%d", tok.Size(), alias.Size(), g.Used())
	}
	if ok, err := alias.ReconcileDown(4); !ok || err != nil {
		t.Fatalf("reconcile: ok=%v err=%v", ok, err)
	}
	if tok.Size() != 4 || g.Used() != 4 || b.AggregateUsed() != 4 {
		t.Fatalf("alias reconcile diverged: size=%d gen=%d cn=%d", tok.Size(), g.Used(), b.AggregateUsed())
	}
	if _, err := tok.ReconcileDown(5); !errors.Is(err, ErrHashBuildReservationUpward) {
		t.Fatalf("upward err=%v", err)
	}
	if !tok.Release() || alias.Release() {
		t.Fatal("copy aliases must release exactly once")
	}
}

func TestHashBuildReservationGrowRejectsWithoutChangingCharge(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	g, _ := b.OpenGeneration(1)
	tok, err := g.Reserve(8)
	if err != nil {
		t.Fatal(err)
	}
	before := g.Snapshot()
	if err = tok.Grow(3); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("grow rejection=%v", err)
	}
	after := g.Snapshot()
	if tok.Size() != 8 || after.Used != before.Used || b.AggregateUsed() != 8 {
		t.Fatalf("rejected grow changed charge: token=%d generation=%d aggregate=%d", tok.Size(), after.Used, b.AggregateUsed())
	}
	if after.RejectCount != before.RejectCount+1 {
		t.Fatalf("reject count=%d, want %d", after.RejectCount, before.RejectCount+1)
	}
	tok.Release()
}

func TestHashBuildReservationGrowHonorsInactiveClosedAndLiveCap(t *testing.T) {
	b := MustNewHashBuildBudget(20, 20)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	cap := uint64(20)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	b.SetAggregateCapProvider(func() (uint64, error) { return cap, nil })
	tok, err := g.Reserve(8)
	if err != nil {
		t.Fatal(err)
	}
	cap = 8
	now = now.Add(hashBuildBudgetCapRefreshTTL + time.Nanosecond)
	if err = tok.Grow(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("live-cap grow=%v", err)
	}
	if tok.Size() != 8 || g.Used() != 8 || b.AggregateUsed() != 8 {
		t.Fatalf("live-cap rejection changed charge")
	}
	tok.Release()
	if err = tok.Grow(1); !errors.Is(err, ErrHashBuildReservationInactive) {
		t.Fatalf("released grow=%v", err)
	}

	cap = 20
	closed, err := g.Reserve(2)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	if err = closed.Grow(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed grow=%v", err)
	}
	closed.Release()
}

func TestHashBuildReservationGrowConcurrentTerminalTransitions(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := MustNewHashBuildBudget(64, 64)
		g, _ := b.OpenGeneration(uint64(i + 1))
		tok, err := g.Reserve(10)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(3)
		movedC := make(chan *HashBuildReservation, 1)
		go func() {
			defer wg.Done()
			_ = tok.Grow(5)
		}()
		go func() {
			defer wg.Done()
			_, _ = tok.ReconcileDown(4)
		}()
		go func() {
			defer wg.Done()
			movedC <- tok.Transfer()
		}()
		wg.Wait()
		close(movedC)
		tok.Release()
		if moved := <-movedC; moved != nil {
			moved.Release()
		}
		if g.Used() != 0 || b.AggregateUsed() != 0 {
			t.Fatalf("iteration %d leaked charge: generation=%d aggregate=%d", i, g.Used(), b.AggregateUsed())
		}
	}
}

func TestHashBuildSpillLedgersTransferReconcile(t *testing.T) {
	b := MustNewHashBuildBudget(64, 64)
	g, _ := b.OpenGeneration(1)
	disk, err := g.ReserveSpillDisk(100)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := g.ReserveSpillFD(2)
	if err != nil {
		t.Fatal(err)
	}
	if b.SpillDiskUsed() != 100 || b.SpillFDUsed() != 2 {
		t.Fatalf("used disk=%d fd=%d", b.SpillDiskUsed(), b.SpillFDUsed())
	}
	if err := disk.Grow(25); err != nil {
		t.Fatalf("disk grow: %v", err)
	}
	if disk.Size() != 125 || b.SpillDiskUsed() != 125 {
		t.Fatalf("grown disk token=%d used=%d", disk.Size(), b.SpillDiskUsed())
	}
	if ok, err := disk.ReconcileDown(40); !ok || err != nil {
		t.Fatalf("disk reconcile: %v %v", ok, err)
	}
	moved := fd.Transfer()
	if moved == nil || fd.Release() {
		t.Fatal("fd transfer")
	}
	g.Close()
	if _, err := g.ReserveSpillDisk(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed spill reserve=%v", err)
	}
	disk.Release()
	moved.Release()
	if b.SpillDiskUsed() != 0 || b.SpillFDUsed() != 0 {
		t.Fatalf("spill leak disk=%d fd=%d", b.SpillDiskUsed(), b.SpillFDUsed())
	}
}

func TestDefaultSpillFDCapAdmitsNormalShuffleRepartitionPeak(t *testing.T) {
	const normalPeak = uint64(16 * (32 + 64))
	b := MustNewHashBuildBudget(192<<20, 192<<20)
	if b.SpillFDCap() < normalPeak {
		t.Fatalf("spill fd cap=%d, want at least normal 16-way peak=%d", b.SpillFDCap(), normalPeak)
	}
}

func TestHashBuildBudgetLiveCapProviderShrinksOpenGeneration(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGenerationWithCap(1, 10)
	cap := uint64(10)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	b.SetAggregateCapProvider(func() (uint64, error) { return cap, nil })
	owned, err := g.Reserve(6)
	if err != nil {
		t.Fatal(err)
	}
	cap = 5
	now = now.Add(hashBuildBudgetCapRefreshTTL + time.Nanosecond)
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("open generation ignored live cap shrink: %v", err)
	}
	owned.Release()
	token, err := g.Reserve(5)
	if err != nil {
		t.Fatalf("reservation at refreshed cap failed: %v", err)
	}
	token.Release()
}

func TestHashBuildBudgetCapProviderCachesWithinTTLAndRefreshes(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	cap := uint64(10)
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return cap, nil
	})
	first, err := g.Reserve(4)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("first reservation provider calls=%d, want 1", got)
	}
	now = now.Add(hashBuildBudgetCapRefreshTTL - time.Nanosecond)
	second, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("TTL reservation provider calls=%d, want 1", got)
	}
	cap = 3
	now = now.Add(2 * time.Nanosecond)
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("expired shrink reservation=%v, want admission rejection", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expired reservation provider calls=%d, want 2", got)
	}
	first.Release()
	second.Release()
}

func TestHashBuildBudgetCapProviderGrowthOnAggregateReject(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	g2, _ := b.OpenGeneration(2)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	cap := uint64(10)
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return cap, nil
	})
	owned, err := g.Reserve(10)
	if err != nil {
		t.Fatal(err)
	}
	cap = 20
	// The cached cap is still 10, so this failed aggregate admission forces a
	// refresh even though the TTL has not elapsed and then succeeds at 20.
	grown, err := g2.Reserve(1)
	if err != nil {
		t.Fatalf("growth refresh reservation=%v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("growth refresh provider calls=%d, want 2", got)
	}
	grown.Release()
	owned.Release()
}

func TestHashBuildBudgetCapProviderConcurrentSingleFlight(t *testing.T) {
	b := MustNewHashBuildBudget(128, 128)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	b.SetAggregateCapProvider(func() (uint64, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return 128, nil
	})
	const workers = 16
	tokens := make(chan *HashBuildReservation, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := g.Reserve(1)
			if err != nil {
				t.Errorf("concurrent reserve: %v", err)
				return
			}
			tokens <- tok
		}()
	}
	<-started
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent provider calls=%d, want 1", got)
	}
	for i := 0; i < workers; i++ {
		(<-tokens).Release()
	}
}

func TestHashBuildBudgetCapProviderErrorCachedFailClosed(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	want := errors.New("cgroup unavailable")
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return 0, want
	})
	if _, err := g.Reserve(1); !errors.Is(err, want) {
		t.Fatalf("provider error=%v, want %v", err, want)
	}
	if _, err := g.Reserve(1); !errors.Is(err, want) {
		t.Fatalf("cached provider error=%v, want %v", err, want)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("cached error provider calls=%d, want 1", got)
	}
	now = now.Add(hashBuildBudgetCapRefreshTTL + time.Nanosecond)
	if _, err := g.Reserve(1); !errors.Is(err, want) {
		t.Fatalf("expired provider error=%v, want %v", err, want)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expired error provider calls=%d, want 2", got)
	}
}

func TestHashBuildBudgetCapProviderSharedByReserveAndGrow(t *testing.T) {
	b := MustNewHashBuildBudget(20, 20)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return 20, nil
	})
	tok, err := g.Reserve(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = tok.Grow(3); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Reserve+Grow provider calls=%d, want 1", got)
	}
	tok.Release()
}

func TestHashBuildBudgetCapProviderZeroTTLRefreshesEveryReservation(t *testing.T) {
	b := MustNewHashBuildBudget(16, 16)
	g, _ := b.OpenGeneration(1)
	b.capRefreshTTL = 0
	var calls atomic.Int32
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return 16, nil
	})
	for i := 0; i < 2; i++ {
		tok, err := g.Reserve(1)
		if err != nil {
			t.Fatal(err)
		}
		tok.Release()
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("zero-TTL provider calls=%d, want 2", got)
	}
}

func TestHashBuildBudgetUpdateAndProviderReinstallReuseCache(t *testing.T) {
	b := MustNewHashBuildBudget(16, 16)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	provider := func() (uint64, error) {
		calls.Add(1)
		return 16, nil
	}
	b.SetAggregateCapProvider(provider)
	first, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.UpdateAggregateCap(16); err != nil {
		t.Fatal(err)
	}
	// GetHashBuildBudget re-installs an equivalent closure before updating the
	// freshly resolved cap. The update seeds the cache for this query.
	b.SetAggregateCapProvider(provider)
	if err = b.UpdateAggregateCap(16); err != nil {
		t.Fatal(err)
	}
	second, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Update+Set provider calls=%d, want 1", got)
	}
	first.Release()
	second.Release()
}

func TestHashBuildBudgetProviderReplacementInvalidatesCache(t *testing.T) {
	b := MustNewHashBuildBudget(16, 16)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var oldCalls, newCalls atomic.Int32
	b.SetAggregateCapProvider(func() (uint64, error) {
		oldCalls.Add(1)
		return 16, nil
	})
	owned, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	b.SetAggregateCapProvider(func() (uint64, error) {
		newCalls.Add(1)
		return 2, nil
	})
	second, err := g.Reserve(1)
	if err != nil {
		t.Fatalf("replacement reserve=%v", err)
	}
	if oldCalls.Load() != 1 || newCalls.Load() != 1 {
		t.Fatalf("provider calls old=%d new=%d, want 1,1", oldCalls.Load(), newCalls.Load())
	}
	owned.Release()
	second.Release()
}

func TestHashBuildBudgetCNSourceTurnoverRetainsRestrictiveFallback(t *testing.T) {
	const gib = uint64(1 << 30)
	oldInputs := HashBuildCeilingInputs{HostMemTotal: 10 * gib, CgroupMemoryMax: 20 * gib}
	newInputs := HashBuildCeilingInputs{HostMemTotal: 20 * gib, CgroupMemoryMax: 10 * gib}
	oldCeiling, err := ResolveHashBuildCeiling(oldInputs)
	if err != nil {
		t.Fatal(err)
	}
	newCeiling, err := ResolveHashBuildCeiling(newInputs)
	if err != nil {
		t.Fatal(err)
	}
	if oldCeiling.CNHashCap != newCeiling.CNHashCap {
		t.Fatalf("test setup caps old=%d new=%d, want equal", oldCeiling.CNHashCap, newCeiling.CNHashCap)
	}
	b := MustNewHashBuildBudget(oldCeiling.CNHashCap, oldCeiling.CNHashCap)
	b.installCNCapProvider(oldInputs)
	b.mergeObservedCNCap(newInputs, newCeiling.CNHashCap)

	// A zero source sample models transient read failures before the installed
	// provider gets a complete view of the turnover. The merged shared snapshot
	// must retain both restrictive observations and cannot reopen the cap.
	b.refreshMu.Lock()
	got, err := b.resolveCNCapSample(HashBuildCeilingInputs{})
	b.refreshMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got > newCeiling.CNHashCap {
		t.Fatalf("fallback cap=%d, exceeds restrictive cap %d", got, newCeiling.CNHashCap)
	}
}

func TestHashBuildBudgetSlowProviderTTLStartsAfterSample(t *testing.T) {
	b := MustNewHashBuildBudget(20, 20)
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	g, _ := b.OpenGeneration(1)
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	var calls atomic.Int32
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		now = now.Add(2 * hashBuildBudgetCapRefreshTTL)
		return 20, nil
	})
	first, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("slow provider calls=%d, want freshly completed sample reused", got)
	}
	first.Release()
	second.Release()
}

func TestHashBuildBudgetClosedSkipsCapProvider(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	g, _ := b.OpenGeneration(1)
	var calls atomic.Int32
	b.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return 10, nil
	})
	b.Close()
	if _, err := g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed reserve=%v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("closed provider calls=%d, want 0", got)
	}
}

func BenchmarkHashBuildBudgetReserveCachedProvider(b *testing.B) {
	budget := MustNewHashBuildBudget(uint64(b.N)+1, uint64(b.N)+1)
	budget.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	gen, _ := budget.OpenGeneration(1)
	var calls atomic.Int64
	budget.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return uint64(b.N) + 1, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tok, err := gen.Reserve(1)
		if err != nil {
			b.Fatal(err)
		}
		tok.Release()
	}
	b.StopTimer()
	b.ReportMetric(float64(calls.Load()), "provider-calls")
}

func TestResolveHashBuildCeiling(t *testing.T) {
	const gib = uint64(1 << 30)
	got, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{
		CgroupMemoryMax:       20 * gib,
		HostMemTotal:          10 * gib,
		GlobalMpoolCap:        30 * gib,
		FileCacheHint:         gib,
		ProcessLimitationSize: 2 * gib,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.EffectiveCN != 10*gib || got.RequestedReserve != 4*gib || got.Reserve != 4*gib || got.CNHashCap != 6*gib || got.QueryCap != 2*gib {
		t.Fatalf("unexpected ceiling: %+v", got)
	}
	if _, err = ResolveHashBuildCeiling(HashBuildCeilingInputs{CgroupMemoryMax: math.MaxUint64, HostMemTotal: 0, GlobalMpoolCap: 0}); !errors.Is(err, ErrHashBuildCeilingMissing) {
		t.Fatalf("missing finite source error = %v", err)
	}
	small, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{HostMemTotal: 3 * gib, FileCacheHint: 3 * gib})
	if err != nil || small.CNHashCap != 3*gib/20 {
		t.Fatalf("small-CN bounded allowance = %+v, err=%v", small, err)
	}
}
