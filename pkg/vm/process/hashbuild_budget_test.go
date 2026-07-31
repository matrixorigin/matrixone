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
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonmpool "github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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

func TestConfiguredSpillFDCapCushionsFirstShuffleRepartitionPeak(t *testing.T) {
	const firstRepartitionPeak = uint64(16 * (64 + 64))
	if got := configuredSpillFDCap(192 << 20); got < firstRepartitionPeak {
		t.Fatalf("configured spill fd cap=%d, want at least first 16-way repartition peak=%d", got, firstRepartitionPeak)
	}
}

func TestClampSpillFDCapBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		configured, processLimit uint64
		limitKnown               bool
		want                     uint64
	}{
		{name: "unknown fails closed", configured: 2048, processLimit: 1 << 20, want: 0},
		{name: "zero configured", configured: 0, processLimit: 1024, limitKnown: true, want: 0},
		{name: "below absolute headroom", configured: 2048, processLimit: 63, limitKnown: true, want: 0},
		{name: "at absolute headroom", configured: 2048, processLimit: 64, limitKnown: true, want: 0},
		{name: "one fd above headroom", configured: 2048, processLimit: 65, limitKnown: true, want: 1},
		{name: "absolute headroom dominates", configured: 2048, processLimit: 128, limitKnown: true, want: 64},
		{name: "quarter headroom dominates", configured: 2048, processLimit: 1024, limitKnown: true, want: 768},
		{name: "explicit finite cap retained", configured: 10, processLimit: 1024, limitKnown: true, want: 10},
		{name: "unlimited retains configured", configured: 2048, processLimit: math.MaxUint64, limitKnown: true, want: 2048},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampSpillFDCap(tc.configured, tc.processLimit, tc.limitKnown); got != tc.want {
				t.Fatalf("clampSpillFDCap(%d, %d, %v)=%d, want %d",
					tc.configured, tc.processLimit, tc.limitKnown, got, tc.want)
			}
		})
	}
}

func TestDefaultSpillFDCapMatchesProcessLimit(t *testing.T) {
	limit, ok := processOpenFileLimit()
	want := clampSpillFDCap(configuredSpillFDCap(192<<20), limit, ok)
	b := MustNewHashBuildBudget(192<<20, 192<<20)
	if got := b.SpillFDCap(); got != want {
		t.Fatalf("spill fd cap=%d, want process-clamped cap=%d (limit=%d known=%v)", got, want, limit, ok)
	}
}

func TestHashBuildSpillFDCapUnderRLIMIT(t *testing.T) {
	const (
		childEnv = "MO_HASHBUILD_RLIMIT_CHILD"
		limitEnv = "MO_HASHBUILD_RLIMIT_NOFILE"
	)
	if os.Getenv(childEnv) == "1" {
		limit, ok := processOpenFileLimit()
		if !ok {
			t.Fatal("RLIMIT_NOFILE unavailable in RLIMIT child")
		}
		rawTarget := os.Getenv(limitEnv)
		target, err := strconv.ParseUint(rawTarget, 10, 64)
		if err != nil {
			t.Fatalf("parse target %q: %v", rawTarget, err)
		}
		if limit != target {
			t.Fatalf("child RLIMIT_NOFILE=%d, want %d", limit, target)
		}

		configured := configuredSpillFDCap(192 << 20)
		want := clampSpillFDCap(configured, limit, true)
		b := MustNewHashBuildBudget(192<<20, 192<<20)
		if got := b.SpillFDCap(); got != want {
			t.Fatalf("child spill fd cap=%d, want %d", got, want)
		}
		g, err := b.OpenGeneration(1)
		if err != nil {
			t.Fatal(err)
		}
		// Simulate a budget/generation opened while the process limit was
		// higher. ReserveSpillFD must sample the current RLIMIT again instead
		// of trusting these stale effective caps.
		b.mu.Lock()
		b.spillFDCap = configured
		g.spillFDCap = configured
		b.mu.Unlock()
		if _, err = g.ReserveSpillFD(want + 1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
			t.Fatalf("RLIMIT+headroom overflow error=%v, want admission rejection", err)
		}
		if got := b.SpillFDCap(); got != want {
			t.Fatalf("runtime preflight refreshed spill fd cap=%d, want %d", got, want)
		}
		token, err := g.ReserveSpillFD(want)
		if err != nil {
			t.Fatalf("exact safe FD cap rejected: %v", err)
		}
		if !token.Release() {
			t.Fatal("exact safe FD reservation did not release")
		}

		if err = b.SetSpillCaps(0, 10); err != nil {
			t.Fatal(err)
		}
		wantExplicit := clampSpillFDCap(10, limit, true)
		if got := b.SpillFDCap(); got != wantExplicit {
			t.Fatalf("explicit finite FD cap=%d, want process-clamped %d", got, wantExplicit)
		}
		return
	}

	switch runtime.GOOS {
	case "darwin", "linux":
	default:
		t.Skip("RLIMIT_NOFILE subprocess is only supported on Darwin and Linux")
	}
	parentLimit, ok := processOpenFileLimit()
	if !ok || parentLimit < hashBuildNonSpillFDHeadroom+1 {
		t.Skipf("parent RLIMIT_NOFILE=%d known=%v is too small for isolated child test", parentLimit, ok)
	}
	target := uint64(128)
	if parentLimit < target {
		target = parentLimit
	}
	targetText := strconv.FormatUint(target, 10)
	cmd := exec.Command(
		"/bin/sh", "-c",
		`ulimit -S -n "$MO_HASHBUILD_RLIMIT_NOFILE" &&
ulimit -H -n "$MO_HASHBUILD_RLIMIT_NOFILE" &&
exec "$@"`,
		"sh", os.Args[0], "-test.run=^TestHashBuildSpillFDCapUnderRLIMIT$", "-test.count=1",
	)
	cmd.Env = append(os.Environ(), childEnv+"=1", limitEnv+"="+targetText)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("RLIMIT child failed: %v\n%s", err, output)
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

func TestHashBuildBudgetCachedFastPathSkipsRefreshGate(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	b.capRefreshTTL = time.Hour
	now := time.Unix(0, 0)
	b.capNow = func() time.Time { return now }
	b.SetAggregateCapProvider(func() (uint64, error) { return 10, nil })
	if _, _, refreshed, err := b.refreshAggregateCap(false, 0); err != nil || !refreshed {
		t.Fatalf("seed refresh: refreshed=%v err=%v", refreshed, err)
	}

	b.refreshMu.Lock()
	resultC := make(chan error, 1)
	go func() {
		_, _, refreshed, err := b.refreshAggregateCap(false, 0)
		if err == nil && refreshed {
			err = errors.New("cached refresh unexpectedly sampled provider")
		}
		resultC <- err
	}()

	var err error
	select {
	case err = <-resultC:
		b.refreshMu.Unlock()
	case <-time.After(time.Second):
		b.refreshMu.Unlock()
		<-resultC
		t.Fatal("cached refresh waited for refreshMu")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestHashBuildBudgetUnchangedObservationSkipsRefreshGate(t *testing.T) {
	inputs := HashBuildCeilingInputs{
		CgroupMemoryMax: 8 << 30,
		HostMemTotal:    16 << 30,
		GlobalMpoolCap:  6 << 30,
		FileCacheHint:   512 << 20,
	}
	ceiling, err := ResolveHashBuildCeiling(inputs)
	if err != nil {
		t.Fatal(err)
	}
	b := MustNewHashBuildBudget(ceiling.CNHashCap, ceiling.CNHashCap)
	b.installCNCapProvider(inputs)

	b.refreshMu.Lock()
	doneC := make(chan struct{})
	go func() {
		b.mergeObservedCNCap(inputs, ceiling.CNHashCap)
		close(doneC)
	}()
	select {
	case <-doneC:
		b.refreshMu.Unlock()
	case <-time.After(time.Second):
		b.refreshMu.Unlock()
		<-doneC
		t.Fatal("unchanged CN cap observation waited for refreshMu")
	}
}

func TestHashBuildBudgetCNProviderKeepsProcessMemorySnapshot(t *testing.T) {
	previousInputs := hashBuildProcessMemoryInputs
	previousCap := commonmpool.GlobalCap()
	previousHint := fileservice.GlobalMemoryCacheSizeHint.Swap(0)
	t.Cleanup(func() {
		hashBuildProcessMemoryInputs = previousInputs
		commonmpool.InitCap(previousCap)
		fileservice.GlobalMemoryCacheSizeHint.Store(previousHint)
	})

	hashBuildProcessMemoryInputs = HashBuildCeilingInputs{
		CgroupMemoryMax: 8 << 30,
		HostMemTotal:    16 << 30,
	}
	commonmpool.InitCap(commonmpool.PB)

	b := MustNewHashBuildBudget(4<<30, 4<<30)
	b.installCNCapProvider(HashBuildCeilingInputs{
		CgroupMemoryMax: 4 << 30,
		HostMemTotal:    8 << 30,
	})
	if _, err := b.sampleCNCap(); err != nil {
		t.Fatal(err)
	}
	if b.liveCapInputs.CgroupMemoryMax != 8<<30 || b.liveCapInputs.HostMemTotal != 16<<30 {
		t.Fatalf("physical snapshot changed: %+v", b.liveCapInputs)
	}
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

func TestHashBuildBudgetCompatibilityAndObservabilitySurface(t *testing.T) {
	var nilBudget *HashBuildBudget
	if !nilBudget.Snapshot().Closed ||
		nilBudget.AggregateCap() != 0 ||
		nilBudget.CNHashCap() != 0 ||
		nilBudget.QueryCap() != 0 ||
		nilBudget.AggregateUsed() != 0 ||
		nilBudget.CNHashUsed() != 0 ||
		nilBudget.Current() != 0 ||
		nilBudget.Capacity() != 0 ||
		!nilBudget.Closed() ||
		nilBudget.SpillDiskCap() != 0 ||
		nilBudget.SpillDiskUsed() != 0 ||
		nilBudget.SpillFDCap() != 0 ||
		nilBudget.SpillFDUsed() != 0 {
		t.Fatal("nil budget accessors must report an inert closed budget")
	}
	nilBudget.Close()
	if err := nilBudget.SetSpillCaps(1, 1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil SetSpillCaps error = %v", err)
	}

	var nilGeneration *HashBuildBudgetGeneration
	if nilGeneration.ID() != 0 ||
		nilGeneration.Cap() != 0 ||
		nilGeneration.QueryCap() != 0 ||
		nilGeneration.Capacity() != 0 ||
		nilGeneration.Used() != 0 ||
		nilGeneration.Current() != 0 ||
		!nilGeneration.Closed() ||
		nilGeneration.SpillDiskCap() != 0 ||
		nilGeneration.SpillDiskUsed() != 0 ||
		nilGeneration.SpillFDCap() != 0 ||
		nilGeneration.SpillFDUsed() != 0 ||
		!nilGeneration.Snapshot().Closed {
		t.Fatal("nil generation accessors must report an inert closed generation")
	}
	nilGeneration.Close()
	if nilGeneration.TryReserve(1) {
		t.Fatal("nil generation reservation succeeded")
	}

	for _, kind := range []HashBuildBudgetErrorKind{
		HashBuildBudgetErrorAdmission,
		HashBuildBudgetErrorClosed,
		HashBuildBudgetErrorInvalid,
		HashBuildBudgetErrorCeilingMissing,
	} {
		err := &HashBuildBudgetError{Kind: kind}
		if err.Error() == "" || err.Unwrap() == nil {
			t.Fatalf("kind %d did not expose an error", kind)
		}
	}
	var nilBudgetErr *HashBuildBudgetError
	if nilBudgetErr.Error() != "<nil>" || nilBudgetErr.Unwrap() != nil || nilBudgetErr.Is(ErrHashBuildBudgetAdmission) {
		t.Fatal("nil budget error must remain inert")
	}
	for _, tc := range []struct {
		kind   HashBuildBudgetErrorKind
		target error
	}{
		{HashBuildBudgetErrorAdmission, ErrHashBuildBudgetAdmission},
		{HashBuildBudgetErrorClosed, ErrHashBuildBudgetClosed},
		{HashBuildBudgetErrorInvalid, ErrHashBuildBudgetInvalid},
		{HashBuildBudgetErrorCeilingMissing, ErrHashBuildCeilingMissing},
	} {
		if !errors.Is(&HashBuildBudgetError{Kind: tc.kind}, tc.target) {
			t.Fatalf("kind %d did not match %v", tc.kind, tc.target)
		}
	}
	if errors.Is(&HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed},
		ErrHashBuildBudgetAdmission,
	) {
		t.Fatal("closed budget must not match a recoverable capacity admission")
	}
	unknown := &HashBuildBudgetError{Kind: HashBuildBudgetErrorKind(255)}
	if errors.Is(unknown, ErrHashBuildBudgetAdmission) ||
		!errors.Is(unknown, ErrHashBuildBudgetInvalid) {
		t.Fatal("unknown error kind must remain a fatal invalid error")
	}
	message := &HashBuildBudgetError{Message: "explicit"}
	if message.Error() != "explicit" {
		t.Fatalf("explicit message = %q", message.Error())
	}

	b, err := NewHashBuildBudgetWithSpillCaps(100, 80, 200, 10)
	if err != nil {
		t.Fatal(err)
	}
	if b.AggregateCap() != 100 || b.CNHashCap() != 100 || b.Capacity() != 100 || b.QueryCap() != 80 {
		t.Fatal("budget cap aliases disagree")
	}
	if b.SpillDiskCap() != 200 || b.SpillFDCap() != 10 {
		t.Fatal("explicit spill caps were not installed")
	}
	if err = b.SetSpillCaps(0, 0); err != nil {
		t.Fatal(err)
	}
	snapshot := b.Snapshot()
	if snapshot.AggregateCap != 100 || snapshot.AggregateUsed != 0 || snapshot.Closed {
		t.Fatalf("unexpected budget snapshot: %+v", snapshot)
	}

	g, err := b.OpenGenerationWithLimits(7, 50, 100, 5)
	if err != nil {
		t.Fatal(err)
	}
	if g.ID() != 7 || g.Cap() != 50 || g.QueryCap() != 50 || g.Capacity() != 50 {
		t.Fatal("generation identity or cap aliases disagree")
	}
	if g.SpillDiskCap() != 100 || g.SpillFDCap() != 5 || g.Current() != 0 {
		t.Fatal("generation spill caps or current usage are wrong")
	}
	if !g.TryReserve(1) {
		t.Fatal("TryReserve rejected an admissible charge")
	}
	token, err := g.Reserve(8)
	if err != nil {
		t.Fatal(err)
	}
	if token.GenerationID() != 7 || token.Size() != 8 || token.Released() {
		t.Fatal("memory reservation accessors are inconsistent")
	}
	if ok, reconcileErr := token.Reconcile(6); !ok || reconcileErr != nil {
		t.Fatalf("compatibility reconcile failed: ok=%v err=%v", ok, reconcileErr)
	}
	moved := token.TransferOwnership()
	if moved == nil || !token.Released() || moved.GenerationID() != 7 {
		t.Fatal("memory ownership transfer failed")
	}
	if !moved.Release() {
		t.Fatal("transferred memory reservation did not release")
	}

	disk, err := g.ReserveSpillDiskBytes(12)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := g.ReserveSpillFileDescriptors(2)
	if err != nil {
		t.Fatal(err)
	}
	if disk.Size() != 12 || disk.Released() || fd.Size() != 2 || fd.Released() {
		t.Fatal("spill reservation accessors are inconsistent")
	}
	if ok, reconcileErr := disk.Reconcile(10); !ok || reconcileErr != nil {
		t.Fatalf("disk reconcile failed: ok=%v err=%v", ok, reconcileErr)
	}
	if ok, reconcileErr := fd.Reconcile(1); !ok || reconcileErr != nil {
		t.Fatalf("fd reconcile failed: ok=%v err=%v", ok, reconcileErr)
	}
	movedDisk := disk.TransferTo()
	movedFD := fd.TransferOwnership()
	if movedDisk == nil || movedFD == nil || !disk.Released() || !fd.Released() {
		t.Fatal("spill ownership transfer failed")
	}
	if !movedDisk.Release() || !movedFD.Release() {
		t.Fatal("transferred spill reservations did not release")
	}
	stats := g.Stats()
	if stats.ID != 7 ||
		g.Peak() == 0 ||
		g.ReserveCount() == 0 ||
		g.ReconcileCount() == 0 ||
		g.ReleaseCount() == 0 ||
		g.RejectCount() != 0 {
		t.Fatalf("unexpected generation stats: %+v", stats)
	}

	other, err := b.NewGeneration(8)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	query, err := b.OpenQueryBudget(9)
	if err != nil {
		t.Fatal(err)
	}
	query.Close()
	explicit, err := b.OpenGenerationWithCapAndSpill(10, 40, 80, 4)
	if err != nil {
		t.Fatal(err)
	}
	explicit.Close()
	g.Close()
	b.Close()
	if !b.Closed() || !g.Closed() {
		t.Fatal("close accessors did not observe terminal state")
	}

	ceiling, err := ResolveHashBuildBudget(HashBuildCeilingInputs{HostMemTotal: 8 << 30})
	if err != nil {
		t.Fatal(err)
	}
	fromCeiling, err := NewHashBuildBudgetFromCeiling(ceiling)
	if err != nil {
		t.Fatal(err)
	}
	fromCeiling.Close()
}

func TestHashBuildBudgetCompatibilityUnhappyPaths(t *testing.T) {
	var nilProcess *Process
	if _, err := nilProcess.GetHashBuildBudget(); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil process error = %v", err)
	}

	var nilBudget *HashBuildBudget
	if _, err := nilBudget.OpenGeneration(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil budget generation error = %v", err)
	}
	if _, err := nilBudget.OpenGenerationWithCap(1, 1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil budget explicit generation error = %v", err)
	}

	b, err := NewHashBuildBudgetWithSpillCaps(100, 80, 20, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpenGenerationWithCap(1, 0); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("zero generation cap error = %v", err)
	}
	if _, err = b.OpenGenerationWithCap(1, 101); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("oversized generation cap error = %v", err)
	}

	g, err := b.OpenGenerationWithSpillCaps(1, 80, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	other, err := b.OpenGenerationWithSpillCaps(2, 80, 20, 4)
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := b.OpenGenerationWithSpillCaps(3, 80, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.SpillDiskCap() != 20 || defaults.SpillFDCap() != 4 {
		t.Fatal("default generation spill caps were not clamped to the CN caps")
	}
	defaults.Close()

	disk, err := g.ReserveSpillDisk(8)
	if err != nil {
		t.Fatal(err)
	}
	if err = disk.Grow(2); err != nil {
		t.Fatal(err)
	}
	if err = disk.Grow(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("query disk admission error = %v", err)
	}
	if _, err = other.ReserveSpillDisk(11); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("CN disk admission error = %v", err)
	}
	if ok, reconcileErr := disk.ReconcileDown(11); ok || !errors.Is(reconcileErr, ErrHashBuildReservationUpward) {
		t.Fatalf("upward disk reconcile: ok=%v err=%v", ok, reconcileErr)
	}

	fd, err := g.ReserveSpillFD(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.ReserveSpillFD(2); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("query FD admission error = %v", err)
	}
	otherFD, err := other.ReserveSpillFD(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.ReserveSpillFD(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("CN FD admission error = %v", err)
	}
	if ok, reconcileErr := fd.ReconcileDown(2); ok || !errors.Is(reconcileErr, ErrHashBuildReservationUpward) {
		t.Fatalf("upward FD reconcile: ok=%v err=%v", ok, reconcileErr)
	}

	memory, err := g.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	movedMemory := memory.TransferTo()
	if movedMemory == nil || memory.Transfer() != nil || memory.Release() {
		t.Fatal("inactive memory token accepted a second terminal transition")
	}
	if !movedMemory.Release() || movedMemory.Release() {
		t.Fatal("memory release was not exactly once")
	}

	g.Close()
	if _, err = g.ReserveSpillDisk(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed disk reservation error = %v", err)
	}
	if _, err = g.ReserveSpillFD(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed FD reservation error = %v", err)
	}
	if err = disk.Grow(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed disk growth error = %v", err)
	}
	if !disk.Release() || disk.Release() || disk.Transfer() != nil {
		t.Fatal("disk release was not exactly once")
	}
	if _, reconcileErr := disk.ReconcileDown(0); !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("inactive disk reconcile error = %v", reconcileErr)
	}
	if !fd.Release() || fd.Release() || fd.Transfer() != nil {
		t.Fatal("FD release was not exactly once")
	}
	if _, reconcileErr := fd.ReconcileDown(0); !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("inactive FD reconcile error = %v", reconcileErr)
	}
	otherFD.Release()

	var nilMemory *HashBuildReservation
	var nilDisk *HashBuildSpillDiskReservation
	var nilFD *HashBuildSpillFDReservation
	var nilGeneration *HashBuildBudgetGeneration
	if nilMemory.Size() != 0 || nilMemory.GenerationID() != 0 || !nilMemory.Released() ||
		nilMemory.Release() || nilMemory.Transfer() != nil || nilMemory.TransferTo() != nil ||
		nilDisk.Size() != 0 || !nilDisk.Released() || nilDisk.Release() ||
		nilDisk.Transfer() != nil || nilDisk.TransferOwnership() != nil ||
		nilFD.Size() != 0 || !nilFD.Released() || nilFD.Release() ||
		nilFD.Transfer() != nil || nilFD.TransferTo() != nil {
		t.Fatal("nil reservation must remain inert")
	}
	if _, err = nilGeneration.ReserveSpillDisk(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil disk generation error = %v", err)
	}
	if _, err = nilGeneration.ReserveSpillFD(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil FD generation error = %v", err)
	}
	if _, reconcileErr := nilMemory.Reconcile(0); !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("nil memory reconcile error = %v", reconcileErr)
	}
	if _, reconcileErr := nilDisk.Reconcile(0); !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("nil disk reconcile error = %v", reconcileErr)
	}
	if _, reconcileErr := nilFD.Reconcile(0); !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("nil FD reconcile error = %v", reconcileErr)
	}
	if err = nilMemory.Grow(1); !errors.Is(err, ErrHashBuildReservationInactive) {
		t.Fatalf("nil memory growth error = %v", err)
	}
	if err = nilDisk.Grow(1); !errors.Is(err, ErrHashBuildReservationInactive) {
		t.Fatalf("nil disk growth error = %v", err)
	}

	b.Close()
	if _, err = b.OpenGeneration(3); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed budget generation error = %v", err)
	}
	if _, err = b.OpenGenerationWithCap(3, 1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed budget explicit generation error = %v", err)
	}
}

func TestGetHashBuildBudgetInitializesAndReusesCNAggregate(t *testing.T) {
	const localService = "__process_local_cn__"
	hashBuildCNBudgets.Delete(localService)
	t.Cleanup(func() { hashBuildCNBudgets.Delete(localService) })

	first := &Process{Base: &BaseProcess{Lim: Limitation{
		Size:      2 << 20,
		SpillSize: 4 << 20,
	}}}
	firstGeneration, err := first.GetHashBuildBudget()
	if err != nil {
		t.Fatal(err)
	}
	if firstGeneration.Cap() != 2<<20 || firstGeneration.SpillDiskCap() != 4<<20 {
		t.Fatalf("unexpected first generation limits: %+v", firstGeneration.Snapshot())
	}
	cached, err := first.GetHashBuildBudget()
	if err != nil || cached != firstGeneration {
		t.Fatalf("process-local generation was not cached: generation=%p err=%v", cached, err)
	}

	second := &Process{Base: &BaseProcess{Lim: Limitation{Size: 1 << 20}}}
	secondGeneration, err := second.GetHashBuildBudget()
	if err != nil {
		t.Fatal(err)
	}
	if secondGeneration == firstGeneration ||
		secondGeneration.budget != firstGeneration.budget ||
		secondGeneration.Cap() != 1<<20 {
		t.Fatal("second process did not reuse the CN aggregate with its own generation")
	}

	aggregate := firstGeneration.budget
	defaultAggregateSpillCap := aggregate.SpillDiskCap()
	raisedSpillCap := defaultAggregateSpillCap + 1<<20
	third := &Process{Base: &BaseProcess{Lim: Limitation{
		Size:      1 << 20,
		SpillSize: int64(raisedSpillCap),
	}}}
	thirdGeneration, err := third.GetHashBuildBudget()
	if err != nil {
		t.Fatal(err)
	}
	if thirdGeneration.SpillDiskCap() != raisedSpillCap ||
		aggregate.SpillDiskCap() != raisedSpillCap {
		t.Fatalf("explicit spill cap was not raised at the shared ledger: generation=%d aggregate=%d want=%d",
			thirdGeneration.SpillDiskCap(), aggregate.SpillDiskCap(), raisedSpillCap)
	}

	lower := &Process{Base: &BaseProcess{Lim: Limitation{
		Size:      1 << 20,
		SpillSize: 2 << 20,
	}}}
	lowerGeneration, err := lower.GetHashBuildBudget()
	if err != nil {
		t.Fatal(err)
	}
	if lowerGeneration.SpillDiskCap() != 2<<20 ||
		aggregate.SpillDiskCap() != raisedSpillCap {
		t.Fatalf("lower per-query spill cap changed the shared ceiling: generation=%d aggregate=%d want aggregate=%d",
			lowerGeneration.SpillDiskCap(), aggregate.SpillDiskCap(), raisedSpillCap)
	}

	firstGeneration.Close()
	secondGeneration.Close()
	thirdGeneration.Close()
	lowerGeneration.Close()
	aggregate.Close()
}

func TestHashBuildBudgetExplicitSpillCapConcurrentRaise(t *testing.T) {
	budget := MustNewHashBuildBudget(100, 100)
	t.Cleanup(budget.Close)
	generation, err := budget.OpenGenerationWithSpillCaps(1, 100, 800, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(generation.Close)
	reservation, err := generation.ReserveSpillDisk(700)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reservation.Release() })

	caps := []uint64{801, 900, 1200, 1100}
	start := make(chan struct{})
	errs := make(chan error, len(caps))
	var wg sync.WaitGroup
	for _, cap := range caps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- budget.raiseSpillDiskCapToExplicitLimit(cap)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for raiseErr := range errs {
		if raiseErr != nil {
			t.Fatal(raiseErr)
		}
	}
	if got := budget.SpillDiskCap(); got != 1200 {
		t.Fatalf("concurrent raised spill cap = %d, want 1200", got)
	}
	if !reservation.Release() || budget.SpillDiskUsed() != 0 {
		t.Fatalf("live reservation did not release after cap growth: %+v", budget.Snapshot())
	}

	budget.Close()
	if err = budget.raiseSpillDiskCapToExplicitLimit(1300); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed budget raise error = %v, want %v", err, ErrHashBuildBudgetClosed)
	}
	if got := budget.SpillDiskCap(); got != 1200 {
		t.Fatalf("closed budget changed spill cap to %d", got)
	}
}

func TestOpenProcessGenerationClampsStaleResolvedCapAtomically(t *testing.T) {
	budget := MustNewHashBuildBudget(100, 100)
	if err := budget.UpdateAggregateCap(40); err != nil {
		t.Fatal(err)
	}

	generation, err := budget.openProcessGeneration(1, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if generation.Cap() != 40 {
		t.Fatalf("generation cap = %d, want current aggregate cap 40",
			generation.Cap())
	}
	if generation.SpillDiskCap() != defaultSpillCap(40) {
		t.Fatalf("spill disk cap = %d, want %d",
			generation.SpillDiskCap(), defaultSpillCap(40))
	}

	// Explicit public configuration remains strict. Only the process path may
	// clamp a ceiling sample that became stale between resolution and opening.
	if _, err = budget.OpenGenerationWithCap(2, 100); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("explicit oversized generation cap returned %v", err)
	}
	generation.Close()
	budget.Close()
}

func TestHashBuildBudgetDefensiveAndProviderFailurePaths(t *testing.T) {
	for _, limits := range [][2]uint64{{0, 1}, {1, 0}, {1, 2}} {
		if _, err := NewHashBuildBudget(limits[0], limits[1]); !errors.Is(err, ErrHashBuildBudgetInvalid) {
			t.Fatalf("invalid limits %v returned %v", limits, err)
		}
		if _, err := NewHashBuildBudgetWithSpillCaps(limits[0], limits[1], 1, 1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
			t.Fatalf("invalid spill budget limits %v returned %v", limits, err)
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("MustNewHashBuildBudget did not panic for invalid limits")
			}
		}()
		MustNewHashBuildBudget(0, 0)
	}()

	var nilBudget *HashBuildBudget
	if _, _, _, err := nilBudget.refreshAggregateCap(false, 0); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil refresh error = %v", err)
	}
	nilBudget.SetAggregateCapProvider(func() (uint64, error) { return 1, nil })
	if err := nilBudget.UpdateAggregateCap(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("nil cap update error = %v", err)
	}

	b, err := NewHashBuildBudgetWithSpillCaps(math.MaxUint64, math.MaxUint64, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	g, err := b.OpenGenerationWithSpillCaps(1, math.MaxUint64, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpenGenerationWithSpillCaps(2, 0, 0, 0); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("invalid spill generation error = %v", err)
	}
	disk, err := g.ReserveSpillDisk(5)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.SetSpillCaps(4, 10); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("spill cap reduction error = %v", err)
	}
	disk.Release()

	b.capNow = nil
	if err = b.UpdateAggregateCap(math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	b.SetAggregateCapProvider(func() (uint64, error) { return 0, nil })
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildCeilingMissing) {
		t.Fatalf("zero provider ceiling error = %v", err)
	}
	b.capCached = true
	b.capRefreshTTL = time.Hour
	b.capRefreshAt = time.Now()
	b.capRefreshEpoch = 2
	b.capRefreshErr = nil
	called := false
	b.capProvider = func() (uint64, error) {
		called = true
		return 1, nil
	}
	if _, _, refreshed, err := b.refreshAggregateCap(true, 1); err != nil || refreshed || called {
		t.Fatalf("concurrent refresh was not reused: refreshed=%v called=%v err=%v", refreshed, called, err)
	}

	failing, err := NewHashBuildBudget(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	failingGeneration, err := failing.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	providerErr := errors.New("provider failed")
	failing.SetAggregateCapProvider(func() (uint64, error) { return 0, providerErr })
	if _, err = failingGeneration.Reserve(1); !errors.Is(err, providerErr) {
		t.Fatalf("reserve provider error = %v", err)
	}
	token := &HashBuildReservation{
		budget:     failing,
		generation: failingGeneration,
		core:       &hashBuildReservationCore{size: 1},
	}
	if err = token.Grow(1); !errors.Is(err, providerErr) {
		t.Fatalf("grow provider error = %v", err)
	}

	forceReserve, err := NewHashBuildBudget(10, 10)
	if err != nil {
		t.Fatal(err)
	}
	forceReserve.capRefreshTTL = time.Hour
	forceReserveGeneration, err := forceReserve.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	reserveCalls := 0
	forceReserve.SetAggregateCapProvider(func() (uint64, error) {
		reserveCalls++
		if reserveCalls == 1 {
			return 5, nil
		}
		return 0, providerErr
	})
	seed, err := forceReserveGeneration.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	seed.Release()
	if _, err = forceReserveGeneration.Reserve(6); !errors.Is(err, providerErr) {
		t.Fatalf("forced reserve refresh error = %v", err)
	}

	forceGrow, err := NewHashBuildBudget(10, 10)
	if err != nil {
		t.Fatal(err)
	}
	forceGrow.capRefreshTTL = time.Hour
	forceGrowGeneration, err := forceGrow.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	growCalls := 0
	forceGrow.SetAggregateCapProvider(func() (uint64, error) {
		growCalls++
		if growCalls == 1 {
			return 5, nil
		}
		return 0, providerErr
	})
	growToken, err := forceGrowGeneration.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = growToken.Grow(5); !errors.Is(err, providerErr) {
		t.Fatalf("forced grow refresh error = %v", err)
	}
	growToken.Release()

	rescueGrow, err := NewHashBuildBudget(10, 10)
	if err != nil {
		t.Fatal(err)
	}
	rescueGrow.capRefreshTTL = time.Hour
	rescueGeneration, err := rescueGrow.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	rescueCalls := 0
	rescueGrow.SetAggregateCapProvider(func() (uint64, error) {
		rescueCalls++
		if rescueCalls == 1 {
			return 5, nil
		}
		return 10, nil
	})
	rescueToken, err := rescueGeneration.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = rescueToken.Grow(5); err != nil || rescueToken.Size() != 6 {
		t.Fatalf("forced growth rescue: size=%d err=%v", rescueToken.Size(), err)
	}
	rescueToken.Release()

	noProvider, err := NewHashBuildBudget(5, 5)
	if err != nil {
		t.Fatal(err)
	}
	noProviderGeneration, err := noProvider.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	noProviderToken, err := noProviderGeneration.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = noProviderToken.Grow(5); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("non-provider aggregate growth error = %v", err)
	}
	noProviderToken.Release()

	empty, err := NewHashBuildBudget(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = empty.resolveCNCapSample(HashBuildCeilingInputs{}); !errors.Is(err, ErrHashBuildCeilingMissing) {
		t.Fatalf("empty live sample error = %v", err)
	}
	previousCap := commonmpool.GlobalCap()
	commonmpool.InitCap(2 << 30)
	previousHint := fileservice.GlobalMemoryCacheSizeHint.Swap(32 << 20)
	func() {
		defer commonmpool.InitCap(previousCap)
		defer fileservice.GlobalMemoryCacheSizeHint.Store(previousHint)
		if _, err = empty.sampleCNCap(); err != nil {
			t.Fatalf("live CN sample error = %v", err)
		}
	}()
	empty.capNow = nil
	empty.installCNCapProvider(HashBuildCeilingInputs{HostMemTotal: 1 << 30})
	empty.mergeObservedCNCap(HashBuildCeilingInputs{
		CgroupMemoryMax: 512 << 20,
		HostMemTotal:    768 << 20,
		GlobalMpoolCap:  640 << 20,
		FileCacheHint:   32 << 20,
	}, 50)
	if empty.AggregateCap() != 50 {
		t.Fatalf("merged aggregate cap = %d", empty.AggregateCap())
	}

	closedBudget, err := NewHashBuildBudget(10, 10)
	if err != nil {
		t.Fatal(err)
	}
	closedGeneration, err := closedBudget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	closedGeneration.closed = true
	closedBudget.mu.Lock()
	_, err, rejected := closedGeneration.reserveLocked(1, true)
	closedBudget.mu.Unlock()
	if rejected || !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed reserveLocked: rejected=%v err=%v", rejected, err)
	}
	closedToken := &HashBuildReservation{
		budget:     closedBudget,
		generation: closedGeneration,
		core:       &hashBuildReservationCore{size: 1},
	}
	if err, rejected = closedToken.growLocked(1, true); rejected || !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed growLocked: rejected=%v err=%v", rejected, err)
	}
	closedToken.core.state.Store(hashBuildReservationReleased)
	if err, rejected = closedToken.growLocked(1, true); rejected || !errors.Is(err, ErrHashBuildReservationInactive) {
		t.Fatalf("inactive growLocked: rejected=%v err=%v", rejected, err)
	}
	budgetless := &HashBuildReservation{core: &hashBuildReservationCore{}}
	if budgetless.Released() {
		t.Fatal("active budgetless token reported released")
	}

	overflowBudget, err := NewHashBuildBudget(math.MaxUint64, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	overflowGeneration, err := overflowBudget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	overflow := &HashBuildReservation{
		budget:     overflowBudget,
		generation: overflowGeneration,
		core:       &hashBuildReservationCore{size: math.MaxUint64},
	}
	if err = overflow.Grow(0); err != nil {
		t.Fatal(err)
	}
	if err = overflow.Grow(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("overflow growth error = %v", err)
	}
	overflowDisk := &HashBuildSpillDiskReservation{
		budget:     overflowBudget,
		generation: overflowGeneration,
		core:       &hashBuildReservationCore{size: math.MaxUint64},
	}
	overflowBudget.spillDiskCap = math.MaxUint64
	overflowGeneration.spillDiskCap = math.MaxUint64
	if err = overflowDisk.Grow(0); err != nil {
		t.Fatal(err)
	}
	if err = overflowDisk.Grow(1); !errors.Is(err, ErrHashBuildBudgetInvalid) {
		t.Fatalf("overflow disk growth error = %v", err)
	}
	overflowDisk.core.state.Store(hashBuildReservationReleased)
	if err = overflowDisk.Grow(1); !errors.Is(err, ErrHashBuildReservationInactive) {
		t.Fatalf("inactive disk growth error = %v", err)
	}
	cnDiskBudget, err := NewHashBuildBudgetWithSpillCaps(100, 100, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	cnDiskFirst, err := cnDiskBudget.OpenGenerationWithSpillCaps(1, 100, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	cnDiskSecond, err := cnDiskBudget.OpenGenerationWithSpillCaps(2, 100, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	firstDisk, err := cnDiskFirst.ReserveSpillDisk(6)
	if err != nil {
		t.Fatal(err)
	}
	secondDisk, err := cnDiskSecond.ReserveSpillDisk(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = secondDisk.Grow(4); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("CN disk growth error = %v", err)
	}
	firstDisk.Release()
	secondDisk.Release()

	corruptMemory := &HashBuildReservation{
		budget:     overflowBudget,
		generation: overflowGeneration,
		core:       &hashBuildReservationCore{size: 5},
	}
	if ok, reconcileErr := corruptMemory.ReconcileDown(0); ok || !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("corrupt memory reconcile: ok=%v err=%v", ok, reconcileErr)
	}
	if !corruptMemory.Release() || overflowBudget.AggregateUsed() != 0 || overflowGeneration.Used() != 0 {
		t.Fatal("defensive memory release did not clamp corrupt counters")
	}

	corruptDisk := &HashBuildSpillDiskReservation{
		budget:     overflowBudget,
		generation: overflowGeneration,
		core:       &hashBuildReservationCore{size: 5},
	}
	if ok, reconcileErr := corruptDisk.ReconcileDown(0); ok || !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("corrupt disk reconcile: ok=%v err=%v", ok, reconcileErr)
	}
	if !corruptDisk.Release() {
		t.Fatal("defensive disk release failed")
	}
	corruptFD := &HashBuildSpillFDReservation{
		budget:     overflowBudget,
		generation: overflowGeneration,
		core:       &hashBuildReservationCore{size: 5},
	}
	if ok, reconcileErr := corruptFD.ReconcileDown(0); ok || !errors.Is(reconcileErr, ErrHashBuildReservationInactive) {
		t.Fatalf("corrupt FD reconcile: ok=%v err=%v", ok, reconcileErr)
	}
	if !corruptFD.Release() {
		t.Fatal("defensive FD release failed")
	}

	largeHint, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{
		HostMemTotal:  10 << 30,
		FileCacheHint: 9 << 30,
	})
	if err != nil || largeHint.RequestedReserve != 9<<30 {
		t.Fatalf("large cache hint ceiling = %+v, err=%v", largeHint, err)
	}
	tiny, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{HostMemTotal: 128 << 20})
	if err != nil || tiny.CNHashCap == 0 {
		t.Fatalf("tiny ceiling = %+v, err=%v", tiny, err)
	}
	if _, err = ResolveHashBuildCeiling(HashBuildCeilingInputs{HostMemTotal: 1}); !errors.Is(err, ErrHashBuildCeilingMissing) {
		t.Fatalf("zero resulting CN cap error = %v", err)
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
