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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// testPhysicalAllocation exercises the capacity controller at the same
// acquire/free boundary used by MPool without exposing a second production
// reservation API.
type testPhysicalAllocation struct {
	generation *ExecutionResourceGeneration
	size       uint64
	released   atomic.Bool
}

func TestExecutionResourceBudgetObserverDimensionsAreFixed(t *testing.T) {
	components := [...]string{"memory", "spill_disk", "spill_fd"}
	events := [...]string{"reserve", "release", "reconcile", "reject"}
	scopes := [...]string{"query", "cn"}
	want := len(components) * len(events) * len(scopes)
	if len(executionResourceBudgetObservers) != want {
		t.Fatalf("observer count = %d, want %d", len(executionResourceBudgetObservers), want)
	}
	for _, component := range components {
		for _, event := range events {
			for _, scope := range scopes {
				key := executionResourceBudgetMetricKey{component: component, event: event, scope: scope}
				if executionResourceBudgetObservers[key] == nil {
					t.Fatalf("missing fixed observer: %+v", key)
				}
			}
		}
	}
	unknown := executionResourceBudgetMetricKey{component: "unknown", event: "reserve", scope: "query"}
	if executionResourceBudgetObservers[unknown] != nil {
		t.Fatal("unknown dimensions must not create a metric observer")
	}
}

func acquireTestPhysicalAllocation(
	generation *ExecutionResourceGeneration,
	size uint64,
) (*testPhysicalAllocation, error) {
	if err := generation.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	return &testPhysicalAllocation{generation: generation, size: size}, nil
}

func (a *testPhysicalAllocation) Release() bool {
	if a == nil || a.generation == nil || !a.released.CompareAndSwap(false, true) {
		return false
	}
	a.generation.ReleaseAllocationCapacity(a.size)
	return true
}

func TestExecutionResourceBudgetPhysicalAllocationLimit(t *testing.T) {
	budget := MustNewExecutionResourceBudget(math.MaxUint64, math.MaxUint64)
	generation, err := budget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := acquireTestPhysicalAllocation(generation, math.MaxUint64)
	if err != nil {
		t.Fatalf("exact limit rejected: %v", err)
	}
	if _, err = acquireTestPhysicalAllocation(generation, 1); !errors.Is(err, ErrExecutionResourceAdmission) {
		t.Fatalf("limit+1 error = %v", err)
	}
	if budget.AggregateUsed() != math.MaxUint64 || generation.Used() != math.MaxUint64 {
		t.Fatal("failed admission changed the physical allocation ledger")
	}
	if !allocation.Release() || allocation.Release() {
		t.Fatal("physical allocation must release exactly once")
	}
	if budget.AggregateUsed() != 0 || generation.Used() != 0 {
		t.Fatal("physical allocation release leaked capacity")
	}
}

func TestExecutionResourceBudgetAdmissionIdentifiesResource(t *testing.T) {
	b := MustNewExecutionResourceBudget(10, 10)
	g, err := b.OpenGenerationWithSpillCaps(1, 10, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)

	tests := []struct {
		name string
		want ExecutionResourceComponent
		call func() error
	}{
		{
			name: "memory",
			want: ExecutionResourceComponentMemory,
			call: func() error {
				return g.AcquireAllocationCapacity(11)
			},
		},
		{
			name: "spill disk",
			want: ExecutionResourceComponentSpillDisk,
			call: func() error {
				_, reserveErr := g.ReserveSpillDisk(6)
				return reserveErr
			},
		},
		{
			name: "spill fd",
			want: ExecutionResourceComponentSpillFD,
			call: func() error {
				_, reserveErr := g.ReserveSpillFD(2)
				return reserveErr
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var budgetErr *ExecutionResourceError
			if err := test.call(); !errors.As(err, &budgetErr) {
				t.Fatalf("error=%v, want typed admission", err)
			}
			if budgetErr.Kind != ExecutionResourceErrorAdmission || budgetErr.Component != test.want {
				t.Fatalf("admission kind/resource=(%d,%d), want=(%d,%d)",
					budgetErr.Kind, budgetErr.Component, ExecutionResourceErrorAdmission, test.want)
			}
		})
	}
}

func TestExecutionResourceBudgetAllocationAccountIsSoleOwner(t *testing.T) {
	budget := MustNewExecutionResourceBudget(10, 10)
	generation, err := budget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	account, err := registry.OpenWithController(11, generation)
	if err != nil {
		t.Fatal(err)
	}
	mp := mpool.MustNewZero()

	buffer, err := mp.AllocAccounted(10, account, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if generation.Used() != 10 || account.Snapshot().Used != 10 {
		t.Fatal("physical allocation was not charged exactly once")
	}
	if _, err = mp.AllocAccounted(1, account, 1, 1); !errors.Is(err, ErrExecutionResourceAdmission) ||
		!errors.Is(err, mpool.ErrAllocationAccountCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if generation.Used() != 10 || account.Snapshot().Used != 10 ||
		registry.LiveAllocationMetadata() != 1 {
		t.Fatal("failed allocation changed account state")
	}

	generation.Close()
	if _, err = mp.AllocAccounted(1, account, 1, 1); !errors.Is(err, mpool.ErrAllocationAccountSealed) {
		t.Fatalf("closed generation error = %v", err)
	}
	mp.Free(buffer)
	if generation.Used() != 0 || account.Snapshot().Used != 0 {
		t.Fatal("MPool.Free did not release the sole charge")
	}
	account.Seal()
	if _, err = registry.Finalize(account); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionResourceBudgetTransientMemoryReservation(t *testing.T) {
	budget := MustNewExecutionResourceBudget(10, 10)
	generation, err := budget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := generation.ReserveTransientMemory(6)
	if err != nil {
		t.Fatal(err)
	}
	if generation.Used() != 6 {
		t.Fatalf("used = %d, want 6", generation.Used())
	}
	if _, err = generation.ReserveTransientMemory(5); !errors.Is(err, ErrExecutionResourceAdmission) {
		t.Fatalf("capacity error = %v", err)
	}
	if !reservation.Release() || reservation.Release() || generation.Used() != 0 {
		t.Fatal("transient reservation was not released exactly once")
	}
	reservation, err = generation.ReserveTransientMemory(10)
	if err != nil {
		t.Fatal(err)
	}
	generation.Close()
	if _, err = generation.ReserveTransientMemory(1); !errors.Is(err, ErrExecutionResourceClosed) {
		t.Fatalf("closed generation error = %v", err)
	}
	if !reservation.Release() || generation.Used() != 0 {
		t.Fatal("closed generation did not accept the live token release")
	}
}

func TestExecutionResourceAllocationAccountRegistryBounds(t *testing.T) {
	budget := MustNewExecutionResourceBudget(16<<10, 16<<10)
	first, err := budget.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := first.AllocationAccountRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if registry.GenerationCapacity() != executionResourceAllocationGenerationSlots ||
		registry.MaxAllocationMetadata() != 16<<10 {
		t.Fatal("allocation registry does not follow byte-conservation bounds")
	}
	second, err := budget.OpenGeneration(2)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := second.AllocationAccountRegistry()
	if err != nil || shared != registry {
		t.Fatal("one CN budget created multiple allocation registries")
	}

	large := MustNewExecutionResourceBudget(executionResourceAllocationMetadataMaxSlots+1, executionResourceAllocationMetadataMaxSlots+1)
	largeGeneration, err := large.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	largeRegistry, err := largeGeneration.AllocationAccountRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if largeRegistry.MaxAllocationMetadata() != executionResourceAllocationMetadataMaxSlots {
		t.Fatal("allocation metadata exceeded its fixed headroom")
	}
}

func TestExecutionResourceBudgetQueryRejectRollsBackCN(t *testing.T) {
	budget := MustNewExecutionResourceBudget(10, 4)
	first, _ := budget.OpenGeneration(1)
	second, _ := budget.OpenGeneration(2)
	allocation, err := acquireTestPhysicalAllocation(first, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = acquireTestPhysicalAllocation(second, 7); !errors.Is(err, ErrExecutionResourceAdmission) {
		t.Fatalf("query rejection error = %v", err)
	}
	if budget.AggregateUsed() != 4 || second.Used() != 0 {
		t.Fatal("query rejection did not roll back the CN charge")
	}
	allocation.Release()
}

func TestExecutionResourceBudgetConcurrentPhysicalAllocations(t *testing.T) {
	const workers = 64
	budget := MustNewExecutionResourceBudget(workers, workers)
	allocations := make(chan *testPhysicalAllocation, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		generation, err := budget.OpenGeneration(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			allocation, acquireErr := acquireTestPhysicalAllocation(generation, 1)
			if acquireErr != nil {
				t.Errorf("acquire: %v", acquireErr)
				return
			}
			allocations <- allocation
		}()
	}
	wg.Wait()
	close(allocations)
	if budget.AggregateUsed() != workers {
		t.Fatalf("aggregate used = %d", budget.AggregateUsed())
	}
	for allocation := range allocations {
		allocation.Release()
	}
	if budget.AggregateUsed() != 0 {
		t.Fatal("concurrent physical allocations leaked")
	}
}

func TestExecutionResourceGenerationIsolationAndClose(t *testing.T) {
	budget := MustNewExecutionResourceBudget(8, 8)
	oldGeneration, _ := budget.OpenGeneration(1)
	oldAllocation, _ := acquireTestPhysicalAllocation(oldGeneration, 6)
	oldGeneration.Close()
	if _, err := acquireTestPhysicalAllocation(oldGeneration, 1); !errors.Is(err, ErrExecutionResourceClosed) {
		t.Fatalf("closed generation error = %v", err)
	}
	newGeneration, _ := budget.OpenGeneration(2)
	newAllocation, err := acquireTestPhysicalAllocation(newGeneration, 2)
	if err != nil {
		t.Fatal(err)
	}
	oldAllocation.Release()
	if newGeneration.Used() != 2 || budget.AggregateUsed() != 2 {
		t.Fatal("old generation release affected the new generation")
	}
	newAllocation.Release()
	budget.Close()
	if _, err = budget.OpenGeneration(3); !errors.Is(err, ErrExecutionResourceClosed) {
		t.Fatalf("closed budget error = %v", err)
	}
}

func TestExecutionResourceBudgetLiveCapRefresh(t *testing.T) {
	budget := MustNewExecutionResourceBudget(10, 10)
	var capValue atomic.Uint64
	capValue.Store(10)
	var calls atomic.Uint64
	budget.SetAggregateCapProvider(func() (uint64, error) {
		calls.Add(1)
		return capValue.Load(), nil
	})
	budget.capRefreshTTL = time.Hour
	generation, _ := budget.OpenGeneration(1)
	first, err := acquireTestPhysicalAllocation(generation, 8)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider calls = %d", calls.Load())
	}
	capValue.Store(6)
	cachedAllocation, err := acquireTestPhysicalAllocation(generation, 1)
	if err != nil {
		t.Fatalf("cached cap should remain valid: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("cached fast path sampled the provider")
	}
	first.Release()
	cachedAllocation.Release()
	// Invalidate the cache to model the next observation interval.
	budget.mu.Lock()
	budget.capCached = false
	budget.mu.Unlock()
	if _, err = acquireTestPhysicalAllocation(generation, 7); !errors.Is(err, ErrExecutionResourceAdmission) {
		t.Fatalf("shrunk live cap error = %v", err)
	}
	if budget.AggregateCap() != 6 {
		t.Fatalf("aggregate cap = %d", budget.AggregateCap())
	}
}

func TestExecutionResourceBudgetSpillLedgers(t *testing.T) {
	budget := MustNewExecutionResourceBudget(64, 64)
	generation, _ := budget.OpenGeneration(1)
	disk, err := generation.ReserveSpillDisk(100)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := generation.ReserveSpillFD(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = disk.Grow(25); err != nil || disk.Size() != 125 {
		t.Fatalf("disk grow: size=%d err=%v", disk.Size(), err)
	}
	if ok, reconcileErr := disk.ReconcileDown(40); !ok || reconcileErr != nil {
		t.Fatalf("disk reconcile: ok=%v err=%v", ok, reconcileErr)
	}
	generation.Close()
	if _, err = generation.ReserveSpillDisk(1); !errors.Is(err, ErrExecutionResourceClosed) {
		t.Fatalf("closed spill error = %v", err)
	}
	disk.Release()
	fd.Release()
	if budget.SpillDiskUsed() != 0 || budget.SpillFDUsed() != 0 {
		t.Fatal("spill reservations leaked")
	}
}

func TestExecutionResourceBudgetSpillReleaseRejectsLedgerUnderflow(t *testing.T) {
	for _, test := range []struct {
		name    string
		reserve func(*ExecutionResourceGeneration) (func() bool, error)
		corrupt func(*ExecutionResourceBudget, *ExecutionResourceGeneration)
	}{
		{
			name: "disk",
			reserve: func(generation *ExecutionResourceGeneration) (func() bool, error) {
				reservation, err := generation.ReserveSpillDisk(2)
				return reservation.Release, err
			},
			corrupt: func(budget *ExecutionResourceBudget, generation *ExecutionResourceGeneration) {
				generation.spillDiskUsed = 1
				budget.spillDiskUsed = 1
			},
		},
		{
			name: "fd",
			reserve: func(generation *ExecutionResourceGeneration) (func() bool, error) {
				reservation, err := generation.ReserveSpillFD(2)
				return reservation.Release, err
			},
			corrupt: func(budget *ExecutionResourceBudget, generation *ExecutionResourceGeneration) {
				generation.spillFDUsed = 1
				budget.spillFDUsed = 1
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := MustNewExecutionResourceBudget(64, 64)
			generation, err := budget.OpenGeneration(1)
			if err != nil {
				t.Fatal(err)
			}
			release, err := test.reserve(generation)
			if err != nil {
				t.Fatal(err)
			}
			budget.mu.Lock()
			test.corrupt(budget, generation)
			budget.mu.Unlock()
			defer func() {
				if recover() == nil {
					t.Fatal("corrupt spill ledger release did not panic")
				}
			}()
			release()
		})
	}
}

func TestClampSpillFDCapBoundaries(t *testing.T) {
	for _, test := range []struct {
		configured, limit uint64
		known             bool
		want              uint64
	}{
		{2048, 1 << 20, false, 0},
		{0, 1024, true, 0},
		{2048, 64, true, 0},
		{2048, 65, true, 1},
		{2048, 128, true, 64},
		{2048, 1024, true, 768},
		{10, 1024, true, 10},
		{2048, math.MaxUint64, true, 2048},
	} {
		if got := clampSpillFDCap(test.configured, test.limit, test.known); got != test.want {
			t.Fatalf("clampSpillFDCap(%d, %d, %v) = %d, want %d",
				test.configured, test.limit, test.known, got, test.want)
		}
	}
}

func TestGetExecutionResourceBudgetInitializesAndReusesCNAggregate(t *testing.T) {
	const localService = "__process_local_cn__"
	executionResourceCNBudgets.Delete(localService)
	t.Cleanup(func() { executionResourceCNBudgets.Delete(localService) })

	first := &Process{Base: &BaseProcess{Lim: Limitation{Size: 2 << 20, SpillSize: 4 << 20}}}
	firstGeneration, err := first.GetExecutionResourceBudget()
	if err != nil {
		t.Fatal(err)
	}
	if firstGeneration.Cap() != 2<<20 || firstGeneration.SpillDiskCap() != 4<<20 {
		t.Fatalf("first generation limits: %+v", firstGeneration.Snapshot())
	}
	if cached, cachedErr := first.GetExecutionResourceBudget(); cachedErr != nil || cached != firstGeneration {
		t.Fatal("process generation was not cached")
	}
	second := &Process{Base: &BaseProcess{Lim: Limitation{Size: 1 << 20}}}
	secondGeneration, err := second.GetExecutionResourceBudget()
	if err != nil {
		t.Fatal(err)
	}
	if secondGeneration == firstGeneration || secondGeneration.budget != firstGeneration.budget ||
		secondGeneration.Cap() != 1<<20 {
		t.Fatal("second process did not reuse the CN aggregate")
	}
	firstGeneration.Close()
	secondGeneration.Close()
	firstGeneration.budget.Close()
}

func TestResolveExecutionMemoryCeiling(t *testing.T) {
	const gib = uint64(1 << 30)
	ceiling, err := ResolveExecutionMemoryCeiling(ExecutionMemoryCeilingInputs{
		CgroupMemoryMax:       20 * gib,
		HostMemTotal:          10 * gib,
		GlobalMpoolCap:        30 * gib,
		FileCacheHint:         gib,
		ProcessLimitationSize: 2 * gib,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ceiling.EffectiveCN != 10*gib || ceiling.Reserve != 4*gib ||
		ceiling.CNMemoryCap != 6*gib || ceiling.QueryCap != 2*gib {
		t.Fatalf("ceiling = %+v", ceiling)
	}
	if _, err = ResolveExecutionMemoryCeiling(ExecutionMemoryCeilingInputs{
		CgroupMemoryMax: math.MaxUint64,
	}); !errors.Is(err, ErrExecutionMemoryCeilingMissing) {
		t.Fatalf("missing finite source error = %v", err)
	}
	if small, smallErr := ResolveExecutionMemoryCeiling(ExecutionMemoryCeilingInputs{
		HostMemTotal:  3 * gib,
		FileCacheHint: 3 * gib,
	}); smallErr != nil || small.CNMemoryCap != 3*gib/20 {
		t.Fatalf("small-CN ceiling = %+v, err=%v", small, smallErr)
	}
}

func TestExecutionResourceBudgetUsesCurrentMemoryInputs(t *testing.T) {
	previous := executionResourceProcessMemoryInputs
	executionResourceProcessMemoryInputs = ExecutionMemoryCeilingInputs{HostMemTotal: 8 << 30}
	t.Cleanup(func() { executionResourceProcessMemoryInputs = previous })
	previousHint := fileservice.GlobalMemoryCacheSizeHint.Load()
	fileservice.GlobalMemoryCacheSizeHint.Store(1 << 30)
	t.Cleanup(func() { fileservice.GlobalMemoryCacheSizeHint.Store(previousHint) })

	inputs := executionResourceProcessMemoryInputs
	inputs.FileCacheHint = uint64(fileservice.GlobalMemoryCacheSizeHint.Load())
	ceiling, err := ResolveExecutionMemoryCeiling(inputs)
	if err != nil || ceiling.CNMemoryCap == 0 {
		t.Fatalf("current memory inputs did not produce a finite cap: %+v %v", ceiling, err)
	}
}

func BenchmarkExecutionResourceBudgetAllocationAccount(b *testing.B) {
	for _, accounted := range []bool{false, true} {
		mode := "unaccounted"
		if accounted {
			mode = "accounted"
		}
		for _, size := range []int{64, 4 << 10, 64 << 10} {
			b.Run(mode+"/alloc-free/"+fmt.Sprint(size), func(b *testing.B) {
				mp := mpool.MustNewZero()
				var generation *ExecutionResourceGeneration
				var registry *mpool.AllocationAccountRegistry
				var account *mpool.AllocationAccount
				if accounted {
					budget := MustNewExecutionResourceBudget(math.MaxUint64, math.MaxUint64)
					var err error
					generation, err = budget.OpenGeneration(1)
					if err != nil {
						b.Fatal(err)
					}
					registry, err = mpool.NewAllocationAccountRegistry(1, 2)
					if err != nil {
						b.Fatal(err)
					}
					account, err = registry.OpenWithController(math.MaxInt64, generation)
					if err != nil {
						b.Fatal(err)
					}
				}

				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for range b.N {
					var allocation []byte
					var err error
					if accounted {
						allocation, err = mp.AllocAccounted(size, account, 1, 1)
					} else {
						allocation, err = mp.Alloc(size, true)
					}
					if err != nil {
						b.Fatal(err)
					}
					mp.Free(allocation)
				}
				b.StopTimer()
				if accounted {
					if generation.Used() != 0 || account.Snapshot().Used != 0 {
						b.Fatal("physical allocation capacity leaked")
					}
					if _, _, err := registry.CompleteTerminal(account); err != nil {
						b.Fatal(err)
					}
					generation.Close()
				}
			})
		}
	}
}
