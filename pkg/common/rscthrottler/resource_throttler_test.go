// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestCgroupBudgetAcrossInstances(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	getCgroupMemoryUsage = func(int) (int64, error) { return 95, nil }
	policies := []struct {
		name    string
		acquire func(*memThrottler, int64) (int64, bool)
	}{
		{"workspace", defaultAcquirePolicy},
		{"s3", AcquirePolicyForCNFlushS3},
		{"branch", AcquirePolicyForDataBranch},
	}
	for _, first := range policies {
		for _, second := range policies {
			t.Run(first.name+"/"+second.name, func(t *testing.T) {
				budget := &cgroupMemoryBudget{}
				makeOwner := func() *memThrottler {
					m := &memThrottler{cgroupBudget: budget, limitRate: 0.8}
					m.actualTotalMemory.Store(100)
					m.total.Store(200)
					m.cgroup.Store(100)
					m.limit.Store(80)
					m.rss.Store(60)
					m.refreshCgroupUsage()
					return m
				}
				a, b := makeOwner(), makeOwner()
				_, ok := first.acquire(a, 4)
				require.True(t, ok)
				// A has reserved but has not allocated yet. Neither a competing
				// owner nor the same owner can spend those four units again.
				for _, owner := range []*memThrottler{a, b} {
					left, granted := second.acquire(owner, 4)
					require.False(t, granted)
					require.Equal(t, int64(1), left)
				}
				b.refreshCgroupUsage()
				require.Equal(t, int64(1), b.Available())
				require.Equal(t, int64(1), cnFlushS3PhysicalAvailable(b, 0, b.cgroupSnapshot()))
				_, ok = second.acquire(b, 4)
				require.False(t, ok)
				require.Equal(t, int64(4), budget.reserved.Load())
				// Failed upstream allocation: release A's claim and retry B.
				a.Release(4)
				_, ok = second.acquire(b, 4)
				require.True(t, ok)
				// An extra release by A must not return B's claim.
				a.Release(4)
				require.Equal(t, int64(4), budget.reserved.Load())
				b.Release(100)
				require.Zero(t, budget.reserved.Load())
				// A local quota failure must leave the shared budget untouched.
				b.limit.Store(1)
				_, ok = second.acquire(b, 4)
				require.False(t, ok)
				require.Zero(t, budget.reserved.Load())
			})
		}
	}
}

func TestCgroupBudgetConcurrentOwnersAndRefresh(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	getCgroupMemoryUsage = func(int) (int64, error) { return 95, nil }
	b := &cgroupMemoryBudget{}
	owners := make([]*memThrottler, 8)
	for i := range owners {
		m := &memThrottler{cgroupBudget: b}
		m.total.Store(200)
		m.cgroup.Store(100)
		m.actualTotalMemory.Store(100)
		m.limit.Store(80)
		m.rss.Store(60)
		m.refreshCgroupUsage()
		owners[i] = m
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var granted atomic.Int64
	for _, m := range owners {
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 20 {
					m.refreshCgroupUsage()
					if _, ok := defaultAcquirePolicy(m, 1); ok {
						granted.Add(1)
					}
				}
			}()
		}
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(5), granted.Load())
	require.Equal(t, int64(5), b.reserved.Load())
	for _, m := range owners {
		m.Release(100)
	}
	require.Zero(t, b.reserved.Load())
}

func TestProductionThrottlersShareCgroupOwner(t *testing.T) {
	a := NewMemThrottler("test-a", 0.8).(*memThrottler)
	b := NewMemThrottler("test-b", 0.8).(*memThrottler)
	require.Same(t, &processCgroupBudget, a.cgroupBudget)
	require.Same(t, a.cgroupBudget, b.cgroupBudget)
}

func TestSharedCgroupClaimsSurvivePressureTransitionAndReadFailure(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	b := &cgroupMemoryBudget{}
	a := &memThrottler{cgroupBudget: b}
	a.total.Store(200)
	a.cgroup.Store(100)
	a.actualTotalMemory.Store(100)
	a.limit.Store(80)
	a.rss.Store(60)
	// The claim predates hard pressure and must still be included afterwards.
	b.sample.Store(&cgroupMemorySample{usage: 80, limit: 100})
	_, ok := defaultAcquirePolicy(a, 4)
	require.True(t, ok)
	other := &memThrottler{cgroupBudget: b}
	other.total.Store(200)
	other.cgroup.Store(100)
	other.actualTotalMemory.Store(100)
	other.limit.Store(80)
	other.rss.Store(60)
	getCgroupMemoryUsage = func(int) (int64, error) { return 95, nil }
	other.refreshCgroupUsage()
	getCgroupMemoryUsage = func(int) (int64, error) { return 0, errors.New("failed read") }
	a.refreshCgroupUsage()
	_, ok = AcquirePolicyForCNFlushS3(other, 4)
	require.False(t, ok)
	require.Equal(t, int64(4), b.reserved.Load())
	a.Release(4)
	_, ok = AcquirePolicyForCNFlushS3(other, 4)
	require.True(t, ok)
	other.Release(4)
	require.Zero(t, b.reserved.Load())
}

// No allocation is performed by the policies themselves. Model an owner paused
// between Acquire and allocation; refreshing memory.current must not cover it.
func TestCgroupPendingReservationSurvivesRefresh(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	getCgroupMemoryUsage = func(int) (int64, error) { return 95, nil }
	for name, policy := range map[string]func(*memThrottler, int64) (int64, bool){
		"default":     defaultAcquirePolicy,
		"S3":          AcquirePolicyForCNFlushS3,
		"data branch": AcquirePolicyForDataBranch,
	} {
		t.Run(name, func(t *testing.T) {
			m := &memThrottler{limitRate: 0.9}
			m.actualTotalMemory.Store(100)
			m.cgroup.Store(100)
			m.total.Store(200)
			m.limit.Store(90)
			m.rss.Store(60)
			m.refreshCgroupUsage()

			reserved := make(chan bool, 1)
			allocationFailed := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, ok := policy(m, 4)
				reserved <- ok
				<-allocationFailed
				if ok {
					m.Release(4) // owner rolls back after allocation failure
				}
			}()
			var release sync.Once
			finish := func() { release.Do(func() { close(allocationFailed) }); <-done }
			defer finish()
			require.True(t, <-reserved)

			left, ok := policy(m, 4)
			require.False(t, ok)
			require.Equal(t, int64(1), left)
			// This is the cgroup sampling stage used by ForceRefresh after denial.
			// Neither sampling nor a later RSS sample proves allocation happened.
			m.refreshCgroupUsage()
			m.rssReservedBase.Store(4)
			left, ok = policy(m, 4)
			require.False(t, ok)
			require.Equal(t, int64(1), left)
			require.Equal(t, int64(4), m.reserved.Load())
			require.Equal(t, int64(1), cnFlushS3PhysicalAvailable(m, 4, m.cgroupSample.Load()))

			finish()
			require.Zero(t, m.reserved.Load())
			_, ok = policy(m, 4)
			require.True(t, ok)
			m.Release(4)
			require.Zero(t, m.reserved.Load())
		})
	}
}

func TestCgroupAdmissionSnapshotDuringRefresh(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	var usage atomic.Int64
	usage.Store(95)
	getCgroupMemoryUsage = func(int) (int64, error) { return usage.Load(), nil }
	m := &memThrottler{}
	m.actualTotalMemory.Store(100)
	m.cgroup.Store(100)
	m.total.Store(200)
	m.limit.Store(90)
	m.rss.Store(60)
	m.reserved.Store(4)
	m.refreshCgroupUsage()

	sampled := make(chan struct{})
	published := make(chan struct{})
	type result struct{ admission, physical int64 }
	results := make(chan result, 1)
	go func() {
		// Pause the reader after the one atomic snapshot load used by an
		// admission attempt, then finish both calculations after publication.
		sample := m.cgroupSample.Load()
		close(sampled)
		<-published
		results <- result{
			sample.capAdmissionAvailable(30, 4),
			cnFlushS3PhysicalAvailable(m, 4, sample),
		}
	}()
	<-sampled
	usage.Store(99)
	m.refreshCgroupUsage()
	close(published)
	r := <-results
	// Old (95/100) and new (99/100) samples both deny ask=2. A reader
	// finishing after publication must keep the original sample throughout.
	require.Equal(t, int64(1), r.admission)
	require.Equal(t, int64(1), r.physical)
	require.Zero(t, m.cgroupSample.Load().capAdmissionAvailable(30, 4))
	for _, policy := range []func(*memThrottler, int64) (int64, bool){defaultAcquirePolicy, AcquirePolicyForCNFlushS3} {
		left, ok := policy(m, 2)
		require.False(t, ok)
		require.Zero(t, left)
		require.Equal(t, int64(4), m.reserved.Load())
	}
}

func TestCgroupRefreshReadFailureKeepsPendingBudget(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	getCgroupMemoryUsage = func(int) (int64, error) { return 0, errors.New("read failed") }
	m := &memThrottler{}
	m.cgroup.Store(100)
	m.total.Store(200)
	sample := &cgroupMemorySample{usage: 95, limit: 100}
	m.cgroupSample.Store(sample)
	m.refreshCgroupUsage()
	require.Same(t, sample, m.cgroupSample.Load())
	require.Equal(t, int64(1), m.cgroupSample.Load().capAdmissionAvailable(90, 4))
}

func TestConcurrentCgroupRefreshDoesNotRenewPendingBudget(t *testing.T) {
	oldRead := getCgroupMemoryUsage
	t.Cleanup(func() { getCgroupMemoryUsage = oldRead })
	getCgroupMemoryUsage = func(int) (int64, error) { return 95, nil }
	for name, policy := range map[string]func(*memThrottler, int64) (int64, bool){
		"default": defaultAcquirePolicy,
		"S3":      AcquirePolicyForCNFlushS3,
	} {
		t.Run(name, func(t *testing.T) {
			m := &memThrottler{}
			m.actualTotalMemory.Store(100)
			m.cgroup.Store(100)
			m.total.Store(200)
			m.limit.Store(90)
			m.rss.Store(60)
			m.refreshCgroupUsage()
			start := make(chan struct{})
			var wg sync.WaitGroup
			var granted atomic.Int64
			wg.Add(17)
			go func() {
				defer wg.Done()
				<-start
				for range 100 {
					m.refreshCgroupUsage()
				}
			}()
			for range 16 {
				go func() {
					defer wg.Done()
					<-start
					for range 10 {
						if _, ok := policy(m, 1); ok {
							granted.Add(1)
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			require.Equal(t, int64(5), granted.Load())
			require.Equal(t, int64(5), m.reserved.Load())
		})
	}
}

func TestMemThrottlerUsesCgroupHeadroomForAdmission(t *testing.T) {
	oldGetCgroupMemoryUsage := getCgroupMemoryUsage
	getCgroupMemoryUsage = func(int) (int64, error) { return 95 * mpool.GB, nil }
	t.Cleanup(func() { getCgroupMemoryUsage = oldGetCgroupMemoryUsage })

	throttler := &memThrottler{}
	throttler.actualTotalMemory.Store(100 * mpool.GB)
	throttler.total.Store(200 * mpool.GB)
	throttler.cgroup.Store(100 * mpool.GB)
	throttler.rss.Store(60 * mpool.GB)
	throttler.limit.Store(90 * mpool.GB)

	// Below hard pressure, reclaimable cgroup charge does not reduce the normal
	// RSS-based component budget.
	throttler.cgroupSample.Store(&cgroupMemorySample{usage: 90 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})
	require.Equal(t, int64(40*mpool.GB), throttler.Available())

	// Once hard pressure is active, cap new admission by the remaining cgroup
	// headroom without treating the whole cgroup charge as non-reclaimable RSS.
	throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})
	require.Equal(t, int64(5*mpool.GB), throttler.Available())

	throttler.options.specializedForMerge = true
	require.Equal(t, int64(5*mpool.GB), throttler.Available())

	t.Run("sequential grants share one sampled budget", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.actualTotalMemory.Store(100 * mpool.GB)
		throttler.limit.Store(90 * mpool.GB)
		throttler.rss.Store(70 * mpool.GB)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})

		left, ok := defaultAcquirePolicy(throttler, 4*mpool.GB)
		require.True(t, ok)
		require.Equal(t, int64(mpool.GB), left)

		left, ok = defaultAcquirePolicy(throttler, 4*mpool.GB)
		require.False(t, ok)
		require.Equal(t, int64(mpool.GB), left)
		require.Equal(t, int64(4*mpool.GB), throttler.reserved.Load())
	})

	t.Run("concurrent grants share one sampled budget", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.actualTotalMemory.Store(100 * mpool.GB)
		throttler.limit.Store(90 * mpool.GB)
		throttler.rss.Store(70 * mpool.GB)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})

		const workers = 32
		start := make(chan struct{})
		var granted atomic.Int32
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				<-start
				if _, ok := defaultAcquirePolicy(throttler, mpool.GB); ok {
					granted.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(5), granted.Load())
		require.Equal(t, int64(5*mpool.GB), throttler.reserved.Load())
	})

	t.Run("outstanding reservations remain charged without allocation confirmation", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.actualTotalMemory.Store(100 * mpool.GB)
		throttler.limit.Store(90 * mpool.GB)
		throttler.rss.Store(70 * mpool.GB)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})
		throttler.reserved.Store(10 * mpool.GB)

		_, ok := defaultAcquirePolicy(throttler, 4*mpool.GB)
		require.False(t, ok)
		left, ok := defaultAcquirePolicy(throttler, 2*mpool.GB)
		require.False(t, ok)
		require.Zero(t, left)
		require.Equal(t, int64(10*mpool.GB), throttler.reserved.Load())
	})
}

func TestMemThrottlerPressureUsesCgroupUsage(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()
	freeOSMemory = func() {}

	var target int64
	now := time.Now().UnixNano()
	throttler := &memThrottler{limitRate: 0.90}
	throttler.options.enableRSSScavenging = true
	throttler.options.rssCacheTargetSetter = func(value int64) { target = value }
	throttler.options.rssCacheEvictor = func(context.Context, int64) {}
	throttler.actualTotalMemory.Store(100 * mpool.GB)
	throttler.limit.Store(90 * mpool.GB)
	throttler.rss.Store(70 * mpool.GB)
	throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95 * mpool.GB, limit: int64(throttler.actualTotalMemory.Load())})

	// RSS is below the hard threshold, but cgroup usage is above it.
	throttler.tryScavengeRSS(now, 70*mpool.GB)
	require.Equal(t, rssPressureHard, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, rssCacheHardTarget, target)
}

func TestBasic(t *testing.T) {
	t.Run("A", func(t *testing.T) {
		throttler := NewMemThrottler("TestBasic", 1)

		throttler.PrintUsage()
		avail1 := throttler.Available()

		for i := 0; i < 10; i++ {
			throttler.Acquire(10)
			throttler.PrintUsage()

			throttler.Release(10)
			throttler.PrintUsage()
		}

		avail2 := throttler.Available()

		require.Equal(t, avail1, avail2)
	})

	t.Run("B", func(t *testing.T) {
		total := int64(mpool.KB)

		throttler := NewMemThrottler(
			"TestBasic",
			1,
			WithConstLimit(total),
		)

		throttler.PrintUsage()
		avail1 := throttler.Available()
		require.Equal(t, total, avail1)

		for i := 0; i < 10; i++ {
			throttler.Acquire(10)
			throttler.PrintUsage()

			throttler.Release(10)
			throttler.PrintUsage()
		}

		avail2 := throttler.Available()
		require.Equal(t, avail1, avail2)

		left, ok := throttler.Acquire(1000)
		require.True(t, ok)
		require.Equal(t, total-1000, left)

		throttler.PrintUsage()

		left, ok = throttler.Acquire(1000)
		require.False(t, ok)
		require.Equal(t, total-1000, left)

		throttler.PrintUsage()
	})

}

func TestParallel(t *testing.T) {
	throttler := NewMemThrottler("TestParallel", 50.0/100.0)

	throttler.PrintUsage()
	available := throttler.Available()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
			}()

			for j := 0; j < 1000*10; j++ {
				avail := throttler.Available()
				rnd := rand.Intn(int(avail+5)/5) + 1000

				for {
					if _, ok := throttler.Acquire(int64(rnd)); ok {
						break
					}
				}

				time.Sleep(time.Microsecond)
				throttler.Release(int64(rnd))
			}
		}()
	}

	wg.Wait()

	throttler.PrintUsage()
	available2 := throttler.Available()
	require.Equal(t, available, available2)
}

func BenchmarkThrottler(b *testing.B) {
	throttler := NewMemThrottler("BenchmarkThrottler", 50.0/100.0)

	for i := 0; i < b.N; i++ {
		throttler.Acquire(10)
		throttler.Release(10)
		throttler.Available()
	}
}

func TestAcquirePolicyForDataBranch(t *testing.T) {
	t.Run("deny when projected usage exceeds rate limit", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.80}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(80)
		throttler.rss.Store(70)
		throttler.reserved.Store(5)

		left, ok := AcquirePolicyForDataBranch(throttler, 11)
		require.False(t, ok)
		require.Equal(t, int64(0), left)
		require.Equal(t, int64(5), throttler.reserved.Load())
	})

	t.Run("allow at boundary and reserve memory", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.80}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(80)
		throttler.rss.Store(70)

		left, ok := AcquirePolicyForDataBranch(throttler, 10)
		require.True(t, ok)
		require.Equal(t, int64(20), left)
		require.Equal(t, int64(10), throttler.reserved.Load())
	})

	t.Run("fallback to default policy when rate check is disabled", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(80)
		throttler.rss.Store(20)

		left, ok := AcquirePolicyForDataBranch(throttler, 10)
		require.True(t, ok)
		require.Equal(t, int64(70), left)
		require.Equal(t, int64(10), throttler.reserved.Load())
	})

	t.Run("cgroup headroom backstops data branch admission under hard pressure", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.80}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(80)
		throttler.rss.Store(60)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})

		left, ok := AcquirePolicyForDataBranch(throttler, 6)
		require.False(t, ok)
		require.Equal(t, int64(5), left)
		require.Equal(t, int64(0), throttler.reserved.Load())
	})

	t.Run("data branch grants cumulatively consume sampled cgroup headroom", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.80}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(80)
		throttler.rss.Store(60)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})

		_, ok := AcquirePolicyForDataBranch(throttler, 4)
		require.True(t, ok)
		left, ok := AcquirePolicyForDataBranch(throttler, 4)
		require.False(t, ok)
		require.Equal(t, int64(1), left)
		require.Equal(t, int64(4), throttler.reserved.Load())
	})
}

func TestMemThrottlerRSSScavenging(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()

	var calls atomic.Int32
	freeOSMemory = func() {
		calls.Add(1)
	}

	now := time.Now().UnixNano()
	throttler := &memThrottler{limitRate: 0.90}
	throttler.options.enableRSSScavenging = true
	throttler.actualTotalMemory.Store(1000 * mpool.GB)
	throttler.limit.Store(900 * mpool.GB)
	throttler.rss.Store(900 * mpool.GB)
	throttler.lastRSSScavenge.Store(now - int64(rssScavengeInterval) - int64(time.Second))

	throttler.tryScavengeRSS(now, 900*mpool.GB)
	require.Equal(t, int32(1), calls.Load())

	throttler.tryScavengeRSS(now+int64(time.Second), 900*mpool.GB)
	require.Equal(t, int32(1), calls.Load())
}

func TestMemThrottlerRSSCacheEvictionByRSSRate(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()

	var freeCalls atomic.Int32
	freeOSMemory = func() {
		freeCalls.Add(1)
	}

	targets := make(chan int64, 4)
	now := time.Now().UnixNano()
	throttler := &memThrottler{limitRate: 0.90}
	throttler.options.enableRSSScavenging = true
	throttler.options.rssCacheEvictor = func(_ context.Context, targetPercent int64) {
		targets <- targetPercent
	}
	throttler.actualTotalMemory.Store(1000 * mpool.GB)
	throttler.limit.Store(900 * mpool.GB)
	throttler.reserved.Store(800 * mpool.GB)
	throttler.lastRSSScavenge.Store(now - int64(rssScavengeInterval) - int64(time.Second))

	throttler.tryScavengeRSS(now, 840*mpool.GB)
	select {
	case target := <-targets:
		t.Fatalf("unexpected cache evict target %d", target)
	default:
	}
	require.Equal(t, int32(0), freeCalls.Load())

	throttler.tryScavengeRSS(now+int64(time.Second), 850*mpool.GB)
	require.Eventually(t, func() bool {
		return recvTarget(targets) == rssCacheSoftTarget
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return freeCalls.Load() == 1
	}, time.Second, time.Millisecond)

	throttler.tryScavengeRSS(now+2*int64(time.Second), 920*mpool.GB)
	require.Eventually(t, func() bool {
		return recvTarget(targets) == rssCacheHardTarget
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return freeCalls.Load() == 2
	}, time.Second, time.Millisecond)

	throttler.tryScavengeRSS(now+3*int64(time.Second), 920*mpool.GB)
	select {
	case target := <-targets:
		t.Fatalf("unexpected duplicate cache evict target %d", target)
	default:
	}
	require.Equal(t, int64(rssCacheHardTarget), throttler.lastRSSCacheTarget.Load())
}

func TestMemThrottlerRSSPressureStateTargetLifecycle(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()
	freeOSMemory = func() {}

	var setTargets []int64
	var clearCalls int
	evictTargets := make(chan int64, 4)

	now := time.Now().UnixNano()
	throttler := &memThrottler{limitRate: 0.90}
	throttler.options.enableRSSScavenging = true
	throttler.options.rssCacheTargetSetter = func(targetPercent int64) {
		setTargets = append(setTargets, targetPercent)
	}
	throttler.options.rssCacheTargetClearer = func() {
		clearCalls++
	}
	throttler.options.rssCacheEvictor = func(_ context.Context, targetPercent int64) {
		evictTargets <- targetPercent
	}
	throttler.actualTotalMemory.Store(1000 * mpool.GB)
	throttler.limit.Store(900 * mpool.GB)
	throttler.reserved.Store(800 * mpool.GB)

	throttler.tryScavengeRSS(now, 850*mpool.GB)
	require.Equal(t, rssPressureSoft, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, []int64{rssCacheSoftTarget}, setTargets)
	require.Eventually(t, func() bool {
		return recvTarget(evictTargets) == rssCacheSoftTarget
	}, time.Second, time.Millisecond)

	throttler.tryScavengeRSS(now+int64(time.Second), 920*mpool.GB)
	require.Equal(t, rssPressureHard, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, []int64{rssCacheSoftTarget, rssCacheHardTarget}, setTargets)
	require.Eventually(t, func() bool {
		return recvTarget(evictTargets) == rssCacheHardTarget
	}, time.Second, time.Millisecond)

	throttler.tryScavengeRSS(now+2*int64(time.Second), 870*mpool.GB)
	require.Equal(t, rssPressureSoft, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, []int64{rssCacheSoftTarget, rssCacheHardTarget, rssCacheSoftTarget}, setTargets)
	require.Equal(t, 1, clearCalls)
	select {
	case target := <-evictTargets:
		t.Fatalf("unexpected downgrade cache evict target %d", target)
	default:
	}

	throttler.tryScavengeRSS(now+3*int64(time.Second), 920*mpool.GB)
	require.Equal(t, rssPressureHard, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, []int64{rssCacheSoftTarget, rssCacheHardTarget, rssCacheSoftTarget, rssCacheHardTarget}, setTargets)
	require.Eventually(t, func() bool {
		return recvTarget(evictTargets) == rssCacheHardTarget
	}, time.Second, time.Millisecond)

	throttler.tryScavengeRSS(now+4*int64(time.Second), 800*mpool.GB)
	require.Equal(t, rssPressureNone, rssPressureState(throttler.rssPressureState.Load()))
	require.Equal(t, 2, clearCalls)
}

func TestMemThrottlerRSSCacheEvictionConcurrentEscalation(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()

	var freeCalls atomic.Int32
	freeOSMemory = func() {
		freeCalls.Add(1)
	}

	var minTarget atomic.Int64
	minTarget.Store(100)

	now := time.Now().UnixNano()
	throttler := &memThrottler{limitRate: 0.90}
	throttler.options.enableRSSScavenging = true
	throttler.options.rssCacheEvictor = func(_ context.Context, targetPercent int64) {
		for {
			old := minTarget.Load()
			if targetPercent >= old || minTarget.CompareAndSwap(old, targetPercent) {
				return
			}
		}
	}
	throttler.actualTotalMemory.Store(1000 * mpool.GB)
	throttler.limit.Store(900 * mpool.GB)
	throttler.reserved.Store(800 * mpool.GB)
	throttler.lastRSSScavenge.Store(now - int64(rssScavengeInterval) - int64(time.Second))

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		throttler.tryScavengeRSS(now, 890*mpool.GB)
	}()
	go func() {
		defer wg.Done()
		<-start
		throttler.tryScavengeRSS(now, 920*mpool.GB)
	}()

	close(start)
	wg.Wait()

	require.Eventually(t, func() bool {
		return minTarget.Load() == rssCacheHardTarget
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return freeCalls.Load() >= 1
	}, time.Second, time.Millisecond)
	require.Equal(t, int64(rssCacheHardTarget), throttler.lastRSSCacheTarget.Load())
}

func recvTarget(ch <-chan int64) int64 {
	select {
	case target := <-ch:
		return target
	default:
		return 0
	}
}

func TestMemThrottlerRSSScavengingDisabled(t *testing.T) {
	oldFreeOSMemory := freeOSMemory
	defer func() { freeOSMemory = oldFreeOSMemory }()

	var calls atomic.Int32
	freeOSMemory = func() {
		calls.Add(1)
	}

	throttler := &memThrottler{limitRate: 0.90}
	throttler.actualTotalMemory.Store(1000 * mpool.GB)
	throttler.limit.Store(900 * mpool.GB)

	throttler.tryScavengeRSS(time.Now().UnixNano(), 900*mpool.GB)
	require.Equal(t, int32(0), calls.Load())
}

func TestAcquirePolicyForCNFlushS3(t *testing.T) {
	t.Run("allow under high rss when pool has headroom", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(91)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 1)
		require.True(t, ok)
		require.Equal(t, int64(89), left)
		require.Equal(t, int64(1), throttler.reserved.Load())
	})

	t.Run("deny when reserved plus ask exceeds pool limit", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(50)
		throttler.reserved.Store(60)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 31)
		require.False(t, ok)
		require.Equal(t, int64(30), left)
		require.Equal(t, int64(60), throttler.reserved.Load())
	})

	t.Run("deny when large ask alone exceeds pool limit", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(50)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 100)
		require.False(t, ok)
		require.Equal(t, int64(90), left)
		require.Equal(t, int64(0), throttler.reserved.Load())
	})

	t.Run("allow when rss and pool both within limits", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(70)
		throttler.reserved.Store(9)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 2)
		require.True(t, ok)
		require.Equal(t, int64(79), left)
		require.Equal(t, int64(11), throttler.reserved.Load())
	})

	t.Run("allow small write under high rss when pool has headroom", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(88)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 1)
		require.True(t, ok)
		require.Equal(t, int64(89), left)
		require.Equal(t, int64(1), throttler.reserved.Load())
	})

	t.Run("allow under rss limit and reserve memory", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(80)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 10)
		require.True(t, ok)
		require.Equal(t, int64(80), left)
		require.Equal(t, int64(10), throttler.reserved.Load())
	})

	t.Run("allow when reserved exceeds rss", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(30)
		throttler.reserved.Store(40)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 10)
		require.True(t, ok)
		require.Equal(t, int64(40), left)
		require.Equal(t, int64(50), throttler.reserved.Load())
	})

	t.Run("deny when reserved plus ask overflows pool", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(30)
		throttler.reserved.Store(60)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 31)
		require.False(t, ok)
		require.Equal(t, int64(30), left)
		require.Equal(t, int64(60), throttler.reserved.Load())
	})

	t.Run("hard ceiling caps a single large ask below the pool limit", func(t *testing.T) {
		// reserved starts under the reject rate (60/90 = 0.66 < 0.80) so the
		// soft entry gate passes, and the ask fits the pool (75 <= 90). Without
		// a hard ceiling the grant would push reserved to 75 (pinnedRate 0.83),
		// overshooting the 0.80 ceiling the RSS-gate removal relies on.
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.reserved.Store(60)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 15)
		require.False(t, ok)
		require.Equal(t, int64(12), left) // hardCap(72) - reserved(60)
		require.Equal(t, int64(60), throttler.reserved.Load())
	})

	t.Run("hard ceiling holds under concurrency", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)

		const (
			workers = 32
			perAsk  = int64(5)
		)
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 16; j++ {
					AcquirePolicyForCNFlushS3(throttler, perAsk)
				}
			}()
		}
		wg.Wait()

		// pinnedRate must never exceed the hard ceiling, regardless of how many
		// acquirers raced through the soft entry gate.
		hardCap := int64(float64(throttler.limit.Load()) * cnFlushS3PinnedRejectRate)
		require.LessOrEqual(t, throttler.reserved.Load(), hardCap)
	})

	t.Run("deny large ask once pinned more than half", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		throttler.actualTotalMemory.Store(200 * mpool.MB)
		throttler.limit.Store(100 * mpool.MB)
		throttler.rss.Store(55 * mpool.MB)
		throttler.rssReservedBase.Store(55 * mpool.MB)
		throttler.rssMpoolLiveBase.Store(currentLive)
		throttler.reserved.Store(55 * mpool.MB)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 20*mpool.MB)
		require.False(t, ok)
		require.Equal(t, int64(0), left)
		require.Equal(t, int64(55*mpool.MB), throttler.reserved.Load())
	})

	t.Run("deny when high non-s3 rss leaves no physical headroom", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(95)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 6)
		require.False(t, ok)
		require.Equal(t, int64(5), left)
		require.Equal(t, int64(0), throttler.reserved.Load())
	})

	t.Run("cgroup headroom backstops new s3 reservation under hard pressure", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(70)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})

		left, ok := AcquirePolicyForCNFlushS3(throttler, 6)
		require.False(t, ok)
		require.Equal(t, int64(5), left)
		require.Equal(t, int64(0), throttler.reserved.Load())
	})

	t.Run("cgroup headroom accounts for cumulative s3 growth", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(70)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})

		_, ok := AcquirePolicyForCNFlushS3(throttler, 4)
		require.True(t, ok)
		left, ok := AcquirePolicyForCNFlushS3(throttler, 4)
		require.False(t, ok)
		require.Equal(t, int64(1), left)
		require.Equal(t, int64(4), throttler.reserved.Load())
	})

	t.Run("rss reuse does not prove pending allocations are covered by cgroup usage", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(95)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})
		throttler.rssReservedBase.Store(60)
		throttler.rssMpoolLiveBase.Store(currentLive + 60)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 6)
		require.False(t, ok)
		require.Equal(t, int64(5), left)
		require.Zero(t, throttler.reserved.Load())
	})

	t.Run("cgroup guard charges reservations added between cgroup and rss samples", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(70)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})
		// The cgroup sample saw only three reserved bytes. Five more bytes were
		// granted before the later RSS sample, so rssReservedBase includes them
		// even though they have already consumed all sampled cgroup headroom.
		throttler.rssReservedBase.Store(8)
		throttler.rssMpoolLiveBase.Store(currentLive)
		throttler.reserved.Store(8)

		require.Equal(t, int64(0), cnFlushS3PhysicalAvailable(throttler, 8, throttler.cgroupSample.Load()))
		left, ok := AcquirePolicyForCNFlushS3(throttler, 1)
		require.False(t, ok)
		require.Equal(t, int64(0), left)
		require.Equal(t, int64(8), throttler.reserved.Load())
	})

	t.Run("concurrent s3 grants share one sampled cgroup budget", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(70)
		throttler.cgroupSample.Store(&cgroupMemorySample{usage: 95, limit: int64(throttler.actualTotalMemory.Load())})

		const workers = 32
		start := make(chan struct{})
		var granted atomic.Int32
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				<-start
				if _, ok := AcquirePolicyForCNFlushS3(throttler, 1); ok {
					granted.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(5), granted.Load())
		require.Equal(t, int64(5), throttler.reserved.Load())
	})

	t.Run("allow reacquire after flush while rss snapshot still includes prior s3 bytes", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(95)
		// The latest RSS sample was taken before the flush, when 60 bytes of S3
		// buffers were still resident. After release, reserved dropped to zero
		// immediately but RSS has not fallen yet; reacquiring a small amount
		// should still succeed because it reuses bytes already present in RSS.
		throttler.rssReservedBase.Store(60)
		throttler.rssMpoolLiveBase.Store(currentLive + 60)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 5)
		require.True(t, ok)
		require.Equal(t, int64(85), left)
		require.Equal(t, int64(5), throttler.reserved.Load())
	})

	t.Run("allow growth that stays within the rss-covered base delta", func(t *testing.T) {
		throttler := &memThrottler{limitRate: 0.90}
		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		throttler.actualTotalMemory.Store(100)
		throttler.limit.Store(90)
		throttler.rss.Store(95)
		throttler.rssReservedBase.Store(60)
		throttler.rssMpoolLiveBase.Store(currentLive + 3)
		throttler.reserved.Store(57)

		left, ok := AcquirePolicyForCNFlushS3(throttler, 7)
		require.True(t, ok)
		require.Equal(t, int64(26), left)
		require.Equal(t, int64(64), throttler.reserved.Load())
	})
}

func TestCurrentCNFlushS3RSSCovered(t *testing.T) {
	t.Run("covers live reserved plus unreused stale pool", func(t *testing.T) {
		covered := currentCNFlushS3RSSCovered(60, 80, 57, 77)
		require.Equal(t, int64(60), covered)
	})

	t.Run("shrinks as pooled bytes are reused", func(t *testing.T) {
		covered := currentCNFlushS3RSSCovered(60, 80, 0, 50)
		require.Equal(t, int64(30), covered)
	})
}

func TestNextCNFlushS3RSSState(t *testing.T) {
	t.Run("keeps pre-release coverage while rss is unchanged", func(t *testing.T) {
		base, liveBase := nextCNFlushS3RSSState(95, 60, 80, 95, 20, 0)
		require.Equal(t, int64(60), base)
		require.Equal(t, int64(80), liveBase)
	})

	t.Run("decays only by observed rss drop and live reuse", func(t *testing.T) {
		base, liveBase := nextCNFlushS3RSSState(95, 60, 80, 80, 20, 0)
		require.Equal(t, int64(45), base)
		require.Equal(t, int64(65), liveBase)
	})

	t.Run("grows to cover current reserved when larger", func(t *testing.T) {
		base, liveBase := nextCNFlushS3RSSState(80, 20, 20, 90, 40, 40)
		require.Equal(t, int64(40), base)
		require.Equal(t, int64(40), liveBase)
	})

	t.Run("shrinks carried coverage when live bytes rise back", func(t *testing.T) {
		base, liveBase := nextCNFlushS3RSSState(95, 60, 80, 95, 50, 0)
		require.Equal(t, int64(30), base)
		require.Equal(t, int64(80), liveBase)
	})
}

func TestMemThrottlerReleaseClampsOverRelease(t *testing.T) {
	throttler := &memThrottler{}
	throttler.actualTotalMemory.Store(100)
	throttler.limit.Store(90)
	throttler.reserved.Store(5)

	left := throttler.Release(10)
	require.Equal(t, int64(0), throttler.reserved.Load())
	require.Equal(t, int64(90), left)
}

func TestMemThrottlerShouldRefreshBeforeRelease(t *testing.T) {
	currentLive := mpool.GlobalStats().NumCurrBytes.Load()

	t.Run("refreshes when current reserved is not yet covered", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.reserved.Store(20)
		require.True(t, throttler.ShouldRefreshBeforeRelease())
	})

	t.Run("skips refresh when existing coverage already covers current reserved", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.reserved.Store(20)
		throttler.rssReservedBase.Store(60)
		throttler.rssMpoolLiveBase.Store(currentLive)
		require.False(t, throttler.ShouldRefreshBeforeRelease())
	})

	t.Run("refreshes when live reuse shrinks covered bytes below current reserved", func(t *testing.T) {
		throttler := &memThrottler{}
		throttler.reserved.Store(40)
		throttler.rssReservedBase.Store(20)
		throttler.rssMpoolLiveBase.Store(currentLive)
		require.True(t, throttler.ShouldRefreshBeforeRelease())
	})
}
