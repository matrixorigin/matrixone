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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

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
