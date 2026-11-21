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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/stretchr/testify/require"
)

func TestRSSPressureSharesRecovery(t *testing.T) {
	var evictions atomic.Int32
	var frees atomic.Int32
	targets := make(chan int64, 2)
	freed := make(chan struct{}, 4)
	c := rssPressureController{
		setTarget:   func(int64, time.Time) {},
		clearTarget: func() {},
		evict: func(_ context.Context, target int64) {
			evictions.Add(1)
			targets <- target
		},
		free: func() {
			frees.Add(1)
			freed <- struct{}{}
		},
	}
	cn, workspace := c.attach(), c.attach()
	defer cn.Close()
	defer workspace.Close()
	now := time.Now()
	const mib = 1 << 20

	var wg sync.WaitGroup
	for _, source := range []*RSSPressureSource{cn, workspace} {
		wg.Go(func() {
			source.observe(now, 3600*mib, 4096*mib, 32*mib, 0)
		})
	}
	wg.Wait()
	require.Equal(t, rssCacheSoftTarget, receivePressureEvent(t, targets))
	receivePressureEvent(t, freed)
	receivePressureEvent(t, freed)
	workspace.Close()
	cn.Close()
	require.Equal(t, int32(1), evictions.Load())
	require.Equal(t, int32(2), frees.Load(), "one immediate free and one after eviction")
}

func TestRSSPressureTransitionsCancelStaleActions(t *testing.T) {
	walks := make(chan context.Context, 3)
	walkTargets := make(chan int64, 3)
	var frees atomic.Int32
	var targets []int64
	var cleared int
	c := rssPressureController{
		setTarget:   func(target int64, _ time.Time) { targets = append(targets, target) },
		clearTarget: func() { cleared++ },
		evict: func(ctx context.Context, target int64) {
			walkTargets <- target
			walks <- ctx
			<-ctx.Done()
		},
		free: func() { frees.Add(1) },
	}
	source := c.attach()
	defer source.Close()
	now := time.Now()
	observe := func(seconds time.Duration, rss int64) {
		source.observe(now.Add(seconds*time.Second), rss, 1000, 800, 0)
	}
	observe(-121, 950) // Expires before it can affect hysteresis or start recovery.
	observe(0, 849)
	require.Empty(t, targets)
	observe(1, 850)
	soft := receivePressureEvent(t, walks)
	require.Equal(t, rssCacheSoftTarget, receivePressureEvent(t, walkTargets))

	observe(2, 920)
	hard := receivePressureEvent(t, walks)
	require.ErrorIs(t, soft.Err(), context.Canceled)
	require.Equal(t, rssCacheHardTarget, receivePressureEvent(t, walkTargets))

	observe(3, 880)
	receivePressureEvent(t, hard.Done())
	require.Equal(t, 1, cleared)
	require.Equal(t, []int64{80, 50, 80}, targets)

	observe(4, 800)
	observe(3, 950) // A late sample must not reopen pressure after recovery.
	source.Close()
	require.Equal(t, 2, cleared)
	require.Equal(t, []int64{80, 50, 80}, targets)
	require.Empty(t, walks)
	require.Zero(t, frees.Load(), "canceled eviction must not trigger a stale free")
}

func TestRSSPressureCloseLeavesAdmittedWalkWithWorker(t *testing.T) {
	type walk struct {
		ctx    context.Context
		target int64
	}
	started := make(chan walk, 2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var frees, concurrent atomic.Int32
	var overlap atomic.Bool
	var targets []int64
	completed := make(chan struct{}, 2)
	c := rssPressureController{
		setTarget:   func(target int64, _ time.Time) { targets = append(targets, target) },
		clearTarget: func() {},
		evict: func(ctx context.Context, target int64) {
			if concurrent.Add(1) > 1 {
				overlap.Store(true)
			}
			defer concurrent.Add(-1)
			started <- walk{ctx, target}
			if target == rssCacheSoftTarget {
				<-release // A callback already admitted may ignore cancellation.
			}
		},
		free: func() { frees.Add(1); completed <- struct{}{} },
	}
	cn, workspace := c.attach(), c.attach()
	defer cn.Close()
	defer workspace.Close()
	defer unblock()
	cn.observe(time.Now(), 860, 1000, 800, 0)
	first := receivePressureEvent(t, started)
	cn.Close()
	require.NoError(t, first.ctx.Err(), "the remaining reporter still owns pressure")

	closed := make(chan struct{})
	go func() {
		workspace.Close()
		close(closed)
	}()
	receivePressureEvent(t, closed)
	require.ErrorIs(t, first.ctx.Err(), context.Canceled)

	attached := make(chan *RSSPressureSource, 1)
	go func() { attached <- c.attach() }()
	next := receivePressureEvent(t, attached)
	defer next.Close()
	cn.observe(time.Now(), 870, 1000, 800, 0) // A retired source cannot schedule work.
	require.Equal(t, []int64{rssCacheSoftTarget}, targets, "retired source cannot publish a target")
	next.observe(time.Now(), 950, 1000, 800, 0)
	require.Equal(t, []int64{rssCacheSoftTarget, rssCacheHardTarget}, targets)
	require.Empty(t, started, "the existing worker still owns the blocked callback")
	unblock()
	require.Equal(t, rssCacheHardTarget, receivePressureEvent(t, started).target)
	receivePressureEvent(t, completed)
	require.False(t, overlap.Load(), "cache walks must remain serialized across reattach")
	require.Equal(t, int32(1), frees.Load(), "only the replacement walk may scavenge")
}

func TestRSSPressureImmediateFreeUsesOneReport(t *testing.T) {
	var frees atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	started := make(chan struct{}, 2)
	c := rssPressureController{
		setTarget:   func(int64, time.Time) {},
		clearTarget: func() {},
		evict:       func(ctx context.Context, _ int64) { <-ctx.Done() },
		free: func() {
			frees.Add(1)
			started <- struct{}{}
			<-release
		},
	}
	cn, workspace := c.attach(), c.attach()
	defer cn.Close()
	defer workspace.Close()
	defer unblock()
	now := time.Now()
	cn.observe(now, 800, 1000, 0, 0)
	observed := make(chan struct{})
	go func() {
		workspace.observe(now.Add(time.Second), 900, 1000, 55, 600)
		close(observed)
	}()
	receivePressureEvent(t, observed)
	require.Zero(t, frees.Load(), "655 visible bytes in the report exceed 70% of RSS")

	finished := make(chan struct{})
	go func() {
		workspace.observe(now.Add(2*time.Second), 900, 1000, 0, 600)
		close(finished)
	}()
	receivePressureEvent(t, started)
	cn.Close()
	retired := make(chan struct{})
	go func() { workspace.Close(); close(retired) }()
	receivePressureEvent(t, retired)

	next := c.attach()
	defer next.Close()
	restarted := make(chan struct{})
	go func() {
		next.observe(now.Add(rssScavengeInterval+3*time.Second), 900, 1000, 0, 600)
		close(restarted)
	}()
	receivePressureEvent(t, restarted)
	require.Equal(t, int32(1), frees.Load(), "an active free survives reporter detach")
	unblock()
	receivePressureEvent(t, finished)
	next.observe(now.Add(rssScavengeInterval+4*time.Second), 900, 1000, 0, 600)
	require.Equal(t, int32(2), frees.Load(), "eligibility returns after completion and cooldown")

	next.Close()
	later := c.attach()
	defer later.Close()
	later.observe(now.Add(rssScavengeInterval+5*time.Second), 900, 1000, 0, 600)
	require.Equal(t, int32(2), frees.Load(), "the cooldown survives last detach")
}

func TestRSSRefreshOrdersByRSSRead(t *testing.T) {
	var targets []int64
	c := rssPressureController{
		setTarget:   func(target int64, _ time.Time) { targets = append(targets, target) },
		clearTarget: func() {},
		evict:       func(context.Context, int64) {},
		free:        func() {},
	}
	first := c.attach()
	defer first.Close()
	second := c.attach()
	defer second.Close()
	cn := &memThrottler{rssPressure: first}
	workspace := &memThrottler{rssPressure: second}
	cn.reserved.Store(800)
	workspace.reserved.Store(800)

	rssStarted := make(chan struct{})
	read := make(chan struct{})
	allowRead := sync.OnceFunc(func() { close(read) })
	defer allowRead()
	rssRead := make(chan struct{})
	limitStarted := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	finished := make(chan struct{})
	go func() {
		cn.refreshMemory(func() (*process.MemoryInfoStat, error) {
			close(rssStarted)
			<-read
			close(rssRead)
			return &process.MemoryInfoStat{RSS: 950}, nil
		}, func() (uint64, error) {
			close(limitStarted)
			<-release
			return 1000, nil
		})
		close(finished)
	}()
	receivePressureEvent(t, rssStarted)
	if c.sampleMu.TryLock() {
		c.sampleMu.Unlock()
		t.Fatal("another reporter could overtake the active RSS read")
	}
	allowRead()
	receivePressureEvent(t, limitStarted)
	select {
	case <-rssRead:
	default:
		t.Fatal("limit lookup started before RSS was sampled")
	}
	readRSS := func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: 950}, nil }
	healthyReported := make(chan struct{})
	go func() {
		workspace.refreshMemory(readRSS, func() (uint64, error) { return 2000, nil })
		close(healthyReported)
	}()
	receivePressureEvent(t, healthyReported)
	unblock()
	receivePressureEvent(t, finished)
	require.Empty(t, targets, "the older measurement cannot replace a newer healthy pair")

	cn.refreshMemory(readRSS, func() (uint64, error) { return 1000, nil })
	require.Equal(t, []int64{rssCacheHardTarget}, targets, "a newer measurement uses its own ceiling")
}

func TestRSSRefreshRejectsIncompleteCeiling(t *testing.T) {
	walk := make(chan context.Context, 1)
	c := rssPressureController{
		setTarget:   func(int64, time.Time) {},
		clearTarget: func() {},
		evict:       func(ctx context.Context, _ int64) { walk <- ctx; <-ctx.Done() },
		free:        func() {},
	}
	cn, workspace := c.attach(), c.attach()
	defer cn.Close()
	defer workspace.Close()
	readRSS := func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: 950}, nil }
	m1, m2 := &memThrottler{rssPressure: cn}, &memThrottler{rssPressure: workspace}
	m1.reserved.Store(1000)
	m2.reserved.Store(1000)
	m1.refreshMemory(readRSS, func() (uint64, error) { return 1000, nil })
	ctx := receivePressureEvent(t, walk)
	c.mu.Lock()
	last, expires := c.lastSample, c.expires
	c.mu.Unlock()
	m2.refreshMemory(readRSS, func() (uint64, error) { return 0, errors.New("cgroup read failed") })
	m2.refreshMemory(readRSS, func() (uint64, error) { return 0, nil })
	c.mu.Lock()
	state, afterLast, afterExpires := c.state, c.lastSample, c.expires
	c.mu.Unlock()
	require.Equal(t, rssPressureHard, state)
	require.Equal(t, last, afterLast)
	require.Equal(t, expires, afterExpires)
	require.NoError(t, ctx.Err())
	require.Equal(t, objectio.TotalMem(), m2.actualTotalMemory.Load())
	m2.refreshMemory(func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: 700}, nil }, func() (uint64, error) { return 1000, nil })
	receivePressureEvent(t, ctx.Done())
	c.mu.Lock()
	state = c.state
	c.mu.Unlock()
	require.Equal(t, rssPressureNone, state)
}

func TestRSSRefreshAcceptsNoCgroupLimit(t *testing.T) {
	total := objectio.TotalMem()
	require.Greater(t, total, uint64(100))
	for _, noLimit := range []error{memlimit.ErrNoLimit, memlimit.ErrNoCgroup, memlimit.ErrCgroupsNotSupported} {
		t.Run(noLimit.Error(), func(t *testing.T) {
			targets := make(chan int64, 1)
			c := rssPressureController{
				setTarget:   func(target int64, _ time.Time) { targets <- target },
				clearTarget: func() {},
				evict:       func(context.Context, int64) {},
				free:        func() {},
			}
			source := c.attach()
			defer source.Close()
			m := &memThrottler{rssPressure: source}
			m.reserved.Store(int64(total))
			m.refreshMemory(func() (*process.MemoryInfoStat, error) {
				return &process.MemoryInfoStat{RSS: total - total/20}, nil
			}, func() (uint64, error) { return 0, fmt.Errorf("wrapped: %w", noLimit) })
			require.Equal(t, rssCacheHardTarget, receivePressureEvent(t, targets))
		})
	}
}

func TestRSSRefreshUsesResolvedUnlimitedHostCeiling(t *testing.T) {
	total := objectio.TotalMem()
	require.Greater(t, total, uint64(2000))
	c := rssPressureController{
		setTarget:   func(int64, time.Time) { t.Error("stale local limit triggered pressure") },
		clearTarget: func() {},
		evict:       func(context.Context, int64) {},
		free:        func() {},
	}
	source := c.attach()
	defer source.Close()
	m := &memThrottler{rssPressure: source}
	m.refreshMemoryWithPressure(
		func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: 950}, nil },
		func() (uint64, error) { return 1000, nil },
		func(uint64, error) (uint64, error) { return 0, memlimit.ErrNoLimit },
	)
	require.Equal(t, uint64(1000), m.actualTotalMemory.Load())
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	require.Equal(t, rssPressureNone, state)
}

func receivePressureEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out awaiting pressure action")
		var zero T
		return zero
	}
}
