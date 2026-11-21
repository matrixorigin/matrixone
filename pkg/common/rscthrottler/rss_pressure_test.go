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
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestRSSPressureLastSourceCloseJoinsRecovery(t *testing.T) {
	started := make(chan context.Context, 1)
	cancelled := make(chan struct{})
	signalCancelled := sync.OnceFunc(func() { close(cancelled) })
	release := make(chan struct{})
	c := rssPressureController{
		setTarget:   func(int64, time.Time) {},
		clearTarget: func() {},
		evict: func(ctx context.Context, _ int64) {
			started <- ctx
			<-ctx.Done()
			signalCancelled()
			<-release
		},
		free: func() {},
	}
	cn, workspace := c.attach(), c.attach()
	defer workspace.Close()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	cn.observe(time.Now(), 860, 1000, 800, 0)
	ctx := receivePressureEvent(t, started)
	cn.Close()
	require.NoError(t, ctx.Err(), "another source still owns process recovery")

	closed := make(chan struct{})
	go func() {
		workspace.Close()
		close(closed)
	}()
	receivePressureEvent(t, cancelled)
	require.Empty(t, closed)
	unblock()
	receivePressureEvent(t, closed)

	next := c.attach()
	defer next.Close()
	cn.observe(time.Now(), 950, 1000, 800, 0)
	require.Empty(t, started, "a retired source cannot publish into a restarted controller")
	next.observe(time.Now(), 860, 1000, 800, 0)
	receivePressureEvent(t, started)
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
