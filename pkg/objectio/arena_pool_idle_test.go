// Copyright 2026 Matrix Origin
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

package objectio

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestArenaPoolReapsOldestWhileKeepingWarmArena(t *testing.T) {
	budget := &arenaByteBudget{limit: 4096}
	p := &arenaFreeList{maxCount: 4, maxBytes: 4096, budget: budget}
	t.Cleanup(p.drain)
	oldest := NewArena(1024)
	warm := NewArena(1024)
	p.put(oldest)
	p.put(warm)

	// Model elapsed idle time without depending on scheduler timing.
	p.mu.Lock()
	p.parked[0].parkedAt = time.Now().Add(-2 * arenaIdleTTL)
	generation := p.reaperGeneration
	stopped := p.reaper.Stop()
	p.mu.Unlock()
	require.True(t, stopped)
	p.reap(generation)

	require.Zero(t, oldest.Allocated())
	require.Equal(t, int32(1), p.count.Load())
	require.Equal(t, int64(1024), p.bytes.Load())
	require.Equal(t, int64(1024), budget.used.Load())
	got := p.get()
	require.Same(t, warm, got)
	got.FreeBuffers()
	require.Zero(t, budget.used.Load())
}

func TestArenaPoolReapsAutomaticallyAfterIdle(t *testing.T) {
	budget := &arenaByteBudget{limit: 4096}
	p := &arenaFreeList{maxCount: 4, maxBytes: 4096, budget: budget, idleTTL: 100 * time.Millisecond}
	t.Cleanup(p.drain)
	a := NewArena(1024)
	p.put(a)
	require.Eventually(t, func() bool { return p.count.Load() == 0 && budget.used.Load() == 0 },
		3*time.Second, 10*time.Millisecond)
	require.Zero(t, p.bytes.Load())
	require.Zero(t, a.Allocated())
}

func TestArenaPoolStaleReaperCannotDrainNewGeneration(t *testing.T) {
	budget := &arenaByteBudget{limit: 4096}
	p := &arenaFreeList{maxCount: 4, maxBytes: 4096, budget: budget}
	t.Cleanup(p.drain)
	old := NewArena(1024)
	p.put(old)
	p.mu.Lock()
	stale := p.reaperGeneration
	p.mu.Unlock()
	p.drain()
	require.Zero(t, old.Allocated())

	current := NewArena(1024)
	p.put(current)
	p.reap(stale)
	require.Equal(t, int32(1), p.count.Load())
	require.Equal(t, int64(1024), budget.used.Load())
	got := p.get()
	require.Same(t, current, got)
	got.FreeBuffers()
	require.Zero(t, budget.used.Load())
}

func TestArenaPoolConcurrentReapReuseAndDrain(t *testing.T) {
	budget := &arenaByteBudget{limit: 4096}
	p := &arenaFreeList{maxCount: 8, maxBytes: 4096, budget: budget, idleTTL: time.Millisecond}
	t.Cleanup(p.drain)
	var wg sync.WaitGroup
	var invalid atomic.Bool
	start := make(chan struct{})
	checkedOut := make(chan struct{}, 4)
	release := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				a := p.get()
				if a == nil {
					a = NewArena(128)
				}
				if i == 0 {
					checkedOut <- struct{}{}
					<-release
				}
				if len(a.data) != 128 {
					invalid.Store(true)
					continue
				}
				a.Alloc(64)[0] = byte(i)
				a.Reset()
				p.put(a)
				if used := budget.used.Load(); used < 0 || used > budget.limit {
					invalid.Store(true)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 4; i++ {
			<-checkedOut
		}
		// A pressure drain cannot free any arena while it is checked out.
		p.drain()
		close(release)
		for i := 0; i < 1000; i++ {
			p.drain()
		}
	}()
	close(start)
	wg.Wait()
	p.drain()
	require.False(t, invalid.Load())
	require.Zero(t, p.count.Load())
	require.Zero(t, p.bytes.Load())
	require.Zero(t, budget.used.Load())
}
