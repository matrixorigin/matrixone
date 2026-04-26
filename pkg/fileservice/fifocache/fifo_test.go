// Copyright 2024 Matrix Origin
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

package fifocache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/assert"
)

func TestCacheSetGet(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil)

	cache.Set(ctx, 1, 1, 1)
	n, ok := cache.Get(ctx, 1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	cache.Set(ctx, 1, 1, 1)
	n, ok = cache.Get(ctx, 1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	_, ok = cache.Get(ctx, 2)
	assert.False(t, ok)
}

func TestCacheEvict(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil)
	for i := 0; i < 64; i++ {
		cache.Set(ctx, i, i, 1)
		if cache.used1+cache.used2 > cache.capacity() {
			t.Fatalf("capacity %v, used1 %v used2 %v", cache.capacity(), cache.used1, cache.used2)
		}
	}
}

func TestCacheEvict2(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(2), ShardInt[int], nil, nil, nil)
	cache.Set(ctx, 1, 1, 1)
	cache.Set(ctx, 2, 2, 1)

	// 1 will be evicted
	cache.Set(ctx, 3, 3, 1)
	v, ok := cache.Get(ctx, 2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(ctx, 3)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	// get 2, set 4, 3 will be evicted first
	cache.Get(ctx, 2)
	cache.Get(ctx, 2)
	cache.Set(ctx, 4, 4, 1)
	v, ok = cache.Get(ctx, 2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(ctx, 4)
	assert.True(t, ok)
	assert.Equal(t, 4, v)
	assert.Equal(t, int64(1), cache.used1)
	assert.Equal(t, int64(1), cache.used2)
}

func TestCacheEvict3(t *testing.T) {
	ctx := context.Background()
	var nEvict, nGet, nSet int
	cache := New(
		fscache.ConstCapacity(1024),
		ShardInt[int],
		func(_ context.Context, _ int, _ bool, _ int64) {
			nSet++
		},
		func(_ context.Context, _ int, _ bool, _ int64) {
			nGet++
		},
		func(_ context.Context, _ int, _ bool, _ int64) {
			nEvict++
		},
	)
	for i := 0; i < 1024; i++ {
		cache.Set(ctx, i, true, 1)
		cache.Get(ctx, i)
		cache.Get(ctx, i)
		assert.True(t, cache.used1+cache.used2 <= 1024)
	}
	assert.Equal(t, 0, nEvict)
	assert.Equal(t, 1024, nSet)
	assert.Equal(t, 2048, nGet)

	for i := 0; i < 1024; i++ {
		cache.Set(ctx, 10000+i, true, 1)
		assert.True(t, cache.used1+cache.used2 <= 1024)
	}
	assert.Equal(t, int64(102), cache.used1)
	assert.Equal(t, int64(922), cache.used2)
	assert.Equal(t, 1024, nEvict)
	assert.Equal(t, 2048, nSet)
	assert.Equal(t, 2048, nGet)
}

func TestDoubleFree(t *testing.T) {
	evicts := make(map[int]int)
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt,
		nil, nil,
		func(ctx context.Context, key int, value int, size int64) {
			evicts[key]++
		},
	)
	// set
	cache.Set(t.Context(), 1, 1, 1)
	// delete, item still in queue
	cache.Delete(t.Context(), 1)
	// set to evict 1
	cache.Set(t.Context(), 2, 2, 1)
	// check
	assert.Equal(t, 1, evicts[1])
}

func TestGhostQueue(t *testing.T) {
	numSet := make(map[int]int)
	numEvict := make(map[int]int)
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt,
		func(ctx context.Context, key int, value int, size int64) {
			numSet[key]++
		},
		nil,
		func(ctx context.Context, key int, value int, size int64) {
			numEvict[key]++
		},
	)
	cache.Set(t.Context(), 1, 1, 1)
	cache.Set(t.Context(), 2, 2, 1)
	assert.Equal(t, 1, numEvict[1])
	// 1 is in the ghost queue now
	_, ok := cache.Get(t.Context(), 1)
	if ok {
		t.Fatal()
	}
	cache.Set(t.Context(), 1, 1, 1)
	assert.Equal(t, 2, numSet[1])
	// 2 is in the ghost queue now
	cache.Set(t.Context(), 3, 3, 1)
	// 2 was evicted from ghost queue
}

// TestPostEvictRunsOutsideQueueLock verifies that postEvict callbacks execute
// outside the queueLock by attempting a concurrent Set() while a postEvict is
// blocked. If postEvict ran under the lock, the concurrent Set would deadlock.
func TestPostEvictRunsOutsideQueueLock(t *testing.T) {
	evictStarted := make(chan struct{})
	evictContinue := make(chan struct{})
	var evictCount atomic.Int32
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt[int],
		nil, nil,
		func(_ context.Context, _ int, _ int, _ int64) {
			// Only the first eviction blocks; subsequent ones skip immediately.
			// Cannot use sync.Once because it blocks callers until f() returns.
			if evictCount.Add(1) == 1 {
				close(evictStarted)
				<-evictContinue
			}
		},
	)
	ctx := t.Context()
	cache.Set(ctx, 1, 1, 1)
	// Trigger eviction: inserting key=2 exceeds capacity, evicts key=1.
	// postEvict for key=1 will block in the callback above.
	go cache.Set(ctx, 2, 2, 1)
	<-evictStarted
	// postEvict is running outside queueLock. If the queueLock were still
	// held, this concurrent Set would deadlock trying to enqueue.
	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 3, 3, 1)
		close(done)
	}()
	select {
	case <-done:
		// success — concurrent Set() completed, queueLock was not held
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: concurrent Set blocked, postEvict likely holds queueLock")
	}
	close(evictContinue)
}

// TestSetLatencyUnderSlowPostEvict checks that the queueLock is released before
// postEvict runs, so concurrent callers don't inherit slow callback latency.
func TestSetLatencyUnderSlowPostEvict(t *testing.T) {
	slowDuration := 200 * time.Millisecond
	evictStarted := make(chan struct{})
	var evictStartedOnce sync.Once
	cache := New(
		fscache.ConstCapacity(2),
		ShardInt[int],
		nil, nil,
		func(_ context.Context, _ int, _ int, _ int64) {
			evictStartedOnce.Do(func() { close(evictStarted) })
			time.Sleep(slowDuration)
		},
	)
	ctx := t.Context()
	cache.Set(ctx, 1, 1, 1)
	cache.Set(ctx, 2, 2, 1)
	// Trigger slow eviction in background
	go cache.Set(ctx, 3, 3, 1) // evicts key=1, postEvict sleeps 200ms
	select {
	case <-evictStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for postEvict to start")
	}
	// queueLock should be released now; used() only needs RLock.
	start := time.Now()
	_ = cache.used()
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("used() took %v during slow postEvict; queueLock appears blocked", elapsed)
	}
}

// TestPostEvictPanicDoesNotBlockDone verifies that if a postEvict callback
// panics, the done channel still receives a value (via nested defer) and
// doesn't deadlock. The panic itself propagates to the caller.
func TestPostEvictPanicDoesNotBlockDone(t *testing.T) {
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt[int],
		nil, nil,
		func(_ context.Context, _ int, _ int, _ int64) {
			panic("boom")
		},
	)
	ctx := t.Context()
	cache.Set(ctx, 1, 1, 1)
	done := make(chan int64, 1)
	// Synchronous Evict with capacityCut=1 forces eviction + postEvict panic.
	// The panic propagates but the nested defer guarantees done is signaled.
	assert.Panics(t, func() {
		cache.Evict(ctx, done, 1)
	})
	select {
	case <-done:
		// success — done received despite panic
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: done channel not signaled after postEvict panic")
	}
}
