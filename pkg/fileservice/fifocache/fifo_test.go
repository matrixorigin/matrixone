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

func TestCacheIndexGrowsWithCollidingKeys(t *testing.T) {
	const count = 1025 // Cross the old per-shard reservation with real entries.
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(count*2), func(int) uint64 { return 0 }, nil, nil, nil)
	for key := range count {
		inserted, rejected := cache.Set(ctx, key, key+1, 1)
		assert.True(t, inserted)
		assert.False(t, rejected)
	}
	for key := range count {
		value, ok := cache.Get(ctx, key)
		assert.True(t, ok)
		assert.Equal(t, key+1, value)
	}
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
		func(_ context.Context, _ int, _ bool, _ int64, _ uint64) {
			nSet++
		},
		func(_ context.Context, _ int, _ bool, _ int64) {
			nGet++
		},
		func(_ context.Context, _ int, _ bool, _ int64, _ uint64) {
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
	cache := New[int, int](
		fscache.ConstCapacity(1),
		ShardInt,
		nil, nil,
		func(ctx context.Context, key int, value int, size int64, _ uint64) {
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

func TestPrepareEvictLifecycle(t *testing.T) {
	var events []string
	cache := NewWithPrepareEvict[int, int](
		fscache.ConstCapacity(1),
		ShardInt[int],
		nil,
		nil,
		func(int, int, int64, uint64) func() {
			events = append(events, "prepare")
			return func() { events = append(events, "finish") }
		},
		func(context.Context, int, int, int64, uint64) {
			events = append(events, "post")
		},
	)
	cache.Set(t.Context(), 1, 1, 1)
	cache.Delete(t.Context(), 1)
	assert.Equal(t, []string{"prepare", "post", "finish"}, events)

	events = events[:0]
	cache.Set(t.Context(), 2, 2, 1)
	cache.Set(t.Context(), 3, 3, 1)
	assert.Equal(t, []string{"prepare", "post", "finish"}, events)

	events = events[:0]
	assert.True(t, cache.Replace(t.Context(), 3, 30, 1))
	assert.Equal(t, []string{"post"}, events)
}

func TestPrepareEvictFinishRunsAfterPostEvictPanic(t *testing.T) {
	var finished atomic.Bool
	cache := NewWithPrepareEvict[int, int](
		fscache.ConstCapacity(1),
		ShardInt[int],
		nil,
		nil,
		func(int, int, int64, uint64) func() {
			return func() { finished.Store(true) }
		},
		func(context.Context, int, int, int64, uint64) {
			panic("post-evict failure")
		},
	)
	cache.Set(t.Context(), 1, 1, 1)
	func() {
		defer func() {
			assert.Equal(t, "post-evict failure", recover())
		}()
		cache.Delete(t.Context(), 1)
	}()
	assert.True(t, finished.Load())
}

func TestPrepareEvictFinishesBatchAfterPostEvictPanic(t *testing.T) {
	var finished atomic.Int64
	var postCalls atomic.Int64
	cache := NewWithPrepareEvict[int, int](
		fscache.ConstCapacity(3),
		ShardInt[int],
		nil,
		nil,
		func(int, int, int64, uint64) func() {
			return func() { finished.Add(1) }
		},
		func(context.Context, int, int, int64, uint64) {
			postCalls.Add(1)
			panic("post-evict failure")
		},
	)
	cache.Set(t.Context(), 1, 1, 1)
	cache.Set(t.Context(), 2, 2, 1)
	cache.Set(t.Context(), 3, 3, 1)
	func() {
		defer func() {
			assert.Equal(t, "post-evict failure", recover())
		}()
		cache.ForceEvictWithWait(t.Context(), cache.Used())
	}()
	assert.Equal(t, int64(1), postCalls.Load())
	assert.Equal(t, int64(3), finished.Load())
}

func TestGhostQueue(t *testing.T) {
	numSet := make(map[int]int)
	numEvict := make(map[int]int)
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt,
		func(ctx context.Context, key int, value int, size int64, _ uint64) {
			numSet[key]++
		},
		nil,
		func(ctx context.Context, key int, value int, size int64, _ uint64) {
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

func TestEvictWithWaitReturnsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := New[int, int](fscache.ConstCapacity(0), ShardInt[int], nil, nil, nil)
	cache.used1 = 1

	done := make(chan struct{})
	go func() {
		cache.EvictWithWait(ctx, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("EvictWithWait did not return after context cancellation")
	}
}

func TestCachePressureAdmissionIsAtomic(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(100),
		ShardInt[int],
		nil, nil, nil,
	)
	cache.SetAdmissionTarget(func(capacity int64) (int64, bool) {
		return 50, true
	})

	ctx := t.Context()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cache.Set(ctx, i, i, 1)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(50), cache.used())

	live := 0
	for i := 0; i < 100; i++ {
		_, ok := cache.Get(ctx, i)
		if ok {
			live++
		}
	}
	assert.Equal(t, 50, live)
}

func TestCachePressureAdmissionAdmitsGhostByEvictingCold(t *testing.T) {
	ctx := t.Context()
	cache := New[int, int](
		fscache.ConstCapacity(3),
		ShardInt[int],
		nil, nil, nil,
	)

	cache.Set(ctx, 1, 1, 1)
	cache.Set(ctx, 2, 2, 1)
	cache.Set(ctx, 3, 3, 1)

	assert.Equal(t, int64(2), cache.EvictToTargetWithWait(ctx, 2))
	assert.False(t, cache.Contains(1))
	assert.True(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))

	cache.SetAdmissionTarget(func(capacity int64) (int64, bool) {
		return 2, true
	})

	cache.Set(ctx, 4, 4, 1)
	assert.False(t, cache.Contains(4))
	assert.Equal(t, int64(2), cache.used())

	cache.Set(ctx, 1, 1, 1)
	assert.True(t, cache.Contains(1))
	assert.False(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))
	assert.Equal(t, int64(2), cache.used())
}

func seedDeletedQueue2(t *testing.T, cache *Cache[int, int], ctx context.Context, hot int, followers ...int) {
	t.Helper()
	keys := append([]int{hot}, followers...)
	cache.queueLock.Lock()
	for index, key := range keys {
		item := &_CacheItem[int, int]{
			key:     key,
			value:   key,
			valueOK: true,
			size:    1,
			queue:   cacheItemQueue2,
			seq:     cache.nextSeq.Add(1),
		}
		if index == 0 {
			// The deleted head is hot. evict2 must first demote its hit count,
			// then continue past deleted followers whose used2 decrement is the
			// only progress signal.
			item.count.Store(1)
		}
		shard := &cache.shards[cache.keyShardFunc(key)%numShards]
		shard.Lock()
		shard.values[key] = item
		shard.Unlock()
		cache.queue2.enqueue(item)
		cache.used2 += item.size
	}
	cache.queueLock.Unlock()

	for _, key := range keys {
		cache.Delete(ctx, key)
	}
}

func seedGhost(t *testing.T, cache *Cache[int, int], key int) {
	t.Helper()
	const size = int64(1)
	item := &_CacheItem[int, int]{
		key:   key,
		size:  size,
		queue: cacheItemGhost,
		seq:   cache.nextSeq.Add(1),
	}
	cache.queueLock.Lock()
	shard := &cache.shards[cache.keyShardFunc(key)%numShards]
	shard.Lock()
	shard.values[key] = item
	shard.Unlock()
	cache.ghost.enqueue(item)
	cache.ghostSize += size
	cache.queueLock.Unlock()
}

func TestEvictContinuesAfterDeletedQueue2Record(t *testing.T) {
	for _, test := range []struct {
		name   string
		target int64
		want   int64
	}{
		{name: "target-zero", target: 0, want: 0},
		{name: "target-nonzero", target: 1, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			cache := New[int, int](fscache.ConstCapacity(10), ShardInt[int], nil, nil, nil)
			seedDeletedQueue2(t, cache, ctx, 1, 2, 3)

			got := cache.EvictToTargetWithWait(ctx, test.target)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.want, cache.used())
			assert.Equal(t, test.want, cache.used2)
			assert.Zero(t, cache.used1)
		})
	}
}

func TestAdmissionContinuesAfterDeletedQueue2Record(t *testing.T) {
	ctx := t.Context()
	cache := New[int, int](fscache.ConstCapacity(10), ShardInt[int], nil, nil, nil)
	seedDeletedQueue2(t, cache, ctx, 1, 2, 3)
	seedGhost(t, cache, 10)
	cache.SetAdmissionTarget(func(int64) (int64, bool) { return 1, true })

	inserted, rejected := cache.Set(ctx, 10, 10, 1)
	assert.True(t, inserted)
	assert.False(t, rejected)
	assert.True(t, cache.Contains(10))
	assert.Equal(t, int64(1), cache.used())
	assert.Equal(t, int64(1), cache.used2)
	assert.Zero(t, cache.used1)
	for _, key := range []int{1, 2, 3} {
		assert.False(t, cache.Contains(key))
	}
}

func TestEvictContinuesAfterDeletedHotQueue2RecordAPI(t *testing.T) {
	for _, test := range []struct {
		name   string
		target int64
	}{
		{name: "target-zero", target: 0},
		{name: "target-nonzero", target: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			cache := New[int, int](fscache.ConstCapacity(6), ShardInt[int], nil, nil, nil)
			for key := 1; key <= 6; key++ {
				inserted, rejected := cache.Set(ctx, key, key, 1)
				assert.True(t, inserted)
				assert.False(t, rejected)
			}

			// Each target reduction promotes one hot queue-1 item into queue 2
			// and evicts one cold follower, leaving three hot queue-2 entries.
			for _, key := range []int{1, 3, 5} {
				for range 2 {
					_, ok := cache.Get(ctx, key)
					assert.True(t, ok)
				}
				want := int64(6 - (key+1)/2)
				assert.Equal(t, want, cache.EvictToTargetWithWait(ctx, want))
			}

			// Delete the hot queue-2 head. Its stale queue record must not
			// short-circuit eviction of the live followers.
			cache.Delete(ctx, 1)
			assert.Equal(t, test.target, cache.EvictToTargetWithWait(ctx, test.target))
			assert.Equal(t, test.target, cache.Used())
			if test.target == 0 {
				assert.False(t, cache.Contains(3))
				assert.False(t, cache.Contains(5))
			}
		})
	}
}

type accountingGuardProbe struct {
	sync.Mutex
	callbackSawUnlocked atomic.Bool
}

func TestPressureAccountingCommitPanicCleansPendingEviction(t *testing.T) {
	ctx := t.Context()
	guard := new(accountingGuardProbe)
	var setResults []bool
	var postEvictCalls atomic.Int64
	var finishEvictCalls atomic.Int64

	cache := newCache[int, int](
		fscache.ConstCapacity(2),
		ShardInt[int],
		func(context.Context, int, int, int64, uint64) func(bool) {
			return func(inserted bool) { setResults = append(setResults, inserted) }
		},
		nil,
		nil,
		func(int, int, int64, uint64) func() {
			return func() { finishEvictCalls.Add(1) }
		},
		func(context.Context, int, int, int64, uint64) {
			postEvictCalls.Add(1)
			if guard.TryLock() {
				guard.callbackSawUnlocked.Store(true)
				guard.Unlock()
			}
		},
	)
	cache.setAccountingGuard(guard, func(value int) {
		if value == 3 {
			panic("accounting commit")
		}
	})

	inserted, rejected := cache.Set(ctx, 1, 1, 1)
	assert.True(t, inserted)
	assert.False(t, rejected)
	inserted, rejected = cache.Set(ctx, 2, 2, 1)
	assert.True(t, inserted)
	assert.False(t, rejected)
	assert.Equal(t, int64(2), cache.Used())

	// Move key 1 to the ghost queue so the pressure admission path can evict
	// key 2 and produce pending post-evict cleanup before commit panics.
	assert.Equal(t, int64(1), cache.EvictToTargetWithWait(ctx, 1))
	cache.SetAdmissionTarget(func(int64) (int64, bool) { return 1, true })
	postEvictCalls.Store(0)
	finishEvictCalls.Store(0)
	guard.callbackSawUnlocked.Store(false)

	assert.PanicsWithValue(t, "accounting commit", func() {
		cache.Set(ctx, 1, 3, 1)
	})

	assert.True(t, cache.Contains(1))
	assert.Equal(t, int64(1), cache.Used())
	assert.Equal(t, int64(1), postEvictCalls.Load())
	assert.Equal(t, int64(1), finishEvictCalls.Load())
	assert.True(t, guard.callbackSawUnlocked.Load())
	assert.True(t, guard.TryLock())
	guard.Unlock()
	assert.Len(t, setResults, 3)
	assert.True(t, setResults[2], "panic after admission must finish prepareSet as inserted")
}

func TestAccountingReservationIsHeldThroughPreEnqueuePostSet(t *testing.T) {
	ctx := t.Context()
	guard := new(sync.Mutex)
	var reserved atomic.Int64
	var committed atomic.Bool
	var callbackUsed atomic.Int64
	var callbackReserved atomic.Int64
	var callbackAdmissionBlocked atomic.Bool
	var commitOutsideGuard atomic.Bool
	var commitUsed atomic.Int64

	var cache *Cache[int, int]
	cache = NewWithPrepareSet[int, int](
		fscache.ConstCapacity(1),
		ShardInt[int],
		nil,
		func(context.Context, int, int, int64, uint64) {
			// Regular enqueue has not charged FIFO usage yet, but the external
			// reservation is still visible to an allocator admission probe.
			callbackUsed.Store(cache.Used())
			callbackReserved.Store(reserved.Load())
			callbackAdmissionBlocked.Store(cache.Used()+reserved.Load()+1 > cache.Capacity())
		},
		nil,
		nil,
	)
	cache.setAccountingGuard(guard, func(int) {
		if guard.TryLock() {
			commitOutsideGuard.Store(true)
			guard.Unlock()
		}
		commitUsed.Store(cache.Used())
		reserved.Add(-1)
		committed.Store(true)
	})
	reserved.Store(1)

	inserted, rejected := cache.Set(ctx, 1, 1, 1)
	assert.True(t, inserted)
	assert.False(t, rejected)
	assert.Zero(t, callbackUsed.Load())
	assert.Equal(t, int64(1), callbackReserved.Load())
	assert.True(t, callbackAdmissionBlocked.Load())
	assert.Equal(t, int64(1), cache.Used())
	assert.Equal(t, int64(1), commitUsed.Load())
	assert.Zero(t, reserved.Load())
	assert.True(t, committed.Load())
	assert.False(t, commitOutsideGuard.Load())
}

func TestAccountingCommitChargesPendingEnqueueBeforeSetReturn(t *testing.T) {
	ctx := t.Context()
	guard := new(sync.Mutex)
	var commits atomic.Int64
	cache := NewWithPrepareSet[int, int](
		fscache.ConstCapacity(4),
		ShardInt[int],
		nil,
		func(context.Context, int, int, int64, uint64) {},
		nil,
		nil,
	)
	var commitOutsideGuard atomic.Bool
	var commitPendingBytes atomic.Int64
	cache.setAccountingGuard(guard, func(int) {
		if guard.TryLock() {
			commitOutsideGuard.Store(true)
			guard.Unlock()
		}
		commitPendingBytes.Store(cache.pendingBytes.Load())
		commits.Add(1)
	})

	cache.queueLock.Lock()
	type setResult struct {
		inserted bool
		rejected bool
	}
	result := make(chan setResult, 1)
	done := make(chan struct{})
	go func() {
		inserted, rejected := cache.Set(ctx, 1, 1, 4)
		result <- setResult{inserted: inserted, rejected: rejected}
		close(done)
	}()
	select {
	case <-done:
		got := <-result
		assert.True(t, got.inserted)
		assert.False(t, got.rejected)
	case <-time.After(2 * time.Second):
		cache.queueLock.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("timed out waiting for pending enqueue Set")
	}
	// queueLock is intentionally still held. Probe the atomic pending charge,
	// not Used(), which would try to acquire the held queue RLock.
	assert.Equal(t, int64(4), cache.pendingBytes.Load())
	assert.Equal(t, int64(1), commits.Load())
	assert.Equal(t, int64(4), commitPendingBytes.Load())
	assert.False(t, commitOutsideGuard.Load())
	cache.queueLock.Unlock()

	cache.Evict(ctx, nil, 0)
	assert.Zero(t, cache.pendingBytes.Load())
	assert.Equal(t, int64(4), cache.Used())
}

func TestReplaceUpdatesUsedBytes(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(20),
		ShardInt[int],
		nil, nil, nil,
	)
	ctx := t.Context()
	cache.Set(ctx, 1, 1, 4)
	assert.Equal(t, int64(4), cache.used())

	assert.True(t, cache.Replace(ctx, 1, 10, 8))
	assert.Equal(t, int64(8), cache.used())

	cache.Set(ctx, 2, 2, 12)
	assert.Equal(t, int64(20), cache.used())
	assert.True(t, cache.Contains(1))
	assert.True(t, cache.Contains(2))
}

func TestReplaceAccountsPendingEnqueueJob(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(20),
		ShardInt[int],
		nil, nil, nil,
	)
	ctx := t.Context()

	cache.queueLock.Lock()
	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 1, 1, 4)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pending enqueue job")
	}
	cache.queueLock.Unlock()

	assert.True(t, cache.Replace(ctx, 1, 10, 8))
	assert.Equal(t, int64(8), cache.used())
}

func TestPendingEnqueueCountsTowardsUsed(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(20),
		ShardInt[int],
		nil, nil, nil,
	)
	ctx := t.Context()

	cache.queueLock.Lock()
	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 1, 1, 4)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pending enqueue job")
	}
	cache.queueLock.Unlock()

	// Set has accepted the item, so its already-allocated backing must be
	// visible to capacity users before another goroutine can reserve space.
	assert.Equal(t, int64(4), cache.used())
	assert.Equal(t, int64(4), cache.pendingBytes.Load())

	cache.Evict(ctx, nil, 0)
	assert.Equal(t, int64(4), cache.used())
	assert.Zero(t, cache.pendingBytes.Load())
}

func TestEvictAccountsPendingEnqueueJob(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(4),
		ShardInt[int],
		nil, nil, nil,
	)
	ctx := t.Context()

	cache.queueLock.Lock()
	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 1, 1, 4)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pending enqueue job")
	}
	cache.queueLock.Unlock()

	cache.Evict(ctx, nil, 0)
	assert.Equal(t, int64(4), cache.used())
}

func TestEvictSkipsDeletedPendingEnqueueJob(t *testing.T) {
	cache := New[int, int](
		fscache.ConstCapacity(4),
		ShardInt[int],
		nil, nil, nil,
	)
	ctx := t.Context()

	cache.queueLock.Lock()
	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 1, 1, 4)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pending enqueue job")
	}
	cache.Delete(ctx, 1)
	cache.queueLock.Unlock()

	cache.Evict(ctx, nil, 0)
	assert.Equal(t, int64(0), cache.used())
	assert.False(t, cache.Contains(1))
}

func TestDirectEnqueueSkipsDeletedItem(t *testing.T) {
	postSetStarted := make(chan struct{})
	unblockPostSet := make(chan struct{})
	cache := New[int, int](
		fscache.ConstCapacity(4),
		ShardInt[int],
		func(_ context.Context, _ int, _ int, _ int64, _ uint64) {
			close(postSetStarted)
			<-unblockPostSet
		},
		nil,
		nil,
	)
	ctx := t.Context()

	done := make(chan struct{})
	go func() {
		cache.Set(ctx, 1, 1, 4)
		close(done)
	}()
	<-postSetStarted
	cache.Delete(ctx, 1)
	close(unblockPostSet)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Set")
	}

	assert.Equal(t, int64(0), cache.used())
	assert.False(t, cache.Contains(1))
}

func TestEvictToTargetWithWaitReturnsActualUsedOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	itemSize := int64(pressureEvictBatchBytes + 1)
	var evicted atomic.Int32
	cache := New(
		fscache.ConstCapacity(3*itemSize),
		ShardInt[int],
		nil,
		nil,
		func(context.Context, int, int, int64, uint64) {
			if evicted.Add(1) == 1 {
				cancel()
			}
		},
	)
	cache.Set(context.Background(), 1, 1, itemSize)
	cache.Set(context.Background(), 2, 2, itemSize)
	cache.Set(context.Background(), 3, 3, itemSize)

	used := cache.EvictToTargetWithWait(ctx, 0)

	assert.Equal(t, int64(2*itemSize), used)
	assert.Equal(t, used, cache.used())
	assert.Equal(t, int32(1), evicted.Load())
}

func TestEvictToTargetWithWaitEvictsHotItemsAfterPromotion(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(3), ShardInt[int], nil, nil, nil)

	cache.Set(ctx, 1, 1, 1)
	cache.Set(ctx, 2, 2, 1)
	cache.Set(ctx, 3, 3, 1)

	_, ok := cache.Get(ctx, 1)
	assert.True(t, ok)
	_, ok = cache.Get(ctx, 1)
	assert.True(t, ok)
	_, ok = cache.Get(ctx, 2)
	assert.True(t, ok)
	_, ok = cache.Get(ctx, 2)
	assert.True(t, ok)
	_, ok = cache.Get(ctx, 3)
	assert.True(t, ok)
	_, ok = cache.Get(ctx, 3)
	assert.True(t, ok)

	used := cache.EvictToTargetWithWait(ctx, 0)

	assert.Equal(t, int64(0), used)
	assert.Equal(t, int64(0), cache.used())
	assert.False(t, cache.Contains(1))
	assert.False(t, cache.Contains(2))
	assert.False(t, cache.Contains(3))
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
		func(_ context.Context, _ int, _ int, _ int64, _ uint64) {
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
	evictStarted := make(chan struct{}, 1)
	cache := New(
		fscache.ConstCapacity(2),
		ShardInt[int],
		nil, nil,
		func(_ context.Context, _ int, _ int, _ int64, _ uint64) {
			select {
			case evictStarted <- struct{}{}:
			default:
			}
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
		func(_ context.Context, _ int, _ int, _ int64, _ uint64) {
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
