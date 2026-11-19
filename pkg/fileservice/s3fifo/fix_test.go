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

package s3fifo

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Concurrent sets under an admission target never take the cache over it.
func TestAdmissionHoldsUnderConcurrency(t *testing.T) {
	var misuse atomic.Int32
	const target = 10 << 10
	c := newCache(1<<20, func(o *Options[int, *value]) {
		o.Admission = func(int64) (int64, bool) { return target, true }
	})
	var over atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 2000; i++ {
				c.Set(r.Intn(1<<20), newValue(i, &misuse), int64(64+r.Intn(512)))
				if c.Used() > target {
					over.Store(true)
				}
			}
		}(int64(g))
	}
	wg.Wait()
	require.False(t, over.Load())
	require.LessOrEqual(t, c.Used(), int64(target))
	require.Zero(t, misuse.Load())
}

// A negative capacity leaves nothing cached and does not spin.
func TestNegativeCapacity(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(-1)
	inserted, _ := c.Set(1, newValue(1, &misuse), 1)
	require.False(t, inserted)
	require.Zero(t, c.EvictTo(context.Background(), 0))
}

// EvictTo stops between batches once its context is done.
func TestEvictToStopsOnCancel(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(1 << 20)
	for i := 0; i < 1000; i++ {
		c.Set(i, newValue(i, &misuse), 10)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	used := c.EvictTo(ctx, 0)
	require.Equal(t, int64((1000-evictBatch)*10), used, "one batch, then stop")
	require.Equal(t, int64(0), c.EvictTo(context.Background(), 0))
}

type panickingValue struct{ retains int }

func (v *panickingValue) Retain() {
	v.retains++
	if v.retains > 1 {
		panic("retain after free")
	}
}
func (v *panickingValue) Release() {}

// When a Get's retain panics, the admission it took is released.
func TestGetReleasesAdmissionOnPanic(t *testing.T) {
	for _, try := range []bool{false, true} {
		t.Run(fmt.Sprintf("try=%t", try), func(t *testing.T) {
			c := New(Options[int, *panickingValue]{Capacity: func() int64 { return 100 }, Hash: hashInt})
			v := &panickingValue{}
			c.Set(1, v, 1)
			released := 0
			lookup := c.Get
			if try {
				lookup = c.TryGet
			}
			require.Panics(t, func() {
				lookup(1, func(int64) (func(), error) { panic("admission failed") })
			})
			require.Equal(t, 1, v.retains, "panicking admission must not retain the value")
			require.True(t, c.Contains(1), "admission panic must release the shard lock")

			admit := func(int64) (func(), error) { return func() { released++ }, nil }
			require.Panics(t, func() {
				lookup(1, admit)
			})
			require.Equal(t, 1, released)
			require.True(t, c.Contains(1), "retain panic must release the shard lock")
		})
	}
}

// A ghost entry counts its bytes until it leaves the queue, after a hit has forgotten its key
// too, and leaving forgets its key only when it is the key's latest entry: a key evicted, set
// again from the ghost queue and evicted again stays remembered after its older entry leaves.
func TestGhostEntriesCountUntilTheyLeave(t *testing.T) {
	var total atomic.Int64
	var g ghost
	g.init(&total)
	g.add(1, 10, 1)
	require.True(t, g.take(1))
	require.False(t, g.contains(1))
	require.Equal(t, int64(10), total.Load(), "the entry still counts")
	g.add(2, 10, 2)
	g.add(1, 10, 3)
	require.Equal(t, uint64(1), g.head.Load())
	g.drop()
	require.True(t, g.contains(1), "the newer entry of key 1 is still in the queue")
	require.True(t, g.contains(2))
	require.Equal(t, int64(20), total.Load())
	require.Equal(t, uint64(2), g.head.Load())
	g.drop()
	g.drop()
	require.False(t, g.contains(1))
	require.Zero(t, total.Load())
	require.Zero(t, g.len())
	require.Equal(t, uint64(noOrder), g.head.Load())
}

// Concurrent sets, replaces, reads and deletes keep Used within the ceiling, a sixteenth over
// the capacity, and the values in flight, and within the capacity once they have returned;
// every value given to the cache is released once it has left it.
func TestUsedStaysWithinCapacity(t *testing.T) {
	var misuse atomic.Int32
	const goroutines, maxSize = 8, 256
	for _, capacity := range []int64{1, 4 << 10, 64 << 10} {
		c := newCache(capacity)
		ceiling := capacity + capacity/16 + goroutines*min(capacity, maxSize)
		var over atomic.Int64
		stop := make(chan struct{})
		sampled := make(chan struct{})
		go func() {
			defer close(sampled)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if u := c.Used(); u > ceiling {
					over.Store(u)
				}
			}
		}()
		var wg sync.WaitGroup
		values := make([][]*value, goroutines)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(g)))
				for i := 0; i < 5000; i++ {
					k := r.Intn(512)
					switch r.Intn(8) {
					case 0:
						c.Delete(k)
					case 1, 2:
						if v, _, ok, _ := c.Get(k, nil); ok {
							v.Release()
						}
					case 3:
						v := newValue(k, &misuse)
						c.Replace(k, v, int64(1+r.Intn(int(min(capacity, maxSize)))))
						v.Release()
						values[g] = append(values[g], v)
					default:
						v := newValue(k, &misuse)
						c.Set(k, v, int64(1+r.Intn(int(min(capacity, maxSize)))))
						v.Release()
						values[g] = append(values[g], v)
					}
				}
			}(g)
		}
		wg.Wait()
		close(stop)
		<-sampled
		require.Zero(t, over.Load(), "capacity %d", capacity)
		require.LessOrEqual(t, c.Used(), capacity)
		c.EvictTo(context.Background(), 0)
		require.Zero(t, c.Used())
		for _, vs := range values {
			for _, v := range vs {
				require.Zero(t, v.refs.Load())
			}
		}
	}
	require.Zero(t, misuse.Load())
}

// An entry pushed again after leaving its queue is popped once, at its new place: its old slot
// stays dead.
func TestQueuePushAgainAfterForget(t *testing.T) {
	var total atomic.Int64
	var q fifo[int, *value]
	q.init(inSmall, &total)
	a, b := &entry[int, *value]{key: 1, size: 1}, &entry[int, *value]{key: 2, size: 2}
	q.push(a, 1)
	q.push(b, 2)
	q.forget(a)
	q.push(a, 3)
	require.Equal(t, int64(3), total.Load())
	require.Equal(t, uint64(2), q.head.Load())
	for _, want := range []*entry[int, *value]{b, a} {
		e, ok := q.pop()
		require.True(t, ok)
		require.Same(t, want, e)
	}
	_, ok := q.pop()
	require.False(t, ok)
	require.Zero(t, total.Load())
	require.Equal(t, uint64(noOrder), q.head.Load())
}

// A panic committing a charge leaves neither the bytes in Used nor the cache's reference.
func TestChargePanicRollsBack(t *testing.T) {
	var misuse atomic.Int32
	var mu sync.Mutex
	c := newCache(1000, func(o *Options[int, *value]) {
		o.ChargeGuard = &mu
		o.OnCharge = func(*value) { panic("commit") }
	})
	v := newValue(1, &misuse)
	require.PanicsWithValue(t, "commit", func() { c.Set(1, v, 10) })
	require.Zero(t, c.Used())
	require.False(t, c.Contains(1))
	require.Equal(t, int32(1), v.refs.Load(), "only the caller's reference is left")
	require.True(t, mu.TryLock(), "the guard is let go")
	mu.Unlock()
	require.Zero(t, misuse.Load())
}

// A value is not evicted before its insertion is notified, so that its removal is never
// notified first.
func TestEvictableAfterInsertNotice(t *testing.T) {
	var misuse atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var events []string
	var mu sync.Mutex
	record := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	c := newCache(1000, func(o *Options[int, *value]) {
		o.OnInsert = func(k int, _ *value, _ int64, _ uint64) {
			if k == 1 {
				close(entered)
				<-release
			}
			record("insert")
		}
		o.OnRemove = func(int, *value, int64, uint64, Reason) { record("remove") }
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Set(1, newValue(1, &misuse), 10)
	}()
	<-entered
	require.Equal(t, int64(10), c.EvictTo(context.Background(), 0), "the value is not evictable yet")
	require.True(t, c.Contains(1))
	close(release)
	<-done
	require.Zero(t, c.EvictTo(context.Background(), 0))
	require.Equal(t, []string{"insert", "remove"}, events)
	require.Zero(t, misuse.Load())
}

// Under admission, a key the ghost queue remembers is admitted into the main queue.
func TestGhostHitAdmittedToMain(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(100, func(o *Options[int, *value]) {
		o.Admission = func(int64) (int64, bool) { return 50, true }
	})
	c.Set(1, newValue(1, &misuse), 10)
	c.EvictTo(context.Background(), 0)
	for k := 2; k < 7; k++ {
		c.Set(k, newValue(k, &misuse), 10)
	}
	require.Equal(t, int64(50), c.Used())
	inserted, rejected := c.Set(7, newValue(7, &misuse), 10)
	require.False(t, inserted)
	require.True(t, rejected, "a key not remembered is rejected")
	inserted, _ = c.Set(1, newValue(1, &misuse), 10)
	require.True(t, inserted)
	require.Equal(t, int64(10), c.mainBytes.Load())
	require.LessOrEqual(t, c.Used(), int64(50))
	require.Zero(t, misuse.Load())
}

// Evicting below the capacity shrinks the small queue's share by the cut, so that values read
// once leave before values read often.
func TestEvictBelowCapacityTakesSmallFirst(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(100)
	c.Set(1, newValue(1, &misuse), 50)
	for range 2 {
		if v, _, ok, _ := c.Get(1, nil); ok {
			v.Release()
		}
	}
	c.Set(2, newValue(2, &misuse), 50)
	c.Set(3, newValue(3, &misuse), 5)
	require.Equal(t, int64(50), c.mainBytes.Load(), "key 1 moved to main")
	require.False(t, c.Contains(2))
	c.EvictTo(context.Background(), 50)
	require.True(t, c.Contains(1), "the value read often stays")
	require.False(t, c.Contains(3), "the value read once leaves")
	require.Zero(t, misuse.Load())
}

// A replacement larger than the capacity deletes its key.
func TestReplaceLargerThanCapacityDeletes(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(100)
	c.Set(1, newValue(1, &misuse), 10)
	v := newValue(1, &misuse)
	require.False(t, c.Replace(1, v, 101))
	require.False(t, c.Contains(1))
	require.Zero(t, c.Used())
	require.Equal(t, int32(1), v.refs.Load())
	require.Zero(t, misuse.Load())
}

// Values without bytes are not remembered in the ghost queue, which they would grow without
// bound.
func TestZeroSizeNotGhosted(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(100)
	for k := 0; k < 1000; k++ {
		c.Set(k, newValue(k, &misuse), 0)
	}
	c.EvictTo(context.Background(), 0)
	c.Set(1000, newValue(1000, &misuse), 1)
	c.EvictTo(context.Background(), 0)
	n := 0
	for i := range c.policy {
		n += c.policy[i].ghostLen()
	}
	require.Equal(t, 1, n)
	require.Zero(t, misuse.Load())
}

// A shard's head deleted after eviction chose that shard does not let eviction take the
// shard's next member ahead of an older member of another shard.
func TestEvictRechecksChosenHead(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(1 << 20)
	shard := func(k int) *policyShard[int, *value] { return c.policyOf(hashInt(k)) }
	a1, b2 := 0, 1
	for shard(b2) == shard(a1) {
		b2++
	}
	a3 := b2 + 1
	for shard(a3) != shard(a1) {
		a3++
	}
	for _, k := range []int{a1, b2, a3} {
		c.Set(k, newValue(k, &misuse), 10)
	}

	ps, head := c.oldest(true)
	require.Same(t, shard(a1), ps)
	c.Delete(a1)
	_, evicted := c.step(ps, true, head)
	require.False(t, evicted)
	require.True(t, c.Contains(a3))

	c.EvictTo(context.Background(), 10)
	require.False(t, c.Contains(b2), "the oldest member left first")
	require.True(t, c.Contains(a3))
	require.Zero(t, misuse.Load())
}
