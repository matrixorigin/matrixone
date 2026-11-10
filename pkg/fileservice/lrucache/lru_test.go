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

package lrucache

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf(format, args...))
}

func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

func newRecorded(capacity int64) (*Cache[string, int], *recorder) {
	r := &recorder{}
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(capacity),
		func(_ context.Context, key string, value int, size int64, _ uint64) {
			r.add("set %s=%d/%d", key, value, size)
		},
		func(_ context.Context, key string, value int, size int64) { r.add("get %s=%d/%d", key, value, size) },
		func(key string, _ int, _ int64, _ uint64) func() {
			r.add("prepare %s", key)
			return func() { r.add("finish %s", key) }
		},
		func(_ context.Context, key string, value int, size int64, _ uint64) {
			r.add("evict %s=%d/%d", key, value, size)
		},
	)
	return c, r
}

// keys returns the keys from the most to the least recently used.
func keys(c *Cache[string, int]) []string {
	var items []*item[string, int]
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for e := s.order.Front(); e != nil; e = e.Next() {
			items = append(items, e.Value.(*item[string, int]))
		}
		s.mu.Unlock()
	}
	slices.SortFunc(items, func(a, b *item[string, int]) int { return cmp.Compare(b.stamp, a.stamp) })
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.key)
	}
	return out
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	c, r := newRecorded(30)
	for i, k := range []string{"a", "b", "c"} {
		inserted, rejected := c.Set(ctx, k, i, 10)
		require.True(t, inserted)
		require.False(t, rejected)
	}
	inserted, _ := c.Set(ctx, "a", 9, 10)
	require.False(t, inserted, "a present key is not replaced by Set")
	v, ok := c.Get(ctx, "a")
	require.True(t, ok)
	require.Equal(t, 0, v)
	require.True(t, c.Contains("b"), "Contains counts no use")
	require.Equal(t, []string{"a", "c", "b"}, keys(c))
	r.take()

	c.Set(ctx, "d", 3, 10)
	require.Equal(t, []string{"d", "a", "c"}, keys(c))
	require.Equal(t, []string{"set d=3/10", "prepare b", "evict b=1/10", "finish b"}, r.take())
	require.Equal(t, int64(30), c.Used())
	require.Equal(t, int64(30), c.Capacity())

	_, ok = c.Get(ctx, "b")
	require.False(t, ok)
	require.Empty(t, r.take(), "a miss runs no callback")

	c.Set(ctx, "big", 4, 40)
	require.Equal(t, []string{"d", "a", "c"}, keys(c), "an item larger than the capacity is evicted first")
	require.Equal(t, []string{"set big=4/40", "prepare big", "evict big=4/40", "finish big"}, r.take())
	require.Equal(t, int64(30), c.Used())
}

func TestSetOldestAddsBehindEveryItem(t *testing.T) {
	ctx := context.Background()
	c, r := newRecorded(30)
	c.Set(ctx, "a", 0, 10)
	require.True(t, c.SetOldest(ctx, "b", 1, 10))
	require.True(t, c.SetOldest(ctx, "c", 2, 10))
	require.False(t, c.SetOldest(ctx, "a", 9, 10), "a present key is left in place")
	require.Equal(t, []string{"a", "b", "c"}, keys(c))
	r.take()

	require.True(t, c.SetOldest(ctx, "d", 3, 10))
	require.Equal(t, []string{"a", "b", "c"}, keys(c), "over capacity the item added oldest goes first")
	require.Equal(t, []string{"set d=3/10", "prepare d", "evict d=3/10", "finish d"}, r.take())
}

func TestEvictablePassesOverItems(t *testing.T) {
	ctx := context.Background()
	c, _ := newRecorded(20)
	held := map[string]bool{"a": true, "b": true}
	c.SetEvictable(func(k string) bool { return !held[k] })
	c.Set(ctx, "a", 0, 10)
	c.Set(ctx, "b", 1, 10)
	c.Set(ctx, "c", 2, 10)
	require.Equal(t, []string{"b", "a"}, keys(c), "the newest unheld item goes when the rest are held")
	c.Set(ctx, "d", 3, 10)
	require.Equal(t, []string{"b", "a"}, keys(c))

	held["b"] = false
	c.Set(ctx, "e", 4, 10)
	require.Equal(t, []string{"e", "a"}, keys(c))

	held["e"] = true
	c.Set(ctx, "f", 5, 30)
	require.Equal(t, []string{"e", "a"}, keys(c))
	held["x"] = true
	c.Set(ctx, "x", 6, 15)
	require.Equal(t, []string{"x", "e", "a"}, keys(c), "held items may keep the cache over capacity")
	require.Equal(t, int64(35), c.Used())
	held = map[string]bool{}
	c.Evict(ctx, nil, 0)
	require.Equal(t, []string{"x"}, keys(c), "released items go least recently used first")
}

func TestDeleteReplaceAndForcedEviction(t *testing.T) {
	ctx := context.Background()
	c, r := newRecorded(100)
	c.Set(ctx, "a", 0, 10)
	c.Set(ctx, "b", 1, 10)
	c.Set(ctx, "c", 2, 10)
	r.take()

	c.Delete(ctx, "b")
	c.Delete(ctx, "missing")
	require.Equal(t, []string{"prepare b", "evict b=1/10", "finish b"}, r.take())
	require.Equal(t, int64(20), c.Used())

	require.True(t, c.Replace(ctx, "a", 7, 50))
	require.False(t, c.Replace(ctx, "missing", 1, 1))
	require.Equal(t, []string{"evict a=0/10", "set a=7/50"}, r.take(), "Replace does not prepare an eviction")
	require.Equal(t, []string{"c", "a"}, keys(c), "Replace keeps the item's place")
	require.Equal(t, int64(60), c.Used())

	require.Equal(t, int64(10), c.ForceEvictWithWait(ctx, 30))
	require.Equal(t, []string{"c"}, keys(c))
	require.Equal(t, int64(10), c.ForceEvictWithWait(ctx, 0))
	c.Set(ctx, "d", 3, 10)
	require.Equal(t, int64(10), c.EvictToTargetWithWait(ctx, 15))
	require.Equal(t, []string{"d"}, keys(c))
	c.ForceEvict(ctx, 10)
	require.Zero(t, c.Used())

	c.Set(ctx, "e", 4, 30)
	done := make(chan int64, 1)
	c.Evict(ctx, done, 80)
	require.Equal(t, int64(20), <-done)
	require.Zero(t, c.Used())
}

func TestEvictionCleanupRunsWhenACallbackPanics(t *testing.T) {
	ctx := context.Background()
	var finished []string
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(30), nil, nil,
		func(key string, _ int, _ int64, _ uint64) func() {
			return func() { finished = append(finished, key) }
		},
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			if key == "a" {
				panic("postEvict")
			}
		},
	)
	c.Set(ctx, "a", 0, 10)
	c.Set(ctx, "b", 1, 10)
	c.Set(ctx, "c", 2, 10)
	require.Panics(t, func() { c.ForceEvictWithWait(ctx, 30) })
	require.ElementsMatch(t, []string{"a", "b", "c"}, finished)
	require.Zero(t, c.Used())
}

func TestEvictionCallbacksMayUseTheCache(t *testing.T) {
	ctx := context.Background()
	var c *Cache[string, int]
	c = NewWithPrepareEvict(
		fscache.ConstCapacity(20), nil, nil,
		func(key string, _ int, _ int64, _ uint64) func() {
			require.False(t, c.Contains(key))
			if key == "a" {
				c.Set(ctx, "a", 0, 10)
			}
			return func() { c.Used() }
		},
		func(context.Context, string, int, int64, uint64) { c.Used() },
	)
	c.Set(ctx, "a", 0, 10)
	c.Set(ctx, "b", 1, 10)
	c.Delete(ctx, "a")
	c.Set(ctx, "c", 2, 10)
	require.True(t, c.Contains("a"))
	require.False(t, c.Contains("b"))
	require.True(t, c.Contains("c"))
}

// lruModel is a least-recently-used cache in one list, most recently used first.
type lruModel struct {
	capacity int64
	keys     []string
	sizes    map[string]int64
	held     map[string]bool
}

func (m *lruModel) used() (n int64) {
	for _, k := range m.keys {
		n += m.sizes[k]
	}
	return n
}

func (m *lruModel) remove(k string) {
	m.keys = slices.DeleteFunc(m.keys, func(x string) bool { return x == k })
	delete(m.sizes, k)
}

func (m *lruModel) evict() {
	for i := len(m.keys) - 1; i >= 0 && m.used() > m.capacity; i-- {
		if k := m.keys[i]; !m.held[k] {
			m.remove(k)
		}
	}
}

func (m *lruModel) set(k string, size int64, oldest bool) {
	if _, ok := m.sizes[k]; ok {
		return
	}
	m.sizes[k] = size
	if oldest {
		m.keys = append(m.keys, k)
	} else {
		m.keys = append([]string{k}, m.keys...)
	}
	m.evict()
}

func (m *lruModel) get(k string) {
	if _, ok := m.sizes[k]; ok {
		size := m.sizes[k]
		m.remove(k)
		m.sizes[k] = size
		m.keys = append([]string{k}, m.keys...)
	}
}

func TestMatchesSingleListModel(t *testing.T) {
	ctx := context.Background()
	m := &lruModel{capacity: 2000, sizes: map[string]int64{}, held: map[string]bool{}}
	c := NewWithPrepareEvict[string, int](fscache.ConstCapacity(m.capacity), nil, nil, nil, nil)
	c.SetEvictable(func(k string) bool { return !m.held[k] })
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		k := strconv.Itoa(r.Intn(300))
		size := int64(1 + r.Intn(100))
		switch op := r.Intn(20); {
		case op < 8:
			c.Get(ctx, k)
			m.get(k)
		case op < 9:
			c.SetOldest(ctx, k, 0, size)
			m.set(k, size, true)
		case op < 10:
			c.Delete(ctx, k)
			m.remove(k)
		case op < 11:
			_, ok := m.sizes[k]
			require.Equal(t, ok, c.Replace(ctx, k, 0, size))
			if ok {
				m.sizes[k] = size
				m.evict()
			}
		case op < 12:
			m.held[k] = !m.held[k]
		default:
			c.Set(ctx, k, 0, size)
			m.set(k, size, false)
		}
		require.Equal(t, m.keys, keys(c), "op %d", i)
		require.Equal(t, m.used(), c.Used(), "op %d", i)
	}
}

func TestConcurrentAccounting(t *testing.T) {
	ctx := context.Background()
	var sets, prepared, finished, evicted atomic.Int64
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(5000),
		func(context.Context, string, int, int64, uint64) { sets.Add(1) },
		nil,
		func(string, int, int64, uint64) func() {
			prepared.Add(1)
			return func() { finished.Add(1) }
		},
		func(context.Context, string, int, int64, uint64) { evicted.Add(1) },
	)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 20000; i++ {
				k := strconv.Itoa(r.Intn(500))
				switch r.Intn(4) {
				case 0:
					c.Get(ctx, k)
				case 1:
					c.Delete(ctx, k)
				default:
					c.Set(ctx, k, 0, int64(1+r.Intn(100)))
				}
			}
		}(g)
	}
	wg.Wait()
	present := keys(c)
	var used int64
	for i := range c.shards {
		for e := c.shards[i].order.Front(); e != nil; e = e.Next() {
			used += e.Value.(*item[string, int]).size
		}
	}
	require.Equal(t, used, c.Used())
	require.LessOrEqual(t, c.Used(), int64(5000))
	require.Equal(t, prepared.Load(), finished.Load())
	require.Equal(t, prepared.Load(), evicted.Load())
	require.Equal(t, sets.Load()-evicted.Load(), int64(len(present)))
}

// keyInShard returns a key with prefix that lands in shard want.
func keyInShard[V any](c *Cache[string, V], prefix string, want int) string {
	for i := 0; ; i++ {
		k := prefix + strconv.Itoa(i)
		if c.shardOf(k) == &c.shards[want] {
			return k
		}
	}
}

// A Get on the oldest item of the shard a scan picked, while the scan reads a later shard,
// does not make eviction take that shard's next item over an older one elsewhere.
func TestScanEvictsOldestAfterConcurrentGet(t *testing.T) {
	ctx := context.Background()
	var evicted []string
	c := NewWithPrepareEvict[string, int](func() int64 { return 1 << 30 }, nil, nil, nil,
		func(_ context.Context, key string, _ int, _ int64, _ uint64) { evicted = append(evicted, key) })
	held := keyInShard(c, "held", 12)
	a1 := keyInShard(c, "a1-", 3)
	b := keyInShard(c, "b-", 9)
	a2 := keyInShard(c, "a2-", 3)
	var armed atomic.Bool
	c.SetEvictable(func(key string) bool {
		if key == held {
			return false
		}
		if key == b && armed.CompareAndSwap(true, false) {
			done := make(chan struct{})
			go func() {
				c.Get(ctx, a1)
				close(done)
			}()
			<-done
		}
		return true
	})
	for _, k := range []string{held, a1, b, a2} {
		c.Set(ctx, k, 0, 1)
	}
	armed.Store(true)
	c.EvictToTargetWithWait(ctx, 3)
	require.Equal(t, []string{b}, evicted)
}

// An item is not evicted before postSet of it returns.
func TestNotEvictedBeforePostSet(t *testing.T) {
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var evicted []string
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(10),
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			if key == "a" {
				close(entered)
				<-release
			}
		},
		nil,
		func(string, int, int64, uint64) func() { return func() {} },
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			mu.Lock()
			evicted = append(evicted, key)
			mu.Unlock()
		},
	)
	setDone := make(chan struct{})
	go func() {
		c.Set(ctx, "a", 0, 10)
		close(setDone)
	}()
	<-entered
	c.Set(ctx, "b", 1, 10)
	require.True(t, c.Contains("a"))
	mu.Lock()
	require.Empty(t, evicted, "nothing is evicted for a pending item")
	mu.Unlock()
	close(release)
	<-setDone
	require.Equal(t, int64(10), c.Used())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"a"}, evicted, "the least recently used, once postSet returned")
}

// While an item larger than the capacity waits for its postSet, other inserts evict
// nothing for it; the item is evicted alone once postSet returns.
func TestPendingOversizedItemEvictsNothingElse(t *testing.T) {
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var evicted []string
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(100),
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			if key == "big" {
				close(entered)
				<-release
			}
		},
		nil,
		func(string, int, int64, uint64) func() { return func() {} },
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			mu.Lock()
			evicted = append(evicted, key)
			mu.Unlock()
		},
	)
	c.Set(ctx, "a", 0, 40)
	c.Set(ctx, "b", 1, 40)
	setDone := make(chan struct{})
	go func() {
		c.Set(ctx, "big", 2, 150)
		close(setDone)
	}()
	<-entered
	c.Set(ctx, "c", 3, 10)
	mu.Lock()
	require.Empty(t, evicted)
	mu.Unlock()
	close(release)
	<-setDone
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"big"}, evicted)
	require.Equal(t, int64(90), c.Used())
}

func TestOversizedItemStaysOldestWhenUsedWhilePending(t *testing.T) {
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(100),
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			if key == "big" {
				close(entered)
				<-release
			}
		},
		nil,
		func(string, int, int64, uint64) func() { return func() {} },
		nil,
	)
	c.Set(ctx, "a", 0, 40)
	c.Set(ctx, "b", 1, 40)
	setDone := make(chan struct{})
	go func() {
		c.Set(ctx, "big", 2, 150)
		close(setDone)
	}()
	<-entered
	_, ok := c.Get(ctx, "big")
	require.True(t, ok)
	close(release)
	<-setDone
	require.True(t, c.Contains("a"))
	require.True(t, c.Contains("b"))
	require.False(t, c.Contains("big"))
	require.Equal(t, int64(80), c.Used())
}

func TestOversizedItemStaysOldestAfterSetOldest(t *testing.T) {
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	c := NewWithPrepareEvict(
		fscache.ConstCapacity(100),
		func(_ context.Context, key string, _ int, _ int64, _ uint64) {
			if key == "big" {
				close(entered)
				<-release
			}
		},
		nil,
		func(string, int, int64, uint64) func() { return func() {} },
		nil,
	)
	c.Set(ctx, "a", 0, 40)
	c.Set(ctx, "b", 1, 40)
	setDone := make(chan struct{})
	go func() {
		c.Set(ctx, "big", 2, 150)
		close(setDone)
	}()
	<-entered
	require.True(t, c.SetOldest(ctx, "c", 3, 10))
	close(release)
	<-setDone
	require.True(t, c.Contains("a"))
	require.True(t, c.Contains("b"))
	require.True(t, c.Contains("c"))
	require.False(t, c.Contains("big"))
	require.Equal(t, int64(90), c.Used())
}

func TestEvictionDoesNotCountPendingInsertion(t *testing.T) {
	ctx := context.Background()
	c := NewWithPrepareEvict[string, int](fscache.ConstCapacity(100), nil, nil, nil, nil)
	c.Set(ctx, "a", 0, 40)
	c.Set(ctx, "b", 1, 40)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.Evict(ctx, nil, 0)
			}
		}
	}()
	for i := 0; i < 20000 && c.Contains("a") && c.Contains("b"); i++ {
		c.Set(ctx, "big", 2, 150)
		c.Delete(ctx, "big")
	}
	close(stop)
	wg.Wait()
	require.True(t, c.Contains("a"))
	require.True(t, c.Contains("b"))
}
