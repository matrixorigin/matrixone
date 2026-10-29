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
	"context"
	"fmt"
	"sync"
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
		nil,
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

func keys(c *Cache[string, int]) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for e := c.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*item[string, int]).key)
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
	require.Empty(t, keys(c), "an item larger than the capacity is evicted with the rest")
	require.Zero(t, c.Used())
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
		fscache.ConstCapacity(30), nil, nil, nil,
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
