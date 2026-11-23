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

package lrucache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/stretchr/testify/require"
)

// Black-box checks of docs/design/20261106-fileservice-cache-ownership.md, disk caches,
// through the public API only.

func newContractLRU(capacity int64, postEvict func(ctx context.Context, key string, _ int, _ int64, _ uint64)) *lrucache.Cache[string, int] {
	return lrucache.NewWithPrepareEvict[string, int](fscache.ConstCapacity(capacity), nil, nil, nil, postEvict)
}

// "Eviction removes the evictable item with the oldest stamp across all shards", where
// Set and Get make an item the most recently used.
func TestDesignLRUEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	var evicted []string
	c := newContractLRU(100, func(_ context.Context, key string, _ int, _ int64, _ uint64) { evicted = append(evicted, key) })
	for i := 0; i < 100; i++ {
		c.Set(ctx, fmt.Sprint(i), i, 1)
	}
	for i := 0; i < 10; i++ {
		_, ok := c.Get(ctx, fmt.Sprint(i))
		require.True(t, ok)
	}
	for i := 100; i < 110; i++ {
		c.Set(ctx, fmt.Sprint(i), i, 1)
	}
	want := make([]string, 0, 10)
	for i := 10; i < 20; i++ {
		want = append(want, fmt.Sprint(i))
	}
	require.Equal(t, want, evicted)
	require.Equal(t, int64(100), c.Used())
}

// "Index-file entries that are held are never evicted."
func TestDesignLRUHeldEntriesStay(t *testing.T) {
	ctx := context.Background()
	c := newContractLRU(10, nil)
	c.SetEvictable(func(key string) bool { return key != "held" })
	c.Set(ctx, "held", 0, 1)
	for i := 0; i < 50; i++ {
		c.Set(ctx, fmt.Sprint(i), i, 1)
	}
	require.True(t, c.Contains("held"))
	require.LessOrEqual(t, c.Used(), int64(10))
}

// "Files found on disk at startup are inserted as oldest."
func TestDesignLRUOldestInsertsGoFirst(t *testing.T) {
	ctx := context.Background()
	var evicted []string
	c := newContractLRU(3, func(_ context.Context, key string, _ int, _ int64, _ uint64) { evicted = append(evicted, key) })
	c.Set(ctx, "new1", 0, 1)
	c.SetOldest(ctx, "startup", 0, 1)
	c.Set(ctx, "new2", 0, 1)
	c.Set(ctx, "new3", 0, 1)
	require.Equal(t, []string{"startup"}, evicted)
}

// "Eviction runs every removal callback outside all locks, so a callback may use the
// cache; if a callback panics, the remaining removals still complete."
func TestDesignLRUCallbacksOutsideLocks(t *testing.T) {
	ctx := context.Background()
	var c *lrucache.Cache[string, int]
	evicted := 0
	c = newContractLRU(10, func(ctx context.Context, key string, _ int, _ int64, _ uint64) {
		evicted++
		c.Contains(key)
		c.Get(ctx, "0")
		if key == "5" {
			panic("callback failed")
		}
	})
	for i := 0; i < 10; i++ {
		c.Set(ctx, fmt.Sprint(i), i, 1)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		c.EvictToTargetWithWait(ctx, 0)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a callback calling into the cache deadlocked")
	}
	require.Equal(t, int64(0), c.Used(), "removals after the panicking callback completed")
	require.Equal(t, 10, evicted, "every removal got its callback")
}
