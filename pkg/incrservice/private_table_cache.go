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

package incrservice

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// lazyPrivateTableCache keeps an ALTER reset transaction-private without
// reserving an allocation range until that same transaction actually needs an
// auto value. This preserves ALTER-only next-value semantics while still
// making ALTER followed by INSERT in one transaction observe the reset.
type lazyPrivateTableCache struct {
	tableID uint64
	cols    []AutoColumn
	build   func(context.Context) (incrTableCache, error)

	lifecycle struct {
		sync.Mutex
		users   int
		retired bool
		closed  bool
	}
	mu struct {
		sync.Mutex
		cache incrTableCache
		build *privateCacheBuild
	}
}

type privateCacheBuild struct {
	ready chan struct{}
	cache incrTableCache
	err   error
}

func newLazyPrivateTableCache(
	tableID uint64,
	cols []AutoColumn,
	build func(context.Context) (incrTableCache, error),
) incrTableCache {
	return &lazyPrivateTableCache{
		tableID: tableID, cols: cols, build: build,
	}
}

func (c *lazyPrivateTableCache) load(ctx context.Context) (incrTableCache, error) {
	c.lifecycle.Lock()
	retired := c.lifecycle.retired
	c.lifecycle.Unlock()
	if retired {
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}

	c.mu.Lock()
	if c.mu.cache != nil {
		cache := c.mu.cache
		cache.acquire()
		c.mu.Unlock()
		return cache, nil
	}
	if c.mu.build != nil {
		build := c.mu.build
		c.mu.Unlock()
		select {
		case <-build.ready:
			if build.err != nil {
				return nil, build.err
			}
			// Serialize the acquire with retire. A completed generation can
			// be retired before this waiter is scheduled after ready closes.
			c.mu.Lock()
			if c.mu.cache != build.cache {
				c.mu.Unlock()
				return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
			}
			cache := build.cache
			cache.acquire()
			c.mu.Unlock()
			return cache, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	build := &privateCacheBuild{ready: make(chan struct{})}
	c.mu.build = build
	c.mu.Unlock()

	cache, err := c.build(ctx)

	c.mu.Lock()
	c.lifecycle.Lock()
	retired = c.lifecycle.retired
	c.lifecycle.Unlock()
	if err == nil && !retired {
		c.mu.cache = cache
		build.cache = cache
		cache.acquire()
	} else if err != nil {
		build.err = err
	} else {
		build.err = moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	c.mu.build = nil
	close(build.ready)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if retired {
		cache.retire()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return cache, nil
}

func (c *lazyPrivateTableCache) table() uint64 { return c.tableID }
func (c *lazyPrivateTableCache) epoch() uint32 { return 0 }
func (c *lazyPrivateTableCache) columns() []AutoColumn {
	return c.cols
}
func (c *lazyPrivateTableCache) acquire() {
	c.lifecycle.Lock()
	c.lifecycle.users++
	c.lifecycle.Unlock()
}
func (c *lazyPrivateTableCache) release() {
	c.lifecycle.Lock()
	c.lifecycle.users--
	c.lifecycle.Unlock()
}
func (c *lazyPrivateTableCache) retire() {
	c.lifecycle.Lock()
	if c.lifecycle.retired {
		c.lifecycle.Unlock()
		return
	}
	c.lifecycle.retired = true
	c.lifecycle.closed = true
	c.lifecycle.Unlock()
	c.mu.Lock()
	cache := c.mu.cache
	c.mu.cache = nil
	c.mu.Unlock()
	if cache != nil {
		cache.retire()
	}
}
func (c *lazyPrivateTableCache) commit() { panic("private reset cache cannot be committed") }
func (c *lazyPrivateTableCache) insertAutoValues(ctx context.Context, tableID uint64, vecs []*vector.Vector, rows int, estimate int64) (uint64, error) {
	cache, err := c.load(ctx)
	if err != nil {
		return 0, err
	}
	defer cache.release()
	return cache.insertAutoValues(ctx, tableID, vecs, rows, estimate)
}
func (c *lazyPrivateTableCache) currentValue(ctx context.Context, tableID uint64, col string) (uint64, error) {
	cache, err := c.load(ctx)
	if err != nil {
		return 0, err
	}
	defer cache.release()
	return cache.currentValue(ctx, tableID, col)
}
func (c *lazyPrivateTableCache) getLastAllocateTS(ctx context.Context, colName string) (timestamp.Timestamp, error) {
	cache, err := c.load(ctx)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	defer cache.release()
	return cache.getLastAllocateTS(ctx, colName)
}
func (c *lazyPrivateTableCache) adjust(ctx context.Context, cols []AutoColumn) error {
	cache, err := c.load(ctx)
	if err != nil {
		return err
	}
	defer cache.release()
	return cache.adjust(ctx, cols)
}
func (c *lazyPrivateTableCache) close() error {
	c.retire()
	return nil
}
