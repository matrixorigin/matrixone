// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fifocache

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// SetWithoutEviction publishes a reserved fill using ordinary queue placement,
// but never makes room by evicting. admit runs under the accounting guard and
// must not re-enter the cache. It validates destination-specific backing usage.
func (d *DataCache) SetWithoutEviction(ctx context.Context, key fscache.CacheKey, data fscache.Data, admit func() bool) (bool, error) {
	logical, size := data.Size(), data.Capacity()
	if logical < 0 || size < logical {
		panic("cache data reports an invalid logical size or backing capacity")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	inserted, rejected := d.fifo.setWithoutEviction(ctx, key, dataCacheValue{data: data, logicalSize: logical}, size, admit)
	if rejected {
		return false, fscache.ErrCacheAdmissionRejected
	}
	return inserted, nil
}

func (c *Cache[K, V]) setWithoutEviction(ctx context.Context, key K, value V, size int64, admit func() bool) (inserted, rejected bool) {
	seq := c.nextSeq.Add(1)
	if c.prepareSet != nil {
		finish := c.prepareSet(ctx, key, value, size, seq)
		if finish != nil {
			defer func() { finish(inserted) }()
		}
	}
	func() {
		account := c.needsAccounting(value)
		if account {
			c.accountingGuard.Lock()
			defer c.accountingGuard.Unlock()
		}
		c.queueLock.Lock()
		defer c.queueLock.Unlock()
		c.helpEnqueue()
		_, pressure := c.currentAdmissionTarget()
		if pressure || size > c.capacity()-c.used1-c.used2 || (admit != nil && !admit()) {
			rejected = true
			return
		}
		shard := &c.shards[c.keyShardFunc(key)%numShards]
		shard.Lock()
		defer shard.Unlock()
		old := shard.values[key]
		if old != nil && old.valueOK {
			return
		}
		item := &_CacheItem[K, V]{key: key, value: value, valueOK: true, size: size, seq: seq}
		if old != nil {
			item.queue = cacheItemQueue2
			c.queue2.enqueue(item)
			c.used2 += size
		} else {
			item.queue = cacheItemQueue1
			c.queue1.enqueue(item)
			c.used1 += size
		}
		shard.values[key] = item
		inserted = true
		if account {
			c.accountingCommit(value)
		}
	}()
	if inserted && c.postSet != nil {
		c.postSet(ctx, key, value, size, seq)
	}
	return inserted, rejected
}
