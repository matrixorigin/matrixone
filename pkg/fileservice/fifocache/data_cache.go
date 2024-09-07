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
	"math"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

type DataCache struct {
	fifo *Cache[fscache.CacheKey, fscache.Data]
}

func NewDataCache(
	capacity fscache.CapacityFunc,
	postSet func(key fscache.CacheKey, value fscache.Data),
	postGet func(key fscache.CacheKey, value fscache.Data),
	postEvict func(key fscache.CacheKey, value fscache.Data),
) *DataCache {
	keyShardFunc := func(key fscache.CacheKey) uint8 {
		return uint8(key.Offset ^ key.Sz)
	}
	return &DataCache{
		fifo: New(capacity, keyShardFunc, postSet, postGet, postEvict),
	}
}

var _ fscache.DataCache = new(DataCache)

func (d *DataCache) Available() int64 {
	d.fifo.queueLock.Lock()
	defer d.fifo.queueLock.Unlock()
	ret := d.fifo.capacity() - d.fifo.used1 - d.fifo.used2
	if ret < 0 {
		ret = 0
	}
	return ret
}

func (d *DataCache) Capacity() int64 {
	return d.fifo.capacity()
}

func (d *DataCache) DeletePaths(ctx context.Context, paths []string) {
	for _, path := range paths {
		for i := 0; i < len(d.fifo.shards); i++ {
			shard := &d.fifo.shards[i]
			shard.Lock()
			for key, item := range shard.values {
				if key.Path == path {
					delete(shard.values, key)
					if d.fifo.postEvict != nil {
						d.fifo.postEvict(item.key, item.value)
					}
				}
			}
			shard.Unlock()
		}
	}
}

func (d *DataCache) EnsureNBytes(n int) {
	d.fifo.queueLock.Lock()
	defer d.fifo.queueLock.Unlock()
	d.fifo.evict(nil, int64(n))
}

func (d *DataCache) Evict(ch chan int64) {
	d.fifo.Evict(ch)
}

func (d *DataCache) Flush() {
	d.fifo.queueLock.Lock()
	defer d.fifo.queueLock.Unlock()
	d.fifo.evict(nil, math.MaxInt64)
}

func (d *DataCache) Get(ctx context.Context, key query.CacheKey) (fscache.Data, bool) {
	return d.fifo.Get(key)
}

func (d *DataCache) Set(ctx context.Context, key query.CacheKey, value fscache.Data) error {
	d.fifo.Set(key, value, int64(len(value.Bytes())))
	return nil
}

func (d *DataCache) Used() int64 {
	d.fifo.queueLock.Lock()
	defer d.fifo.queueLock.Unlock()
	return d.fifo.used1 + d.fifo.used2
}
