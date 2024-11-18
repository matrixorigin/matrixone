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
	"hash/maphash"
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

type DataCache struct {
	fifo *Cache[fscache.CacheKey, fscache.Data]
}

func NewDataCache(
	capacity fscache.CapacityFunc,
	postSet func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64),
	postGet func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64),
	postEvict func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64),
) *DataCache {
	return &DataCache{
		fifo: New(capacity, shardCacheKey, postSet, postGet, postEvict),
	}
}

var seed = maphash.MakeSeed()

func shardCacheKey(key fscache.CacheKey) uint64 {
	data := unsafe.Slice(
		unsafe.StringData(key.Path),
		len(key.Path),
	)
	data = append(data, unsafe.Slice(
		(*byte)(unsafe.Pointer(&key.Offset)),
		unsafe.Sizeof(key.Offset),
	)...)
	data = append(data, unsafe.Slice(
		(*byte)(unsafe.Pointer(&key.Sz)),
		unsafe.Sizeof(key.Sz),
	)...)
	return maphash.Bytes(seed, data)
}

var _ fscache.DataCache = new(DataCache)

func (d *DataCache) Available() int64 {
	d.fifo.queueLock.RLock()
	defer d.fifo.queueLock.RUnlock()
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
			d.deletePath(ctx, i, path)
		}
	}
}

func (d *DataCache) deletePath(ctx context.Context, shardIndex int, path string) {
	shard := &d.fifo.shards[shardIndex]
	shard.Lock()
	defer shard.Unlock()
	for key, item := range shard.values {
		if key.Path == path {
			delete(shard.values, key)
			if d.fifo.postEvict != nil {
				d.fifo.postEvict(ctx, item.key, item.value, item.size)
			}
		}
	}
}

func (d *DataCache) EnsureNBytes(ctx context.Context, want int) {
	d.fifo.Evict(ctx, nil, int64(want))
}

func (d *DataCache) Evict(ctx context.Context, ch chan int64) {
	d.fifo.Evict(ctx, ch, 0)
}

func (d *DataCache) Flush(ctx context.Context) {
	d.fifo.Evict(ctx, nil, math.MaxInt64)
}

func (d *DataCache) Get(ctx context.Context, key query.CacheKey) (fscache.Data, bool) {
	return d.fifo.Get(ctx, key)
}

func (d *DataCache) Set(ctx context.Context, key query.CacheKey, value fscache.Data) error {
	d.fifo.Set(ctx, key, value, int64(len(value.Bytes())))
	return nil
}

func (d *DataCache) Used() int64 {
	d.fifo.queueLock.RLock()
	defer d.fifo.queueLock.RUnlock()
	return d.fifo.used1 + d.fifo.used2
}
