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

	"github.com/matrixorigin/matrixone/pkg/common/util"
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
	var hasher maphash.Hash
	hasher.SetSeed(seed)
	hasher.Write(util.UnsafeToBytes(&key.Offset))
	hasher.Write(util.UnsafeToBytes(&key.Sz))
	hasher.WriteString(key.Path)
	return hasher.Sum64()
}

var _ fscache.DataCache = new(DataCache)

func (d *DataCache) Available() int64 {
	ret := d.fifo.capacity() - d.fifo.Used()
	if ret < 0 {
		ret = 0
	}
	return ret
}

func (d *DataCache) Capacity() int64 {
	return d.fifo.capacity()
}

func (d *DataCache) DeletePaths(ctx context.Context, paths []string) {
	deletes := make([]*_CacheItem[fscache.CacheKey, fscache.Data], 0, 10)
	for _, path := range paths {

		key := fscache.CacheKey{Path: path}
		d.fifo.htab.CompareAndDelete(key, func(key1, key2 fscache.CacheKey) bool {
			return key1.Path == key2.Path
		}, func(value *_CacheItem[fscache.CacheKey, fscache.Data]) {
			deletes = append(deletes, value)
		})
	}

	if SingleMutexFlag {
		d.fifo.mutex.Lock()
		defer d.fifo.mutex.Unlock()
	}
	// FSCACHEDATA RELEASE
	for _, item := range deletes {
		item.MarkAsDeleted(ctx, d.fifo.postEvict)
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
	return d.fifo.Used()
}
