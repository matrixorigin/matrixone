// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type MemCache struct {
	cache       fscache.DataCache
	counterSets []*perfcounter.CounterSet
}

func NewMemCache(
	capacity fscache.CapacityFunc,
	callbacks *CacheCallbacks,
	counterSets []*perfcounter.CounterSet,
	name string,
) *MemCache {

	postSetFn := func(key fscache.CacheKey, value fscache.Data) {
		value.Retain()

		if callbacks != nil {
			for _, fn := range callbacks.PostSet {
				fn(key, value)
			}
		}
	}

	postGetFn := func(key fscache.CacheKey, value fscache.Data) {
		value.Retain()

		if callbacks != nil {
			for _, fn := range callbacks.PostGet {
				fn(key, value)
			}
		}
	}

	postEvictFn := func(key fscache.CacheKey, value fscache.Data) {
		value.Release()

		if callbacks != nil {
			for _, fn := range callbacks.PostEvict {
				fn(key, value)
			}
		}
	}

	capacityFunc := func() int64 {
		// read from global hint
		if n := GlobalMemoryCacheSizeHint.Load(); n > 0 {
			return n
		}
		// fallback
		return capacity()
	}
	dataCache := fifocache.NewDataCache(capacityFunc, postSetFn, postGetFn, postEvictFn)

	return &MemCache{
		cache:       dataCache,
		counterSets: counterSets,
	}
}

var _ IOVectorCache = new(MemCache)

func (m *MemCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {

	if vector.Policy.Any(SkipMemoryCacheReads) {
		return nil
	}

	var numHit, numRead int64
	defer func() {
		metric.FSReadHitMemCounter.Add(float64(numHit))
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Memory.Read.Add(numRead)
			c.FileService.Cache.Memory.Hit.Add(numHit)
		}, m.counterSets...)
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		key := fscache.CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}
		bs, ok := m.cache.Get(ctx, key)
		numRead++
		if ok {
			vector.Entries[i].CachedData = bs
			vector.Entries[i].done = true
			vector.Entries[i].fromCache = m
			numHit++
		}
	}

	return
}

func (m *MemCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) error {

	if vector.Policy.Any(SkipMemoryCacheWrites) {
		return nil
	}

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for _, entry := range vector.Entries {
		if entry.CachedData == nil {
			continue
		}
		if entry.fromCache == m {
			continue
		}

		key := fscache.CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}

		m.cache.Set(ctx, key, entry.CachedData)
	}
	return nil
}

func (m *MemCache) Flush() {
	m.cache.Flush()
}

func (m *MemCache) DeletePaths(
	ctx context.Context,
	paths []string,
) error {
	m.cache.DeletePaths(ctx, paths)
	return nil
}

func (m *MemCache) Evict(done chan int64) {
	m.cache.Evict(done)
}

func (m *MemCache) Close() {
	m.Flush()
	allMemoryCaches.Delete(m)
}
