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
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/checks/interval"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type MemCache struct {
	cache                DataCache
	counterSets          []*perfcounter.CounterSet
	overlapChecker       *interval.OverlapChecker
	enableOverlapChecker bool
}

func NewMemCache(opts ...MemCacheOptionFunc) *MemCache {
	initOpts := defaultMemCacheOptions()
	for _, optFunc := range opts {
		optFunc(&initOpts)
	}

	return &MemCache{
		overlapChecker:       initOpts.overlapChecker,
		enableOverlapChecker: initOpts.enableOverlapChecker,
		cache:                initOpts.cache,
		counterSets:          initOpts.counterSets,
	}
}

func WithLRU(capacity int64) MemCacheOptionFunc {
	return func(o *memCacheOptions) {
		o.overlapChecker = interval.NewOverlapChecker("MemCache_LRU")
		o.enableOverlapChecker = true

		postSetFn := func(key CacheKey, valSet RCBytes, isNewEntry bool) {
			if o.enableOverlapChecker && isNewEntry {
				if err := o.overlapChecker.Insert(key.Path, key.Offset, key.Offset+key.Size); err != nil {
					panic(err)
				}
			}
		}

		postEvictFn := func(key CacheKey, valEvicted RCBytes) {
			if o.enableOverlapChecker {
				if err := o.overlapChecker.Remove(key.Path, key.Offset, key.Offset+key.Size); err != nil {
					panic(err)
				}
			}
		}

		o.cache = lrucache.New[CacheKey, RCBytes](capacity, postSetFn, postEvictFn)
	}
}

func WithPerfCounterSets(counterSets []*perfcounter.CounterSet) MemCacheOptionFunc {
	return func(o *memCacheOptions) {
		o.counterSets = append(o.counterSets, counterSets...)
	}
}

type MemCacheOptionFunc func(*memCacheOptions)

type memCacheOptions struct {
	cache                DataCache
	overlapChecker       *interval.OverlapChecker
	counterSets          []*perfcounter.CounterSet
	enableOverlapChecker bool
}

func defaultMemCacheOptions() memCacheOptions {
	return memCacheOptions{}
}

var _ IOVectorCache = new(MemCache)

func (m *MemCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {
	if vector.NoCache {
		return nil
	}

	var numHit, numRead int64
	defer func() {
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Memory.Read.Add(numRead)
			c.FileService.Cache.Memory.Hit.Add(numHit)
			c.FileService.Cache.Memory.Capacity.Swap(m.cache.Capacity())
			c.FileService.Cache.Memory.Used.Swap(m.cache.Used())
			c.FileService.Cache.Memory.Available.Swap(m.cache.Available())
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
		if entry.ToCacheData == nil {
			continue
		}
		key := CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Size:   entry.Size,
		}
		bs, ok := m.cache.Get(ctx, key, vector.Preloading)
		numRead++
		if ok {
			vector.Entries[i].CachedData = bs
			vector.Entries[i].done = true
			vector.Entries[i].fromCache = m
			numHit++
			m.cacheHit(time.Nanosecond)
		}
	}

	return
}

func (m *MemCache) cacheHit(duration time.Duration) {
	FSProfileHandler.AddSample(duration)
}

func (m *MemCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) error {
	if vector.NoCache {
		return nil
	}

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for _, entry := range vector.Entries {
		if entry.CachedData.Len() == 0 {
			continue
		}
		if entry.fromCache == m {
			continue
		}

		key := CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Size:   entry.Size,
		}

		m.cache.Set(ctx, key, entry.CachedData, vector.Preloading)

	}
	return nil
}

func (m *MemCache) Flush() {
	m.cache.Flush()
}
