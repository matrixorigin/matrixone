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
	cache       DataCache
	counterSets []*perfcounter.CounterSet
}

func NewMemCache(
	dataCache DataCache,
	counterSets []*perfcounter.CounterSet,
) *MemCache {
	ret := &MemCache{
		cache:       dataCache,
		counterSets: counterSets,
	}
	return ret
}

func NewLRUCache(
	capacity int64,
	checkOverlaps bool,
	callbacks *CacheCallbacks,
) *lrucache.LRU[CacheKey, CacheData] {

	var overlapChecker *interval.OverlapChecker
	if checkOverlaps {
		overlapChecker = interval.NewOverlapChecker("MemCache_LRU")
	}

	postSetFn := func(key CacheKey, value CacheData) {
		value.Retain()

		if overlapChecker != nil {
			if err := overlapChecker.Insert(key.Path, key.Offset, key.Offset+key.Sz); err != nil {
				panic(err)
			}
		}

		if callbacks != nil {
			for _, fn := range callbacks.PostSet {
				fn(key, value)
			}
		}
	}

	postGetFn := func(key CacheKey, value CacheData) {
		value.Retain()

		if callbacks != nil {
			for _, fn := range callbacks.PostGet {
				fn(key, value)
			}
		}
	}

	postEvictFn := func(key CacheKey, value CacheData) {
		value.Release()

		if overlapChecker != nil {
			if err := overlapChecker.Remove(key.Path, key.Offset, key.Offset+key.Sz); err != nil {
				panic(err)
			}
		}

		if callbacks != nil {
			for _, fn := range callbacks.PostEvict {
				fn(key, value)
			}
		}
	}

	return lrucache.New[CacheKey, CacheData](capacity, postSetFn, postGetFn, postEvictFn)
}

var _ IOVectorCache = new(MemCache)

func (m *MemCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {

	if vector.CachePolicy.Any(SkipMemoryReads) {
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
		key := CacheKey{
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

	if vector.CachePolicy.Any(SkipMemoryWrites) {
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

		key := CacheKey{
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
