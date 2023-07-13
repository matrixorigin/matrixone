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
	"github.com/matrixorigin/matrixone/pkg/fileservice/checks/interval"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/objcache/lruobjcache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type MemCache struct {
	objCache       ObjectCache
	ch             chan func()
	counterSets    []*perfcounter.CounterSet
	overlapChecker *interval.OverlapChecker
}

func NewMemCache(opts ...MemCacheOptionFunc) *MemCache {
	ch := make(chan func(), 65536)
	go func() {
		for fn := range ch {
			fn()
		}
	}()

	initOpts := defaultMemCacheOptions()
	for _, optFunc := range opts {
		optFunc(&initOpts)
	}

	return &MemCache{
		overlapChecker: initOpts.overlapChecker,
		objCache:       initOpts.objCache,
		ch:             ch,
		counterSets:    initOpts.counterSets,
	}
}

func WithLRU(capacity int64) MemCacheOptionFunc {
	return func(o *memCacheOptions) {
		o.overlapChecker = interval.NewOverlapChecker("MemCache_LRU")
		o.objCache = lruobjcache.New(capacity, func(key any, value []byte, _ int64) {
			_key := key.(IOVectorCacheKey)
			if err := o.overlapChecker.Remove(_key.Path, _key.Offset, _key.Offset+_key.Size); err != nil {
				panic(err)
			}
		})
	}
}

func WithPerfCounterSets(counterSets []*perfcounter.CounterSet) MemCacheOptionFunc {
	return func(o *memCacheOptions) {
		o.counterSets = append(o.counterSets, counterSets...)
	}
}

type MemCacheOptionFunc func(*memCacheOptions)

type memCacheOptions struct {
	objCache       ObjectCache
	overlapChecker *interval.OverlapChecker
	counterSets    []*perfcounter.CounterSet
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
			c.FileService.Cache.Memory.Capacity.Swap(m.objCache.Capacity())
			c.FileService.Cache.Memory.Used.Swap(m.objCache.Used())
			c.FileService.Cache.Memory.Available.Swap(m.objCache.Available())
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
		if entry.ToObjectBytes == nil {
			continue
		}
		key := IOVectorCacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Size:   entry.Size,
		}
		bs, size, ok := m.objCache.Get(key, vector.Preloading)
		numRead++
		if ok {
			vector.Entries[i].ObjectBytes = bs
			vector.Entries[i].ObjectSize = size
			vector.Entries[i].done = true
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
		if entry.ObjectBytes == nil {
			continue
		}
		key := IOVectorCacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Size:   entry.Size,
		}

		var isNewEntry bool
		if async {
			obj := entry.ObjectBytes // copy from loop variable
			objSize := entry.ObjectSize
			m.ch <- func() {
				m.objCache.Set(key, obj, objSize, vector.Preloading)

				// Update overlap checker when new key-interval is inserted into the cache.
				// If we are replacing the data for an existing key, we don't have issue of wasted memory space.
				if isNewEntry {
					if err = m.overlapChecker.Insert(key.Path, key.Offset, key.Offset+key.Size); err != nil {
						panic(err)
					}
				}
			}
		} else {
			isNewEntry = m.objCache.Set(key, entry.ObjectBytes, entry.ObjectSize, vector.Preloading)

			// Update overlap checker when new key-interval is inserted into the cache.
			// If we are replacing the data for an existing key, we don't have issue of wasted memory space.
			if isNewEntry {
				if err = m.overlapChecker.Insert(key.Path, key.Offset, key.Offset+key.Size); err != nil {
					panic(err)
				}
			}
		}

	}
	return nil
}

func (m *MemCache) Flush() {
	m.objCache.Flush()
}
