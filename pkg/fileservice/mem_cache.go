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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type MemCache struct {
	lru   *LRU
	stats *CacheStats
}

func NewMemCache(capacity int64) *MemCache {
	return &MemCache{
		lru:   NewLRU(capacity),
		stats: new(CacheStats),
	}
}

var _ Cache = new(MemCache)

func (m *MemCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {
	_, span := trace.Start(ctx, "MemCache.Read")
	defer span.End()

	numHit := 0
	defer func() {
		if m.stats != nil {
			atomic.AddInt64(&m.stats.NumRead, int64(len(vector.Entries)))
			atomic.AddInt64(&m.stats.NumHit, int64(numHit))
		}
	}()

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.ToObject == nil {
			continue
		}
		key := CacheKey{
			Path:   vector.FilePath,
			Offset: entry.Offset,
			Size:   entry.Size,
		}
		obj, size, ok := m.lru.Get(key)
		if ok {
			vector.Entries[i].Object = obj
			vector.Entries[i].ObjectSize = size
			vector.Entries[i].done = true
			numHit++
		}
	}

	return
}

func (m *MemCache) Update(
	ctx context.Context,
	vector *IOVector,
) error {
	for _, entry := range vector.Entries {
		if entry.Object == nil {
			continue
		}
		key := CacheKey{
			Path:   vector.FilePath,
			Offset: entry.Offset,
			Size:   entry.Size,
		}
		m.lru.Set(key, entry.Object, entry.ObjectSize)
	}
	return nil
}

func (m *MemCache) Flush() {
	m.lru.Flush()
}

func (m *MemCache) CacheStats() *CacheStats {
	return m.stats
}
