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
	"github.com/matrixorigin/matrixone/pkg/fileservice/memcachepolicy"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memcachepolicy/clockpolicy"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memcachepolicy/lrupolicy"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"sync/atomic"
)

type MemCache struct {
	policy memcachepolicy.Policy
	stats  *CacheStats
	ch     chan func()
}

func NewMemCache(opts ...Options) *MemCache {
	ch := make(chan func(), 65536)
	go func() {
		for fn := range ch {
			fn()
		}
	}()

	initOpts := defaultOptions()
	for _, optFunc := range opts {
		optFunc(&initOpts)
	}

	return &MemCache{
		policy: initOpts.policy,
		stats:  new(CacheStats),
		ch:     ch,
	}
}

func WithLRU(capacity int64) Options {
	return func(o *options) {
		o.policy = lrupolicy.New(capacity)
	}
}

func WithClock(capacity int64) Options {
	return func(o *options) {
		o.policy = clockpolicy.New(capacity)
	}
}

type Options func(*options)

type options struct {
	policy memcachepolicy.Policy
}

func defaultOptions() options {
	return options{}
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
		obj, size, ok := m.policy.Get(key)
		updateCounters(ctx, func(c *Counter) {
			atomic.AddInt64(&c.MemCacheRead, 1)
		})
		if ok {
			vector.Entries[i].Object = obj
			vector.Entries[i].ObjectSize = size
			vector.Entries[i].done = true
			numHit++
			m.cacheHit()
			updateCounters(ctx, func(c *Counter) {
				atomic.AddInt64(&c.MemCacheHit, 1)
			})
		}
	}

	return
}

func (m *MemCache) cacheHit() {
	FSProfileHandler.AddSample()
}

func (m *MemCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
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
		if async {
			obj := entry.Object // copy from loop variable
			objSize := entry.ObjectSize
			m.ch <- func() {
				m.policy.Set(key, obj, objSize)
			}
		} else {
			m.policy.Set(key, entry.Object, entry.ObjectSize)
		}
	}
	return nil
}

func (m *MemCache) Flush() {
	m.policy.Flush()
}

func (m *MemCache) CacheStats() *CacheStats {
	return m.stats
}
