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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type MemCache struct {
	policy   memcachepolicy.Policy
	ch       chan func()
	counters []*perfcounter.Counter
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
		policy:   initOpts.policy,
		ch:       ch,
		counters: initOpts.counters,
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

func WithPerfCounters(counters []*perfcounter.Counter) Options {
	return func(o *options) {
		o.counters = append(o.counters, counters...)
	}
}

type Options func(*options)

type options struct {
	policy   memcachepolicy.Policy
	counters []*perfcounter.Counter
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

	var numHit, numRead int64
	defer func() {
		perfcounter.Update(ctx, func(c *perfcounter.Counter) {
			c.Cache.Read.Add(numRead)
			c.Cache.Hit.Add(numHit)
			c.Cache.MemRead.Add(numRead)
			c.Cache.MemHit.Add(numHit)
		}, m.counters...)
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
		numRead++
		if ok {
			vector.Entries[i].Object = obj
			vector.Entries[i].ObjectSize = size
			vector.Entries[i].done = true
			numHit++
			m.cacheHit()
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
