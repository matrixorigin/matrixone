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

package memorycache

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache/lrucache"
	cache "github.com/matrixorigin/matrixone/pkg/pb/query"
)

type Cache struct {
	size      atomic.Int64
	l         *lrucache.LRU[cache.CacheKey, *Data]
	allocator malloc.Allocator
}

func NewCache(
	capacity fscache.CapacityFunc,
	postSet func(key cache.CacheKey, value fscache.Data),
	postGet func(key cache.CacheKey, value fscache.Data),
	postEvict func(key cache.CacheKey, value fscache.Data),
	allocator malloc.Allocator,
) *Cache {

	c := &Cache{
		allocator: allocator,
	}

	setFunc := func(key cache.CacheKey, value *Data) {
		value.acquire()
		if postSet != nil {
			postSet(key, value)
		}
	}

	getFunc := func(key cache.CacheKey, value *Data) {
		value.acquire()
		if postGet != nil {
			postGet(key, value)
		}
	}

	evictFunc := func(key cache.CacheKey, value *Data) {
		value.Release()
		if postEvict != nil {
			postEvict(key, value)
		}
	}

	c.l = lrucache.New(capacity, setFunc, getFunc, evictFunc)

	return c
}

func (c *Cache) Alloc(n int) fscache.Data {
	c.l.EnsureNBytes(n)
	data := newData(c.allocator, n, &c.size)
	return data
}

func (c *Cache) Get(ctx context.Context, key cache.CacheKey) (fscache.Data, bool) {
	return c.l.Get(ctx, key)
}

func (c *Cache) Set(ctx context.Context, key cache.CacheKey, value fscache.Data) error {
	data := value.(*Data)
	// freeze
	var freeze malloc.Freezer
	if data.deallocator.As(&freeze) {
		freeze.Freeze()
	}
	// set
	c.l.Set(ctx, key, data)
	return nil
}

func (c *Cache) Flush() {
	c.l.Flush()
}

func (c *Cache) Size() int64 {
	return c.size.Load()
}

func (c *Cache) Capacity() int64 {
	return c.l.Capacity()
}

func (c *Cache) Used() int64 {
	return c.l.Used()
}

func (c *Cache) Available() int64 {
	return c.l.Available()
}

func (c *Cache) DeletePaths(_ context.Context, _ []string) {
	//TODO
}

func (c *Cache) Evict(done chan int64) {
	c.l.Evict(done)
}
