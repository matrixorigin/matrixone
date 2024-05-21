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

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache/lrucache"
	cache "github.com/matrixorigin/matrixone/pkg/pb/query"
)

func NewCache(
	capacity int64,
	postSet func(key cache.CacheKey, value CacheData),
	postGet func(key cache.CacheKey, value CacheData),
	postEvict func(key cache.CacheKey, value CacheData),
) *Cache {
	var c Cache

	setFunc := func(key cache.CacheKey, value *Data) {
		var r RCBytes

		value.acquire()
		if postSet != nil {
			r.d = value
			r.size = &c.size
			postSet(key, r)
		}
	}
	getFunc := func(key cache.CacheKey, value *Data) {
		var r RCBytes

		value.acquire()
		if postGet != nil {
			r.d = value
			r.size = &c.size
			postGet(key, r)
		}
	}
	evictFunc := func(key cache.CacheKey, value *Data) {
		var r RCBytes

		value.release(&c.size)
		if postEvict != nil {
			r.d = value
			r.size = &c.size
			postEvict(key, r)
		}
	}
	c.l = lrucache.New(capacity, setFunc, getFunc, evictFunc)
	c.allocator = malloc.NewDefault(nil)
	return &c
}

func (c *Cache) Alloc(n int) CacheData {
	d := newData(c.allocator, n, &c.size)
	return RCBytes{d: d, size: &c.size}
}

func (c *Cache) Get(ctx context.Context, key cache.CacheKey) (CacheData, bool) {
	var r RCBytes

	v, ok := c.l.Get(ctx, key)
	if ok { // find the value, set the value to the return value
		r.d = v
		r.size = &c.size
	}
	return r, ok
}

func (c *Cache) Set(ctx context.Context, key cache.CacheKey, value CacheData) error {
	if r, ok := value.(RCBytes); ok {
		c.l.Set(ctx, key, r.d)
	}
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
