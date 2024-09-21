// Copyright 2021 - 2024 Matrix Origin
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

package clusterservice

import (
	"math"
	"regexp"
	"sync"
	"time"
)

const cacheTTL = time.Hour * 24

// cacheItem contains the cached regexp.Regexp instance and
// a time value which indicates the expiration of the instance.
type cacheItem struct {
	regexp   *regexp.Regexp
	expireAt time.Time
}

// regexpCache is the cache. We use sync.Map here because the reads
// is much more than writes. There is a TTL value for the instance in
// the cache.
type regexpCache struct {
	ttl   time.Duration
	cache sync.Map
}

// newRegexCache creates a new regexpCache instance. If the ttl is 0,
// set it to max.
func newRegexCache(ttl time.Duration) *regexpCache {
	if ttl == 0 {
		ttl = math.MaxInt64
	}
	return &regexpCache{
		ttl: ttl,
	}
}

// get returns the regexp.Regexp
func (c *regexpCache) get(pattern string) (*regexp.Regexp, error) {
	now := time.Now()
	if item, ok := c.cache.Load(pattern); ok {
		ci := item.(cacheItem)
		return ci.regexp, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	ci := cacheItem{
		regexp:   re,
		expireAt: now.Add(c.ttl),
	}
	c.cache.Store(pattern, ci)
	return re, nil
}

// gc cleans the cache and deletes the items which are expired.
func (c *regexpCache) gc() {
	now := time.Now()
	c.cache.Range(func(key, value interface{}) bool {
		ci := value.(cacheItem)
		if now.After(ci.expireAt) {
			c.cache.Delete(key)
		}
		return true
	})
}

// count returns the number of items in the cache.
func (c *regexpCache) count() int {
	var num int
	c.cache.Range(func(key, value any) bool {
		num++
		return true
	})
	return num
}
