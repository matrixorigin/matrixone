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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegexpCacheGet(t *testing.T) {
	cache := newRegexCache(0)
	cases := []struct {
		p string
		e bool
	}{
		{p: "*", e: true},
		{p: ".*", e: false},
	}
	for i := 0; i < 3; i++ {
		for _, c := range cases {
			r, err := cache.get(c.p)
			if c.e {
				assert.Error(t, err)
				assert.Nil(t, r)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, r)
			}
		}
	}
}

func TestRegexpCacheGC(t *testing.T) {
	ttl := time.Millisecond * 100
	cache := newRegexCache(ttl)
	beforeGet := time.Now()
	r, err := cache.get(".*")
	afterGet := time.Now()
	assert.Equal(t, 1, cache.count())
	assert.NoError(t, err)
	assert.NotNil(t, r)
	item, ok := cache.cache.Load(".*")
	assert.True(t, ok)
	if !ok {
		return
	}
	ci, ok := item.(cacheItem)
	assert.True(t, ok)
	if !ok {
		return
	}
	assert.False(t, ci.expireAt.Before(beforeGet.Add(ttl)), "cache entry should receive its configured TTL")
	assert.False(t, ci.expireAt.After(afterGet.Add(ttl)), "cache entry should receive its configured TTL")
	cache.cache.Store(".*", cacheItem{regexp: r, expireAt: time.Now().Add(-time.Nanosecond)})
	cache.gc()
	assert.Equal(t, 0, cache.count())
}
