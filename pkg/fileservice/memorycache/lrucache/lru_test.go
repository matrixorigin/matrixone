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

package lrucache

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
)

func TestLRU(t *testing.T) {
	l := New[int, Bytes](1, nil, nil, nil)
	ctx := context.Background()

	l.Set(ctx, 1, []byte{42})
	val, _ := l.Get(ctx, 1)
	assert.Equal(t, Bytes([]byte{42}), val)

	l.Set(ctx, 2, []byte{43})
	val, _ = l.Get(ctx, 2)
	assert.Equal(t, Bytes([]byte{43}), val)
}

func TestLRUCallbacks(t *testing.T) {
	ctx := context.Background()

	postSetInvokedMap := make(map[int]bool)

	evictEntryMap := make(map[int][]byte)
	postEvictInvokedMap := make(map[int]bool)

	l := New(1,
		func(key int, _ Bytes) {
			postSetInvokedMap[key] = true
		},
		nil,
		func(key int, value Bytes) {
			evictEntryMap[key] = value
			postEvictInvokedMap[key] = true
		})
	s := &l.shards[0]
	s.capacity = 1

	// PostSet
	h := l.hasher.Hash(1)
	s.Set(ctx, h, 1, []byte{42})
	assert.True(t, postSetInvokedMap[1])
	postSetInvokedMap[1] = false // resetting
	assert.False(t, postEvictInvokedMap[1])

	// PostSet and PostEvict
	h = l.hasher.Hash(2)
	s.Set(ctx, h, 2, []byte{44})
	assert.True(t, postEvictInvokedMap[1])        //postEvictInvokedMap is updated by PostEvict
	assert.Equal(t, []byte{42}, evictEntryMap[1]) //evictEntryMap is updated by PostEvict
}

func BenchmarkLRUSet(b *testing.B) {
	var k query.CacheKey

	k.Path = "tmp"
	ctx := context.Background()
	const capacity = 1024
	l := New[query.CacheKey, Bytes](capacity, nil, nil, nil)
	v := make([]byte, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k.Offset = int64(i) % (capacity)
		v[0] = byte(i)
		l.Set(ctx, k, v)
	}
}

func BenchmarkLRUParallelSet(b *testing.B) {
	ctx := context.Background()
	const capacity = 1024
	l := New[query.CacheKey, Bytes](capacity, nil, nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var k query.CacheKey

		k.Path = "tmp"
		v := make([]byte, 1)
		for i := 0; pb.Next(); i++ {
			k.Offset = int64(i) % (capacity)
			v[0] = byte(i)
			l.Set(ctx, k, v)
		}
	})
}

func BenchmarkLRUParallelSetOrGet(b *testing.B) {
	ctx := context.Background()
	const capacity = 1024
	l := New[query.CacheKey, Bytes](capacity, nil, nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var k query.CacheKey

		k.Path = "tmp"
		v := make([]byte, 1)
		for i := 0; pb.Next(); i++ {
			k.Offset = int64(i) % (capacity)
			v[0] = byte(i)
			l.Set(ctx, k, v)
			if i%2 == 0 {
				l.Get(ctx, k)
			}
		}
	})
}

func BenchmarkLRULargeParallelSetOrGet(b *testing.B) {
	var wg sync.WaitGroup

	ctx := context.Background()
	const capacity = 1024
	l := New[query.CacheKey, Bytes](capacity, nil, nil, nil)
	b.ResetTimer()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var k query.CacheKey
			k.Path = "tmp"
			v := make([]byte, 1)
			for i := 0; i < b.N; i++ {
				k.Offset = int64(i) % (capacity * 10)
				v[0] = byte(i)
				l.Set(ctx, k, v)
				if i%2 == 0 {
					l.Get(ctx, k)
				}
			}
		}()
	}
	wg.Wait()
}
