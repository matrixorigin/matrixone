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
	"runtime"
	"sync/atomic"

	"github.com/dolthub/maphash"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func New[K comparable, V BytesLike](
	capacity fscache.CapacityFunc,
	postSet func(keySet K, valSet V),
	postGet func(key K, value V),
	postEvict func(keyEvicted K, valEvicted V),
) *LRU[K, V] {
	var l LRU[K, V]

	// How many cache shards should we create?
	//
	// Note that the probability two processors will try to access the same
	// shard at the same time increases superlinearly with the number of
	// processors (Eg, consider the brithday problem where each CPU is a person,
	// and each shard is a possible birthday).
	//
	// We could consider growing the number of shards superlinearly, but
	// increasing the shard count may reduce the effectiveness of the caching
	// algorithm if frequently-accessed blocks are insufficiently distributed
	// across shards. If a shard's size is smaller than a single frequently
	// scanned sstable, then the shard will be unable to hold the entire
	// frequently-scanned table in memory despite other shards still holding
	// infrequently accessed blocks.
	//
	// Experimentally, we've observed contention contributing to tail latencies
	// at 2 shards per processor. For now we use 4 shards per processor,
	// recognizing this may not be final word.
	m := 4 * runtime.GOMAXPROCS(0)
	pool := newPool[K, V](func() *lruItem[K, V] {
		return &lruItem[K, V]{}
	})
	l.capacity = capacity
	l.hasher = maphash.NewHasher[K]()
	l.shards = make([]shard[K, V], m)
	for i := range l.shards {
		l.shards[i].totalSize = &l.size
		l.shards[i].pool = pool
		l.shards[i].postSet = postSet
		l.shards[i].postGet = postGet
		l.shards[i].postEvict = postEvict
		l.shards[i].evicts = newList[K, V]()
		l.shards[i].capacity = func() int64 {
			ret := capacity() / int64(len(l.shards))
			if ret < 1 {
				ret = 1
			}
			return ret
		}
		l.shards[i].kv = make(map[K]*lruItem[K, V])
	}
	return &l
}

func (l *LRU[K, V]) Set(ctx context.Context, key K, value V) {
	h := l.hash(key)
	s := &l.shards[h%uint64(len(l.shards))]
	s.Set(ctx, key, value)
}

func (l *LRU[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	h := l.hash(key)
	s := &l.shards[h%uint64(len(l.shards))]
	return s.Get(ctx, h, key)
}

func (l *LRU[K, V]) Flush() {
	for i := range l.shards {
		l.shards[i].Flush()
	}
}

func (l *LRU[K, V]) DeletePaths(ctx context.Context, paths []string) {
	//TODO
}

func (l *LRU[K, V]) Capacity() int64 {
	return l.capacity()
}

func (l *LRU[K, V]) Used() int64 {
	return atomic.LoadInt64(&l.size)
}

func (l *LRU[K, V]) Available() int64 {
	return l.capacity() - atomic.LoadInt64(&l.size)
}

func (l *LRU[K, V]) hash(k K) uint64 {
	return l.hasher.Hash(k)
}

func (l *LRU[K, V]) EnsureNBytes(n int) {
	want := int64(n * 2)
	for {
		for i := 0; i < len(l.shards); i++ {
			if l.Available() > want {
				return
			}
			shard := &l.shards[i]
			shard.evictOne()
		}
	}
}

func (l *LRU[K, V]) Evict(done chan int64) {
	if done != nil && cap(done) < 1 {
		panic("should be buffered chan")
	}
	var target int64
	ok := make(chan int64, 1)
	for i := 0; i < len(l.shards); i++ {
		l.shards[i].evict(context.TODO(), ok)
		target += <-ok
	}
	if done != nil {
		done <- target
	}
}
