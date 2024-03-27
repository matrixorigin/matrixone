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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache/lrucache/internal/hashmap"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

func (s *shard[K, V]) Set(ctx context.Context, h uint64, key K, value V) {
	size := int64(len(value.Bytes()))
	item := s.allocItem()
	item.h = h
	item.Key = key
	item.Value = value
	item.Size = size
	s.Lock()
	defer s.Unlock()
	if _, ok := s.kv.Get(h, key); ok {
		return
	}
	s.kv.Set(h, key, item)
	s.size += size
	atomic.AddInt64(s.totalSize, size)
	s.evicts.PushFront(item)
	if s.postSet != nil {
		s.postSet(key, value)
	}
	if atomic.LoadInt64(&s.size) >= s.capacity {
		s.evict(ctx)
	}
}

func (s *shard[K, V]) evict(ctx context.Context) {
	var numEvict, numEvictWithZeroRead int64
	defer func() {
		if numEvict > 0 || numEvictWithZeroRead > 0 {
			perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
				set.FileService.Cache.LRU.Evict.Add(numEvict)
				set.FileService.Cache.LRU.EvictWithZeroRead.Add(numEvictWithZeroRead)
			})
		}
	}()

	for {
		if s.size <= s.capacity {
			return
		}
		if s.kv.Len() == 0 {
			return
		}

		elem := s.evicts.Back()
		for {
			if elem == nil {
				return
			}
			s.kv.Delete(elem.h, elem.Key)
			s.size -= elem.Size
			atomic.AddInt64(s.totalSize, -elem.Size)
			s.evicts.Remove(elem)
			if s.postEvict != nil {
				s.postEvict(elem.Key, elem.Value)
			}
			numEvict++
			if atomic.LoadInt64(&elem.NumRead) == 0 {
				numEvictWithZeroRead++
			}
			s.freeItem(elem)
			break
		}

	}
}

func (s *shard[K, V]) Get(ctx context.Context, h uint64, key K) (value V, ok bool) {
	s.RLock()
	defer s.RUnlock()
	if elem, ok := s.kv.Get(h, key); ok {
		atomic.AddInt64(&elem.NumRead, 1)
		if s.postGet != nil {
			s.postGet(key, elem.Value)
		}
		return elem.Value, true
	}
	return
}

func (s *shard[K, V]) Flush() {
	s.Lock()
	defer s.Unlock()
	for elem := s.evicts.Back(); elem != nil; elem = s.evicts.Back() {
		s.kv.Delete(elem.h, elem.Key)
		s.evicts.Remove(elem)
		if s.postEvict != nil {
			s.postEvict(elem.Key, elem.Value)
		}
		s.freeItem(elem)
	}
	s.size = 0
	s.evicts = newList[K, V]()
	s.kv = hashmap.New[K, lruItem[K, V]](int(s.capacity))
}

func (s *shard[K, V]) allocItem() *lruItem[K, V] {
	return s.pool.get()
}

func (s *shard[K, V]) freeItem(item *lruItem[K, V]) {
	item.NumRead = 0
	s.pool.put(item)
}
