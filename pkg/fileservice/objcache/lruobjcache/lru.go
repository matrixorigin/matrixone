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

package lruobjcache

import (
	"container/list"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type LRU[K comparable] struct {
	sync.Mutex
	capacity  int64
	size      int64
	evicts    *list.List
	kv        map[K]*list.Element
	postSet   func(key K, value []byte, sz int64, isNewEntry bool)
	postEvict func(key K, value []byte, sz int64)
}

type lruItem[K comparable] struct {
	Key     K
	Value   []byte
	Size    int64
	NumRead int
}

func New[K comparable](
	capacity int64,
	postSet func(keySet K, valSet []byte, szSet int64, isNewEntry bool),
	postEvict func(keyEvicted K, valEvicted []byte, szEvicted int64,
	)) *LRU[K] {
	return &LRU[K]{
		capacity:  capacity,
		evicts:    list.New(),
		kv:        make(map[K]*list.Element),
		postSet:   postSet,
		postEvict: postEvict,
	}
}

func (l *LRU[K]) Set(ctx context.Context, key K, value []byte, size int64, preloading bool) {
	l.Lock()
	defer l.Unlock()

	var isNewEntry bool
	if elem, ok := l.kv[key]; ok {
		// replace
		isNewEntry = false
		item := elem.Value.(*lruItem[K])
		l.size -= item.Size
		l.size += size
		if !preloading {
			l.evicts.MoveToFront(elem)
		}
		item.Size = size
		item.Key = key
		item.Value = value

	} else {
		// insert
		isNewEntry = true
		item := &lruItem[K]{
			Key:   key,
			Value: value,
			Size:  size,
		}
		var elem *list.Element
		if preloading {
			elem = l.evicts.PushBack(item)
		} else {
			elem = l.evicts.PushFront(item)
		}
		l.kv[key] = elem
		l.size += size
	}

	if l.postSet != nil {
		l.postSet(key, value, size, isNewEntry)
	}

	l.evict(ctx)
}

func (l *LRU[K]) evict(ctx context.Context) {
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
		if l.size <= l.capacity {
			return
		}
		if len(l.kv) == 0 {
			return
		}

		elem := l.evicts.Back()
		for {
			if elem == nil {
				return
			}
			item := elem.Value.(*lruItem[K])
			l.size -= item.Size
			l.evicts.Remove(elem)
			delete(l.kv, item.Key)
			if l.postEvict != nil {
				l.postEvict(item.Key, item.Value, item.Size)
			}
			numEvict++
			if item.NumRead == 0 {
				numEvictWithZeroRead++
			}
			break
		}

	}
}

func (l *LRU[K]) Get(ctx context.Context, key K, preloading bool) (value []byte, size int64, ok bool) {
	l.Lock()
	defer l.Unlock()
	if elem, ok := l.kv[key]; ok {
		if !preloading {
			l.evicts.MoveToFront(elem)
		}
		item := elem.Value.(*lruItem[K])
		item.NumRead++
		return item.Value, item.Size, true
	}
	return nil, 0, false
}

func (l *LRU[K]) Flush() {
	l.Lock()
	defer l.Unlock()
	l.size = 0
	l.evicts = list.New()
	l.kv = make(map[K]*list.Element)
}

func (l *LRU[K]) Capacity() int64 {
	return l.capacity
}

func (l *LRU[K]) Used() int64 {
	l.Lock()
	defer l.Unlock()
	return l.size
}

func (l *LRU[K]) Available() int64 {
	l.Lock()
	defer l.Unlock()
	return l.capacity - l.size
}
