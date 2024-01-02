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
	"container/list"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type LRU[K comparable, V BytesLike] struct {
	sync.Mutex
	capacity  int64
	size      int64
	evicts    *list.List
	kv        map[K]*list.Element
	postSet   func(key K, value V)
	postGet   func(key K, value V)
	postEvict func(key K, value V)
}

type BytesLike interface {
	Bytes() []byte
}

type Bytes []byte

func (b Bytes) Size() int64 {
	return int64(len(b))
}

func (b Bytes) Bytes() []byte {
	return b
}

func (b Bytes) SetBytes() {
	panic("not supported")
}

type lruItem[K comparable, V BytesLike] struct {
	Key     K
	Value   V
	Size    int64
	NumRead int
}

func New[K comparable, V BytesLike](
	capacity int64,
	postSet func(keySet K, valSet V),
	postGet func(key K, value V),
	postEvict func(keyEvicted K, valEvicted V),
) *LRU[K, V] {
	return &LRU[K, V]{
		capacity:  capacity,
		evicts:    list.New(),
		kv:        make(map[K]*list.Element),
		postSet:   postSet,
		postGet:   postGet,
		postEvict: postEvict,
	}
}

func (l *LRU[K, V]) Set(ctx context.Context, key K, value V) {
	l.Lock()
	defer l.Unlock()

	if elem, ok := l.kv[key]; ok {
		// replace
		item := elem.Value.(*lruItem[K, V])
		l.size -= item.Size
		size := int64(len(value.Bytes()))
		l.size += size
		item.Size = size
		item.Key = key
		if l.postEvict != nil {
			l.postEvict(item.Key, item.Value)
		}
		item.Value = value

	} else {
		// insert
		size := int64(len(value.Bytes()))
		item := &lruItem[K, V]{
			Key:   key,
			Value: value,
			Size:  size,
		}
		elem := l.evicts.PushFront(item)
		l.kv[key] = elem
		l.size += size
	}

	if l.postSet != nil {
		l.postSet(key, value)
	}

	l.evict(ctx)
}

func (l *LRU[K, V]) evict(ctx context.Context) {
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
			item := elem.Value.(*lruItem[K, V])
			l.size -= item.Size
			l.evicts.Remove(elem)
			delete(l.kv, item.Key)
			if l.postEvict != nil {
				l.postEvict(item.Key, item.Value)
			}
			numEvict++
			if item.NumRead == 0 {
				numEvictWithZeroRead++
			}
			break
		}

	}
}

func (l *LRU[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	l.Lock()
	defer l.Unlock()
	if elem, ok := l.kv[key]; ok {
		item := elem.Value.(*lruItem[K, V])
		item.NumRead++
		if l.postGet != nil {
			l.postGet(key, item.Value)
		}
		return item.Value, true
	}
	return
}

func (l *LRU[K, V]) Flush() {
	l.Lock()
	defer l.Unlock()
	l.size = 0
	l.evicts = list.New()
	l.kv = make(map[K]*list.Element)
}

func (l *LRU[K, V]) DeletePaths(ctx context.Context, paths []string) {
}

func (l *LRU[K, V]) Capacity() int64 {
	return l.capacity
}

func (l *LRU[K, V]) Used() int64 {
	l.Lock()
	defer l.Unlock()
	return l.size
}

func (l *LRU[K, V]) Available() int64 {
	l.Lock()
	defer l.Unlock()
	return l.capacity - l.size
}
