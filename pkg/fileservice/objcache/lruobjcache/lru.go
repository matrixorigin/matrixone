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
	"sync"
)

type LRU struct {
	sync.Mutex
	capacity  int64
	size      int64
	evicts    *list.List
	kv        map[any]*list.Element
	postEvict func(key any, value []byte, sz int64)
}

type lruItem struct {
	Key   any
	Value []byte
	Size  int64
}

func New(capacity int64, postEvict func(key any, value []byte, sz int64)) *LRU {
	return &LRU{
		capacity:  capacity,
		evicts:    list.New(),
		kv:        make(map[any]*list.Element),
		postEvict: postEvict,
	}
}

func (l *LRU) Set(key any, value []byte, size int64, preloading bool, postSet func(bool)) {
	l.Lock()
	defer l.Unlock()

	var isNewEntry bool
	if elem, ok := l.kv[key]; ok {
		// replace
		isNewEntry = false
		item := elem.Value.(*lruItem)
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
		item := &lruItem{
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
	if postSet != nil {
		postSet(isNewEntry)
	}

	l.evict()
	return
}

func (l *LRU) evict() {
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
			item := elem.Value.(*lruItem)
			l.size -= item.Size
			l.evicts.Remove(elem)
			delete(l.kv, item.Key)
			if l.postEvict != nil {
				l.postEvict(item.Key, item.Value, item.Size)
			}
			break
		}

	}
}

func (l *LRU) Get(key any, preloading bool) (value []byte, size int64, ok bool) {
	l.Lock()
	defer l.Unlock()
	if elem, ok := l.kv[key]; ok {
		if !preloading {
			l.evicts.MoveToFront(elem)
		}
		item := elem.Value.(*lruItem)
		return item.Value, item.Size, true
	}
	return nil, 0, false
}

func (l *LRU) Flush() {
	l.Lock()
	defer l.Unlock()
	l.size = 0
	l.evicts = list.New()
	l.kv = make(map[any]*list.Element)
}

func (l *LRU) Capacity() int64 {
	return l.capacity
}

func (l *LRU) Used() int64 {
	l.Lock()
	defer l.Unlock()
	return l.size
}

func (l *LRU) Available() int64 {
	l.Lock()
	defer l.Unlock()
	return l.capacity - l.size
}
