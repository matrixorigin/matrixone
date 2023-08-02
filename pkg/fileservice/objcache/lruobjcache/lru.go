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
	"time"
)

type LRU struct {
	sync.RWMutex
	capacity  int64
	size      int64
	evicts    *list.List
	kv        map[any]*list.Element
	postSet   func(key any, value []byte, sz int64, isNewEntry bool)
	postEvict func(key any, value []byte, sz int64)
	jobsChan  chan func()
	closed    chan struct{}
}

type lruItem struct {
	Key          any
	Value        []byte
	Size         int64
	LastPromoted time.Time
}

var (
	minPromotionInterval = time.Second * 60
)

func New(capacity int64,
	postSet func(keySet any, valSet []byte, szSet int64, isNewEntry bool),
	postEvict func(keyEvicted any, valEvicted []byte, szEvicted int64)) *LRU {
	ch := make(chan func(), 32)
	closed := make(chan struct{})
	go func() {
		for {
			select {
			case fn := <-ch:
				fn()
			case <-closed:
				return
			}
		}
	}()
	return &LRU{
		capacity:  capacity,
		evicts:    list.New(),
		kv:        make(map[any]*list.Element),
		postSet:   postSet,
		postEvict: postEvict,
		jobsChan:  ch,
		closed:    closed,
	}
}

func (l *LRU) Close() error {
	close(l.closed)
	return nil
}

func (l *LRU) Set(key any, value []byte, size int64, preloading bool) {
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

	if l.postSet != nil {
		l.postSet(key, value, size, isNewEntry)
	}

	l.evict()
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
	l.RLock()
	elem, ok := l.kv[key]
	if !ok {
		l.RUnlock()
		return nil, 0, false
	}

	item := elem.Value.(*lruItem)
	value = item.Value
	size = item.Size
	ok = true
	lastPromoted := item.LastPromoted
	l.RUnlock()

	if !preloading && time.Since(lastPromoted) > minPromotionInterval {
		select {
		case l.jobsChan <- func() {
			l.Lock()
			item.LastPromoted = time.Now()
			l.evicts.MoveToFront(elem)
			l.Unlock()
		}:
		case <-l.closed:
			return
		default:
			// skip promotion if busy
		}
	}

	return
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
