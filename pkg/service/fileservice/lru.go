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

package fileservice

import (
	"container/list"
	"sync"
)

type LRU struct {
	sync.Mutex
	capacity int
	size     int
	evicts   *list.List
	kv       map[any]*list.Element
}

type lruItem struct {
	Key   any
	Value any
	Size  int
}

func NewLRU(capacity int) *LRU {
	return &LRU{
		capacity: capacity,
		evicts:   list.New(),
		kv:       make(map[any]*list.Element),
	}
}

func (l *LRU) Set(key any, value any, size int) {
	l.Lock()
	defer l.Unlock()

	if elem, ok := l.kv[key]; ok {
		// replace
		item := elem.Value.(*lruItem)
		l.size -= item.Size
		l.size += size
		l.evicts.MoveToFront(elem)
		item.Size = size
		item.Key = key
		item.Value = value

	} else {
		// insert
		item := &lruItem{
			Key:   key,
			Value: value,
			Size:  size,
		}
		elem := l.evicts.PushFront(item)
		l.kv[key] = elem
		l.size += size
	}

	l.evict()
}

func (l *LRU) evict() {
	for {
		if l.size < l.capacity {
			return
		}
		if len(l.kv) == 0 {
			return
		}
		elem := l.evicts.Back()
		if elem == nil {
			return
		}
		item := elem.Value.(*lruItem)
		l.size -= item.Size
		l.evicts.Remove(elem)
		delete(l.kv, item.Key)
	}
}

func (l *LRU) Get(key any) (value any, ok bool) {
	l.Lock()
	defer l.Unlock()
	if elem, ok := l.kv[key]; ok {
		l.evicts.MoveToFront(elem)
		item := elem.Value.(*lruItem)
		return item.Value, true
	}
	return nil, false
}
