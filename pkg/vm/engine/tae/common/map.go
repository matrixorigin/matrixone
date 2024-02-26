// Copyright 2021 Matrix Origin
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

package common

import (
	"sync"

	"github.com/dolthub/maphash"
)

type mapShard[K comparable, V any] struct {
	sync.RWMutex
	kv map[K]V
}

type Map[K comparable, V any] struct {
	shards []*mapShard[K, V]
	hasher maphash.Hasher[K]
}

func NewMap[K comparable, V any](shardCount int) *Map[K, V] {
	m := &Map[K, V]{shards: make([]*mapShard[K, V], shardCount)}
	for i := 0; i < shardCount; i++ {
		m.shards[i] = &mapShard[K, V]{kv: make(map[K]V)}
	}
	m.hasher = maphash.NewHasher[K]()
	return m
}

func (m *Map[K, V]) getShard(key K) *mapShard[K, V] {
	h := m.hasher.Hash(key)
	return m.shards[h%uint64(len(m.shards))]
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	shard := m.getShard(key)
	shard.RLock()
	value, ok = shard.kv[key]
	shard.RUnlock()
	return
}

func (m *Map[K, V]) Store(key K, value V) {
	shard := m.getShard(key)
	shard.Lock()
	shard.kv[key] = value
	shard.Unlock()
}

func (m *Map[K, V]) Delete(key K) {
	shard := m.getShard(key)
	shard.Lock()
	delete(shard.kv, key)
	shard.Unlock()
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	shard := m.getShard(key)
	shard.Lock()
	value, loaded = shard.kv[key]
	if loaded {
		delete(shard.kv, key)
	}
	shard.Unlock()
	return
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	for _, shard := range m.shards {
		shard.RLock()
		for k, v := range shard.kv {
			if !f(k, v) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

func (m *Map[K, V]) DeleteIf(f func(key K, value V) bool) {
	for _, shard := range m.shards {
		shard.Lock()
		for k, v := range shard.kv {
			if f(k, v) {
				delete(shard.kv, k)
			}
		}
		shard.Unlock()
	}
}
