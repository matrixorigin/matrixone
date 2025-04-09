// Copyright 2024 Matrix Origin
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

package fifocache

import (
	"sync"

	"golang.org/x/sys/cpu"
)

const numShards = 256

type ShardMap[K comparable, V any] struct {
	shards [numShards]struct {
		sync.Mutex
		values map[K]V
		_      cpu.CacheLinePad
	}
	hashfn func(K) uint64
}

func NewShardMap[K comparable, V any](hashfn func(K) uint64) *ShardMap[K, V] {
	m := &ShardMap[K, V]{hashfn: hashfn}

	for i := range m.shards {
		m.shards[i].values = make(map[K]V, 1024)
	}
	return m
}

func (m *ShardMap[K, V]) Set(key K, value V) bool {

	s := &m.shards[m.hashfn(key)%numShards]
	s.Lock()
	defer s.Unlock()

	_, ok := s.values[key]
	if ok {
		return false
	}

	s.values[key] = value
	return true
}

func (m *ShardMap[K, V]) Get(key K) (V, bool) {

	s := &m.shards[m.hashfn(key)%numShards]
	s.Lock()
	defer s.Unlock()
	v, ok := s.values[key]
	return v, ok
}

func (m *ShardMap[K, V]) Remove(key K) {

	s := &m.shards[m.hashfn(key)%numShards]
	s.Lock()
	defer s.Unlock()
	delete(s.values, key)
}

func (m *ShardMap[K, V]) CompareAndDelete(key K, fn func(k1, k2 K) bool, postfn func(V)) {

	for _, s := range m.shards {
		s.Lock()
		for k, v := range s.values {
			if fn(k, key) {
				delete(s.values, k)
				postfn(v)
			}
		}
		s.Unlock()
	}
}
