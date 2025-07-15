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

const numShards = 256

type ShardMap[K comparable, V any] struct {
	values map[K]V
	hashfn func(K) uint64
}

func NewShardMap[K comparable, V any](hashfn func(K) uint64) *ShardMap[K, V] {
	m := &ShardMap[K, V]{hashfn: hashfn,
		values: make(map[K]V, 1024),
	}
	return m
}

func (m *ShardMap[K, V]) Set(key K, value V, postfn func(V)) bool {

	_, ok := m.values[key]
	if ok {
		return false
	}

	m.values[key] = value

	if postfn != nil {
		// call postSet protected by mutex.Lock
		postfn(value)
	}
	return true
}

func (m *ShardMap[K, V]) Get(key K, postfn func(V)) (V, bool) {

	v, ok := m.values[key]
	if !ok {
		return v, ok
	}

	if postfn != nil {
		// call postGet protected the mutex RLock.
		postfn(v)
	}
	return v, ok
}

func (m *ShardMap[K, V]) Remove(key K) {
	delete(m.values, key)
}

func (m *ShardMap[K, V]) CompareAndDelete(key K, fn func(k1, k2 K) bool, postfn func(V)) {

	for k, v := range m.values {
		if fn(k, key) {
			delete(m.values, k)
			if postfn != nil {
				// call postfn to let parent know the item get deleted. (protected by mutex.Lock)
				postfn(v)
			}
		}
	}
}

func (m *ShardMap[K, V]) GetAndDelete(key K, postfn func(V)) (V, bool) {
	v, ok := m.values[key]
	if !ok {
		return v, ok
	}

	delete(m.values, key)

	if postfn != nil {
		// call postfn to let parent know the item get deleted. (protected by mutex.Lock)
		postfn(v)
	}

	return v, ok
}
