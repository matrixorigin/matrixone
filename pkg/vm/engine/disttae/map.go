// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"sync"
)

type Map[K, V any] struct {
	l sync.Mutex // for Update. will be removed after sync.Map.CompareAndSwap is available
	m sync.Map
}

func (m *Map[K, V]) Set(key K, value *V) {
	m.m.Store(key, value)
}

func (m *Map[K, V]) Get(key K) (v *V, ok bool) {
	value, ok := m.m.Load(key)
	if !ok {
		return
	}
	v = value.(*V)
	return
}

func (m *Map[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *Map[K, V]) Range(fn func(K, *V) bool) {
	m.m.Range(func(k, v any) bool {
		return fn(k.(K), v.(*V))
	})
}

func (m *Map[K, V]) Update(key K, fn func(*V) *V) {
	for {
		value, ok := m.m.Load(key)

		if !ok {
			newValue := fn(nil)
			_, loaded := m.m.LoadOrStore(key, newValue)
			if loaded {
				continue
			}
			break
		}

		newValue := fn(value.(*V))
		//TODO CompareAndSwap will be available in go1.20
		//if m.m.CompareAndSwap(key, value, newValue) {
		//	break
		//}

		m.l.Lock()
		v, ok := m.m.Load(key)
		if !ok {
			// not swapped
			m.l.Unlock()
			continue
		}
		if v != value {
			// not equal
			m.l.Unlock()
			continue
		}
		m.m.Store(key, newValue)
		m.l.Unlock()
		break

	}
}
