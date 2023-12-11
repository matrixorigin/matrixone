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

import "sync"

// pool is a generic wrapper around sync.Pool.
type pool[K comparable, V BytesLike] struct {
	pool sync.Pool
}

// newPool is a generic wrapper around sync.Pool's New method.
func newPool[K comparable, V BytesLike](fn func() *lruItem[K, V]) *pool[K, V] {
	return &pool[K, V]{
		pool: sync.Pool{New: func() any { return fn() }},
	}
}

// Get is a generic wrapper around sync.Pool's Get method.
func (p *pool[K, V]) get() *lruItem[K, V] {
	return p.pool.Get().(*lruItem[K, V])
}

// Get is a generic wrapper around sync.Pool's Put method.
func (p *pool[K, V]) put(v *lruItem[K, V]) {
	p.pool.Put(v)
}
