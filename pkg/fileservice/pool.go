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

import "sync/atomic"

type Pool[T any] struct {
	newFunc  func() T
	pool     []_PoolElem[T]
	capacity int32
	n        int32
}

type _PoolElem[T any] struct {
	Taken int32
	Put   func()
	Value T
}

func NewPool[T any](
	capacity int32,
	newFunc func() T,
) *Pool[T] {

	pool := &Pool[T]{
		capacity: capacity,
		newFunc:  newFunc,
	}

	for i := int32(0); i < capacity; i++ {
		i := i
		pool.pool = append(pool.pool, _PoolElem[T]{
			Value: newFunc(),
			Put: func() {
				if !atomic.CompareAndSwapInt32(&pool.pool[i].Taken, 1, 0) {
					panic("bad put")
				}
			},
		})
	}

	return pool
}

func (p *Pool[T]) Get() (value T, put func()) {
	for i := 0; i < 4; i++ {
		idx := atomic.AddInt32(&p.n, 1) % p.capacity
		if atomic.CompareAndSwapInt32(&p.pool[idx].Taken, 0, 1) {
			value = p.pool[idx].Value
			put = p.pool[idx].Put
			return
		}
	}
	value = p.newFunc()
	put = noopPut
	return
}

func noopPut() {}

var bytesPoolDefaultBlockSize = NewPool(
	1024,
	func() []byte {
		return make([]byte, _DefaultBlockSize)
	},
)
