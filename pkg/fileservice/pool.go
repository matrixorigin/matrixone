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
	"sync/atomic"
	_ "unsafe"
)

type Pool[T any] struct {
	newFunc     func() T
	finallyFunc func(T)
	pool        []_PoolElem[T]
	capacity    uint32
}

type _PoolElem[T any] struct {
	Taken uint32
	Put   func(value T)
	Value T
}

func NewPool[T any](
	capacity uint32,
	newFunc func() T,
	resetFunc func(T),
	finallyFunc func(T),
) *Pool[T] {

	pool := &Pool[T]{
		capacity:    capacity,
		newFunc:     newFunc,
		finallyFunc: finallyFunc,
	}

	for i := uint32(0); i < capacity; i++ {
		i := i
		value := newFunc()
		pool.pool = append(pool.pool, _PoolElem[T]{
			Value: value,
			Put: func(resource T) {
				if resetFunc != nil {
					resetFunc(resource)
				}
				if !atomic.CompareAndSwapUint32(&pool.pool[i].Taken, 1, 0) {
					panic("bad put")
				}
			},
		})
	}

	return pool
}

func (p *Pool[T]) Get() (value T, put func(value T)) {
	for i := 0; i < 4; i++ {
		idx := fastrand() % p.capacity
		if atomic.CompareAndSwapUint32(&p.pool[idx].Taken, 0, 1) {
			value = p.pool[idx].Value
			put = p.pool[idx].Put
			return
		}
	}
	value = p.newFunc()
	if p.finallyFunc == nil {
		put = p.noopPut
	} else {
		put = func(value T) {
			p.finallyFunc(value)
		}
	}
	return
}

func (p *Pool[T]) noopPut(value T) {}

var bytesPoolDefaultBlockSize = NewPool(
	1024,
	func() []byte {
		return make([]byte, _DefaultBlockSize)
	},
	nil, nil,
)

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
