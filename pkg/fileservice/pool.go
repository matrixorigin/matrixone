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
	resetFunc   func(T)
	pool        []_PoolElem[T]
	capacity    uint32
}

type _PoolElem[T any] struct {
	Taken atomic.Uint32
	Value T
}

type PutBack[T any] struct {
	idx  int
	ptr  *T
	pool *Pool[T]
}

func (pb PutBack[T]) Put() {
	if pb.pool != nil {
		pb.pool.Put(pb.idx, pb.ptr)
	}
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
		resetFunc:   resetFunc,
	}

	pool.pool = make([]_PoolElem[T], capacity)

	for i := uint32(0); i < capacity; i++ {
		pool.pool[i].Value = newFunc()
	}
	return pool
}

func (p *Pool[T]) Get(ptr *T) PutBack[T] {
	for i := 0; i < 4; i++ {
		idx := fastrand() % p.capacity
		if p.pool[idx].Taken.CompareAndSwap(0, 1) {
			*ptr = p.pool[idx].Value
			return PutBack[T]{int(idx), &p.pool[idx].Value, p}
		}
	}

	value := p.newFunc()
	*ptr = value
	return PutBack[T]{-1, &value, p}
}

func (p *Pool[T]) Put(idx int, ptr *T) {
	if idx >= 0 {
		if p.resetFunc != nil {
			p.resetFunc(p.pool[idx].Value)
		}
		if !p.pool[idx].Taken.CompareAndSwap(1, 0) {
			panic("bad put")
		}
	} else {
		if p.finallyFunc != nil {
			p.finallyFunc(*ptr)
		}
	}
}

var bytesPoolDefaultBlockSize = NewPool(
	1024,
	func() []byte {
		return make([]byte, _DefaultBlockSize)
	},
	nil, nil,
)

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
