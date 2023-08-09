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
	"sync"
	"sync/atomic"
)

// RCPool represents a pool of reference counting objects
type RCPool[T any] struct {
	pool sync.Pool
}

type RCPoolItem[T any] struct {
	pool  *RCPool[T]
	count atomic.Int32
	Value T
}

func NewRCPool[T any](
	newFunc func() T,
) *RCPool[T] {
	var ret *RCPool[T]
	ret = &RCPool[T]{
		pool: sync.Pool{
			New: func() any {
				item := &RCPoolItem[T]{
					pool:  ret,
					Value: newFunc(),
				}
				return item
			},
		},
	}
	return ret
}

func (r *RCPool[T]) Get() *RCPoolItem[T] {
	item := r.pool.Get().(*RCPoolItem[T])
	if !item.count.CompareAndSwap(0, 1) {
		panic("invalid object reference count")
	}
	return item
}

func (r *RCPoolItem[T]) Retain() {
	r.count.Add(1)
}

func (r *RCPoolItem[T]) Release() {
	if c := r.count.Add(-1); c == 0 {
		if r.pool != nil {
			r.pool.pool.Put(r)
		}
	} else if c < 0 {
		panic("bad release")
	}
}
