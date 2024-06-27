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

package malloc

import (
	"sync"
)

type ClosureDeallocator[T any] struct {
	argument T
	fn       func(Hints, T)
}

func (a *ClosureDeallocator[T]) SetArgument(arg T) {
	a.argument = arg
}

var _ Deallocator = &ClosureDeallocator[int]{}

func (a *ClosureDeallocator[T]) Deallocate(hints Hints) {
	a.fn(hints, a.argument)
}

type ClosureDeallocatorPool[T any] struct {
	pool sync.Pool
}

func NewClosureDeallocatorPool[T any](
	deallocateFunc func(Hints, T),
) *ClosureDeallocatorPool[T] {
	ret := new(ClosureDeallocatorPool[T])

	ret.pool.New = func() any {
		closure := new(ClosureDeallocator[T])
		closure.fn = func(hints Hints, args T) {
			deallocateFunc(hints, args)
			ret.pool.Put(closure)
		}
		return closure
	}

	return ret
}

func (c *ClosureDeallocatorPool[T]) Get(args T) Deallocator {
	closure := c.pool.Get().(*ClosureDeallocator[T])
	closure.SetArgument(args)
	return closure
}
