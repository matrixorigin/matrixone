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

type ClosureDeallocator[T any, P interface {
	*T
	TraitHolder
}] struct {
	argument T
	fn       func(Hints, *T)
}

func (a *ClosureDeallocator[T, P]) SetArgument(arg T) {
	a.argument = arg
}

func (a *ClosureDeallocator[T, P]) Deallocate(hints Hints) {
	a.fn(hints, &a.argument)
}

func (a *ClosureDeallocator[T, P]) As(target Trait) bool {
	return P(&a.argument).As(target)
}

type ClosureDeallocatorPool[T any, P interface {
	*T
	TraitHolder
}] struct {
	pool sync.Pool
}

func NewClosureDeallocatorPool[T any, P interface {
	*T
	TraitHolder
}](
	deallocateFunc func(Hints, *T),
) *ClosureDeallocatorPool[T, P] {
	ret := new(ClosureDeallocatorPool[T, P])

	ret.pool.New = func() any {
		closure := new(ClosureDeallocator[T, P])
		closure.fn = func(hints Hints, args *T) {
			deallocateFunc(hints, args)
			ret.pool.Put(closure)
		}
		return closure
	}

	return ret
}

func (c *ClosureDeallocatorPool[T, P]) Get(args T) Deallocator {
	closure := c.pool.Get().(*ClosureDeallocator[T, P])
	closure.SetArgument(args)
	return closure
}

func (c *ClosureDeallocatorPool[T, P]) Get2(fn func(*T) T) Deallocator {
	closure := c.pool.Get().(*ClosureDeallocator[T, P])
	closure.SetArgument(fn(&closure.argument))
	return closure
}
