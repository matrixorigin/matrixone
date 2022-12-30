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

package memorytable

import "sync/atomic"

//TODO move to common?

// Atomic is type-safe wrapper of atomic.Value
type Atomic[T any] struct {
	value atomic.Value
}

// NewAtomic creates a new atomic variable with passed value
func NewAtomic[T any](value T) *Atomic[T] {
	t := new(Atomic[T])
	t.value.Store(value)
	return t
}

// Load loads the value
func (a *Atomic[T]) Load() T {
	return a.value.Load().(T)
}

// Store stores a new value
func (a *Atomic[T]) Store(value T) {
	a.value.Store(value)
}
