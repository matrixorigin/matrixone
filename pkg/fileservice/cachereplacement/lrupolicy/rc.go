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

package lrupolicy

import "sync/atomic"

// RC represents a reference counted value that will not evict in LRU if refs is greater than 0
type RC[T any] struct {
	refs  int64
	Value T
}

// NewRC creates an RC value with 0 reference
func NewRC[T any](value T) *RC[T] {
	return &RC[T]{
		Value: value,
		refs:  0,
	}
}

// IncRef increases reference count
func (r *RC[T]) IncRef() {
	atomic.AddInt64(&r.refs, 1)
}

// DecRef decreases reference count
func (r *RC[T]) DecRef() {
	atomic.AddInt64(&r.refs, -1)
}

// RefCount returns reference count
func (r *RC[T]) RefCount() int64 {
	return atomic.LoadInt64(&r.refs)
}
