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

// Pinned represents a pinned value that will not evict in LRU
type Pinned[T any] struct {
	Value T
	unpin int32
}

// Pin creates a Pinned value
func Pin[T any](value T) *Pinned[T] {
	return &Pinned[T]{
		Value: value,
	}
}

// Unpin marks the value as unpin to be evictable in LRU
func (p *Pinned[T]) Unpin() {
	atomic.StoreInt32(&p.unpin, 1)
}

// IsPinned returns whether the value is pinned
func (p *Pinned[T]) IsPinned() bool {
	return atomic.LoadInt32(&p.unpin) == 0
}
