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

import "golang.org/x/sys/cpu"

type ShardedAllocator[T Allocator] []allocatorShard[T]

type allocatorShard[T Allocator] struct {
	Allocator T
	_         cpu.CacheLinePad
}

func NewShardedAllocator[T Allocator](
	numShards int,
	newShard func() T,
) ShardedAllocator[T] {
	var ret ShardedAllocator[T]
	for i := 0; i < numShards; i++ {
		ret = append(ret, allocatorShard[T]{
			Allocator: newShard(),
		})
	}
	return ret
}

var _ Allocator = ShardedAllocator[Allocator]{}

func (s ShardedAllocator[T]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	pid := runtime_procPin()
	runtime_procUnpin()
	return s[pid%len(s)].Allocator.Allocate(size, hints)
}
