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

	"golang.org/x/sys/cpu"
)

type allocatorShard[T Allocator] struct {
	allocator T
	_         cpu.CacheLinePad
}

// ShardedAllocator directs allocations to CPU-local allocators. Its shard
// configuration is private so the backing-size contract, once validated,
// cannot be invalidated through slicing, reordering, or mixing shard views.
type ShardedAllocator[T Allocator] struct {
	shards                   []allocatorShard[T]
	backingSizeContractState *shardedBackingSizeContractState
}

type shardedBackingSizeContractState struct {
	once     sync.Once
	contract BackingSizeContract
	err      error
}

func NewShardedAllocator[T Allocator](
	numShards int,
	newShard func() T,
) ShardedAllocator[T] {
	if numShards <= 0 {
		return ShardedAllocator[T]{}
	}

	ret := ShardedAllocator[T]{
		shards:                   make([]allocatorShard[T], numShards),
		backingSizeContractState: new(shardedBackingSizeContractState),
	}
	for i := range ret.shards {
		ret.shards[i].allocator = newShard()
	}
	return ret
}

var _ Allocator = ShardedAllocator[Allocator]{}

func (s ShardedAllocator[T]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	pid := runtime_procPin()
	runtime_procUnpin()
	return s.shards[pid%len(s.shards)].allocator.Allocate(size, hints)
}
