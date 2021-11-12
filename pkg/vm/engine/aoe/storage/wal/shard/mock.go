// Copyright 2021 Matrix Origin
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

package shard

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type MockShardIndexGenerator struct {
	Host    *MockIndexAllocator
	ShardId uint64
}

func (alloc *MockShardIndexGenerator) First() *Index {
	return &Index{
		ShardId: alloc.ShardId,
		Id:      SimpleIndexId(0),
	}
}

func (alloc *MockShardIndexGenerator) Alloc() uint64 {
	return alloc.Host.Alloc(alloc.ShardId)
}

func (alloc *MockShardIndexGenerator) Get() uint64 {
	return alloc.Host.Get(alloc.ShardId)
}

func (alloc *MockShardIndexGenerator) Next() *Index {
	return alloc.Host.Next(alloc.ShardId)
}

func (alloc *MockShardIndexGenerator) Curr() *Index {
	return alloc.Host.Curr(alloc.ShardId)
}

type MockIndexAllocator struct {
	sync.RWMutex
	Shards map[uint64]*common.IdAlloctor
}

func NewMockIndexAllocator() *MockIndexAllocator {
	return &MockIndexAllocator{
		Shards: make(map[uint64]*common.IdAlloctor),
	}
}

func (alloc *MockIndexAllocator) Get(shardId uint64) uint64 {
	alloc.RLock()
	defer alloc.RUnlock()
	shardAlloc := alloc.Shards[shardId]
	if shardAlloc == nil {
		return 0
	}
	return shardAlloc.Get()
}

func (alloc *MockIndexAllocator) Alloc(shardId uint64) uint64 {
	alloc.Lock()
	shardAlloc := alloc.Shards[shardId]
	if shardAlloc == nil {
		shardAlloc = new(common.IdAlloctor)
		alloc.Shards[shardId] = shardAlloc
	}
	alloc.Unlock()
	return shardAlloc.Alloc()
}

func (alloc *MockIndexAllocator) Curr(shardId uint64) *Index {
	id := alloc.Get(shardId)
	return &Index{
		ShardId: shardId,
		Id:      SimpleIndexId(id),
	}
}

func (alloc *MockIndexAllocator) Next(shardId uint64) *Index {
	id := alloc.Alloc(shardId)
	return &Index{
		ShardId: shardId,
		Id:      SimpleIndexId(id),
	}
}

func (alloc *MockIndexAllocator) Shard(shardId uint64) *MockShardIndexGenerator {
	return &MockShardIndexGenerator{
		ShardId: shardId,
		Host:    alloc,
	}
}
