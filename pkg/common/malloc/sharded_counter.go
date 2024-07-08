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

type AtomicInteger[T any] interface {
	Add(T) T
	Load() T
}

type CounterInteger interface {
	int32 | int64 | uint32 | uint64
}

type ShardedCounter[T CounterInteger, A any, P interface {
	*A
	AtomicInteger[T]
}] struct {
	shards []shardedCounterShard[A]
}

type shardedCounterShard[T any] struct {
	value T
	_     cpu.CacheLinePad
}

func NewShardedCounter[T CounterInteger, A any, P interface {
	*A
	AtomicInteger[T]
}](shards int) *ShardedCounter[T, A, P] {
	return &ShardedCounter[T, A, P]{
		shards: make([]shardedCounterShard[A], shards),
	}
}

func (s *ShardedCounter[T, A, P]) Add(v T) {
	pid := runtime_procPin()
	runtime_procUnpin()
	shard := pid % len(s.shards)
	P(&s.shards[shard].value).Add(v)
}

func (s *ShardedCounter[T, A, P]) Load() (ret T) {
	for i := 0; i < len(s.shards); i++ {
		ret += P(&s.shards[i].value).Load()
	}
	return ret
}
