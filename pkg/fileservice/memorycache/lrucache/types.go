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

package lrucache

import (
	"sync"

	"github.com/dolthub/maphash"
)

type LRU[K comparable, V BytesLike] struct {
	size     int64
	capacity int64
	shards   []shard[K, V]
	hasher   maphash.Hasher[K]
}

type shard[K comparable, V BytesLike] struct {
	sync.RWMutex
	capacity  int64
	size      int64
	totalSize *int64
	evicts    *list[K, V]
	pool      *pool[K, V]
	kv        map[K]*lruItem[K, V]
	postSet   func(key K, value V)
	postGet   func(key K, value V)
	postEvict func(key K, value V)
}

type BytesLike interface {
	Bytes() []byte
}

type Bytes []byte

func (b Bytes) Size() int64 {
	return int64(len(b))
}

func (b Bytes) Bytes() []byte {
	return b
}

func (b Bytes) SetBytes() {
	panic("not supported")
}

type lruItem[K comparable, V BytesLike] struct {
	Key     K
	Value   V
	Size    int64
	NumRead int64

	// list element
	next, prev *lruItem[K, V]
	list       *list[K, V]
}
