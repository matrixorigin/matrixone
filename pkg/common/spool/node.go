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

package spool

import (
	"sync"
	"sync/atomic"
)

type node[T Element] struct {
	mu   sync.Mutex
	cond *sync.Cond
	nodeState[T]
}

type nodeState[T Element] struct {
	next        *node[T]
	value       T
	valueOK     bool
	maxConsumer atomic.Int64
	target      *Cursor[T]
	numCursors  atomic.Int64
	stop        bool
}

func (s *Spool[T]) recycleNode(node *node[T]) {
	node.nodeState = nodeState[T]{}
	s.nodePool.Put(node)
}

func (s *Spool[T]) newNode() *node[T] {
	return s.nodePool.Get().(*node[T])
}
