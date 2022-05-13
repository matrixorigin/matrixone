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

package buffer

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	sq "github.com/yireyun/go-queue"
)

type EvictNode struct {
	Handle base.IEvictHandle
	Iter   uint64
}

type IEvictHolder interface {
	sync.Locker
	Enqueue(n *EvictNode)
	Dequeue() *EvictNode
}

type SimpleEvictHolder struct {
	Queue *sq.EsQueue
	sync.Mutex
}

const (
	EVICT_HOLDER_CAPACITY uint64 = 100000
)

type SimpleEvictHolderCtx struct {
	QCapacity uint64
}

func NewSimpleEvictHolder(ctx ...interface{}) IEvictHolder {
	c := EVICT_HOLDER_CAPACITY
	if len(ctx) > 0 {
		context := ctx[0].(*SimpleEvictHolderCtx)
		if context != nil {
			c = context.QCapacity
		}
	}
	holder := &SimpleEvictHolder{
		Queue: sq.NewQueue(uint32(c)),
	}
	return holder
}

func (holder *SimpleEvictHolder) Enqueue(node *EvictNode) {
	holder.Queue.Put(node)
}

func (holder *SimpleEvictHolder) Dequeue() *EvictNode {
	r, ok, _ := holder.Queue.Get()
	if !ok {
		return nil
	}
	return r.(*EvictNode)
}

func (node *EvictNode) String() string {
	return fmt.Sprintf("EvictNode(%v, %d)", node.Handle, node.Iter)
}

func (node *EvictNode) Unloadable(h base.IEvictHandle) bool {
	if node.Handle != h {
		panic("Logic error")
	}
	return h.Iteration() == node.Iter
}
