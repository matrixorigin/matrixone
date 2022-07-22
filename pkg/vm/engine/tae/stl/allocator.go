// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultAllocator MemAllocator

type MemNode interface {
	GetBuf() []byte
	Size() int
}

type MemAllocator interface {
	Alloc(size int) MemNode
	Free(n MemNode)
	Usage() int
	String() string
}

func init() {
	simple := NewSimpleAllocator()
	// DefaultAllocator = DebugOneAllocator(simple)
	DefaultAllocator = simple
}

func NewSimpleAllocator() MemAllocator {
	allocator := new(simpleAllocator)
	allocator.pool = common.NewMempool(common.UNLIMIT)
	return allocator
}

type simpleAllocator struct {
	pool *common.Mempool
}

func (alloc *simpleAllocator) Alloc(size int) MemNode {
	return alloc.pool.Alloc(uint64(size))
}

func (alloc *simpleAllocator) Free(n MemNode) {
	alloc.pool.Free(n.(*common.MemNode))
}

func (alloc *simpleAllocator) Usage() int {
	return int(alloc.pool.Usage())
}

func (alloc *simpleAllocator) String() string {
	return alloc.pool.String()
}
