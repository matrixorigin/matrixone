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

package buf

import (
	"io"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type MemoryFreeFunc func(IMemoryNode)
type MemoryAllocFunc func() (mem []byte, err error)
type MemoryNodeConstructor func(common.IVFile, bool, MemoryFreeFunc) IMemoryNode

type IMemoryPool interface {
	Free(size uint64)
	Alloc(vf common.IVFile, useCompress bool, constructor MemoryNodeConstructor) IMemoryNode
	GetCapacity() uint64
	SetCapacity(uint64) error
	GetUsage() uint64
}

type IMemoryNode interface {
	io.ReaderFrom
	io.WriterTo
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	FreeMemory()
	Reset()
	GetMemorySize() uint64
	GetMemoryCapacity() uint64
}

var WithFreeWithPool = func(pool IMemoryPool) MemoryFreeFunc {
	return func(node IMemoryNode) {
		pool.Free(node.GetMemoryCapacity())
		node.Reset()
	}
}

type IBuffer interface {
	io.Closer
	GetCapacity() uint64
	GetDataNode() IMemoryNode
}

type Buffer struct {
	Node IMemoryNode
}
