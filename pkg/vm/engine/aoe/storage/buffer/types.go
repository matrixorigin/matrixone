package buf

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
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
	Marshall() ([]byte, error)
	Unmarshall([]byte) error
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
