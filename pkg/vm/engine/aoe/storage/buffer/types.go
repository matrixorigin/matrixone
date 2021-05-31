package buf

import (
	"io"
)

type IMemoryPool interface {
	MakeNode(uint64) (node *Node)
	FreeNode(*Node)
	GetCapacity() uint64
	SetCapacity(uint64) error
	GetUsage() uint64
}

type Node struct {
	Data     []byte
	Capacity uint64
	Pool     IMemoryPool
}

type IBuffer interface {
	io.Closer
	Clear()
	GetCapacity() uint64
	GetDataNode() *Node
}

type Buffer struct {
	Node       *Node
	DataSize   uint64
	HeaderSize uint64
}
