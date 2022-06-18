package container

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Options struct {
	Capacity  int
	Allocator stl.MemAllocator
}

type stdVector[T any] struct {
	alloc    stl.MemAllocator
	node     stl.MemNode
	buf      []byte
	slice    []T
	capacity int
}

type strVector[T any] struct {
	offsets *stdVector[uint32]
	lengths *stdVector[uint32]
	data    *stdVector[byte]
}

type Vector[T any] struct {
	stl.Vector[T]
}
