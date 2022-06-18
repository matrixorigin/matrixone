package container

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Options[T any] struct {
	Capacity  int
	Allocator stl.MemAllocator[T]
}

type stlVector[T any] struct {
	alloc    stl.MemAllocator[T]
	node     stl.MemNode
	buf      []byte
	slice    []T
	capacity int
}
