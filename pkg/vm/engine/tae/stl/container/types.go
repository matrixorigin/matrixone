package container

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Options struct {
	Capacity  int
	Allocator stl.MemAllocator
}

type stlVector[T any] struct {
	alloc    stl.MemAllocator
	node     stl.MemNode
	buf      []byte
	slice    []T
	capacity int
}

type strVector struct {
	alloc    stl.MemAllocator
	offNode  stl.MemNode
	lenNode  stl.MemNode
	dataNode stl.MemNode
	capacity int
}

// func New2[T any]()
