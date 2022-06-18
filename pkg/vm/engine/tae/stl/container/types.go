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

func New[T any](opts ...*Options) *Vector[T] {
	var v T
	_, ok := any(v).([]byte)
	if !ok {
		return &Vector[T]{
			Vector: NewStdVector[T](opts...),
		}
	}
	return &Vector[T]{
		Vector: NewStrVector[T](opts...),
	}
}
