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

// type strVector struct {
// 	alloc    stl.MemAllocator
// 	offNode  stl.MemNode
// 	lenNode  stl.MemNode
// 	dataNode stl.MemNode
// 	capacity int
// }

type Vector[T any] struct {
	stl.Vector[T]
}

func NewVector[T any](opts ...*Options) *Vector[T] {
	var v T
	_, ok := any(v).([]byte)
	if ok {
		return &Vector[T]{
			Vector: NewStdVector[T](opts...),
			// stdVector: New[T](opts...),
		}
	}
	return nil
}
