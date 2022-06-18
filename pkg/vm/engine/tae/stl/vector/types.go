package vector

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

func New[T any](opts ...*Options[T]) *stlVector[T] {
	vec := &stlVector[T]{
		buf:   make([]byte, 0),
		slice: make([]T, 0),
	}
	var capacity int
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		vec.alloc = opt.Allocator
	}
	if vec.alloc == nil {
		vec.alloc = stl.NewSimpleAllocator[T]()
	}
	if capacity == 0 {
		capacity = 4
	}
	vec.tryExpand(capacity)
	return vec
}
