package container

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Options struct {
	Capacity  int
	Allocator stl.MemAllocator
	Data      *stl.Bytes
}

func (opts *Options) HasData() bool { return opts.Data != nil }
func (opts *Options) DataSize() int {
	if opts.Data == nil {
		return 0
	}
	return len(opts.Data.Data)
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

type stdSpan[T any] struct {
	slice []T
	buf   []byte
}

type strSpan[T any] struct {
	buf     []byte
	offsets []uint32
	lengths []uint32
}
