package containers

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

type StdVector[T any] struct {
	alloc    stl.MemAllocator
	node     stl.MemNode
	buf      []byte
	slice    []T
	capacity int
}

type StrVector[T any] struct {
	offsets *StdVector[uint32]
	lengths *StdVector[uint32]
	data    *StdVector[byte]
}

type Vector[T any] struct {
	stl.Vector[T]
}
