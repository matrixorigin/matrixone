package stl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultAllocator MemAllocator

type MemNode interface {
	GetBuf() []byte
	Size() int
}

type MemAllocator interface {
	Alloc(size int) MemNode
	Free(n MemNode)
	Usage() int
	String() string
}

func init() {
	simple := NewSimpleAllocator()
	// DefaultAllocator = DebugOneAllocator(simple)
	DefaultAllocator = simple
}

func NewSimpleAllocator() MemAllocator {
	allocator := new(simpleAllocator)
	allocator.pool = common.NewMempool(common.UNLIMIT)
	return allocator
}

type simpleAllocator struct {
	pool *common.Mempool
}

func (alloc *simpleAllocator) Alloc(size int) MemNode {
	return alloc.pool.Alloc(uint64(size))
}

func (alloc *simpleAllocator) Free(n MemNode) {
	alloc.pool.Free(n.(*common.MemNode))
}

func (alloc *simpleAllocator) Usage() int {
	return int(alloc.pool.Usage())
}

func (alloc *simpleAllocator) String() string {
	return alloc.pool.String()
}
