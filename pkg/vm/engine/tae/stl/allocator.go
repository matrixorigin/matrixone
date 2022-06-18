package stl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultPool *common.Mempool

type MemNode interface {
	GetBuf() []byte
	Size() int
}

type MemAllocator[T any] interface {
	Alloc(size int) MemNode
	Free(n MemNode)
	Usage() int
}

func init() {
	DefaultPool = common.NewMempool(common.UNLIMIT)
}

func NewSimpleAllocator[T any]() MemAllocator[T] {
	return new(simpleAllocator[T])
}

type simpleAllocator[T any] struct{}

func (alloc *simpleAllocator[T]) Alloc(size int) MemNode {
	sz := uint64(LengthOfVals[T](size))
	return DefaultPool.Alloc(sz)
}

func (alloc *simpleAllocator[T]) Free(n MemNode) {
	DefaultPool.Free(n.(*common.MemNode))
}

func (alloc *simpleAllocator[T]) Usage() int {
	return int(DefaultPool.Usage())
}
