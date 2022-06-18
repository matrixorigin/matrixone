package stl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultPool *common.Mempool
var DefaultAllocator MemAllocator

type MemNode interface {
	GetBuf() []byte
	Size() int
}

type MemAllocator interface {
	Alloc(size int) MemNode
	Free(n MemNode)
	Usage() int
}

func init() {
	DefaultPool = common.NewMempool(common.UNLIMIT)
	DefaultAllocator = new(simpleAllocator)
}

func NewSimpleAllocator() MemAllocator {
	return new(simpleAllocator)
}

type simpleAllocator struct{}

func (alloc *simpleAllocator) Alloc(size int) MemNode {
	return DefaultPool.Alloc(uint64(size))
}

func (alloc *simpleAllocator) Free(n MemNode) {
	DefaultPool.Free(n.(*common.MemNode))
}

func (alloc *simpleAllocator) Usage() int {
	return int(DefaultPool.Usage())
}
