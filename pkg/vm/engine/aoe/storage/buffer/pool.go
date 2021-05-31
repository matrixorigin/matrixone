package buf

import (
	"errors"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type SimpleMemoryPool struct {
	Capacity uint64
	Usage    uint64
}

func NewSimpleMemoryPool(capacity uint64) IMemoryPool {
	pool := &SimpleMemoryPool{
		Capacity: capacity,
	}
	return pool
}

func (pool *SimpleMemoryPool) GetCapacity() uint64 {
	return atomic.LoadUint64(&(pool.Capacity))
}

func (pool *SimpleMemoryPool) SetCapacity(capacity uint64) error {
	if capacity < atomic.LoadUint64(&(pool.Capacity)) {
		return errors.New("logic error")
	}
	atomic.StoreUint64(&(pool.Capacity), capacity)
	return nil
}

func (pool *SimpleMemoryPool) GetUsage() uint64 {
	return atomic.LoadUint64(&(pool.Usage))
}

// Only for temp test
func (pool *SimpleMemoryPool) MakeNode(size uint64) (node *Node) {
	capacity := atomic.LoadUint64(&(pool.Capacity))
	currsize := atomic.LoadUint64(&(pool.Usage))
	postsize := size + currsize
	if postsize > capacity {
		return nil
	}
	for !atomic.CompareAndSwapUint64(&(pool.Usage), currsize, postsize) {
		currsize = atomic.LoadUint64(&(pool.Usage))
		postsize += currsize + size
		if postsize > capacity {
			return nil
			// return &Node{Data: []byte{}, Pool: pool}
		}
	}
	// buf := make([]byte, 0, size)
	buf := make([]byte, size)
	return &Node{Data: buf, Pool: pool, Capacity: size}
}

func (pool *SimpleMemoryPool) FreeNode(node *Node) {
	size := uint64(cap(node.Data))
	if size == 0 {
		return
	}
	usagesize := atomic.AddUint64(&(pool.Usage), ^uint64(size-1))
	if usagesize > pool.Capacity {
		panic("")
	}
	node.Data = nil
}

func (n *Node) Size() uint64 {
	return uint64(len(n.Data))
}
