package manager

import (
	"fmt"
	sq "github.com/yireyun/go-queue"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"sync"
)

type SimpleEvictHolder struct {
	Queue *sq.EsQueue
	sync.Mutex
}

const (
	EVICT_HOLDER_CAPACITY uint64 = 100000
)

type SimpleEvictHolderCtx struct {
	QCapacity uint64
}

func NewSimpleEvictHolder(ctx ...interface{}) IEvictHolder {
	c := EVICT_HOLDER_CAPACITY
	if len(ctx) > 0 {
		context := ctx[0].(*SimpleEvictHolderCtx)
		if context != nil {
			c = context.QCapacity
		}
	}
	holder := &SimpleEvictHolder{
		Queue: sq.NewQueue(uint32(c)),
	}
	return holder
}

func (holder *SimpleEvictHolder) Enqueue(node *EvictNode) {
	holder.Queue.Put(node)
}

func (holder *SimpleEvictHolder) Dequeue() *EvictNode {
	r, ok, _ := holder.Queue.Get()
	if !ok {
		return nil
	}
	return r.(*EvictNode)
}

func (node *EvictNode) String() string {
	return fmt.Sprintf("EvictNode(%v, %d)", node.Handle, node.Iter)
}

func (node *EvictNode) Unloadable(h nif.INodeHandle) bool {
	if node.Handle != h {
		panic("Logic error")
	}
	return h.Iteration() == node.Iter
}
