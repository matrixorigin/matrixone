package memtable

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"sync"
)

type nodeManager struct {
	sync.RWMutex
	limiter *memtableLimiter
	nodes   map[common.ID]base.INodeHandle
	evicter manager.IEvictHolder
}

func newNodeManager(limiter *memtableLimiter, evicter manager.IEvictHolder) *nodeManager {
	mgr := &nodeManager{
		limiter: limiter,
		nodes:   make(map[common.ID]base.INodeHandle),
		evicter: evicter,
	}
	return mgr
}

func (mgr *nodeManager) Count() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.nodes)
}

func (mgr *nodeManager) RegisterNode(node base.INodeHandle) {
	id := node.GetID()
	mgr.Lock()
	defer mgr.Unlock()
	handle, ok := mgr.nodes[id]
	if ok {
		panic(fmt.Sprintf("Duplicate node: %s", id.BlockString()))
	}
	mgr.nodes[id] = handle
	return
}

func (mgr *nodeManager) UnregisterNode(node base.INodeHandle) {
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.nodes, node.GetID())
	node.Destroy()
}

func (mgr *nodeManager) makeRoom(node base.INodeHandle) bool {
	ok := mgr.limiter.ApplySizeQuota(node.Size())
	for !ok {
		evicted := mgr.evicter.Dequeue()
		if evicted == nil {
			return false
		}
		if evicted.Handle.IsClosed() {
			continue
		}

		if !evicted.Unloadable(evicted.Handle) {
			continue
		}

		{
			evicted.Handle.Lock()
			if !evicted.Unloadable(evicted.Handle) {
				evicted.Handle.Unlock()
				continue
			}
			if !evicted.Handle.Unloadable() {
				evicted.Handle.Unlock()
				continue
			}
			evicted.Handle.Unload()
			evicted.Handle.Unlock()
		}
		ok = mgr.limiter.ApplySizeQuota(node.Size())
	}
	return ok
}

func (mgr *nodeManager) Pin(node base.INodeHandle) bool {
	node.RLock()
	if node.IsLoaded() {
		node.RUnlock()
		return true
	}
	node.RUnlock()

	node.Lock()
	defer node.Unlock()
	if node.IsLoaded() {
		return true
	}
	ok := mgr.makeRoom(node)
	if !ok {
		return false
	}
	node.Load()
	return true
}

func (mgr *nodeManager) Unpin(node base.INodeHandle) {
	node.Lock()
	defer node.Unlock()
	node.Unref()
	if node.RefCount() == 0 {
		toevict := &manager.EvictNode{Handle: node, Iter: node.IncIteration()}
		mgr.evicter.Enqueue(toevict)
	}
}
