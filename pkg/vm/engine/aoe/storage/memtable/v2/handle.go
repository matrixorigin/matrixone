package memtable

import (
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
	"sync/atomic"
)

type nodeHandle struct {
	common.RefHelper
	sync.RWMutex
	mgr            *nodeManager
	id             common.ID
	state          iface.NodeState
	size           uint64
	iter           uint64
	closed         bool
	destoryFunc    func()
	loadFunc       func()
	unloadableFunc func() bool
	unloadFunc     func()
}

func newNodeHandle(mgr *nodeManager, id common.ID, size uint64) *nodeHandle {
	return &nodeHandle{
		mgr:  mgr,
		id:   id,
		size: size,
	}
}

func (n *nodeHandle) Size() uint64 {
	return n.size
}

func (n *nodeHandle) GetID() common.ID {
	return n.id
}

func (n *nodeHandle) Close() error {
	n.Lock()
	defer n.Unlock()
	if n.closed == true {
		return nil
	}
	n.closed = true
	if n.state == iface.NODE_LOADED {
		n.Unload()
	}
	n.mgr.UnregisterNode(n)
	return nil
}

func (n *nodeHandle) IsClosed() bool {
	n.RLock()
	defer n.RUnlock()
	return n.closed
}

func (n *nodeHandle) Destroy() {
	if n.destoryFunc != nil {
		n.destoryFunc()
	}
}

// Should be guarded by lock
func (n *nodeHandle) Load() {
	if n.state == iface.NODE_LOADED {
		return
	}
	if n.loadFunc != nil {
		n.loadFunc()
	}
	n.state = iface.NODE_LOADED
}

// Should be guarded by lock
func (n *nodeHandle) Unload() {
	if n.state == iface.NODE_UNLOAD {
		return
	}
	n.mgr.limiter.RetuernQuota(n.size)
	if n.unloadFunc != nil {
		n.unloadFunc()
	}
	n.state = iface.NODE_UNLOAD
}

// Should be guarded by lock
func (n *nodeHandle) Unloadable() bool {
	if n.state == iface.NODE_UNLOAD {
		return false
	}
	if n.RefCount() > 0 {
		return false
	}
	ret := true
	if n.unloadableFunc != nil {
		ret = n.unloadableFunc()
	}
	return ret
}

// Should be guarded by lock
func (n *nodeHandle) GetState() iface.NodeState {
	return n.state
}

// Should be guarded by lock
func (n *nodeHandle) IsLoaded() bool { return n.state == iface.NODE_LOADED }

func (n *nodeHandle) IncIteration() uint64 {
	return atomic.AddUint64(&n.iter, uint64(1))
}

func (n *nodeHandle) Iteration() uint64 {
	return atomic.LoadUint64(&n.iter)
}
