package memtable

import (
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/memtable/v2/base"
	"sync"
	"sync/atomic"
)

type nodeHandle struct {
	n   *node
	mgr *nodeManager
}

func newNodeHandle(n *node, mgr *nodeManager) *nodeHandle {
	return &nodeHandle{
		n:   n,
		mgr: mgr,
	}
}

func (h *nodeHandle) GetID() common.ID {
	return h.n.GetID()
}

func (h *nodeHandle) GetNode() base.INode {
	return h.n
}

func (h *nodeHandle) Close() error {
	h.mgr.Unpin(h.n)
	return nil
}

type node struct {
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

func newNode(mgr *nodeManager, id common.ID, size uint64) *node {
	return &node{
		mgr:  mgr,
		id:   id,
		size: size,
	}
}

func (n *node) Size() uint64 {
	return n.size
}

func (n *node) GetID() common.ID {
	return n.id
}

func (n *node) MakeHandle() base.INodeHandle {
	return newNodeHandle(n, n.mgr)
}

func (n *node) Close() error {
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

func (n *node) IsClosed() bool {
	n.RLock()
	defer n.RUnlock()
	return n.closed
}

func (n *node) Destroy() {
	if n.destoryFunc != nil {
		n.destoryFunc()
	}
}

// Should be guarded by lock
func (n *node) Load() {
	if n.state == iface.NODE_LOADED {
		return
	}
	if n.loadFunc != nil {
		n.loadFunc()
	}
	n.state = iface.NODE_LOADED
}

// Should be guarded by lock
func (n *node) Unload() {
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
func (n *node) Unloadable() bool {
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
func (n *node) GetState() iface.NodeState {
	return n.state
}

// Should be guarded by lock
func (n *node) IsLoaded() bool { return n.state == iface.NODE_LOADED }

func (n *node) IncIteration() uint64 {
	return atomic.AddUint64(&n.iter, uint64(1))
}

func (n *node) Iteration() uint64 {
	return atomic.LoadUint64(&n.iter)
}
