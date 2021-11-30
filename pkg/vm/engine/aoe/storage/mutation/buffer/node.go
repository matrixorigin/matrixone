// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package buffer

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"sync"
	"sync/atomic"
)

type nodeHandle struct {
	n   base.INode
	mgr base.INodeManager
}

func newNodeHandle(n base.INode, mgr base.INodeManager) *nodeHandle {
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

type Node struct {
	common.RefHelper
	sync.RWMutex
	mgr            base.INodeManager
	id             common.ID
	state          iface.NodeState
	size           uint64
	iter           uint64
	closed         bool
	impl           base.INode
	DestroyFunc    func()
	LoadFunc       func()
	UnloadableFunc func() bool
	UnloadFunc     func()
}

func NewNode(impl base.INode, mgr base.INodeManager, id common.ID, size uint64) *Node {
	return &Node{
		mgr:  mgr,
		id:   id,
		size: size,
		impl: impl,
	}
}

func (n *Node) Size() uint64 {
	return n.size
}

func (n *Node) GetID() common.ID {
	return n.id
}

func (n *Node) MakeHandle() base.INodeHandle {
	if n.impl != nil {
		return newNodeHandle(n.impl, n.mgr)
	}
	return newNodeHandle(n, n.mgr)
}

func (n *Node) Close() error {
	n.Lock()
	if n.closed == true {
		n.Unlock()
		return nil
	}
	n.closed = true
	if n.state == iface.NODE_LOADED {
		n.Unload()
	}
	n.Unlock()
	n.mgr.UnregisterNode(n)
	return nil
}

func (n *Node) IsClosed() bool {
	n.RLock()
	defer n.RUnlock()
	return n.closed
}

func (n *Node) Destroy() {
	if n.DestroyFunc != nil {
		n.DestroyFunc()
	}
}

// Should be guarded by lock
func (n *Node) Load() {
	if n.state == iface.NODE_LOADED {
		return
	}
	if n.LoadFunc != nil {
		n.LoadFunc()
	}
	n.state = iface.NODE_LOADED
}

// Should be guarded by lock
func (n *Node) Unload() {
	if n.state == iface.NODE_UNLOAD {
		return
	}
	n.mgr.RetuernQuota(n.size)
	if n.UnloadFunc != nil {
		n.UnloadFunc()
	}
	n.state = iface.NODE_UNLOAD
}

// Should be guarded by lock
func (n *Node) Unloadable() bool {
	if n.state == iface.NODE_UNLOAD {
		return false
	}
	if n.RefCount() > 0 {
		return false
	}
	ret := true
	if n.UnloadableFunc != nil {
		ret = n.UnloadableFunc()
	}
	return ret
}

// Should be guarded by lock
func (n *Node) GetState() iface.NodeState {
	return n.state
}

func (n *Node) prepareExpand(delta uint64) bool {
	return n.mgr.MakeRoom(delta)
}

func (n *Node) Expand(delta uint64, fn func() error) error {
	if !n.prepareExpand(delta) {
		return errors.New("aoe node expand: no enough space")
	}
	if fn != nil {
		if err := fn(); err != nil {
			n.rollbackExpand(delta)
			return err
		}
	}
	n.commitExpand(delta)
	return nil
}

func (n *Node) commitExpand(delta uint64) {
	n.size += delta
}

func (n *Node) rollbackExpand(delta uint64) {
	n.mgr.RetuernQuota(delta)
}

// Should be guarded by lock
func (n *Node) IsLoaded() bool { return n.state == iface.NODE_LOADED }

func (n *Node) IncIteration() uint64 {
	return atomic.AddUint64(&n.iter, uint64(1))
}

func (n *Node) Iteration() uint64 {
	return atomic.LoadUint64(&n.iter)
}
