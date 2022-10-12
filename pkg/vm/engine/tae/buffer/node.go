// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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

func (h *nodeHandle) Key() any {
	return h.n.Key()
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
	mgr               base.INodeManager
	key               any
	state             base.NodeState
	size              uint64
	iter              atomic.Uint64
	closed            bool
	impl              base.INode
	HardEvictableFunc func() bool
	DestroyFunc       func()
	LoadFunc          func()
	UnloadableFunc    func() bool
	UnloadFunc        func()
}

func NewNode(impl base.INode, mgr base.INodeManager, key any, size uint64) *Node {
	return &Node{
		mgr:  mgr,
		key:  key,
		size: size,
		impl: impl,
	}
}

func (n *Node) Size() uint64 {
	return n.size
}

func (n *Node) Key() any {
	return n.key
}

func (n *Node) MakeHandle() base.INodeHandle {
	if n.impl != nil {
		return newNodeHandle(n.impl, n.mgr)
	}
	return newNodeHandle(n, n.mgr)
}

func (n *Node) TryClose() (closed bool) {
	n.Lock()
	if n.RefCount() > 0 {
		return false
	}
	if n.closed {
		n.Unlock()
		return true
	}
	n.closed = true
	if n.state == base.NodeLoaded {
		n.Unload()
	}
	n.Unlock()
	n.mgr.UnregisterNode(n)
	return true
}

func (n *Node) Close() error {
	n.Lock()
	if n.closed {
		n.Unlock()
		return nil
	}
	n.closed = true
	if n.state == base.NodeLoaded {
		n.Unload()
	}
	n.Unlock()
	n.mgr.UnregisterNode(n)
	return nil
}

func (n *Node) IsClosed() bool {
	return n.closed
}

func (n *Node) HardEvictable() bool {
	if n.HardEvictableFunc != nil {
		return n.HardEvictableFunc()
	}
	return false
}

func (n *Node) Destroy() {
	if n.DestroyFunc != nil {
		n.DestroyFunc()
	}
}

// Load should be guarded by lock
func (n *Node) Load() {
	if n.state == base.NodeLoaded {
		return
	}
	if n.LoadFunc != nil {
		n.LoadFunc()
	}
	n.state = base.NodeLoaded
}

// Unload should be guarded by lock
func (n *Node) Unload() {
	if n.state == base.NodeUnload {
		return
	}
	n.mgr.RetuernQuota(n.size)
	if n.UnloadFunc != nil {
		n.UnloadFunc()
	}
	n.state = base.NodeUnload
}

// Unloadable should be guarded by lock
func (n *Node) Unloadable() bool {
	if n.state == base.NodeUnload {
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

// GetState should be guarded by lock
func (n *Node) GetState() base.NodeState {
	return n.state
}

func (n *Node) prepareExpand(delta uint64) bool {
	return n.mgr.MakeRoom(delta)
}

func (n *Node) Expand(delta uint64, fn func() error) error {
	if !n.prepareExpand(delta) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := common.DoRetry(func() error {
			if !n.prepareExpand(delta) {
				return base.ErrNoSpace
			}
			return nil
		}, ctx)
		if err != nil {
			return err
		}
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

// IsLoaded should be guarded by lock
func (n *Node) IsLoaded() bool { return n.state == base.NodeLoaded }

func (n *Node) IncIteration() uint64 {
	return n.iter.Add(1)
}

func (n *Node) Iteration() uint64 {
	return n.iter.Load()
}
