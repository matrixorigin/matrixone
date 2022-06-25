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
// limitations under the License.

package manager

import (
	"fmt"
	"strings"
	"sync/atomic"

	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node"
	nif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	// log "github.com/sirupsen/logrus"
)

var (
	_                  mgrif.IBufferManager = (*BufferManager)(nil)
	TRANSIENT_START_ID                      = ^(uint64(0)) / 2
)

func NewBufferManager(dir string, capacity uint64, evict_ctx ...interface{}) mgrif.IBufferManager {
	mgr := &BufferManager{
		IMemoryPool:     buf.NewSimpleMemoryPool(capacity),
		Nodes:           make(map[uint64]nif.INodeHandle),
		EvictHolder:     NewSimpleEvictHolder(evict_ctx...),
		NextID:          uint64(0),
		NextTransientID: TRANSIENT_START_ID,
		Dir:             []byte(dir),
	}

	return mgr
}

func (mgr *BufferManager) NodeCount() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.Nodes)
}

func (mgr *BufferManager) String() string {
	mgr.RLock()
	defer mgr.RUnlock()
	s := fmt.Sprintf("BMgr[Cap:%d,Usage:%d,Nodes:%d,LoadTimes:%d,EvictTimes:%d,UnregisterTimes:%d]:\n", mgr.GetCapacity(), mgr.GetUsage(),
		len(mgr.Nodes), atomic.LoadInt64(&mgr.LoadTimes), atomic.LoadInt64(&mgr.EvictTimes), atomic.LoadInt64(&mgr.UnregisterTimes))
	for _, node := range mgr.Nodes {
		s = fmt.Sprintf("%s\n\t%d | %s | Cap: %d ", s, node.GetID(), nif.NodeStateString(mgr.Nodes[node.GetID()].GetState()), mgr.Nodes[node.GetID()].GetCapacity())
	}
	return s
}

func (mgr *BufferManager) GetNextID() uint64 {
	return atomic.AddUint64(&mgr.NextID, uint64(1)) - 1
}

func (mgr *BufferManager) GetNextTransientID() uint64 {
	return atomic.AddUint64(&mgr.NextTransientID, uint64(1)) - 1
}

func (mgr *BufferManager) CreateNode(vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor) mgrif.INode {
	return newNode(mgr, vf, useCompress, constructor)
}

func (mgr *BufferManager) RegisterMemory(vf common.IVFile, spillable bool, constructor buf.MemoryNodeConstructor) nif.INodeHandle {
	pNode := mgr.makePoolNode(vf, false, constructor)
	if pNode == nil {
		return nil
	}
	id := mgr.GetNextTransientID()
	ctx := node.NodeHandleCtx{
		ID:          id,
		Manager:     mgr,
		Buff:        node.NewNodeBuffer(id, pNode),
		Spillable:   spillable,
		Constructor: constructor,
		File:        vf,
		Dir:         mgr.Dir,
	}
	handle := node.NewNodeHandle(&ctx)
	return handle
}

func (mgr *BufferManager) RegisterSpillableNode(vf common.IVFile, node_id uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle {
	// log.Infof("RegisterSpillableNode %s", node_id.String())
	{
		mgr.RLock()
		handle, ok := mgr.Nodes[node_id]
		if ok {
			if !handle.IsClosed() {
				mgr.RUnlock()
				return handle
			}
		}
		mgr.RUnlock()
	}

	pNode := mgr.makePoolNode(vf, false, constructor)
	if pNode == nil {
		return nil
	}
	ctx := node.NodeHandleCtx{
		ID:          node_id,
		Manager:     mgr,
		Buff:        node.NewNodeBuffer(node_id, pNode),
		Spillable:   true,
		File:        vf,
		Constructor: constructor,
		Dir:         mgr.Dir,
	}
	handle := node.NewNodeHandle(&ctx)

	mgr.Lock()
	defer mgr.Unlock()
	h, ok := mgr.Nodes[node_id]
	if ok {
		if !h.IsClosed() {
			go func() { pNode.FreeMemory() }()
			return h
		}
	}

	mgr.Nodes[node_id] = handle
	return handle
}

func (mgr *BufferManager) RegisterNode(vf common.IVFile, useCompress bool, node_id uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle {
	mgr.Lock()
	defer mgr.Unlock()
	// log.Infof("RegisterNode %s", node_id.String())

	handle, ok := mgr.Nodes[node_id]
	if ok {
		if !handle.IsClosed() {
			return handle
		}
	}
	ctx := node.NodeHandleCtx{
		ID:          node_id,
		Manager:     mgr,
		Spillable:   false,
		File:        vf,
		UseCompress: useCompress,
		Constructor: constructor,
	}
	handle = node.NewNodeHandle(&ctx)
	mgr.Nodes[node_id] = handle
	return handle
}

func (mgr *BufferManager) UnregisterNode(h nif.INodeHandle) {
	atomic.AddInt64(&mgr.UnregisterTimes, int64(1))
	node_id := h.GetID()
	// log.Infof("UnRegisterNode %s", node_id.String())
	if h.IsSpillable() {
		if node_id >= TRANSIENT_START_ID {
			err := h.Clean()
			if err != nil {
				panic(err)
			}
			return
		} else {
			mgr.Lock()
			delete(mgr.Nodes, node_id)
			err := h.Clean()
			if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
				panic(err)
			}
			mgr.Unlock()
			return
		}
	}
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.Nodes, node_id)
}

func (mgr *BufferManager) Unpin(handle nif.INodeHandle) {
	handle.Lock()
	defer handle.Unlock()
	if !handle.UnRef() {
		panic("logic error")
	}
	if !handle.HasRef() {
		atomic.AddInt64(&mgr.EvictTimes, int64(1))
		evict_node := &EvictNode{Handle: handle, Iter: handle.IncIteration()}
		mgr.EvictHolder.Enqueue(evict_node)
	}
}

func (mgr *BufferManager) makePoolNode(vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor) buf.IMemoryNode {
	node := mgr.Alloc(vf, useCompress, constructor)
	if node != nil {
		return node
	}
	for node == nil {
		// log.Printf("makePoolNode capacity %d now %d", capacity, mgr.GetUsageSize())
		evict_node := mgr.EvictHolder.Dequeue()
		// log.Infof("Evict node %s", evict_node.String())
		if evict_node == nil {
			// log.Printf("Cannot get node from queue")
			return nil
		}
		if evict_node.Handle.IsClosed() {
			continue
		}

		if !evict_node.Unloadable(evict_node.Handle) {
			continue
		}

		{
			evict_node.Handle.Lock()
			if !evict_node.Unloadable(evict_node.Handle) {
				evict_node.Handle.Unlock()
				continue
			}
			if !evict_node.Handle.Unloadable() {
				evict_node.Handle.Unlock()
				continue
			}
			evict_node.Handle.Unload()
			evict_node.Handle.Unlock()
		}
		node = mgr.Alloc(vf, useCompress, constructor)
	}
	return node
}

func (mgr *BufferManager) Pin(handle nif.INodeHandle) nif.IBufferHandle {
	handle.Lock()
	defer handle.Unlock()
	if handle.PrepareLoad() {
		n := mgr.makePoolNode(handle.GetFile(), handle.IsCompress(), handle.GetNodeCreator())
		if n == nil {
			handle.RollbackLoad()
			// log.Warnf("Cannot makeSpace(%d,%d)", handle.GetCapacity(), mgr.GetCapacity())
			return nil
		}
		buf := node.NewNodeBuffer(handle.GetID(), n)
		err := handle.SetBuffer(buf)
		if err != nil {
			panic(err)
		}
		if err := handle.CommitLoad(); err != nil {
			handle.RollbackLoad()
			panic(err.Error())
		}
		atomic.AddInt64(&mgr.LoadTimes, int64(1))
	}
	handle.Ref()
	return handle.MakeHandle()
}

func MockBufMgr(capacity uint64) mgrif.IBufferManager {
	dir := "/tmp/mockbufdir"
	return NewBufferManager(dir, capacity)
}
