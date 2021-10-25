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

package node

import (
	"errors"
	"fmt"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

func NewNodeHandle(ctx *NodeHandleCtx) nif.INodeHandle {
	state := nif.NODE_UNLOAD
	if ctx.Buff != nil {
		state = nif.NODE_LOADED
	}
	handle := &NodeHandle{
		ID:          ctx.ID,
		Buff:        ctx.Buff,
		File:        ctx.File,
		State:       state,
		RTState:     nif.NODE_RT_RUNNING,
		Manager:     ctx.Manager,
		Spillable:   ctx.Spillable,
		UseCompress: ctx.UseCompress,
		Constructor: ctx.Constructor,
	}

	if ctx.File != nil && !ctx.Spillable {
		handle.IO = NewNodeIOWithReader(handle, ctx.File)
	} else if ctx.Spillable {
		handle.IO = NewNodeIO(handle, ctx.Dir)
	}

	return handle
}

func (h *NodeHandle) Iteration() uint64 {
	return atomic.LoadUint64(&h.Iter)
}

func (h *NodeHandle) IncIteration() uint64 {
	return atomic.AddUint64(&h.Iter, uint64(1))
}

func (h *NodeHandle) FlushData() error {
	if !h.Spillable {
		return nil
	}
	return h.IO.Flush()
}

func (h *NodeHandle) GetBuffer() buf.IBuffer {
	return h.Buff
}

func (h *NodeHandle) String() string {
	s := fmt.Sprintf("<NodeHandle>(RefCount=%d)", atomic.LoadUint64(&h.Refs))
	return s
}

func (h *NodeHandle) Unload() {
	if nif.AtomicLoadState(&h.State) == nif.NODE_UNLOAD {
		return
	}
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADED, nif.NODE_UNLOADING) {
		panic("logic error")
	}
	err := h.FlushData()
	if err != nil {
		panic(fmt.Sprintf("flush data err: %s", err))
	}
	h.Buff.Close()
	h.Buff = nil
	nif.AtomicStoreState(&(h.State), nif.NODE_UNLOAD)
	// log.Infof("Unload %s", h.RelationName.String())
}

func (h *NodeHandle) GetNodeCreator() buf.MemoryNodeConstructor {
	return h.Constructor
}

func (h *NodeHandle) GetCapacity() uint64 {
	if h.UseCompress {
		return uint64(h.File.Stat().Size())
	} else {
		return uint64(h.File.Stat().OriginSize())
	}
}

func (h *NodeHandle) Ref() {
	atomic.AddUint64(&h.Refs, 1)
}

func (h *NodeHandle) UnRef() bool {
	old := atomic.LoadUint64(&(h.Refs))
	if old == uint64(0) {
		return false
	}
	return atomic.CompareAndSwapUint64(&(h.Refs), old, old-1)
}

func (h *NodeHandle) HasRef() bool {
	return atomic.LoadUint64(&h.Refs) != 0
}

func (h *NodeHandle) GetID() uint64 {
	return h.ID
}

func (h *NodeHandle) GetState() nif.NodeState {
	return atomic.LoadUint32(&h.State)
}

func (h *NodeHandle) IsSpillable() bool {
	return h.Spillable
}

func (h *NodeHandle) Clean() error {
	return h.IO.Clean()
}

func (h *NodeHandle) Close() error {
	h.Lock()
	defer h.Unlock()
	if !nif.AtomicCASRTState(&(h.RTState), nif.NODE_RT_RUNNING, nif.NODE_RT_CLOSED) {
		// Cocurrent senario that other client already call Close before
		return nil
	}
	if h.Buff != nil {
		h.Buff.Close()
	}
	// log.Infof("UnregisterNode %v", h.RelationName)
	h.Manager.UnregisterNode(h)
	return nil
}

func (h *NodeHandle) IsClosed() bool {
	state := nif.AtomicLoadRTState(&(h.RTState))
	return state == nif.NODE_RT_CLOSED
}

func (h *NodeHandle) Unloadable() bool {
	state := atomic.LoadUint32(&h.State)
	if state == nif.NODE_UNLOAD {
		return false
	}
	if h.HasRef() {
		return false
	}

	// rtState := atomic.LoadUint32(&h.RTState)
	// if rtState == nif.NODE_RT_CLOSED {
	// 	return false
	// }

	return true
}

func (h *NodeHandle) RollbackLoad() {
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADING, nif.NODE_ROOLBACK) {
		return
	}
	// h.UnRef()
	if h.Buff != nil {
		h.Buff.Close()
		h.Buff = nil
	}
	nif.AtomicStoreState(&(h.State), nif.NODE_UNLOAD)
}

func (h *NodeHandle) PrepareLoad() bool {
	return nif.AtomicCASState(&(h.State), nif.NODE_UNLOAD, nif.NODE_LOADING)
}

func (h *NodeHandle) CommitLoad() error {
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADING, nif.NODE_COMMIT) {
		return errors.New("logic error")
	}

	if h.Spillable {
		// log.Infof("loading transient node %v", h.RelationName)
		err := h.IO.Load()
		if err != nil {
			return err
		}
	} else {
		// log.Infof("loading persistent node %v", h.RelationName)
		err := h.IO.Load()
		if err != nil {
			return err
		}
	}

	if !nif.AtomicCASState(&(h.State), nif.NODE_COMMIT, nif.NODE_LOADED) {
		return errors.New("logic error")
	}
	return nil
}

func (h *NodeHandle) GetFile() common.IVFile {
	return h.File
}

func (h *NodeHandle) IsCompress() bool {
	return h.UseCompress
}

func (h *NodeHandle) MakeHandle() nif.IBufferHandle {
	if nif.AtomicLoadState(&(h.State)) != nif.NODE_LOADED {
		panic(fmt.Sprintf("Should not call MakeHandle not NODE_LOADED: %d", h.State))
	}
	return NewBufferHandle(h, h.Manager)
}

func (h *NodeHandle) SetBuffer(buf buf.IBuffer) error {
	if h.Buff != nil || h.GetCapacity() != uint64(buf.GetCapacity()) {
		return errors.New("logic error")
	}
	h.Buff = buf
	return nil
}

func NewBufferHandle(n nif.INodeHandle, mgr mgrif.IBufferManager) nif.IBufferHandle {
	h := &BufferHandle{
		Handle:  n,
		Manager: mgr,
	}
	return h
}

func (h *BufferHandle) GetID() uint64 {
	return h.Handle.GetID()
}

func (h *BufferHandle) Close() error {
	h.Manager.Unpin(h.Handle)
	return nil
}

func (h *BufferHandle) GetHandle() nif.INodeHandle {
	return h.Handle
}
