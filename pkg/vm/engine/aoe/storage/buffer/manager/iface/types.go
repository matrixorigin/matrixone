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

package iface

import (
	"io"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	nif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
)

type INode interface {
	io.Closer
	GetManagedNode() MangaedNode
	GetBufferHandle() nif.IBufferHandle
}

type MangaedNode struct {
	Handle   nif.IBufferHandle
	DataNode buf.IMemoryNode
}

func (h *MangaedNode) Close() error {
	hh := h.Handle
	h.Handle = nil
	h.DataNode = nil
	if hh != nil {
		return hh.Close()
	}
	return nil
}

type IBufferManager interface {
	sync.Locker
	RLock()
	RUnlock()
	buf.IMemoryPool

	String() string
	NodeCount() int
	GetNextID() uint64
	GetNextTransientID() uint64

	RegisterMemory(vf common.IVFile, spillable bool, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	RegisterSpillableNode(vf common.IVFile, nodeID uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	RegisterNode(vf common.IVFile, useCompress bool, nodeID uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	UnregisterNode(nif.INodeHandle)

	CreateNode(vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor) INode

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}
