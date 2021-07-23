package iface

import (
	"io"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
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
	RegisterSpillableNode(vf common.IVFile, node_id uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	RegisterNode(vf common.IVFile, useCompress bool, node_id uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	UnregisterNode(nif.INodeHandle)

	CreateNode(vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor) INode

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}
