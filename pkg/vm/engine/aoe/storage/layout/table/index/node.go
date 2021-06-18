package index

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type INode interface {
	GetHandle() NodeHandle
}

type Node struct {
	Constructor buf.MemoryNodeConstructor
	BufMgr      bmgrif.IBufferManager
	BufNode     nif.INodeHandle
	Capacity    uint64
	VFile       base.IVirtaulFile
}

type NodeHandle struct {
	Handle nif.IBufferHandle
	Index  Index
}

func (h *NodeHandle) Close() error {
	return h.Handle.Close()
}

func NewNode(bufMgr bmgrif.IBufferManager, vf base.IVirtaulFile, constructor buf.MemoryNodeConstructor, capacity uint64) INode {
	node := &Node{
		BufMgr:      bufMgr,
		VFile:       vf,
		Constructor: constructor,
		Capacity:    capacity,
	}
	node.VFile.Ref()
	node.BufNode = node.BufMgr.RegisterNode(node.Capacity, bufMgr.GetNextID(), node.VFile, node.Constructor)
	return node
}

func (n *Node) GetHandle() NodeHandle {
	handle := NodeHandle{
		Index: n.BufNode.GetBuffer().GetDataNode().(Index),
	}

	handle.Handle = n.BufMgr.Pin(n.BufNode)
	for handle.Handle == nil {
		handle.Handle = n.BufMgr.Pin(n.BufNode)
	}
	return handle
}

func (n *Node) Close() {
	if n.BufNode != nil {
		err := n.BufNode.Close()
		if err != nil {
			panic("logic error")
		}
		n.BufNode = nil
	}
	if n.VFile != nil {
		n.VFile.Unref()
	}
}
