package manager

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	// log "github.com/sirupsen/logrus"
)

type Node struct {
	Constructor buf.MemoryNodeConstructor
	BufMgr      bmgrif.IBufferManager
	BufNode     nif.INodeHandle
	Capacity    uint64
	VFile       common.IVFile
}

func newNode(bufMgr bmgrif.IBufferManager, vf common.IVFile, constructor buf.MemoryNodeConstructor, capacity uint64) bmgrif.INode {
	node := &Node{
		BufMgr:      bufMgr,
		VFile:       vf,
		Constructor: constructor,
		Capacity:    capacity,
	}
	if node.VFile != nil {
		// node.VFile.Ref()
		node.BufNode = node.BufMgr.RegisterNode(node.Capacity, bufMgr.GetNextID(), node.VFile, node.Constructor)
	} else {
		node.BufNode = node.BufMgr.RegisterSpillableNode(node.Capacity, bufMgr.GetNextID(), node.Constructor)
		if node.BufNode == nil {
			return nil
		}
	}
	return node
}

func (n *Node) GetManagedNode() bmgrif.MangaedNode {
	mnode := bmgrif.MangaedNode{}
	mnode.Handle = n.BufMgr.Pin(n.BufNode)
	for mnode.Handle == nil {
		mnode.Handle = n.BufMgr.Pin(n.BufNode)
	}
	b := mnode.Handle.GetHandle().GetBuffer()
	mnode.DataNode = b.GetDataNode()
	return mnode
}

func (n *Node) GetBufferHandle() nif.IBufferHandle {
	nh := n.BufMgr.Pin(n.BufNode)
	for nh == nil {
		nh = n.BufMgr.Pin(n.BufNode)
	}
	return nh
}

func (n *Node) Close() error {
	if n.BufNode != nil {
		err := n.BufNode.Close()
		if err != nil {
			panic("logic error")
		}
		n.BufNode = nil
	}
	if n.VFile != nil {
		n.VFile.Unref()
		n.VFile = nil
	}
	return nil
}
