package buffer

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type fileNode struct {
	node base.INode
	mgr  base.INodeManager
	file *dataio.TransientBlockFile
}

func (n *fileNode) GetHandle() base.INodeHandle {
	h := n.mgr.Pin(n.node)
	for h == nil {
		h = n.mgr.Pin(n.node)
	}
	return h
}
