package node

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	// log "github.com/sirupsen/logrus"
)

var (
	_ buf.IBuffer       = (*NodeBuffer)(nil)
	_ iface.INodeBuffer = (*NodeBuffer)(nil)
)

func NewNodeBuffer(id layout.ID, node *buf.Node) iface.INodeBuffer {
	if node == nil {
		return nil
	}
	ibuf := buf.NewBuffer(node)
	nb := &NodeBuffer{
		IBuffer: ibuf,
		ID:      id,
	}
	// nb.IBuffer.(*buf.Buffer).Type = buf.BLOCK_BUFFER
	return nb
}

func (nb *NodeBuffer) GetID() layout.ID {
	return nb.ID
}

// func (nb *NodeBuffer) GetType() iface.BufferType {
// 	return nb.Type
// }
