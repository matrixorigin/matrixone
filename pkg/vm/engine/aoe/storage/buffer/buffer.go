package buf

import (
	"matrixone/pkg/vm/engine/aoe/common/util"
)

var (
	_ IBuffer = (*Buffer)(nil)
)

func NewBuffer(node *Node) IBuffer {
	if node == nil {
		return nil
	}
	buf := &Buffer{
		Node:     node,
		DataSize: node.Capacity,
	}
	return buf
}

func (b *Buffer) Close() error {
	b.Node.Pool.FreeNode(b.Node)
	return nil
}

func (buf *Buffer) Clear() {
	util.MemsetRepeatByte(buf.Node.Data, byte(0))
}

func (buf *Buffer) GetCapacity() uint64 {
	if buf.Node == nil || buf.Node.Data == nil {
		return 0
	}
	return buf.DataSize + buf.HeaderSize
}

func (buf *Buffer) GetDataNode() *Node {
	return buf.Node
}
