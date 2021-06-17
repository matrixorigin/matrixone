package buf

var (
	_ IBuffer = (*Buffer)(nil)
)

func NewBuffer(node IMemoryNode) IBuffer {
	if node == nil {
		return nil
	}
	buf := &Buffer{
		Node:     node,
		DataSize: node.GetMemoryCapacity(),
	}
	return buf
}

func (b *Buffer) GetNodeSize() uint64 {
	if b.Node == nil {
		return 0
	}
	return b.Node.GetMemorySize()
}

func (b *Buffer) Close() error {
	b.Node.FreeMemory()
	return nil
}

func (buf *Buffer) GetCapacity() uint64 {
	if buf.Node == nil {
		return 0
	}
	return buf.DataSize + buf.HeaderSize
}

func (buf *Buffer) GetDataNode() IMemoryNode {
	return buf.Node
}
