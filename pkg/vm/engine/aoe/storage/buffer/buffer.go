package buf

var (
	_ IBuffer = (*Buffer)(nil)
)

func NewBuffer(node IMemoryNode) IBuffer {
	if node == nil {
		return nil
	}
	buf := &Buffer{
		Node: node,
	}
	return buf
}

func (b *Buffer) Close() error {
	b.Node.FreeMemory()
	b.Node = nil
	return nil
}

func (buf *Buffer) GetCapacity() uint64 {
	return buf.Node.GetMemoryCapacity()
}

func (buf *Buffer) GetDataNode() IMemoryNode {
	return buf.Node
}
