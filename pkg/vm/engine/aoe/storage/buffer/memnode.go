package buf

import "io"

func RawMemoryNodeConstructor(capacity uint64, freeFunc MemoryFreeFunc) IMemoryNode {
	return NewRawMemoryNode(capacity, freeFunc)
}

type RawMemoryNode struct {
	Data     []byte
	Capacity uint64
	FreeFunc MemoryFreeFunc
}

func NewRawMemoryNode(capacity uint64, freeFunc MemoryFreeFunc) IMemoryNode {
	node := &RawMemoryNode{
		Capacity: capacity,
		FreeFunc: freeFunc,
		Data:     make([]byte, capacity),
	}
	return node
}

func (mn *RawMemoryNode) GetMemoryCapacity() uint64 {
	return mn.Capacity
}

func (mn *RawMemoryNode) GetMemorySize() uint64 {
	return uint64(len(mn.Data))
}

func (mn *RawMemoryNode) FreeMemory() {
	mn.FreeFunc(mn)
}

func (mn *RawMemoryNode) Reset() {
	mn.Data = mn.Data[:0]
}

func (mn *RawMemoryNode) WriteTo(w io.Writer) (n int64, err error) {
	nw, err := w.Write(mn.Data)
	return int64(nw), err
}

func (mn *RawMemoryNode) ReadFrom(r io.Reader) (n int64, err error) {
	if len(mn.Data) != cap(mn.Data) {
		panic("logic error")
	}
	nr, err := r.Read(mn.Data)
	return int64(nr), err
}

func (mn *RawMemoryNode) Marshall() (buf []byte, err error) {
	buf = append(mn.Data[0:0:0], mn.Data...)
	return buf, err
}

func (mn *RawMemoryNode) Unmarshall(buf []byte) error {
	length := int(mn.Capacity)
	if length > len(buf) {
		length = len(buf)
	}
	copy(mn.Data, buf[0:length])
	return nil
}
