package adaptors

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
)

func NewBuffer(buf []byte) *Buffer {
	b := &Buffer{
		storage: containers.NewStdVector[byte](),
	}
	if len(buf) > 0 {
		bs := stl.NewBytes()
		bs.Data = buf
		b.storage.ReadBytes(bs, true)
	}
	return b
}

func (b *Buffer) Reset()         { b.storage.Reset() }
func (b *Buffer) Close()         { b.storage.Close() }
func (b *Buffer) String() string { return b.storage.String() }

func (b *Buffer) Write(p []byte) (n int, err error) {
	n = len(p)
	b.storage.AppendMany(p...)
	return
}

// TODO: avoid string to []byte copy
func (b *Buffer) WriteString(s string) (n int, err error) {
	n = len(s)
	b.storage.AppendMany([]byte(s)...)
	return
}

func (b *Buffer) Bytes() []byte  { return b.storage.Data() }
func (b *Buffer) Len() int       { return b.storage.Length() }
func (b *Buffer) Cap() int       { return b.storage.Capacity() }
func (b *Buffer) Allocated() int { return b.storage.Allocated() }
