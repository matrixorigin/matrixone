package frontend

import (
	"container/list"
)

const (
	fixBufferSize           = 1024 * 1024
	defaultMinGrowSize      = 256
	defaultIOCopyBufferSize = 1024 * 4
)

type Allocator interface {
	// Alloc allocate a []byte with len(data) >= size, and the returned []byte cannot
	// be expanded in use.
	Alloc(capacity int) []byte
	// Free free the allocated memory
	Free([]byte)
}

type ByteBuf struct {
	header     []byte // buf data, auto +/- size
	fixBuf     []byte
	dynamicBuf *list.List
	fixIndex   int
	cutIndex   int
	length     int
	alloc      Allocator
}

func NewByteBuf(allocator Allocator) *ByteBuf {
	b := &ByteBuf{
		fixIndex:   0,
		dynamicBuf: list.New(),
		cutIndex:   0,
		length:     0,
		alloc:      allocator,
	}
	b.header = b.alloc.Alloc(HeaderLengthOfTheProtocol)
	b.fixBuf = b.alloc.Alloc(fixBufferSize)
	return b
}

// Close close the ByteBuf
func (b *ByteBuf) Close() {
	b.alloc.Free(b.header)
	b.alloc.Free(b.fixBuf)

	for e := b.dynamicBuf.Front(); e != nil; e = e.Next() {
		b.alloc.Free(e.Value.([]byte))
	}
}
