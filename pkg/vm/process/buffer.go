package process

import (
	"io"
)

const defaultBufSize = 1024000

func NewBuffer() *Buffer {
	return &Buffer{
		ch: make(chan []byte, defaultBufSize),
	}
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	if b.empty {
		return 0, io.EOF
	}
	if b.buf == nil || b.off >= len(b.buf) {
		b.buf = <-b.ch
		b.off = 0
	}
	if b.buf == nil {
		b.empty = true
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return n, nil
}
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.ch <- p
	return len(p), nil
}
