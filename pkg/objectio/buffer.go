package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
)

type ObjectBuffer struct {
	common.RefHelper
	buf  *bytes.Buffer
	name string
}

func NewObjectBuffer(name string) *ObjectBuffer {
	buffer := &ObjectBuffer{name: name}
	buffer.buf = new(bytes.Buffer)
	buffer.Ref()
	return buffer
}

func (buffer *ObjectBuffer) Write(buf []byte) (int, int, error) {
	offset := buffer.buf.Len()
	if err := binary.Write(buffer.buf, binary.BigEndian, buf); err != nil {
		return offset, 0, err
	}
	length := buffer.buf.Len() - offset
	return offset, length, nil
}

func (buffer *ObjectBuffer) Length() int {
	return buffer.buf.Len()
}

func (buffer *ObjectBuffer) GetData() []byte {
	return buffer.buf.Bytes()
}

type ObjectBufferPool struct {
	sync.RWMutex
	buffers map[string]*ObjectBuffer
	count   uint32
}

func NewObjectBufferPool(count uint32) *ObjectBufferPool {
	pool := &ObjectBufferPool{count: count}
	pool.buffers = make(map[string]*ObjectBuffer)
	return pool
}

func (pool *ObjectBufferPool) GetBuffer(name string) *ObjectBuffer {
	buffer := NewObjectBuffer(name)

	pool.Lock()
	defer pool.Unlock()
	pool.buffers[name] = buffer
	return buffer
}
