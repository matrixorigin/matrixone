// Copyright 2021 - 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func New() *Buffer {
	return new(Buffer)
}

func (b *Buffer) Free() {
	b.Lock()
	defer b.Unlock()
	for i := range b.chunks {
		unix.Munmap(b.chunks[i].data)
	}
	b.chunks = nil
}

func Alloc[T any](b *Buffer) *T {
	var v T

	data := b.alloc(int(unsafe.Sizeof(v)))
	return (*T)(unsafe.Pointer(unsafe.SliceData(data)))
}

func Free[T any](b *Buffer, v *T) {
	b.free(unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v)))
}

func MakeSlice[T any](b *Buffer, len, cap int) []T {
	var v T

	data := b.alloc(int(unsafe.Sizeof(v)) * cap)
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(data))), cap)[:len]
}

func FreeSlice[T any](b *Buffer, vs []T) {
	var v T

	b.free(unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(vs))),
		cap(vs)*int(unsafe.Sizeof(v))))
}

func (b *Buffer) pop() *chunk {
	b.Lock()
	defer b.Unlock()
	if len(b.chunks) == 0 {
		return nil
	}
	c := b.chunks[0]
	b.chunks = b.chunks[1:]
	return c
}

func (b *Buffer) push(c *chunk) {
	b.Lock()
	defer b.Unlock()
	b.chunks = append(b.chunks, c)
}

func (b *Buffer) newChunk() *chunk {
	data, err := unix.Mmap(-1, 0, DefaultChunkBufferSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}
	c := (*chunk)(unsafe.Pointer(unsafe.SliceData(data)))
	c.data = data
	c.off = uint32(ChunkSize)
	return c
}

func (b *Buffer) alloc(sz int) []byte {
	c := b.pop()
	if c == nil {
		c = b.newChunk()
	}
	data := c.alloc(sz)
	if data == nil {
		c = b.newChunk()
		data = c.alloc(sz)
	}
	b.push(c)
	return data
}

func (b *Buffer) free(data []byte) {
	ptr := *((*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(data)), -PointerSize)))
	c := (*chunk)(ptr)
	c.free()
}
