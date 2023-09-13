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
	"math"
	"unsafe"

	"golang.org/x/sys/unix"
)

func New(limit int) *Buffer {
	data, err := unix.Mmap(-1, 0, (limit+PageSize-1)&(-PageSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}
	b := &Buffer{data: data}
	b.init()
	return b
}

func (b *Buffer) Free() {
	unix.Munmap(b.data)
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

func (b *Buffer) init() {
	b.freeList = new(freeList)
	pageCount := len(b.data) / PageSize
	b.ptr = uintptr(unsafe.Pointer(unsafe.SliceData(b.data)))
	for i := range b.pages {
		b.pages[i] = &pageHeader{
			freeList: new(freeList),
			fullList: new(freeList),
		}
	}
	for i := 0; i < pageCount; i++ {
		data := b.data[i*PageSize:][:PageSize]
		p := (*page)(unsafe.Pointer(unsafe.SliceData(data)))
		p.head = math.MaxUint64
		p.data = data[pageHeaderSize:]
		p.ptr = uintptr(unsafe.Pointer(unsafe.SliceData(p.data)))
		push(p, b.freeList)
	}
}

func (b *Buffer) alloc(sz int) []byte {
	return b.pages[sz/WordSize].alloc(sz, b)
}

func (b *Buffer) free(data []byte) {
	sz := len(data)
	b.pages[sz/WordSize].free(data, b)
}
