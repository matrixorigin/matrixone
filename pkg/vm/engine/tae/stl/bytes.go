// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stl

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func NewBytes() *Bytes {
	return new(Bytes)
}

func NewFixedTypeBytes[T any]() *Bytes {
	return &Bytes{
		IsFixedType:   true,
		FixedTypeSize: Sizeof[T](),
	}
}

func (bs *Bytes) Size() int {
	return bs.StorageSize() + bs.HeaderSize()
}

func (bs *Bytes) Length() int {
	if bs.IsFixedType {
		return len(bs.Storage) / bs.FixedTypeSize
	}
	return len(bs.Header)
}

func (bs *Bytes) StorageSize() int {
	return len(bs.Storage)
}

func (bs *Bytes) StorageBuf() []byte {
	return bs.Storage
}

func (bs *Bytes) HeaderSize() int {
	return len(bs.Header) * types.VarlenaSize
}

func (bs *Bytes) HeaderBuf() (buf []byte) {
	if len(bs.Header) == 0 {
		return
	}
	buf = unsafe.Slice((*byte)(unsafe.Pointer(&bs.Header[0])), bs.HeaderSize())
	return
}

func (bs *Bytes) SetHeaderBuf(buf []byte) {
	if len(buf) == 0 {
		return
	}
	bs.Header = unsafe.Slice((*types.Varlena)(unsafe.Pointer(&buf[0])), len(buf)/Sizeof[types.Varlena]())
}

func (bs *Bytes) SetStorageBuf(buf []byte) {
	bs.Storage = buf
}
