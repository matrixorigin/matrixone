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
)

func NewBytes() *Bytes {
	return &Bytes{
		Data:   make([]byte, 0),
		Length: make([]uint32, 0),
		Offset: make([]uint32, 0),
	}
}

func (bs *Bytes) Get(i int) []byte {
	if len(bs.Length) == 0 {
		return []byte{}
	}
	return bs.Data[bs.Offset[i] : bs.Offset[i]+bs.Length[i]]
}

func (bs *Bytes) Window(offset, length int) *Bytes {
	win := NewBytes()
	if len(bs.Length) == 0 || length == 0 {
		return win
	}
	win.Offset = bs.Offset[offset : offset+length]
	win.Length = bs.Length[offset : offset+length]
	win.Data = bs.Data
	return win
}

func (bs *Bytes) DataSize() int   { return len(bs.Data) }
func (bs *Bytes) LengthSize() int { return len(bs.Length) }
func (bs *Bytes) OffSetSize() int { return len(bs.Offset) }

func (bs *Bytes) DataBuf() (buf []byte) { return bs.Data }
func (bs *Bytes) LengthBuf() (buf []byte) {
	if len(bs.Length) == 0 {
		return
	}
	buf = unsafe.Slice((*byte)(unsafe.Pointer(&bs.Length[0])), len(bs.Length)*Sizeof[uint32]())
	return
}

func (bs *Bytes) OffsetBuf() (buf []byte) {
	if len(bs.Offset) == 0 {
		return
	}
	buf = unsafe.Slice((*byte)(unsafe.Pointer(&bs.Offset[0])), len(bs.Offset)*Sizeof[uint32]())
	return
}

func (bs *Bytes) SetLengthBuf(buf []byte) {
	if len(buf) == 0 {
		return
	}
	bs.Length = unsafe.Slice((*uint32)(unsafe.Pointer(&buf[0])), len(buf)/Sizeof[uint32]())
}

func (bs *Bytes) SetOffsetBuf(buf []byte) {
	if len(buf) == 0 {
		return
	}
	bs.Offset = unsafe.Slice((*uint32)(unsafe.Pointer(&buf[0])), len(buf)/Sizeof[uint32]())
}
