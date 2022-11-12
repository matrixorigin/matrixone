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

package containers

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStdVector[T any](opts ...*Options) *StdVector[T] {
	vec := &StdVector[T]{
		buf:   make([]byte, 0),
		slice: make([]T, 0),
	}
	var capacity int
	var buf []byte
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		vec.alloc = opt.Allocator
		if opt.HasData() {
			buf = opt.Data.StorageBuf()
			capacity = len(buf) / stl.Sizeof[T]()
		}
	}
	if vec.alloc == nil {
		vec.alloc = common.DefaultAllocator
	}
	// if capacity == 0 {
	// 	capacity = 4
	// }
	if buf != nil {
		vec.buf = buf
		vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), capacity)
		vec.capacity = capacity
	} else {
		vec.tryExpand(capacity)
	}
	return vec
}

func (vec *StdVector[T]) tryExpand(capacity int) {
	if vec.capacity >= capacity {
		return
	}
	newSize := stl.SizeOfMany[T](capacity)
	vec.capacity = capacity
	oldn := vec.node
	if oldn != nil && newSize <= len(oldn) {
		return
	} else if oldn == nil {
		if newSize <= cap(vec.buf) {
			return
		}
	}
	newn, err := vec.alloc.Alloc(capacity * stl.Sizeof[T]())
	if err != nil {
		panic(err)
	}
	buf := newn[:0:len(newn)]
	buf = append(buf, vec.buf...)
	vec.buf = buf
	vec.node = newn
	if oldn != nil {
		vec.alloc.Free(oldn)
	}
}

func (vec *StdVector[T]) Close() {
	if vec.node != nil {
		vec.alloc.Free(vec.node)
	}
	vec.node = nil
	vec.slice = nil
	vec.buf = nil
	vec.alloc = nil
}

func (vec *StdVector[T]) GetAllocator() *mpool.MPool { return vec.alloc }

func (vec *StdVector[T]) IsView() bool  { return false }
func (vec *StdVector[T]) Length() int   { return len(vec.slice) }
func (vec *StdVector[T]) Capacity() int { return vec.capacity }
func (vec *StdVector[T]) Allocated() int {
	if vec.node != nil {
		return len(vec.node)
	}
	return 0
}
func (vec *StdVector[T]) Data() []byte { return vec.buf }
func (vec *StdVector[T]) Slice() []T   { return vec.slice }
func (vec *StdVector[T]) SliceWindow(offset, length int) []T {
	return vec.slice[offset : offset+length]
}
func (vec *StdVector[T]) DataWindow(offset, length int) []byte {
	start := offset * stl.Sizeof[T]()
	end := start + length*stl.Sizeof[T]()
	return vec.buf[start:end]
}
func (vec *StdVector[T]) Desc() string {
	var v T
	s := fmt.Sprintf("StdVector<%s>:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		reflect.TypeOf(v).Name(),
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
	return s
}
func (vec *StdVector[T]) String() string {
	s := vec.Desc()
	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
	}
	data := ";Vals=["
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, vec.Get(i))
	}
	if vec.Length() > end {
		s = fmt.Sprintf("%s %s...]", s, data)
	} else {
		s = fmt.Sprintf("%s %s]", s, data)
	}
	return s
}

func (vec *StdVector[T]) Append(v T) {
	if len(vec.slice) == vec.capacity {
		var newCap int
		if vec.capacity < 2 {
			newCap = 4
		} else {
			newCap = vec.capacity * 2
		}
		vec.tryExpand(newCap)
	}
	vec.buf = append(vec.buf, unsafe.Slice((*byte)(unsafe.Pointer(&v)), int(unsafe.Sizeof(v)))...)
	size := len(vec.slice)
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), size+1)
}

func (vec *StdVector[T]) Get(i int) (v T) {
	v = vec.slice[i]
	return
}

func (vec *StdVector[T]) Update(i int, v T) {
	vec.slice[i] = v
}

func (vec *StdVector[T]) Delete(i int) (deleted T) {
	deleted = vec.slice[i]
	vec.slice = append(vec.slice[:i], vec.slice[i+1:]...)
	size := len(vec.buf) - stl.Sizeof[T]()
	vec.buf = vec.buf[:size]
	return
}

func (vec *StdVector[T]) RangeDelete(offset, length int) {
	vec.slice = append(vec.slice[:offset], vec.slice[offset+length:]...)
	size := len(vec.buf) - stl.SizeOfMany[T](length)
	vec.buf = vec.buf[:size]
}

func (vec *StdVector[T]) AppendMany(vals ...T) {
	if len(vals) == 0 {
		return
	}
	predictSize := len(vals) + len(vec.slice)
	if predictSize > vec.capacity {
		var newCap int
		if vec.capacity < 2 {
			newCap = 4
		} else {
			newCap = vec.capacity * 2
		}
		if newCap < predictSize {
			newCap = predictSize
		}
		vec.tryExpand(newCap)
	}

	// vec.buf is the pre-allocted memory buffer. Here copy vals into the vec.buf
	vec.buf = append(vec.buf, unsafe.Slice(
		(*byte)(unsafe.Pointer(&vals[0])),
		int(stl.SizeOfMany[T](len(vals))))...)
	// vec.slice shares the buffer with vec.buf. Here change the slice length
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), predictSize)
}

func (vec *StdVector[T]) Clone(offset, length int, allocator ...*mpool.MPool) stl.Vector[T] {
	opts := &Options{
		Capacity: length,
	}

	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}
	cloned := NewStdVector[T](opts)
	cloned.AppendMany(vec.slice[offset : offset+length]...)
	return cloned
}

func (vec *StdVector[T]) Reset() {
	if vec.Length() == 0 {
		return
	}
	vec.slice = vec.slice[:0]
	vec.buf = vec.buf[:0]
}

func (vec *StdVector[T]) WindowAsBytes(offset, length int) *stl.Bytes {
	bs := stl.NewFixedTypeBytes[T]()
	start := offset * stl.Sizeof[T]()
	end := start + length*stl.Sizeof[T]()
	bs.Storage = vec.buf[start:end]
	return bs
}

func (vec *StdVector[T]) Bytes() *stl.Bytes {
	bs := stl.NewFixedTypeBytes[T]()
	bs.Storage = vec.buf
	return bs
}

func (vec *StdVector[T]) ReadBytes(data *stl.Bytes, share bool) {
	if data == nil {
		return
	}
	if share {
		vec.readBytesShared(data.StorageBuf())
		return
	}
	vec.readBytesNotShared(data.StorageBuf())
}

func (vec *StdVector[T]) readBytesNotShared(bs []byte) {
	vec.Reset()
	newSize := len(bs)
	if newSize == 0 {
		return
	}
	capacity := newSize / stl.Sizeof[T]()
	vec.tryExpand(capacity)
	vec.buf = vec.node[:newSize]
	copy(vec.buf[0:], bs)
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), vec.capacity)
}

func (vec *StdVector[T]) readBytesShared(bs []byte) {
	if vec.node != nil {
		vec.alloc.Free(vec.node)
		vec.node = nil
	}
	vec.capacity = 0
	if len(bs) == 0 {
		vec.buf = vec.buf[:0]
		vec.slice = vec.slice[:0]
		return
	}
	vec.buf = bs
	vec.capacity = len(vec.buf) / stl.Sizeof[T]()
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), vec.capacity)
}

func (vec *StdVector[T]) InitFromSharedBuf(buf []byte) (n int64, err error) {
	sizeBuf := buf[:stl.Sizeof[uint32]()]
	size := *(*uint32)(unsafe.Pointer(&sizeBuf[0]))
	n = int64(stl.Sizeof[uint32]())
	if size == 0 {
		vec.Reset()
		return
	}
	buf = buf[stl.Sizeof[uint32]():]
	bs := stl.NewFixedTypeBytes[T]()
	bs.Storage = buf[:size]
	vec.ReadBytes(bs, true)
	n += int64(size)
	return
}

func (vec *StdVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	var nr int
	sizeBuf := make([]byte, stl.Sizeof[uint32]())
	if nr, err = r.Read(sizeBuf); err != nil {
		return
	}
	n += int64(nr)
	size := *(*uint32)(unsafe.Pointer(&sizeBuf[0]))
	if size == 0 {
		return
	}
	capacity := (int(size) + stl.Sizeof[T]() - 1) / stl.Sizeof[T]()
	vec.tryExpand(capacity)
	vec.buf = vec.node[:size]
	if nr, err = r.Read(vec.buf); err != nil {
		return
	}
	n += int64(nr)
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), capacity)
	return
}

func (vec *StdVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int
	dataSize := uint32(len(vec.buf))
	if nr, err = w.Write(unsafe.Slice((*byte)(unsafe.Pointer(&dataSize)), int(unsafe.Sizeof(dataSize)))); err != nil {
		return
	}
	n += int64(nr)
	if nr, err = w.Write(vec.buf); err != nil {
		return
	}
	n += int64(nr)
	return
}
