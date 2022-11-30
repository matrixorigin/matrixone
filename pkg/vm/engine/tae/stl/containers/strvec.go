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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStrVector[T any](opts ...Options) *StrVector[T] {
	var capacity int
	var alloc *mpool.MPool
	vdataOpt := Options{}
	areaOpt := Options{}
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		alloc = opt.Allocator
		if opt.HasData() {
			if opt.Data.HeaderSize() > 0 {
				vdataOpt.Data = stl.NewFixedTypeBytes[types.Varlena]()
				vdataOpt.Data.Storage = opt.Data.HeaderBuf()
				vdataOpt.Capacity = opt.Data.Length()
			}
			if opt.Data.StorageSize() > 0 {
				vdataOpt.Data = stl.NewFixedTypeBytes[byte]()
				areaOpt.Data.Storage = opt.Data.StorageBuf()
				areaOpt.Capacity = opt.Data.StorageSize()
			}
		}
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	if vdataOpt.Capacity < capacity {
		vdataOpt.Capacity = capacity
	}
	vdataOpt.Allocator = alloc
	areaOpt.Allocator = alloc

	vdata := NewStdVector[types.Varlena](vdataOpt)
	area := NewStdVector[byte](areaOpt)
	return &StrVector[T]{
		area:  area,
		vdata: vdata,
	}
}

func (vec *StrVector[T]) Close() {
	if vec.vdata != nil {
		vec.vdata.Close()
	}
	if vec.area != nil {
		vec.area.Close()
	}
}

func (vec *StrVector[T]) GetAllocator() *mpool.MPool {
	return vec.vdata.GetAllocator()
}

func (vec *StrVector[T]) Length() int   { return vec.vdata.Length() }
func (vec *StrVector[T]) Capacity() int { return vec.vdata.Capacity() }
func (vec *StrVector[T]) Allocated() int {
	size := vec.vdata.Allocated()
	size += vec.area.Allocated()
	return size
}
func (vec *StrVector[T]) IsView() bool                         { return false }
func (vec *StrVector[T]) Data() []byte                         { panic("not support") }
func (vec *StrVector[T]) DataWindow(offset, length int) []byte { panic("not support") }
func (vec *StrVector[T]) Bytes() *stl.Bytes {
	bs := stl.NewBytesWithTypeSize(-types.VarlenaSize)
	bs.Header = vec.vdata.Slice()
	bs.Storage = vec.area.Slice()

	return bs
}
func (vec *StrVector[T]) SlicePtr() unsafe.Pointer { panic("not support") }
func (vec *StrVector[T]) Slice() []T               { panic("not support") }
func (vec *StrVector[T]) SliceWindow(_, _ int) []T { panic("not support") }
func (vec *StrVector[T]) WindowAsBytes(offset, length int) *stl.Bytes {
	bs := vec.Bytes()
	if offset == 0 && length == vec.vdata.Length() {
		return bs
	}
	bs.ToWindow(offset, length)
	return bs
}
func (vec *StrVector[T]) Desc() string {
	s := fmt.Sprintf("StrVector:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
	return s
}
func (vec *StrVector[T]) String() string {
	s := vec.Desc()
	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
	}
	data := ""
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, vec.Get(i))
	}
	s = fmt.Sprintf("%s %s", s, data)
	return s
}

func (vec *StrVector[T]) Append(v T) {
	var vv types.Varlena
	val := any(v).([]byte)
	length := len(val)

	// If the length of the data is not larger than types.VarlenaInlineSize, store the
	// data in vec.vdata
	if length <= types.VarlenaInlineSize {
		vv[0] = byte(length)
		copy(vv[1:1+length], val)
		vec.vdata.Append(vv)
		return
	}

	// If the length of the data is larger than types.VarlenaInlineSize, store the
	// offset and length in vec.vdata and store the data in vec.area
	vv.SetOffsetLen(uint32(vec.area.Length()), uint32(length))
	vec.vdata.Append(vv)
	vec.area.AppendMany(val...)
}

func (vec *StrVector[T]) Update(i int, v T) {
	val := any(v).([]byte)
	length := len(val)
	var vv types.Varlena
	if length <= types.VarlenaInlineSize {
		vv[0] = byte(length)
		copy(vv[1:1+length], val)
		vec.vdata.Update(i, vv)
		return
	}
	vv.SetOffsetLen(uint32(vec.area.Length()), uint32(length))
	vec.vdata.Update(i, vv)
	vec.area.AppendMany(val...)
}

func (vec *StrVector[T]) Get(i int) T {
	v := vec.vdata.Get(i)
	if v.IsSmall() {
		return any(v.ByteSlice()).(T)
	}
	vOff, vLen := v.OffsetLen()
	return any(vec.area.Slice()[vOff : vOff+vLen]).(T)
}

func (vec *StrVector[T]) Delete(i int) (deleted T) {
	deleted = vec.Get(i)
	vec.BatchDeleteInts(i)
	return
}

func (vec *StrVector[T]) BatchDelete(rowGen common.RowGen, cnt int) {
	vec.vdata.BatchDelete(rowGen, cnt)
}
func (vec *StrVector[T]) BatchDeleteUint32s(sels ...uint32) {
	vec.vdata.BatchDeleteUint32s(sels...)
}
func (vec *StrVector[T]) BatchDeleteInts(sels ...int) {
	vec.vdata.BatchDeleteInts(sels...)
}

func (vec *StrVector[T]) AppendMany(vals ...T) {
	for _, val := range vals {
		vec.Append(val)
	}
}

func (vec *StrVector[T]) Clone(offset, length int, allocator ...*mpool.MPool) stl.Vector[T] {
	opts := Options{
		Capacity: length,
	}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}
	cloned := NewStrVector[T](opts)

	if offset == 0 && length == vec.Length() {
		cloned.vdata.AppendMany(vec.vdata.Slice()...)
		cloned.area.AppendMany(vec.area.Slice()...)
	} else {
		for i := offset; i < offset+length; i++ {
			cloned.Append(vec.Get(i))
		}
	}
	return cloned
}

func (vec *StrVector[T]) Reset() {
	vec.vdata.Reset()
	if vec.area != nil {
		vec.area.Reset()
	}
}

func (vec *StrVector[T]) ReadBytes(data *stl.Bytes, share bool) {
	if data == nil {
		return
	}
	d1 := stl.NewFixedTypeBytes[types.Varlena]()
	d1.Storage = data.HeaderBuf()
	vec.vdata.ReadBytes(d1, share)

	d2 := stl.NewFixedTypeBytes[byte]()
	d2.Storage = data.StorageBuf()
	vec.area.ReadBytes(d2, share)
}

func (vec *StrVector[T]) InitFromSharedBuf(buf []byte) (n int64, err error) {
	var nr int64
	if nr, err = vec.vdata.InitFromSharedBuf(buf); err != nil {
		return
	}
	n += nr
	buf = buf[nr:]
	if nr, err = vec.area.InitFromSharedBuf(buf); err != nil {
		return
	}
	n += nr
	return
}

func (vec *StrVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	var nr int64
	if nr, err = vec.vdata.ReadFrom(r); err != nil {
		return
	}
	n += nr
	if nr, err = vec.area.ReadFrom(r); err != nil {
		return
	}
	n += nr
	return
}

func (vec *StrVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int64
	if nr, err = vec.vdata.WriteTo(w); err != nil {
		return
	}
	n += nr
	if nr, err = vec.area.WriteTo(w); err != nil {
		return
	}
	n += nr
	return
}

func (vec *StrVector[T]) getAreaRange(offset, length int) (min, max int) {
	var pos int
	slice := vec.vdata.Slice()
	end := offset + length
	for pos = offset; pos < end; pos++ {
		if slice[pos].IsSmall() {
			continue
		}
		min32, _ := slice[pos].OffsetLen()
		min = int(min32)
		break
	}
	// no data stored in area
	if pos == end {
		return
	}
	minPos := pos
	for pos = end - 1; pos >= minPos; pos-- {
		if slice[pos].IsSmall() {
			continue
		}
		max32, maxLen := slice[pos].OffsetLen()
		max = int(max32 + maxLen)
		break
	}
	return
}
