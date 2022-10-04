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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStrVector[T any](opts ...*Options) *StrVector[T] {
	var capacity int
	var alloc stl.MemAllocator
	vdataOpt := new(Options)
	areaOpt := new(Options)
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
		alloc = stl.DefaultAllocator
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

func (vec *StrVector[T]) GetAllocator() stl.MemAllocator {
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
func (vec *StrVector[T]) Slice() []T               { panic("not support") }
func (vec *StrVector[T]) SliceWindow(_, _ int) []T { panic("not support") }
func (vec *StrVector[T]) WindowAsBytes(offset, length int) *stl.Bytes {
	bs := vec.Bytes()
	if offset == 0 && length == vec.vdata.Length() {
		return bs
	}
	bs.ToWindow(offset, length)
	return bs
	// if offset == 0 && length == vec.vdata.Length() {
	// 	return vec.Bytes()
	// }
	// end := offset + length
	// bs := &stl.Bytes{}
	// bs.Header = vec.vdata.Slice()[offset:end]

	// // If vec has no data stored in area, skip to prepare area data
	// if vec.area.Length() == 0 {
	// 	return bs
	// }

	// // Get area data range in between [offset, offset+length)
	// min, max := vec.getAreaRange(offset, length)

	// // If min == max, no area data is stored in between [offset, offset+length)
	// if min == max {
	// 	return bs
	// }

	// // Window the area data in [min, max)
	// bs.Storage = vec.area.SliceWindow(min, max-min)
	// return bs
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
	val := any(v).([]byte)
	length := len(val)
	var vv types.Varlena

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

func (vec *StrVector[T]) Get(i int) T {
	v := vec.vdata.Get(i)
	if v.IsSmall() {
		return any(v.ByteSlice()).(T)
	}
	vOff, vLen := v.OffsetLen()
	return any(vec.area.Slice()[vOff : vOff+vLen]).(T)
}

func (vec *StrVector[T]) Update(i int, v T) {
	val := any(v).([]byte)
	nlen := len(val)

	oldVdata := vec.vdata.Get(i)
	oldOff, oldLen := oldVdata.OffsetLen()
	var newVdata types.Varlena

	if oldVdata.IsSmall() && nlen <= types.VarlenaInlineSize {
		// If both old and new data size is not larger than types.VarlenaInlineSize,
		// this update will not change vec.area
		newVdata[0] = byte(nlen)
		copy(newVdata[1:1+nlen], val)
		vec.vdata.Update(i, newVdata)
		return
	} else if !oldVdata.IsSmall() && oldLen == uint32(nlen) {
		// If both old and new data size is larger than types.VarlenaInlineSize
		// and the size is equal, this update only changes vec.area
		copy(vec.area.Slice()[oldOff:], val)
		return
	} else if !oldVdata.IsSmall() && nlen <= types.VarlenaInlineSize {
		// If the new is small and the old is not.

		// 1. Remove data from vec.area
		vec.area.RangeDelete(int(oldOff), int(oldLen))
		// 2. Update vdata in vec.vdata
		newVdata[0] = byte(nlen)
		copy(newVdata[1:1+nlen], val)
		vec.vdata.Update(i, newVdata)
		// 3. Adjust offset
		vec.adjustOffsetLen(i+1, -int(oldLen))
		return
	} else if oldVdata.IsSmall() && nlen > types.VarlenaInlineSize {
		// If the old is small and the new is not
		var offset int
		min, max := vec.getAreaRange(0, i)
		if min == max {
			offset = 0
		} else {
			offset = max
		}
		tail := vec.area.Slice()[offset:]
		val = append(val, tail...)
		vec.area.RangeDelete(offset, vec.area.Length()-offset)
		vec.area.AppendMany(val...)
		newVdata.SetOffsetLen(uint32(offset), uint32(nlen))
		vec.vdata.Update(i, newVdata)
		vec.adjustOffsetLen(i+1, nlen)
		return
	} else {
		// If both the old and new are not small and not equal

		offset := int(oldOff + oldLen)
		tail := vec.area.Slice()[offset:]
		val = append(val, tail...)
		vec.area.RangeDelete(int(oldOff), vec.area.Length()-int(oldOff))
		vec.area.AppendMany(val...)
		newVdata.SetOffsetLen(oldOff, uint32(nlen))
		vec.vdata.Update(i, newVdata)
		vec.adjustOffsetLen(i+1, nlen-int(oldLen))
	}
}

func (vec *StrVector[T]) Delete(i int) (deleted T) {
	deleted = vec.Get(i)
	vec.RangeDelete(i, 1)
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

func (vec *StrVector[T]) rangeDeleteNoArea(offset, length int) {
	vec.vdata.RangeDelete(offset, length)
}

func (vec *StrVector[T]) adjustOffsetLen(from int, delta int) {
	pos := from
	slice := vec.vdata.Slice()
	areaLen := uint32(vec.area.Length())
	var newOff uint32
	for pos < vec.Length() {
		if slice[pos].IsSmall() {
			pos++
			continue
		}
		oldOff, oldLen := slice[pos].OffsetLen()
		if delta >= 0 {
			newOff = oldOff + uint32(delta)
		} else {
			newOff = oldOff - uint32(-delta)
		}
		slice[pos].SetOffsetLen(newOff, oldLen)
		if newOff+oldLen == areaLen {
			break
		}
		pos++
	}
}

func (vec *StrVector[T]) RangeDelete(offset, length int) {
	if vec.area.Length() == 0 {
		vec.rangeDeleteNoArea(offset, length)
		return
	}
	min, max := vec.getAreaRange(offset, length)
	if min == max {
		vec.rangeDeleteNoArea(offset, length)
		return
	}
	vec.area.RangeDelete(min, max-min)
	vec.rangeDeleteNoArea(offset, length)
	vec.adjustOffsetLen(offset, -(max - min))
}

func (vec *StrVector[T]) AppendMany(vals ...T) {
	for _, val := range vals {
		vec.Append(val)
	}
}

func (vec *StrVector[T]) Clone(offset, length int, allocator ...stl.MemAllocator) stl.Vector[T] {
	opts := &Options{
		Capacity: length,
	}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}
	cloned := NewStrVector[T](opts)

	if offset == 0 {
		cloned.vdata.AppendMany(vec.vdata.SliceWindow(offset, length)...)
		min, max := vec.getAreaRange(offset, length)
		if min == max {
			return cloned
		}

		cloned.area.AppendMany(vec.area.SliceWindow(min, max-min)...)
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
