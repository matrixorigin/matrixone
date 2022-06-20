package container

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStrVector[T any](opts ...*Options) *strVector[T] {
	var capacity int
	var alloc stl.MemAllocator
	lenOpt := new(Options)
	offOpt := new(Options)
	dataOpt := new(Options)
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		alloc = opt.Allocator
		if opt.HasData() {
			lenOpt.Data = new(stl.Bytes)
			offOpt.Data = new(stl.Bytes)
			dataOpt.Data = new(stl.Bytes)
			data := opt.Data.Length
			lenOpt.Data.Data = unsafe.Slice((*byte)(unsafe.Pointer(&data[0])),
				len(data)*stl.Sizeof[uint32]())
			data = opt.Data.Offset
			offOpt.Data.Data = unsafe.Slice((*byte)(unsafe.Pointer(&data[0])),
				len(data)*stl.Sizeof[uint32]())
			capacity = len(data)
			dataOpt.Data.Data = opt.Data.Data
		}
	}
	if alloc == nil {
		alloc = stl.DefaultAllocator
	}
	if capacity == 0 {
		capacity = 4
	}
	lenOpt.Capacity = capacity
	lenOpt.Allocator = alloc
	offOpt.Capacity = capacity
	offOpt.Allocator = alloc
	dataOpt.Capacity = capacity
	dataOpt.Allocator = alloc
	offsets := NewStdVector[uint32](offOpt)
	lengths := NewStdVector[uint32](lenOpt)
	data := NewStdVector[byte](dataOpt)

	return &strVector[T]{
		offsets: offsets,
		lengths: lengths,
		data:    data,
	}
}

func (vec *strVector[T]) Close() {
	if vec.offsets != nil {
		vec.offsets.Close()
	}
	if vec.lengths != nil {
		vec.lengths.Close()
	}
	if vec.data != nil {
		vec.data.Close()
	}
}

func (vec *strVector[T]) GetAllocator() stl.MemAllocator {
	return vec.offsets.GetAllocator()
}

func (vec *strVector[T]) Length() int   { return vec.lengths.Length() }
func (vec *strVector[T]) Capacity() int { return vec.lengths.Capacity() }
func (vec *strVector[T]) Allocated() int {
	return vec.lengths.Allocated() + vec.offsets.Allocated() + vec.data.Allocated()
}
func (vec *strVector[T]) IsView() bool             { return false }
func (vec *strVector[T]) Data() []byte             { return vec.data.Data() }
func (vec *strVector[T]) Slice() []T               { panic("not support") }
func (vec *strVector[T]) SliceWindow(_, _ int) []T { panic("not support") }
func (vec *strVector[T]) DataWindow(offset, length int) []byte {
	start := vec.offsets.Get(offset)
	eoff := vec.offsets.Get(offset + length - 1)
	elen := vec.lengths.Get(offset + length - 1)
	return vec.data.Data()[start:(eoff + elen)]
}
func (vec *strVector[T]) Desc() string {
	s := fmt.Sprintf("StrVector:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
	return s
}
func (vec *strVector[T]) String() string {
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

func (vec *strVector[T]) Append(v T) {
	val := any(v).([]byte)
	length := len(val)
	offset := vec.data.Length()
	vec.lengths.Append(uint32(length))
	vec.offsets.Append(uint32(offset))
	vec.data.AppendMany(val...)
}

func (vec *strVector[T]) Get(i int) T {
	s := vec.offsets.Get(i)
	l := vec.lengths.Get(i)
	return any(vec.data.Slice()[s : s+l]).(T)
}

func (vec *strVector[T]) Update(i int, v T) {
	val := any(v).([]byte)
	nlen := len(val)

	olen := vec.lengths.Get(i)
	offset := vec.offsets.Get(i)
	if int(olen) == nlen {
		copy(vec.data.Slice()[offset:], val)
		return
	}
	tail := vec.data.Slice()[olen+offset:]
	val = append(val, tail...)
	vec.data.RangeDelete(int(offset), vec.data.Length()-int(offset))
	vec.data.AppendMany(val...)
	vec.lengths.Update(i, uint32(nlen))
	delta := uint32(nlen) - olen
	for j := i + 1; j < vec.Length(); j++ {
		old := vec.offsets.Get(j)
		vec.offsets.Update(j, old+delta)
	}
}

func (vec *strVector[T]) Delete(i int) (deleted T) {
	s := vec.offsets.Get(i)
	l := vec.lengths.Get(i)

	deleted = any(vec.data.Slice()[s : s+l]).(T)
	vec.data.RangeDelete(int(s), int(l))
	vec.offsets.Delete(i)
	vec.lengths.Delete(i)
	for j := i; j < vec.Length(); j++ {
		old := vec.offsets.Get(j)
		vec.offsets.Update(j, old-l)
	}
	return
}

func (vec *strVector[T]) RangeDelete(offset, length int) {
	for i := offset + length - 1; i >= offset; i-- {
		vec.Delete(i)
	}
}

func (vec *strVector[T]) AppendMany(vals ...T) {
	for _, val := range vals {
		vec.Append(val)
	}
}

func (vec *strVector[T]) Clone(offset, length int) stl.Vector[T] {
	opts := &Options{
		Capacity:  length,
		Allocator: vec.GetAllocator(),
	}
	cloned := NewStrVector[T](opts)
	if offset == 0 {
		cloned.offsets.AppendMany(vec.offsets.Slice()[:length]...)
	} else {
		delta := vec.offsets.Get(offset)
		slice := vec.offsets.Slice()[offset : offset+length]
		for _, off := range slice {
			cloned.offsets.Append(off - delta)
		}
	}
	cloned.lengths.AppendMany(vec.lengths.Slice()[offset : offset+length]...)
	start := vec.offsets.Get(offset)
	eoff := vec.offsets.Get(offset + length - 1)
	elen := vec.lengths.Get(offset + length - 1)
	cloned.data.AppendMany(vec.data.Slice()[start : eoff+elen]...)

	return cloned
}

func (vec *strVector[T]) Reset() {
	vec.data.Reset()
	vec.offsets.Reset()
	vec.lengths.Reset()
}

func (vec *strVector[T]) Bytes() *stl.Bytes {
	bs := new(stl.Bytes)
	bs.Data = vec.data.Slice()
	bs.Offset = vec.offsets.Slice()
	bs.Length = vec.lengths.Slice()
	return bs
}

func (vec *strVector[T]) ReadBytes(bs *stl.Bytes, share bool) {
	if bs == nil {
		return
	}
	bs1 := stl.NewBytes()
	bs1.Data = bs.Data
	vec.data.ReadBytes(bs1, share)
	bs1.Data = bs.LengthBuf()
	vec.lengths.ReadBytes(bs1, share)
	bs1.Data = bs.OffsetBuf()
	vec.offsets.ReadBytes(bs1, share)
}

func (vec *strVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	var nr int64
	if nr, err = vec.data.ReadFrom(r); err != nil {
		return
	}
	n += nr
	if nr, err = vec.offsets.ReadFrom(r); err != nil {
		return
	}
	n += nr
	if nr, err = vec.lengths.ReadFrom(r); err != nil {
		return
	}
	n += nr
	return
}

func (vec *strVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int64
	if nr, err = vec.data.WriteTo(w); err != nil {
		return
	}
	n += nr
	if nr, err = vec.offsets.WriteTo(w); err != nil {
		return
	}
	n += nr
	if nr, err = vec.lengths.WriteTo(w); err != nil {
		return
	}
	n += nr
	return
}
