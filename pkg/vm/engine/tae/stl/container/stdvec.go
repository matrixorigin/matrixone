package container

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStdVector[T any](opts ...*Options) *stdVector[T] {
	vec := &stdVector[T]{
		buf:   make([]byte, 0),
		slice: make([]T, 0),
	}
	var capacity int
	var buf []byte
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		vec.alloc = opt.Allocator
		if opt.DataSize() > 0 {
			buf = opt.Data.Data
			capacity = len(buf) / stl.Sizeof[T]()
		}
	}
	if vec.alloc == nil {
		vec.alloc = stl.DefaultAllocator
	}
	if capacity == 0 {
		capacity = 4
	}
	if buf != nil {
		vec.buf = buf
		vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), capacity)
		vec.capacity = capacity
	} else {
		vec.tryExpand(capacity)
	}
	return vec
}

func (vec *stdVector[T]) tryExpand(capacity int) {
	if vec.capacity >= capacity {
		return
	}
	newSize := stl.SizeOfMany[T](capacity)
	vec.capacity = capacity
	oldn := vec.node
	if oldn != nil && newSize <= oldn.Size() {
		return
	} else if oldn == nil {
		if newSize <= cap(vec.buf) {
			return
		}
	}
	newn := vec.alloc.Alloc(capacity * stl.Sizeof[T]())
	buf := newn.GetBuf()[:0:newn.Size()]
	buf = append(buf, vec.buf...)
	vec.buf = buf
	vec.node = newn
	if oldn != nil {
		vec.alloc.Free(oldn)
	}
}

func (vec *stdVector[T]) Close() {
	if vec.node != nil {
		vec.alloc.Free(vec.node)
	}
	vec.node = nil
	vec.slice = nil
	vec.buf = nil
	vec.alloc = nil
}

func (vec *stdVector[T]) GetAllocator() stl.MemAllocator { return vec.alloc }

func (vec *stdVector[T]) IsView() bool  { return false }
func (vec *stdVector[T]) Length() int   { return len(vec.slice) }
func (vec *stdVector[T]) Capacity() int { return vec.capacity }
func (vec *stdVector[T]) Allocated() int {
	if vec.node != nil {
		return vec.node.Size()
	}
	return 0
}
func (vec *stdVector[T]) Data() []byte { return vec.buf }
func (vec *stdVector[T]) Slice() []T   { return vec.slice }
func (vec *stdVector[T]) SliceWindow(offset, length int) []T {
	return vec.slice[offset : offset+length]
}
func (vec *stdVector[T]) DataWindow(offset, length int) []byte {
	start := offset * stl.Sizeof[T]()
	end := start + length*stl.Sizeof[T]()
	return vec.buf[start:end]
}
func (vec *stdVector[T]) Desc() string {
	var v T
	s := fmt.Sprintf("StdVector<%s>:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		reflect.TypeOf(v).Name(),
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
	return s
}
func (vec *stdVector[T]) String() string {
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
	s = fmt.Sprintf("%s %s]", s, data)
	return s
}

func (vec *stdVector[T]) Append(v T) {
	if len(vec.slice) == vec.capacity {
		var newCap int
		if vec.capacity < 1024 {
			newCap = vec.capacity * 2
		} else {
			newCap = vec.capacity + 1024
		}
		vec.tryExpand(newCap)
	}
	vec.buf = append(vec.buf, unsafe.Slice((*byte)(unsafe.Pointer(&v)), int(unsafe.Sizeof(v)))...)
	size := len(vec.slice)
	vec.slice = unsafe.Slice((*T)(unsafe.Pointer(&vec.buf[0])), size+1)
}

func (vec *stdVector[T]) Get(i int) (v T) {
	v = vec.slice[i]
	return
}

func (vec *stdVector[T]) Update(i int, v T) {
	vec.slice[i] = v
}

func (vec *stdVector[T]) Delete(i int) (deleted T) {
	deleted = vec.slice[i]
	vec.slice = append(vec.slice[:i], vec.slice[i+1:]...)
	return
}

func (vec *stdVector[T]) RangeDelete(offset, length int) {
	vec.slice = append(vec.slice[:offset], vec.slice[offset+length:]...)
}

func (vec *stdVector[T]) AppendMany(vals ...T) {
	predictSize := len(vals) + len(vec.slice)
	if predictSize > vec.capacity {
		vec.tryExpand(predictSize)
	}
	vec.slice = append(vec.slice, vals...)
	vec.buf = unsafe.Slice((*byte)(unsafe.Pointer(&vec.slice[0])), stl.SizeOfMany[T](predictSize))
}

func (vec *stdVector[T]) Clone(offset, length int) stl.Vector[T] {
	opts := &Options{
		Capacity:  length,
		Allocator: vec.GetAllocator(),
	}
	cloned := NewStdVector[T](opts)
	cloned.AppendMany(vec.slice[offset : offset+length]...)
	return cloned
}

func (vec *stdVector[T]) Bytes() *stl.Bytes {
	bs := new(stl.Bytes)
	bs.Data = vec.buf
	return bs
}
