package container

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func New[T any](opts ...*Options) *stlVector[T] {
	vec := &stlVector[T]{
		buf:   make([]byte, 0),
		slice: make([]T, 0),
	}
	var capacity int
	if len(opts) > 0 {
		opt := opts[0]
		capacity = opt.Capacity
		vec.alloc = opt.Allocator
	}
	if vec.alloc == nil {
		vec.alloc = stl.DefaultAllocator
	}
	if capacity == 0 {
		capacity = 4
	}
	vec.tryExpand(capacity)
	return vec
}

func (vec *stlVector[T]) tryExpand(capacity int) {
	if vec.capacity >= capacity {
		return
	}
	newSize := stl.LengthOfVals[T](capacity)
	vec.capacity = capacity
	oldn := vec.node
	if oldn != nil && newSize <= oldn.Size() {
		return
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

func (vec *stlVector[T]) Close() {
	if vec.node != nil {
		vec.alloc.Free(vec.node)
	}
	vec.node = nil
	vec.slice = nil
	vec.buf = nil
	vec.alloc = nil
}

func (vec *stlVector[T]) GetAllocator() stl.MemAllocator { return vec.alloc }

func (vec *stlVector[T]) Length() int    { return len(vec.slice) }
func (vec *stlVector[T]) Capacity() int  { return vec.capacity }
func (vec *stlVector[T]) Allocated() int { return vec.node.Size() }
func (vec *stlVector[T]) Data() []byte   { return vec.buf }
func (vec *stlVector[T]) Slice() []T     { return vec.slice }
func (vec *stlVector[T]) String() string {
	var v T
	s := fmt.Sprintf("Vector<%s>:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		reflect.TypeOf(v).Name(),
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
	return s
}

func (vec *stlVector[T]) Append(v T) {
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

func (vec *stlVector[T]) Get(i int) (v T) {
	v = vec.slice[i]
	return
}

func (vec *stlVector[T]) Set(i int, v T) (old T) {
	old = vec.slice[i]
	vec.slice[i] = v
	return
}

func (vec *stlVector[T]) Delete(i int) (deleted T) {
	deleted = vec.slice[i]
	vec.slice = append(vec.slice[:i], vec.slice[i+1:]...)
	return
}

func (vec *stlVector[T]) AppendMany(vals ...T) {
	predictSize := len(vals) + len(vec.slice)
	if predictSize > vec.capacity {
		vec.tryExpand(predictSize)
	}
	vec.slice = append(vec.slice, vals...)
}
