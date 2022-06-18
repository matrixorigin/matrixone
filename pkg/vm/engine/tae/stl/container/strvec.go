package container

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStrVector[T any](opts ...*Options) *strVector[T] {
	var capacity int
	var alloc stl.MemAllocator
	if len(opts) > 0 {
		capacity = opts[0].Capacity
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = stl.DefaultAllocator
	}
	if capacity == 0 {
		capacity = 4
	}
	opt := new(Options)
	opt.Capacity = capacity
	opt.Allocator = alloc
	offsets := NewStdVector[uint32](opt)
	lengths := NewStdVector[uint32](opt)
	data := NewStdVector[byte](opt)

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
func (vec *strVector[T]) Data() []byte { return vec.data.Data() }
func (vec *strVector[T]) Slice() []T   { panic("not support") }
func (vec *strVector[T]) String() string {
	s := fmt.Sprintf("StrVector:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]",
		vec.Length(),
		vec.Capacity(),
		vec.Allocated())
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

func (vec *strVector[T]) Set(i int, v T) (old T) {
	return
}

func (vec *strVector[T]) Delete(i int) (deleted T) {
	return
}

func (vec *strVector[T]) AppendMany(vals ...T) {
	for _, val := range vals {
		vec.Append(val)
	}
}
