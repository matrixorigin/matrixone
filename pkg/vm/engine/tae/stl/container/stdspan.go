package container

import (
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

func NewStdSpan[T any](slice []T) *stdSpan[T] {
	if len(slice) == 0 {
		return &stdSpan[T]{
			slice: slice,
			buf:   make([]byte, 0),
		}
	}
	span := &stdSpan[T]{
		slice: slice,
	}
	span.buf = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), stl.Sizeof[T]()*len(slice))
	return span
}

func (span *stdSpan[T]) GetAllocator() stl.MemAllocator { return nil }

func (span *stdSpan[T]) Close() {
	span.slice = nil
	span.buf = nil
}
func (span *stdSpan[T]) IsView() bool   { return true }
func (span *stdSpan[T]) Length() int    { return len(span.slice) }
func (span *stdSpan[T]) Capacity() int  { return len(span.slice) }
func (span *stdSpan[T]) Allocated() int { return 0 }
func (span *stdSpan[T]) Slice() []T     { return span.slice }
func (span *stdSpan[T]) SliceWindow(offset, length int) []T {
	return span.slice[offset : offset+length]
}
func (span *stdSpan[T]) Data() []byte { return span.buf }
func (span *stdSpan[T]) Bytes() *stl.Bytes {
	bs := new(stl.Bytes)
	bs.Data = span.buf
	return bs
}
func (span *stdSpan[T]) DataWindow(offset, length int) []byte {
	start := offset * stl.Sizeof[T]()
	end := start + length*stl.Sizeof[T]()
	return span.buf[start:end]
}

func (span *stdSpan[T]) Get(i int) (v T)                           { return span.slice[i] }
func (span *stdSpan[T]) Update(i int, v T)                         { panic("span cannot modify") }
func (span *stdSpan[T]) Delete(i int) (deleted T)                  { panic("span cannot modify") }
func (span *stdSpan[T]) RangeDelete(offset, length int)            { panic("span cannot modify") }
func (span *stdSpan[T]) AppendMany(vals ...T)                      { panic("span cannot modify") }
func (span *stdSpan[T]) Append(v T)                                { panic("span cannot modify") }
func (span *stdSpan[T]) Clone(_, _ int) stl.Vector[T]              { panic("span cannot clone") }
func (span *stdSpan[T]) Desc() string                              { return "" }
func (span *stdSpan[T]) String() string                            { return "" }
func (span *stdSpan[T]) WriteTo(_ io.Writer) (n int64, err error)  { return }
func (span *stdSpan[T]) ReadFrom(_ io.Reader) (n int64, err error) { return }
