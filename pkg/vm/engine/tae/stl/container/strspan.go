package container

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"

func NewStrSpan[T any](buf []byte, offsets, lengths []uint32) *strSpan[T] {
	return &strSpan[T]{
		buf:     buf,
		offsets: offsets,
		lengths: lengths,
	}
}
func (span *strSpan[T]) IsView() bool                   { return true }
func (span *strSpan[T]) GetAllocator() stl.MemAllocator { return nil }

func (span *strSpan[T]) Close() {
	span.buf = nil
	span.offsets = nil
	span.lengths = nil
}
func (span *strSpan[T]) Length() int                        { return len(span.offsets) }
func (span *strSpan[T]) Capacity() int                      { return len(span.offsets) }
func (span *strSpan[T]) Allocated() int                     { return 0 }
func (span *strSpan[T]) Slice() []T                         { panic("not support") }
func (span *strSpan[T]) SliceWindow(offset, length int) []T { panic("not support") }
func (span *strSpan[T]) Bytes() *stl.Bytes {
	bs := new(stl.Bytes)
	bs.Data = span.buf
	bs.Offset = span.offsets
	bs.Length = span.lengths
	return bs
}
func (span *strSpan[T]) Data() []byte { return span.buf }
func (span *strSpan[T]) DataWindow(offset, length int) []byte {
	start := span.offsets[offset]
	eoff := span.offsets[offset+length-1]
	elen := span.lengths[offset+length-1]
	return span.buf[start:(eoff + elen)]
}

func (span *strSpan[T]) Get(i int) (v T) {
	s := span.offsets[i]
	l := span.lengths[i]
	return any(span.buf[s : s+l]).(T)
}
func (span *strSpan[T]) Update(i int, v T)              { panic("span cannot modify") }
func (span *strSpan[T]) Delete(i int) (deleted T)       { panic("span cannot modify") }
func (span *strSpan[T]) RangeDelete(offset, length int) { panic("span cannot modify") }
func (span *strSpan[T]) AppendMany(vals ...T)           { panic("span cannot modify") }
func (span *strSpan[T]) Append(v T)                     { panic("span cannot modify") }
func (span *strSpan[T]) Clone(v T)                      { panic("span cannot clone") }
func (span *strSpan[T]) Desc() string                   { return "" }
func (span *strSpan[T]) String() string                 { return "" }
