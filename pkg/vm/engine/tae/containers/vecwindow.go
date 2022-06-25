package containers

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type windowBase struct {
	offset, length int
}

func (win *windowBase) IsView() bool                          { return true }
func (win *windowBase) Update(i int, v any)                   { panic("cannot modify window") }
func (win *windowBase) Delete(i int)                          { panic("cannot modify window") }
func (win *windowBase) Append(v any)                          { panic("cannot modify window") }
func (win *windowBase) Compact(*roaring.Bitmap)               { panic("cannot modify window") }
func (win *windowBase) AppendMany(vs ...any)                  { panic("cannot modify window") }
func (win *windowBase) Extend(o Vector)                       { panic("cannot modify window") }
func (win *windowBase) ExtendWithOffset(_ Vector, _, _ int)   { panic("cannot modify window") }
func (win *windowBase) Length() int                           { return win.length }
func (win *windowBase) Capacity() int                         { return win.length }
func (win *windowBase) Allocated() int                        { return 0 }
func (win *windowBase) Window(offset, length int) Vector      { panic("cannot window a window") }
func (win *windowBase) DataWindow(offset, length int) []byte  { panic("cannot window a window") }
func (win *windowBase) CloneWindow(offset, length int) Vector { panic("not implemented") }
func (win *windowBase) GetView() VectorView                   { panic("not implemented") }
func (win *windowBase) Close()                                {}
func (win *windowBase) Equals(o Vector) bool                  { panic("implement me") }
func (win *windowBase) ReadFrom(io.Reader) (int64, error)     { panic("not implemented") }

func (win *windowBase) ReadFromFile(common.IVFile, *bytes.Buffer) error { panic("not implemented") }
func (win *windowBase) ResetWithData(*Bytes, *roaring64.Bitmap)         { panic("not implemented") }

type vectorWindow[T any] struct {
	*windowBase
	ref *vector[T]
}

func (win *vectorWindow[T]) Data() []byte {
	return win.ref.DataWindow(win.offset, win.length)
}
func (win *vectorWindow[T]) Get(i int) (v any) {
	return win.ref.Get(i + win.offset)
}

func (win *vectorWindow[T]) Nullable() bool { return win.ref.Nullable() }
func (win *vectorWindow[T]) HasNull() bool  { return win.ref.HasNull() }
func (win *vectorWindow[T]) NullMask() *roaring64.Bitmap {
	mask := win.ref.NullMask()
	if win.offset == 0 || mask == nil {
		return mask
	}
	return common.BM64Window(mask, win.offset, win.offset+win.length)
}
func (win *vectorWindow[T]) IsNull(i int) bool {
	return win.ref.IsNull(i + win.offset)
}
func (win *vectorWindow[T]) GetAllocator() MemAllocator { return win.ref.GetAllocator() }
func (win *vectorWindow[T]) GetType() types.Type        { return win.ref.GetType() }
func (win *vectorWindow[T]) String() string             { return win.ref.String() }
func (win *vectorWindow[T]) Slice() any {
	var v T
	if _, ok := any(v).([]byte); ok {
		base := win.ref.Slice().(*Bytes)
		return base.Window(win.offset, win.length)
	} else {
		return win.ref.Slice().([]T)[win.offset : win.offset+win.length]
	}
}
func (win *vectorWindow[T]) Bytes() *Bytes {
	var v T
	if _, ok := any(v).([]byte); ok {
		base := win.ref.Slice().(*Bytes)
		return base.Window(win.offset, win.length)
	} else {
		bs := win.ref.Bytes()
		bs.Data = bs.Data[win.offset*stl.Sizeof[T]() : (win.offset+win.length)*stl.Sizeof[T]()]
		return bs
	}
}
func (win *vectorWindow[T]) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	return win.ref.impl.forEachWindowWithBias(0, win.length, op, sels, win.offset)
}
func (win *vectorWindow[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	if offset+length > win.length {
		panic("bad param")
	}
	return win.ref.impl.forEachWindowWithBias(offset, length, op, sels, win.offset)
}
func (win *vectorWindow[T]) WriteTo(w io.Writer) (n int64, err error) { panic("implement me") }
