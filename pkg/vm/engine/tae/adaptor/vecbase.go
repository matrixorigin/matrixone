package adaptor

import (
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type vecBase[T any] struct {
	derived *vector[T]
}

func newVecBase[T any](derived *vector[T]) *vecBase[T] {
	return &vecBase[T]{
		derived: derived,
	}
}

func (base *vecBase[T]) IsView() bool                { return false }
func (base *vecBase[T]) Nullable() bool              { return false }
func (base *vecBase[T]) IsNull(i int) bool           { return false }
func (base *vecBase[T]) HasNull() bool               { return false }
func (base *vecBase[T]) NullMask() *roaring64.Bitmap { return base.derived.nulls }
func (base *vecBase[T]) Data() []byte                { return base.derived.stlvec.Data() }
func (base *vecBase[T]) DataWindow(offset, length int) []byte {
	return base.derived.stlvec.DataWindow(offset, length)
}
func (base *vecBase[T]) Bytes() *Bytes       { return base.derived.stlvec.Bytes() }
func (base *vecBase[T]) Get(i int) (v any)   { return base.derived.stlvec.Get(i) }
func (base *vecBase[T]) Update(i int, v any) { base.derived.stlvec.Update(i, v.(T)) }
func (base *vecBase[T]) Delete(i int)        { base.derived.stlvec.Delete(i) }
func (base *vecBase[T]) Append(v any)        { base.derived.stlvec.Append(v.(T)) }
func (base *vecBase[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		base.Append(v)
	}
}
func (base *vecBase[T]) Extend(o Vector) {
	ovec := o.(*vector[T])
	base.derived.stlvec.AppendMany(ovec.stlvec.Slice()...)
}
func (base *vecBase[T]) Length() int    { return base.derived.stlvec.Length() }
func (base *vecBase[T]) Capacity() int  { return base.derived.stlvec.Capacity() }
func (base *vecBase[T]) Allocated() int { return base.derived.stlvec.Allocated() }

func (base *vecBase[T]) GetAllocator() MemAllocator { return base.derived.stlvec.GetAllocator() }
func (base *vecBase[T]) GetType() types.Type        { return base.derived.typ }
func (base *vecBase[T]) String() string             { return base.derived.stlvec.String() }
func (base *vecBase[T]) Window() Vector             { return nil }

func (base *vecBase[T]) Close()                                    { base.derived.stlvec.Close() }
func (base *vecBase[T]) WriteTo(w io.Writer) (n int64, err error)  { return }
func (base *vecBase[T]) ReadFrom(r io.Reader) (n int64, err error) { return }
