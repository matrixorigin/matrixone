package adaptor

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type vector[T any] struct {
	impl stl.Vector[T]
	typ  types.Type
}

func NewVector[T any](typ types.Type, opts ...*Options) *vector[T] {
	return &vector[T]{
		impl: container.NewVector[T](opts...),
		typ:  typ,
	}
}

func (vec *vector[T]) IsView() bool                { return false }
func (vec *vector[T]) Nullable() bool              { return false }
func (vec *vector[T]) IsNull(i int) bool           { return false }
func (vec *vector[T]) HasNull() bool               { return false }
func (vec *vector[T]) NullMask() *roaring64.Bitmap { return nil }
func (vec *vector[T]) Data() []byte                { return vec.impl.Data() }
func (vec *vector[T]) DataWindow(offset, length int) []byte {
	return vec.impl.DataWindow(offset, length)
}
func (vec *vector[T]) Get(i int) (v any)   { return vec.impl.Get(i) }
func (vec *vector[T]) Update(i int, v any) { vec.impl.Update(i, v.(T)) }
func (vec *vector[T]) Delete(i int)        { vec.impl.Delete(i) }
func (vec *vector[T]) Append(v any)        { vec.impl.Append(v.(T)) }
func (vec *vector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}
func (vec *vector[T]) Extend(o Vector) {
	ovec := o.(*vector[T])
	vec.impl.AppendMany(ovec.impl.Slice()...)
}
func (vec *vector[T]) Length() int    { return vec.impl.Length() }
func (vec *vector[T]) Capacity() int  { return vec.impl.Capacity() }
func (vec *vector[T]) Allocated() int { return vec.impl.Allocated() }

func (vec *vector[T]) GetAllocator() MemAllocator { return vec.impl.GetAllocator() }
func (vec *vector[T]) GetType() types.Type        { return vec.typ }
func (vec *vector[T]) String() string             { return vec.impl.String() }
func (vec *vector[T]) Close()                     { vec.impl.Close() }

func (vec *vector[T]) Window() Vector { return nil }
