package adaptor

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type vector[T any] struct {
	impl stl.Vector[T]
	typ  types.Type
}

type nullableVector[T any] struct {
	*vector[T]
	nulls *roaring64.Bitmap
}

func NewVector[T any](typ types.Type, opts ...*Options) *vector[T] {
	return &vector[T]{
		impl: container.NewVector[T](opts...),
		typ:  typ,
	}
}

func NewNullableVector[T any](typ types.Type, opts ...*Options) *nullableVector[T] {
	return &nullableVector[T]{
		vector: NewVector[T](typ, opts...),
	}
}

func (vec *vector[T]) Nullable() bool              { return false }
func (vec *vector[T]) IsNull(i int) bool           { return false }
func (vec *vector[T]) HasNull() bool               { return false }
func (vec *vector[T]) NullMask() *roaring64.Bitmap { return nil }
func (vec *vector[T]) Data() []byte                { return vec.impl.Data() }
func (vec *vector[T]) Get(i int) (v any)           { return vec.impl.Get(i) }
func (vec *vector[T]) Update(i int, v any)         { vec.impl.Update(i, v.(T)) }
func (vec *vector[T]) Delete(i int)                { vec.impl.Delete(i) }
func (vec *vector[T]) Append(v any)                { vec.impl.Append(v.(T)) }
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

func (vec *nullableVector[T]) Nullable() bool { return true }
func (vec *nullableVector[T]) IsNull(i int) bool {
	return vec.nulls != nil && vec.nulls.Contains(uint64(i))
}

func (vec *nullableVector[T]) HasNull() bool {
	return vec.nulls != nil && !vec.nulls.IsEmpty()
}

func (vec *nullableVector[T]) NullMask() *roaring64.Bitmap { return vec.nulls }

func (vec *nullableVector[T]) Get(i int) (v any) {
	if vec.IsNull(i) {
		return types.Null{}
	}
	return vec.impl.Get(i)
}

func (vec *nullableVector[T]) Update(i int, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		if vec.nulls == nil {
			vec.nulls = roaring64.BitmapOf(uint64(i))
		} else {
			vec.nulls.Add(uint64(i))
		}
		return
	}
	if vec.IsNull(i) {
		vec.nulls.Remove(uint64(i))
	}
	vec.impl.Update(i, v.(T))
}

func (vec *nullableVector[T]) Delete(i int) {
	if vec.IsNull(i) {
		vec.nulls.Remove(uint64(i))
	}
	vec.impl.Delete(i)
}

func (vec *nullableVector[T]) Append(v any) {
	offset := vec.impl.Length()
	_, isNull := v.(types.Null)
	if isNull {
		if vec.nulls == nil {
			vec.nulls = roaring64.BitmapOf(uint64(offset))
		} else {
			vec.nulls.Add(uint64(offset))
		}
		vec.impl.Append(types.DefaultVal[T]())
	} else {
		vec.impl.Append(v.(T))
	}
}
func (vec *nullableVector[T]) Extend(o Vector) {
	if o.Nullable() {
		ovec := o.(*nullableVector[T])
		if !ovec.HasNull() {
			vec.impl.AppendMany(ovec.impl.Slice()...)
			return
		} else {
			if vec.nulls == nil {
				vec.nulls = roaring64.New()
			}
			it := ovec.nulls.Iterator()
			offset := vec.impl.Length()
			for it.HasNext() {
				pos := it.Next()
				vec.nulls.Add(uint64(offset) + pos)
			}
			vec.impl.AppendMany(ovec.impl.Slice()...)
			return
		}
	}
	ovec := o.(*vector[T])
	vec.impl.AppendMany(ovec.impl.Slice()...)
}
func (vec *nullableVector[T]) String() string {
	s := vec.impl.String()
	if vec.HasNull() {
		s = fmt.Sprintf("%s:Nulls=%s", s, vec.nulls.String())
	}
	return s
}
