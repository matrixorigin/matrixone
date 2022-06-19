package adaptor

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type nullableVector[T any] struct {
	*vector[T]
	nulls *roaring64.Bitmap
}

func NewNullableVector[T any](typ types.Type, opts ...*Options) *nullableVector[T] {
	return &nullableVector[T]{
		vector: NewVector[T](typ, opts...),
	}
}

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
