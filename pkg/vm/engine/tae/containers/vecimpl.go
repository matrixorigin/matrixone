package adaptor

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type vecImpl[T any] struct {
	*vecBase[T]
}

func newVecImpl[T any](derived *vector[T]) *vecImpl[T] {
	return &vecImpl[T]{
		vecBase: newVecBase[T](derived),
	}
}

type nullableVecImpl[T any] struct {
	*vecBase[T]
}

func newNullableVecImpl[T any](derived *vector[T]) *nullableVecImpl[T] {
	return &nullableVecImpl[T]{
		vecBase: newVecBase[T](derived),
	}
}
func (impl *nullableVecImpl[T]) Nullable() bool { return true }
func (impl *nullableVecImpl[T]) IsNull(i int) bool {
	return impl.derived.nulls != nil && impl.derived.nulls.Contains(uint64(i))
}

func (impl *nullableVecImpl[T]) HasNull() bool {
	return impl.derived.nulls != nil && !impl.derived.nulls.IsEmpty()
}

func (impl *nullableVecImpl[T]) Get(i int) (v any) {
	if impl.IsNull(i) {
		return types.Null{}
	}
	return impl.derived.stlvec.Get(i)
}

func (impl *nullableVecImpl[T]) Update(i int, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		if impl.derived.nulls == nil {
			impl.derived.nulls = roaring64.BitmapOf(uint64(i))
		} else {
			impl.derived.nulls.Add(uint64(i))
		}
		return
	}
	if impl.IsNull(i) {
		impl.derived.nulls.Remove(uint64(i))
	}
	impl.derived.stlvec.Update(i, v.(T))
}

func (impl *nullableVecImpl[T]) Delete(i int) {
	if impl.IsNull(i) {
		impl.derived.nulls.Remove(uint64(i))
	}
	impl.derived.stlvec.Delete(i)
}

func (impl *nullableVecImpl[T]) Append(v any) {
	offset := impl.derived.stlvec.Length()
	_, isNull := v.(types.Null)
	if isNull {
		if impl.derived.nulls == nil {
			impl.derived.nulls = roaring64.BitmapOf(uint64(offset))
		} else {
			impl.derived.nulls.Add(uint64(offset))
		}
		impl.derived.stlvec.Append(types.DefaultVal[T]())
	} else {
		impl.derived.stlvec.Append(v.(T))
	}
}
func (impl *nullableVecImpl[T]) Extend(o Vector) {
	if o.Nullable() {
		ovec := o.(*vector[T])
		if !ovec.HasNull() {
			impl.derived.stlvec.AppendMany(ovec.stlvec.Slice()...)
			return
		} else {
			if impl.derived.nulls == nil {
				impl.derived.nulls = roaring64.New()
			}
			it := ovec.nulls.Iterator()
			offset := impl.derived.stlvec.Length()
			for it.HasNext() {
				pos := it.Next()
				impl.derived.nulls.Add(uint64(offset) + pos)
			}
			impl.derived.stlvec.AppendMany(ovec.stlvec.Slice()...)
			return
		}
	}
	ovec := o.(*vector[T])
	impl.derived.stlvec.AppendMany(ovec.stlvec.Slice()...)
}
func (impl *nullableVecImpl[T]) String() string {
	s := impl.derived.stlvec.String()
	if impl.HasNull() {
		s = fmt.Sprintf("%s:Nulls=%s", s, impl.derived.nulls.String())
	}
	return s
}
