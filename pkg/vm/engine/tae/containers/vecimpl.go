// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type vecImpl[T any] struct {
	*vecBase[T]
}

func newVecImpl[T any](derived *vector[T]) *vecImpl[T] {
	return &vecImpl[T]{
		vecBase: newVecBase(derived),
	}
}

type nullableVecImpl[T any] struct {
	*vecBase[T]
}

func newNullableVecImpl[T any](derived *vector[T]) *nullableVecImpl[T] {
	return &nullableVecImpl[T]{
		vecBase: newVecBase(derived),
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

// Modification
func (impl *nullableVecImpl[T]) Update(i int, v any) {
	impl.tryCOW()
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
	impl.tryCOW()
	if !impl.HasNull() {
		impl.vecBase.Delete(i)
		return
	}
	nulls := impl.derived.nulls
	max := nulls.Maximum()
	if max < uint64(i) {
		impl.vecBase.Delete(i)
		return
	} else if max == uint64(i) {
		nulls.Remove(uint64(i))
		impl.vecBase.Delete(i)
		return
	}
	nulls.Remove(uint64(i))
	dels := impl.derived.nulls.ToArray()
	for pos := len(dels) - 1; pos >= 0; pos-- {
		if dels[pos] < uint64(i) {
			break
		}
		nulls.Remove(dels[pos])
		nulls.Add(dels[pos] - 1)
	}
	impl.derived.stlvec.Delete(i)
}

func (impl *nullableVecImpl[T]) DeleteBatch(deletes *roaring.Bitmap) {
	impl.tryCOW()
	if !impl.HasNull() {
		arr := deletes.ToArray()
		for i := len(arr) - 1; i >= 0; i-- {
			impl.vecBase.Delete(int(arr[i]))
		}
		return
	}
	nulls := impl.derived.nulls
	max := nulls.Maximum()
	min := deletes.Minimum()
	if max < uint64(min) {
		arr := deletes.ToArray()
		for i := len(arr) - 1; i >= 0; i-- {
			impl.vecBase.Delete(int(arr[i]))
		}
		return
	} else if max == uint64(min) {
		arr := deletes.ToArray()
		for i := len(arr) - 1; i >= 0; i-- {
			impl.vecBase.Delete(int(arr[i]))
		}
		nulls.Remove(uint64(min))
		return
	}
	nullsIt := nulls.Iterator()
	arr := deletes.ToArray()
	deleted := 0
	arr = append(arr, uint32(impl.Length()))
	newNulls := roaring64.New()
	for _, idx := range arr {
		for nullsIt.HasNext() {
			null := nullsIt.PeekNext()
			if null < uint64(idx) {
				nullsIt.Next()
				newNulls.Add(null - uint64(deleted))
			} else {
				if null == uint64(idx) {
					nullsIt.Next()
				}
				break
			}
		}
		deleted++
	}
	impl.derived.nulls = newNulls
	impl.vecBase.DeleteBatch(deletes)
}

func (impl *nullableVecImpl[T]) Append(v any) {
	impl.tryCOW()
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
	impl.ExtendWithOffset(o, 0, o.Length())
}

func (impl *nullableVecImpl[T]) ExtendWithOffset(o Vector, srcOff, srcLen int) {
	impl.tryCOW()
	if o.Nullable() {
		if !o.HasNull() {
			impl.extendData(o, srcOff, srcLen)
			return
		} else {
			if impl.derived.nulls == nil {
				impl.derived.nulls = roaring64.New()
			}
			it := o.NullMask().Iterator()
			offset := impl.derived.stlvec.Length()
			for it.HasNext() {
				pos := it.Next()
				impl.derived.nulls.Add(uint64(offset) + pos - uint64(srcOff))
			}
			impl.extendData(o, srcOff, srcLen)
			return
		}
	}
	impl.extendData(o, srcOff, srcLen)
}
func (impl *nullableVecImpl[T]) String() string {
	s := impl.derived.stlvec.String()
	if impl.HasNull() {
		s = fmt.Sprintf("%s:Nulls=%s", s, impl.derived.nulls.String())
	}
	return s
}

func (impl *nullableVecImpl[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return impl.forEachWindowWithBias(offset, length, op, sels, 0)
}

func (impl *nullableVecImpl[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int) (err error) {
	if !impl.HasNull() {
		return impl.vecBase.forEachWindowWithBias(offset, length, op, sels, bias)
	}
	var v T
	if _, ok := any(v).([]byte); !ok {
		slice := impl.derived.stlvec.Slice()
		slice = slice[offset+bias : offset+length+bias]
		if sels == nil || sels.IsEmpty() {
			for i, elem := range slice {
				var v any
				if impl.IsNull(i + offset + bias) {
					v = types.Null{}
				} else {
					v = elem
				}
				if err = op(v, i+offset); err != nil {
					break
				}
			}
		} else {
			idxes := sels.ToArray()
			end := offset + length
			for _, idx := range idxes {
				if int(idx) < offset {
					continue
				} else if int(idx) >= end {
					break
				}
				var v any
				if impl.IsNull(int(idx) + bias) {
					v = types.Null{}
				} else {
					v = slice[int(idx)-offset]
				}
				if err = op(v, int(idx)); err != nil {
					break
				}
			}
		}
		return
	}

	if sels == nil || sels.IsEmpty() {
		for i := offset; i < offset+length; i++ {
			elem := impl.Get(i + bias)
			if err = op(elem, i); err != nil {
				break
			}
		}
		return
	}

	idxes := sels.ToArray()
	end := offset + length
	for _, idx := range idxes {
		if int(idx) < offset {
			continue
		} else if int(idx) >= end {
			break
		}
		elem := impl.Get(int(idx) + bias)
		if err = op(elem, int(idx)); err != nil {
			break
		}
	}
	return
}
