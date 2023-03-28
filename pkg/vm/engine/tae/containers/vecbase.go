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
	"bytes"
	"fmt"
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type vecBase[T any] struct {
	derived *vector[T]
}

func newVecBase[T any](derived *vector[T]) *vecBase[T] {
	return &vecBase[T]{
		derived: derived,
	}
}

func (base *vecBase[T]) Window(offset, length int) Vector               { panic("not supported") }
func (base *vecBase[T]) CloneWindow(_, _ int, _ ...*mpool.MPool) Vector { panic("not supported") }
func (base *vecBase[T]) ResetWithData(_ *Bytes, _ *roaring64.Bitmap)    { panic("not supported") }
func (base *vecBase[T]) Reset()                                         { panic("not supported") }
func (base *vecBase[T]) Equals(o Vector) bool                           { panic("not supported") }
func (base *vecBase[T]) IsView() bool                                   { return false }
func (base *vecBase[T]) Nullable() bool                                 { return false }
func (base *vecBase[T]) IsNull(i int) bool                              { return false }
func (base *vecBase[T]) HasNull() bool                                  { return false }
func (base *vecBase[T]) NullMask() *roaring64.Bitmap                    { return base.derived.nulls }
func (base *vecBase[T]) Slice() any                                     { panic("not supported") }
func (base *vecBase[T]) SlicePtr() unsafe.Pointer                       { panic("not supported") }
func (base *vecBase[T]) Data() []byte                                   { return base.derived.stlvec.Data() }
func (base *vecBase[T]) DataWindow(offset, length int) []byte {
	return base.derived.stlvec.DataWindow(offset, length)
}
func (base *vecBase[T]) Bytes() *Bytes            { return base.derived.stlvec.Bytes() }
func (base *vecBase[T]) Get(i int) (v any)        { return base.derived.stlvec.Get(i) }
func (base *vecBase[T]) ShallowGet(i int) (v any) { return base.derived.stlvec.ShallowGet(i) }

func (base *vecBase[T]) tryCOW() {
	if base.derived.roStorage != nil {
		base.derived.cow()
	}
}

// Modification
func (base *vecBase[T]) Update(i int, v any) { base.derived.stlvec.Update(i, v.(T)) }
func (base *vecBase[T]) Delete(i int)        { base.derived.stlvec.Delete(i) }
func (base *vecBase[T]) Compact(deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	base.derived.stlvec.BatchDelete(
		deletes.Iterator(),
		int(deletes.GetCardinality()))
}
func (base *vecBase[T]) Append(v any) {
	base.tryCOW()
	base.derived.stlvec.Append(v.(T))
}
func (base *vecBase[T]) AppendMany(vs ...any) {
	base.tryCOW()
	for _, v := range vs {
		base.Append(v)
	}
}
func (base *vecBase[T]) AppendNoNulls(s any) {
	base.tryCOW()
	slice := s.([]T)
	base.derived.stlvec.AppendMany(slice...)
}
func (base *vecBase[T]) Extend(o Vector) {
	base.ExtendWithOffset(o, 0, o.Length())
}

func (base *vecBase[T]) extendData(src Vector, srcOff, srcLen int) {
	if srcLen < 1 {
		return
	}
	if base.derived.typ.IsVarlen() {
		bs := src.Bytes()
		for i := srcOff; i < srcOff+srcLen; i++ {
			base.derived.stlvec.Append(any(bs.GetVarValueAt(i)).(T))
		}
		return
	}
	slice := unsafe.Slice((*T)(src.SlicePtr()), srcOff+srcLen)
	base.derived.stlvec.AppendMany(slice[srcOff : srcOff+srcLen]...)
}

func (base *vecBase[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	if srcLen <= 0 {
		return
	}
	base.tryCOW()
	base.extendData(src, srcOff, srcLen)
}

func (base *vecBase[T]) Length() int   { return base.derived.stlvec.Length() }
func (base *vecBase[T]) Capacity() int { return base.derived.stlvec.Capacity() }
func (base *vecBase[T]) Allocated() int {
	if base.derived.roStorage != nil {
		return len(base.derived.roStorage)
	}
	return base.derived.stlvec.Allocated()
}

func (base *vecBase[T]) GetAllocator() *mpool.MPool { return base.derived.stlvec.GetAllocator() }
func (base *vecBase[T]) GetType() types.Type        { return base.derived.typ }
func (base *vecBase[T]) String() string {
	s := base.derived.stlvec.String()
	if base.derived.roStorage != nil {
		s = fmt.Sprintf("%s;[RoAlloc=%d]", s, len(base.derived.roStorage))
	}
	return s
}
func (base *vecBase[T]) PPString(num int) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[T=%s][Len=%d][Data=(", base.GetType().String(), base.Length()))
	limit := base.Length()
	if num > 0 && num < limit {
		limit = num
	}
	size := base.Length()
	long := false
	if size > limit {
		long = true
		size = limit
	}
	for i := 0; i < size; i++ {
		if base.IsNull(i) {
			_, _ = w.WriteString("null")
			continue
		}
		if base.GetType().IsVarlen() {
			_, _ = w.WriteString(fmt.Sprintf("%s, ", base.ShallowGet(i).([]byte)))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("%v, ", base.Get(i)))
		}
	}
	if long {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(")]")
	return w.String()
}

func (base *vecBase[T]) Close() {
	base.derived.releaseRoStorage()
	base.derived.stlvec.Close()
}
func (base *vecBase[T]) WriteTo(w io.Writer) (n int64, err error)  { panic("not supported") }
func (base *vecBase[T]) ReadFrom(r io.Reader) (n int64, err error) { panic("not supported") }
func (base *vecBase[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	panic("not supported")
}

func (base *vecBase[T]) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	panic("not supported")
}

func (base *vecBase[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return base.forEachWindowWithBias(offset, length, op, sels, 0, false)
}

func (base *vecBase[T]) ForeachShallow(op ItOp, sels *roaring.Bitmap) (err error) {
	panic("not supported")
}

func (base *vecBase[T]) ForeachWindowShallow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return base.forEachWindowWithBias(offset, length, op, sels, 0, true)
}

func (base *vecBase[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int, shallow bool) (err error) {
	var v T
	if _, ok := any(v).([]byte); !ok {
		slice := base.derived.stlvec.Slice()
		slice = slice[offset+bias : offset+length+bias]
		if sels == nil || sels.IsEmpty() {
			for i, elem := range slice {
				if err = op(elem, false, i+offset); err != nil {
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
				if err = op(slice[int(idx)-offset], false, int(idx)); err != nil {
					break
				}
			}
		}
		return
	}
	if sels == nil || sels.IsEmpty() {
		for i := offset; i < offset+length; i++ {
			var elem any
			if shallow {
				elem = base.derived.stlvec.ShallowGet(i + bias)
			} else {
				elem = base.derived.stlvec.Get(i + bias)
			}
			if err = op(elem, false, i); err != nil {
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
		var elem any
		if shallow {
			elem = base.derived.stlvec.ShallowGet(int(idx) + bias)
		} else {
			elem = base.derived.stlvec.Get(int(idx) + bias)
		}
		if err = op(elem, false, int(idx)); err != nil {
			break
		}
	}
	return
}

func (base *vecBase[T]) GetView() (view VectorView) { panic("not supported") }
