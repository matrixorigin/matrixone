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

type windowBase struct {
	offset, length int
}

func (win *windowBase) IsView() bool                         { return true }
func (win *windowBase) Update(i int, v any)                  { panic("cannot modify window") }
func (win *windowBase) Delete(i int)                         { panic("cannot modify window") }
func (win *windowBase) Compact(deletes *roaring.Bitmap)      { panic("cannot modify window") }
func (win *windowBase) Append(v any)                         { panic("cannot modify window") }
func (win *windowBase) AppendMany(vs ...any)                 { panic("cannot modify window") }
func (win *windowBase) AppendNoNulls(s any)                  { panic("cannot modify window") }
func (win *windowBase) Extend(o Vector)                      { panic("cannot modify window") }
func (win *windowBase) ExtendWithOffset(_ Vector, _, _ int)  { panic("cannot modify window") }
func (win *windowBase) Length() int                          { return win.length }
func (win *windowBase) Capacity() int                        { return win.length }
func (win *windowBase) Allocated() int                       { return 0 }
func (win *windowBase) DataWindow(offset, length int) []byte { panic("cannot window a window") }
func (win *windowBase) Close()                               {}
func (win *windowBase) ReadFrom(io.Reader) (int64, error)    { panic("cannot modify window") }

func (win *windowBase) ReadFromFile(common.IVFile, *bytes.Buffer) error {
	panic("cannot modify window")
}
func (win *windowBase) Reset()                                  { panic("cannot modify window") }
func (win *windowBase) ResetWithData(*Bytes, *roaring64.Bitmap) { panic("cannot modify window") }

type vectorWindow[T any] struct {
	*windowBase
	ref *vector[T]
}

func (win *vectorWindow[T]) Equals(o Vector) bool {
	if win.Length() != o.Length() {
		return false
	}
	if win.GetType() != o.GetType() {
		return false
	}
	if win.Nullable() != o.Nullable() {
		return false
	}
	if win.HasNull() != o.HasNull() {
		return false
	}
	if win.HasNull() {
		if !win.NullMask().Equals(o.NullMask()) {
			return false
		}
	}
	mask := win.NullMask()
	for i := 0; i < win.Length(); i++ {
		if mask != nil && mask.ContainsInt(i) {
			continue
		}
		var v T
		if _, ok := any(v).([]byte); ok {
			if !bytes.Equal(win.ShallowGet(i).([]byte), o.ShallowGet(i).([]byte)) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal64); ok {
			d := win.Get(i).(types.Decimal64)
			od := win.Get(i).(types.Decimal64)
			if d != od {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := win.Get(i).(types.Decimal128)
			od := win.Get(i).(types.Decimal128)
			if d != od {
				return false
			}
		} else if _, ok := any(v).(types.TS); ok {
			d := win.Get(i).(types.TS)
			od := win.Get(i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if _, ok := any(v).(types.Rowid); ok {
			d := win.Get(i).(types.Rowid)
			od := win.Get(i).(types.Rowid)
			if types.CompareRowidRowidAligned(d, od) != 0 {
				return false
			}
		} else {
			if win.Get(i) != o.Get(i) {
				return false
			}
		}
	}
	return true

}

func (win *vectorWindow[T]) GetView() VectorView {
	return &vectorWindow[T]{
		ref: win.ref,
		windowBase: &windowBase{
			offset: win.offset,
			length: win.length,
		},
	}
}

func (win *vectorWindow[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	return win.ref.CloneWindow(offset+win.offset, length, allocator...)
}

func (win *vectorWindow[T]) Data() []byte {
	return win.ref.DataWindow(win.offset, win.length)
}
func (win *vectorWindow[T]) Get(i int) (v any) {
	return win.ref.Get(i + win.offset)
}
func (win *vectorWindow[T]) ShallowGet(i int) (v any) {
	return win.ref.ShallowGet(i + win.offset)
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
func (win *vectorWindow[T]) GetAllocator() *mpool.MPool { return win.ref.GetAllocator() }
func (win *vectorWindow[T]) GetType() types.Type        { return win.ref.GetType() }
func (win *vectorWindow[T]) String() string {
	s := fmt.Sprintf("[Window[%d,%d)];%s", win.offset, win.offset+win.length, win.ref.String())
	return s
}
func (win *vectorWindow[T]) PPString(num int) string {
	s := fmt.Sprintf("[Window[%d,%d)];%s", win.offset, win.offset+win.length, win.ref.PPString(num))
	return s
}
func (win *vectorWindow[T]) Slice() any {
	return win.ref.fastSlice()[win.offset : win.offset+win.length]
}
func (win *vectorWindow[T]) SlicePtr() unsafe.Pointer {
	slice := win.ref.fastSlice()[win.offset : win.offset+win.length]
	return unsafe.Pointer(&slice[0])
}
func (win *vectorWindow[T]) Bytes() *Bytes {
	bs := win.ref.Bytes()
	bs = bs.Window(win.offset, win.length)
	return bs
}
func (win *vectorWindow[T]) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	return win.ref.impl.forEachWindowWithBias(0, win.length, op, sels, win.offset, false)
}
func (win *vectorWindow[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	if offset+length > win.length {
		panic("bad param")
	}
	return win.ref.impl.forEachWindowWithBias(offset, length, op, sels, win.offset, false)
}

func (win *vectorWindow[T]) ForeachShallow(op ItOp, sels *roaring.Bitmap) (err error) {
	return win.ref.impl.forEachWindowWithBias(0, win.length, op, sels, win.offset, true)
}

func (win *vectorWindow[T]) ForeachWindowShallow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	if offset+length > win.length {
		panic("bad param")
	}
	return win.ref.impl.forEachWindowWithBias(offset, length, op, sels, win.offset, true)
}
func (win *vectorWindow[T]) WriteTo(w io.Writer) (n int64, err error) { panic("implement me") }
func (win *vectorWindow[T]) Window(offset, length int) Vector {
	if offset+length > win.length {
		panic("bad window param")
	}
	return &vectorWindow[T]{
		ref: win.ref,
		windowBase: &windowBase{
			offset: offset + win.offset,
			length: length,
		},
	}
}
