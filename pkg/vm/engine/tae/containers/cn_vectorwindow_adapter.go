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
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
	"unsafe"
)

type CnTaeVectorWindow[T any] struct {
	*windowBase
	ref *CnTaeVector[T]
}

func (win *CnTaeVectorWindow[T]) Nullable() bool {
	return win.ref.Nullable()
}

func (win *CnTaeVectorWindow[T]) IsNull(i int) bool {
	return win.ref.IsNull(i + win.offset)
}

func (win *CnTaeVectorWindow[T]) HasNull() bool {
	return win.ref.HasNull()
}

func (win *CnTaeVectorWindow[T]) NullMask() *roaring64.Bitmap {
	mask := win.ref.NullMask()
	if win.offset == 0 || mask == nil {
		return mask
	}
	return common.BM64Window(mask, win.offset, win.offset+win.length)
}

func (win *CnTaeVectorWindow[T]) Bytes() *Bytes {
	bs := win.ref.Bytes()
	bs = bs.Window(win.offset, win.length)
	return bs
}

func (win *CnTaeVectorWindow[T]) Slice() any {
	return win.ref.Slice().([]T)[win.offset : win.offset+win.length]
}

func (win *CnTaeVectorWindow[T]) Get(i int) any {
	return win.ref.Get(i + win.offset)
}

func (win *CnTaeVectorWindow[T]) GetAllocator() *mpool.MPool {
	return win.ref.GetAllocator()
}

func (win *CnTaeVectorWindow[T]) GetType() types.Type {
	return win.ref.GetType()
}

func (win *CnTaeVectorWindow[T]) String() string {
	s := fmt.Sprintf("[Window[%d,%d)];%s", win.offset, win.offset+win.length, win.ref.String())
	return s
}

func (win *CnTaeVectorWindow[T]) PPString(num int) string {
	s := fmt.Sprintf("[Window[%d,%d)];%s", win.offset, win.offset+win.length, win.ref.PPString(num))
	return s
}

func (win *CnTaeVectorWindow[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return win.ref.ForeachWindow(win.offset, win.length, op, sels)
}

func (win *CnTaeVectorWindow[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error {
	if offset+length > win.length {
		panic("bad param")
	}
	return win.ref.ForeachWindow(win.offset, win.length, op, sels)
}

func (win *CnTaeVectorWindow[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	return win.ref.CloneWindow(win.offset, win.length)
}

func (win *CnTaeVectorWindow[T]) Equals(o Vector) bool {

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
			if !bytes.Equal(win.Get(i).([]byte), o.Get(i).([]byte)) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal64); ok {
			d := win.Get(i).(types.Decimal64)
			od := win.Get(i).(types.Decimal64)
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := win.Get(i).(types.Decimal128)
			od := win.Get(i).(types.Decimal128)
			if d.Ne(od) {
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
func (win *CnTaeVectorWindow[T]) Window(offset, length int) Vector {
	if offset+length > win.length {
		panic("bad window param")
	}
	return &CnTaeVectorWindow[T]{
		ref: win.ref,
		windowBase: &windowBase{
			offset: offset + win.offset,
			length: length,
		},
	}
}

func (win *CnTaeVectorWindow[T]) WriteTo(w io.Writer) (int64, error) {
	panic("not implemented")
}

func (win *CnTaeVectorWindow[T]) GetView() VectorView {
	panic("Soon Deprecated")
}
func (win *CnTaeVectorWindow[T]) Data() []byte {
	panic("Soon Deprecated")
}

func (win *CnTaeVectorWindow[T]) SlicePtr() unsafe.Pointer {
	slice := win.ref.Slice().([]T)[win.offset : win.offset+win.length]
	return unsafe.Pointer(&slice[0])
}
