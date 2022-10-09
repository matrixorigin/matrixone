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
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type vecView struct {
	impl Vector
}

func newVecView(impl Vector) *vecView {
	return &vecView{
		impl: impl,
	}
}

//	func (vec *vecView) Equals(o Vector) bool {
//		if vec.Length() != o.Length() {
//			return false
//		}
//		if vec.GetType() != o.GetType() {
//			return false
//		}
//		if vec.Nullable() != o.Nullable() {
//			return false
//		}
//		if vec.HasNull() != o.HasNull() {
//			return false
//		}
//		if vec.HasNull() {
//			if !vec.NullMask().Equals(o.NullMask()) {
//				return false
//			}
//		}
//		for i := 0; i < vec.Length(); i++ {
//			if vec.Get(i) != o.Get(i) {
//				return false
//			}
//		}
//		return true
//	}
func (vec *vecView) IsView() bool                { return true }
func (vec *vecView) Nullable() bool              { return vec.impl.Nullable() }
func (vec *vecView) IsNull(i int) bool           { return vec.impl.IsNull(i) }
func (vec *vecView) HasNull() bool               { return vec.impl.HasNull() }
func (vec *vecView) NullMask() *roaring64.Bitmap { return vec.impl.NullMask() }
func (vec *vecView) Bytes() *Bytes               { return vec.impl.Bytes() }
func (vec *vecView) Data() []byte                { return vec.impl.Data() }
func (vec *vecView) DataWindow(offset, length int) []byte {
	return vec.impl.DataWindow(offset, length)
}
func (vec *vecView) Get(i int) (v any) { return vec.impl.Get(i) }
func (vec *vecView) Length() int       { return vec.impl.Length() }
func (vec *vecView) Capacity() int     { return vec.impl.Capacity() }
func (vec *vecView) Allocated() int    { return vec.impl.Allocated() }

func (vec *vecView) GetAllocator() *mpool.MPool { return vec.impl.GetAllocator() }
func (vec *vecView) GetType() types.Type        { return vec.impl.GetType() }
func (vec *vecView) String() string             { return vec.impl.String() }
func (vec *vecView) Close()                     {}
func (vec *vecView) Slice() any                 { return vec.impl.Slice() }

func (vec *vecView) ResetWithData(_ *Bytes, _ *roaring64.Bitmap) { panic("not supported") }
func (vec *vecView) Window() VectorView                          { panic("not implemented") }

func (vec *vecView) WriteTo(w io.Writer) (n int64, err error) {
	return vec.impl.WriteTo(w)
}

func (vec *vecView) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(0, vec.Length(), op, sels)
}

func (vec *vecView) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(offset, length, op, sels)
}
