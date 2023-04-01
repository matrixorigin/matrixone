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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
)

// DN vector is different from CN vector by
// 2. Window - DN uses shared-memory Window
// 3. Mpool  - DN stores mpool reference within the vector
// 4. SharedMemory Logic - DN ResetWithData() doesn't allocate mpool memory unless Append() is called.
type vector[T any] struct {
	downstreamVector *cnVector.Vector

	// Used in Append()
	mpool *mpool.MPool

	// isOwner is used to implement the SharedMemory Logic from the previous DN vector implementation.
	isOwner bool
}

func NewVector[T any](typ types.Type, opts ...Options) *vector[T] {
	vec := &vector[T]{
		downstreamVector: cnVector.NewVec(typ),
	}

	// setting mpool variables
	var alloc *mpool.MPool
	if len(opts) > 0 {
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	vec.mpool = alloc

	// So far no mpool allocation. So isOwner defaults to false.
	vec.isOwner = false

	return vec
}

func (vec *vector[T]) Get(i int) any {
	if vec.IsNull(i) {
		return types.Null{}
	}

	if vec.GetType().IsVarlen() {
		bs := vec.ShallowGet(i).([]byte)
		ret := make([]byte, len(bs))
		copy(ret, bs)
		return any(ret).(T)
	}

	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vector[T]) ShallowGet(i int) any {
	if vec.IsNull(i) {
		return types.Null{}
	}
	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *vector[T]) Append(v any) {
	vec.tryCoW()

	_, isNull := v.(types.Null)
	var err error
	if isNull {
		err = cnVector.AppendAny(vec.downstreamVector, types.DefaultVal[T](), true, vec.mpool)
	} else {
		err = cnVector.AppendAny(vec.downstreamVector, v, false, vec.mpool)
	}
	if err != nil {
		panic(err)
	}
}

func (vec *vector[T]) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec *vector[T]) IsNull(i int) bool {
	return vec.downstreamVector.GetNulls() != nil && vec.downstreamVector.GetNulls().Contains(uint64(i))
}

func (vec *vector[T]) NullMask() *cnNulls.Nulls {
	return vec.downstreamVector.GetNulls()
}

func (vec *vector[T]) GetType() types.Type {
	return *vec.downstreamVector.GetType()
}

func (vec *vector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *vector[T]) Update(i int, v any) {
	vec.tryCoW()
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec *vector[T]) Slice() any {
	return cnVector.MustFixedCol[T](vec.downstreamVector)
}

func (vec *vector[T]) WriteTo(w io.Writer) (n int64, err error) {

	// 1. Vector bytes
	var buf []byte
	if buf, err = vec.downstreamVector.MarshalBinary(); err != nil {
		return 0, err
	}

	//0. Length of vector bytes [8 bytes]
	i64 := int64(len(buf))
	buf = append(types.EncodeInt64(&i64), buf...)

	var writtenBytes int
	if writtenBytes, err = w.Write(buf); err != nil {
		return 0, err
	}

	return int64(writtenBytes), nil
}

func (vec *vector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	// 0. Length [8 bytes]
	lengthBytes := make([]byte, 8)
	if _, err = r.Read(lengthBytes); err != nil {
		return 0, err
	}
	length := types.DecodeInt64(lengthBytes[:8])
	n += 8

	// 1. Whole DN Vector
	buf := make([]byte, length)
	if _, err = r.Read(buf); err != nil {
		return 0, err
	}

	n += int64(len(buf))

	vec.releaseDownstream()
	if err = vec.downstreamVector.UnmarshalBinary(buf); err != nil {
		return 0, err
	}

	return n, nil
}

func (vec *vector[T]) HasNull() bool {
	return vec.NullMask() != nil && vec.NullMask().Any()
}

func (vec *vector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.Length(), op, sels)
}
func (vec *vector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	err = vec.forEachWindowWithBias(offset, length, op, sels, 0, false)
	return
}
func (vec *vector[T]) ForeachShallow(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindowShallow(0, vec.Length(), op, sels)
}
func (vec *vector[T]) ForeachWindowShallow(offset, length int, op ItOp, sels *roaring.Bitmap) error {
	return vec.forEachWindowWithBias(offset, length, op, sels, 0, true)
}

func (vec *vector[T]) Close() {
	vec.releaseDownstream()
}

func (vec *vector[T]) releaseDownstream() {
	if vec.isOwner {
		vec.downstreamVector.Free(vec.mpool)
		vec.isOwner = false
	}
}

func (vec *vector[T]) Allocated() int {
	if !vec.isOwner {
		return 0
	}
	return vec.downstreamVector.Size()
}

// When a new Append() is happening on a SharedMemory vector, we allocate the data[] from the mpool.
func (vec *vector[T]) tryCoW() {

	if !vec.isOwner {
		newCnVector, err := vec.downstreamVector.Dup(vec.mpool)
		if err != nil {
			panic(err)
		}
		vec.downstreamVector = newCnVector
		vec.isOwner = true
	}
}

func (vec *vector[T]) Window(offset, length int) Vector {

	// In DN Vector, we are using SharedReference for Window.
	// In CN Vector, we are creating a new Clone for Window.
	// So inorder to retain the nature of DN vector, we had use vectorWindow Adapter.
	return &vectorWindow[T]{
		ref: vec,
		windowBase: &windowBase{
			offset: offset,
			length: length,
		},
	}
}

func (vec *vector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	cloned := NewVector[T](vec.GetType(), opts)
	var err error
	cloned.downstreamVector, err = vec.downstreamVector.CloneWindow(offset, offset+length, cloned.GetAllocator())
	if err != nil {
		panic(err)
	}
	cloned.isOwner = true

	return cloned
}

func (vec *vector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	if srcLen <= 0 {
		return
	}

	sels := make([]int32, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int32(j) + int32(srcOff)
	}
	err := vec.downstreamVector.Union(src.getDownstreamVector(), sels, vec.GetAllocator())
	if err != nil {
		panic(err)
	}
}

func (vec *vector[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int, shallow bool) (err error) {

	if !vec.HasNull() {
		var v T
		if _, ok := any(v).([]byte); !ok {
			// Optimization for :- Vectors which are 1. not containing nulls & 2. not byte[]
			slice := vec.Slice().([]T)
			slice = slice[offset+bias : offset+length+bias]
			if sels == nil || sels.IsEmpty() {
				for i, elem := range slice {
					var vv any
					isNull := false
					if vec.IsNull(i + offset + bias) {
						isNull = true
						vv = types.Null{}
					} else {
						vv = elem
					}
					if err = op(vv, isNull, i+offset); err != nil {
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

					var vv any
					isNull := false
					if vec.IsNull(int(idx) + bias) {
						isNull = true
						vv = types.Null{}
					} else {
						vv = slice[int(idx)-offset]
					}
					if err = op(vv, isNull, int(idx)); err != nil {
						break
					}
				}
			}
			return
		}

	}
	if sels == nil || sels.IsEmpty() {
		for i := offset; i < offset+length; i++ {
			var elem any
			if shallow {
				elem = vec.ShallowGet(i + bias)
			} else {
				elem = vec.Get(i + bias)
			}
			if err = op(elem, vec.IsNull(i+bias), i); err != nil {
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
			var elem any
			if shallow {
				elem = vec.ShallowGet(int(idx) + bias)
			} else {
				elem = vec.Get(int(idx) + bias)
			}

			if err = op(elem, vec.IsNull(int(idx)+bias), int(idx)); err != nil {
				break
			}
		}
	}
	return
}

func (vec *vector[T]) Compact(deletes *roaring.Bitmap) {
	vec.tryCoW()

	var dels []int64
	itr := deletes.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		dels = append(dels, int64(r))
	}

	vec.downstreamVector.Shrink(dels, true)
}

func (vec *vector[T]) getDownstreamVector() *cnVector.Vector {
	return vec.downstreamVector
}

func (vec *vector[T]) setDownstreamVector(dsVec *cnVector.Vector) {
	vec.isOwner = false
	vec.downstreamVector = dsVec
}

/****** Below functions are not used in critical path. Used mainly for testing */

// String value of vector
// Deprecated: Only use for test functions
func (vec *vector[T]) String() string {
	s := fmt.Sprintf("DN Vector: Len=%d[Rows];Allocted:%d[Bytes]", vec.Length(), vec.Allocated())

	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
	}

	data := "Vals=["
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, vec.Get(i))
	}
	if vec.Length() > end {
		s = fmt.Sprintf("%s %s...]", s, data)
	} else {
		s = fmt.Sprintf("%s %s]", s, data)
	}

	return s
}

// PPString Pretty Print
// Deprecated: Only use for test functions
func (vec *vector[T]) PPString(num int) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[T=%s][Len=%d][Data=(", vec.GetType().String(), vec.Length()))
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	size := vec.Length()
	long := false
	if size > limit {
		long = true
		size = limit
	}
	for i := 0; i < size; i++ {
		if vec.IsNull(i) {
			_, _ = w.WriteString("null")
			continue
		}
		if vec.GetType().IsVarlen() {
			_, _ = w.WriteString(fmt.Sprintf("%s, ", vec.Get(i).([]byte)))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("%v, ", vec.Get(i)))
		}
	}
	if long {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(")]")
	return w.String()
}

// AppendMany appends multiple values
// Deprecated: Only use for test functions
func (vec *vector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}

// Delete Deletes an item from vector
// Deprecated: Only use for test functions
func (vec *vector[T]) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}

// Equals Compares two vectors
// Deprecated: Only use for test functions
func (vec *vector[T]) Equals(o Vector) bool {
	if vec.Length() != o.Length() {
		return false
	}
	if vec.GetType() != o.GetType() {
		return false
	}
	if vec.HasNull() != o.HasNull() {
		return false
	}
	if vec.HasNull() {
		if !vec.NullMask().IsSame(o.NullMask()) {
			return false
		}
	}
	mask := vec.NullMask()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.Contains(uint64(i)) {
			continue
		}
		var v T
		if _, ok := any(v).([]byte); ok {
			if !bytes.Equal(vec.ShallowGet(i).([]byte), o.ShallowGet(i).([]byte)) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal64); ok {
			d := vec.Get(i).(types.Decimal64)
			od := vec.Get(i).(types.Decimal64)
			if d != od {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := vec.Get(i).(types.Decimal128)
			od := vec.Get(i).(types.Decimal128)
			if d != od {
				return false
			}
		} else if _, ok := any(v).(types.TS); ok {
			d := vec.Get(i).(types.TS)
			od := vec.Get(i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if _, ok := any(v).(types.Rowid); ok {
			d := vec.Get(i).(types.Rowid)
			od := vec.Get(i).(types.Rowid)
			if types.CompareRowidRowidAligned(d, od) != 0 {
				return false
			}
		} else {
			if vec.Get(i) != o.Get(i) {
				return false
			}
		}
	}
	return true
}
