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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

// DN vector is different from CN vector by
// 2. Window - DN uses shared-memory Window
// 3. Mpool  - DN stores mpool reference within the vector
// 4. SharedMemory Logic - DN ResetWithData() doesn't allocate mpool memory unless Append() is called.
type vector[T any] struct {
	downstreamVector *cnVector.Vector

	// Used in Append()
	mpool *mpool.MPool
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

	return vec
}

func (vec *vector[T]) Get(i int) any {
	if vec.GetType().IsVarlen() {
		bs := vec.ShallowGet(i).([]byte)
		ret := make([]byte, len(bs))
		copy(ret, bs)
		return any(ret).(T)
	}

	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vector[T]) ShallowGet(i int) any {
	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *vector[T]) Append(v any, isNull bool) {
	vec.tryCoW()

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

func (vec *vector[T]) GetType() *types.Type {
	return vec.downstreamVector.GetType()
}

func (vec *vector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *vector[T]) Update(i int, v any, isNull bool) {
	vec.tryCoW()
	UpdateValue(vec.downstreamVector, uint32(i), v, isNull)
}

func (vec *vector[T]) Slice() any {
	return cnVector.MustFixedCol[T](vec.downstreamVector)
}

func (vec *vector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var bs bytes.Buffer

	var size int64
	_, _ = bs.Write(types.EncodeInt64(&size))

	if err = vec.downstreamVector.MarshalBinaryWithBuffer(&bs); err != nil {
		return
	}

	size = int64(bs.Len() - 8)

	buf := bs.Bytes()
	copy(buf[:8], types.EncodeInt64(&size))

	if _, err = w.Write(buf); err != nil {
		return
	}

	n = int64(len(buf))
	return
}

func (vec *vector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = r.Read(buf); err != nil {
		return
	}

	n += 8

	// 1. Whole DN Vector
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = r.Read(buf); err != nil {
		return
	}

	n += int64(len(buf))

	t := vec.downstreamVector.GetType()
	vec.releaseDownstream()
	vec.downstreamVector = cnVector.NewVec(*t)
	if err = vec.downstreamVector.UnmarshalBinary(buf); err != nil {
		return
	}

	return
}

func (vec *vector[T]) HasNull() bool {
	return vec.NullMask() != nil && vec.NullMask().Any()
}

func (vec *vector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.downstreamVector.Length(), op, sels)
}

func (vec *vector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	if vec.downstreamVector.GetType().IsVarlen() {
		return ForeachWindowVarlen(vec, offset, length, nil, op, sels)
	}
	return ForeachWindowFixed[T](vec, offset, length, nil, op, sels)
}

func (vec *vector[T]) Close() {
	vec.releaseDownstream()
}

func (vec *vector[T]) releaseDownstream() {
	if vec.downstreamVector == nil {
		return
	}
	if !vec.downstreamVector.NeedDup() {
		vec.downstreamVector.Free(vec.mpool)
	}
	vec.downstreamVector = nil
}

func (vec *vector[T]) Allocated() int {
	if vec.downstreamVector.NeedDup() {
		return 0
	}
	return vec.downstreamVector.Size()
}

// When a new Append() is happening on a SharedMemory vector, we allocate the data[] from the mpool.
func (vec *vector[T]) tryCoW() {
	if !vec.downstreamVector.NeedDup() {
		return
	}

	newCnVector, err := vec.downstreamVector.Dup(vec.mpool)
	if err != nil {
		panic(err)
	}
	vec.downstreamVector = newCnVector
}

func (vec *vector[T]) Window(offset, length int) Vector {
	var err error
	win := new(vector[T])
	win.mpool = vec.mpool
	win.downstreamVector, err = vec.downstreamVector.Window(offset, offset+length)
	if err != nil {
		panic(err)
	}

	return win
}

func (vec *vector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	cloned := NewVector[T](*vec.GetType(), opts)
	var err error
	cloned.downstreamVector, err = vec.downstreamVector.CloneWindow(offset, offset+length, cloned.GetAllocator())
	if err != nil {
		panic(err)
	}

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
	err := vec.downstreamVector.Union(src.GetDownstreamVector(), sels, vec.GetAllocator())
	if err != nil {
		panic(err)
	}
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

func (vec *vector[T]) GetDownstreamVector() *cnVector.Vector {
	return vec.downstreamVector
}

func (vec *vector[T]) setDownstreamVector(dsVec *cnVector.Vector) {
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
func (vec *vector[T]) AppendMany(vs []any, isNulls []bool) {
	for i, v := range vs {
		vec.Append(v, isNulls[i])
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
	if *vec.GetType() != *o.GetType() {
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
