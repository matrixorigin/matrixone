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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

// DN vectorWrapper is different from CN vector by
// 2. Window - DN uses shared-memory Window
// 3. Mpool  - DN stores mpool reference within the vector
// 4. SharedMemory Logic - DN ResetWithData() doesn't allocate mpool memory unless Append() is called.
type vectorWrapper struct {
	downstreamVector *cnVector.Vector

	// Used in Append()
	mpool *mpool.MPool
}

func NewVector(typ types.Type, opts ...Options) *vectorWrapper {
	vec := &vectorWrapper{
		downstreamVector: cnVector.NewVec(typ),
	}

	// setting mpool variables
	var (
		alloc    *mpool.MPool
		capacity int
	)
	if len(opts) > 0 {
		alloc = opts[0].Allocator
		capacity = opts[0].Capacity
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	vec.mpool = alloc
	if capacity > 0 {
		if err := vec.downstreamVector.PreExtend(capacity, vec.mpool); err != nil {
			panic(err)
		}
	}
	return vec
}

func (vec *vectorWrapper) PreExtend(length int) (err error) {
	vec.tryCoW()
	return vec.downstreamVector.PreExtend(length, vec.mpool)
}

func (vec *vectorWrapper) Get(i int) any {
	if vec.downstreamVector.IsConstNull() {
		return nil
	}
	if vec.GetType().IsVarlen() {
		bs := vec.ShallowGet(i).([]byte)
		ret := make([]byte, len(bs))
		copy(ret, bs)
		return any(ret)
	}

	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vectorWrapper) ShallowGet(i int) any {
	return getNonNullValue(vec.downstreamVector, uint32(i))
}

func (vec *vectorWrapper) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *vectorWrapper) Append(v any, isNull bool) {
	// innsert AppendAny will check IsConst
	vec.tryCoW()

	var err error
	if isNull {
		err = cnVector.AppendAny(vec.downstreamVector, nil, true, vec.mpool)
	} else {
		err = cnVector.AppendAny(vec.downstreamVector, v, false, vec.mpool)
	}
	if err != nil {
		panic(err)
	}
}

func (vec *vectorWrapper) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec *vectorWrapper) IsNull(i int) bool {
	inner := vec.downstreamVector
	if inner.IsConstNull() {
		return true
	}
	if inner.IsConst() {
		return false
	}
	return cnNulls.Contains(inner.GetNulls(), uint64(i))
}

func (vec *vectorWrapper) NullMask() *cnNulls.Nulls {
	return vec.downstreamVector.GetNulls()
}

func (vec *vectorWrapper) GetType() *types.Type {
	return vec.downstreamVector.GetType()
}

func (vec *vectorWrapper) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *vectorWrapper) Update(i int, v any, isNull bool) {
	if vec.downstreamVector.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("update to const vectorWrapper"))
	}
	vec.tryCoW()
	UpdateValue(vec.downstreamVector, uint32(i), v, isNull)
}

func (vec *vectorWrapper) WriteTo(w io.Writer) (n int64, err error) {
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

func (vec *vectorWrapper) ReadFrom(r io.Reader) (n int64, err error) {
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

func (vec *vectorWrapper) HasNull() bool {
	if vec.downstreamVector.IsConstNull() {
		return true
	}
	if vec.downstreamVector.IsConst() {
		return false
	}
	return vec.NullMask() != nil && !vec.NullMask().IsEmpty()
}

func (vec *vectorWrapper) NullCount() int {
	if vec.downstreamVector.IsConstNull() {
		return vec.Length()
	}
	if vec.downstreamVector.IsConst() {
		return 0
	}
	if vec.NullMask() != nil {
		return vec.NullMask().GetCardinality()
	}
	return 0
}

// conver a const vectorWrapper to a normal one, getting ready to edit
func (vec *vectorWrapper) TryConvertConst() Vector {
	if vec.downstreamVector.IsConstNull() {
		ret := NewVector(*vec.GetType())
		ret.mpool = vec.mpool
		for i := 0; i < vec.Length(); i++ {
			ret.Append(nil, true)
		}
		return ret
	}

	if vec.downstreamVector.IsConst() {
		ret := NewVector(*vec.GetType())
		ret.mpool = vec.mpool
		v := vec.Get(0)
		for i := 0; i < vec.Length(); i++ {
			ret.Append(v, false)
		}
		return ret
	}
	return vec
}

func (vec *vectorWrapper) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.downstreamVector.Length(), op, sels)
}

func (vec *vectorWrapper) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return ForeachVectorWindow(vec, offset, length, nil, op, sels)
}

func (vec *vectorWrapper) Close() {
	vec.releaseDownstream()
}

func (vec *vectorWrapper) releaseDownstream() {
	if vec.downstreamVector == nil {
		return
	}
	if !vec.downstreamVector.NeedDup() {
		vec.downstreamVector.Free(vec.mpool)
	}
	vec.downstreamVector = nil
}

func (vec *vectorWrapper) Allocated() int {
	if vec.downstreamVector.NeedDup() {
		return 0
	}
	return vec.downstreamVector.Size()
}

// When a new Append() is happening on a SharedMemory vectorWrapper, we allocate the data[] from the mpool.
func (vec *vectorWrapper) tryCoW() {
	if !vec.downstreamVector.NeedDup() {
		return
	}

	newCnVector, err := vec.downstreamVector.Dup(vec.mpool)
	if err != nil {
		panic(err)
	}
	vec.downstreamVector = newCnVector
}

func (vec *vectorWrapper) Window(offset, length int) Vector {
	if vec.downstreamVector.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("foreach to const vectorWrapper"))
	}
	var err error
	win := new(vectorWrapper)
	win.mpool = vec.mpool
	win.downstreamVector, err = vec.downstreamVector.Window(offset, offset+length)
	if err != nil {
		panic(err)
	}

	return win
}

func (vec *vectorWrapper) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	cloned := NewVector(*vec.GetType(), opts)
	if vec.downstreamVector.IsConstNull() {
		cloned.downstreamVector = cnVector.NewConstNull(*vec.GetType(), length, vec.GetAllocator())
		return cloned
	}

	if vec.downstreamVector.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("cloneWindow to const vectorWrapper"))
	}

	var err error
	cloned.downstreamVector, err = vec.downstreamVector.CloneWindow(offset, offset+length, cloned.GetAllocator())
	if err != nil {
		panic(err)
	}

	return cloned
}

func (vec *vectorWrapper) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	if err := vec.extendWithOffset(src.GetDownstreamVector(), srcOff, srcLen); err != nil {
		panic(err)
	}
}

func (vec *vectorWrapper) ExtendVec(src *cnVector.Vector) (err error) {
	return vec.extendWithOffset(src, 0, src.Length())
}

func (vec *vectorWrapper) extendWithOffset(src *cnVector.Vector, srcOff, srcLen int) (err error) {
	if vec.downstreamVector.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("extend to const vectorWrapper"))
	}
	if srcLen <= 0 {
		return
	}

	if srcOff == 0 && srcLen == src.Length() {
		err = cnVector.GetUnionAllFunction(
			*vec.GetType(),
			vec.mpool,
		)(vec.downstreamVector, src)
		return
	}

	sels := make([]int32, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int32(j) + int32(srcOff)
	}
	err = vec.downstreamVector.Union(src, sels, vec.mpool)
	return
}

func (vec *vectorWrapper) Compact(deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	vec.tryCoW()

	dels := vec.mpool.GetSels()
	itr := deletes.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		dels = append(dels, int64(r))
	}

	vec.downstreamVector.Shrink(dels, true)
	vec.mpool.PutSels(dels)
}

func (vec *vectorWrapper) GetDownstreamVector() *cnVector.Vector {
	return vec.downstreamVector
}

func (vec *vectorWrapper) setDownstreamVector(dsVec *cnVector.Vector) {
	vec.downstreamVector = dsVec
}

/****** Below functions are not used in critical path. Used mainly for testing */

// String value of vectorWrapper
// Deprecated: Only use for test functions
func (vec *vectorWrapper) String() string {
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
func (vec *vectorWrapper) PPString(num int) string {
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
func (vec *vectorWrapper) AppendMany(vs []any, isNulls []bool) {
	for i, v := range vs {
		vec.Append(v, isNulls[i])
	}
}

// Delete Deletes an item from vectorWrapper
// Deprecated: Only use for test functions
func (vec *vectorWrapper) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}

// Equals Compares two vectors
// Deprecated: Only use for test functions
func (vec *vectorWrapper) Equals(o Vector) bool {
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
	typ := vec.downstreamVector.GetType()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.Contains(uint64(i)) {
			continue
		}
		if typ.IsVarlen() {
			if !bytes.Equal(vec.ShallowGet(i).([]byte), o.ShallowGet(i).([]byte)) {
				return false
			}
		} else if typ.Oid == types.T_decimal64 {
			d := vec.Get(i).(types.Decimal64)
			od := vec.Get(i).(types.Decimal64)
			if d != od {
				return false
			}
		} else if typ.Oid == types.T_decimal128 {
			d := vec.Get(i).(types.Decimal128)
			od := vec.Get(i).(types.Decimal128)
			if d != od {
				return false
			}
		} else if typ.Oid == types.T_TS {
			d := vec.Get(i).(types.TS)
			od := vec.Get(i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if typ.Oid == types.T_Rowid {
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
