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
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type vectorWrapper struct {
	// wrapped is the vector.Vector that is wrapped by this vectorWrapper.
	wrapped *vector.Vector

	// mpool is the memory pool used by the wrapped vector.Vector.
	mpool *mpool.MPool

	// it is used to call Close in an idempotent way
	closed atomic.Bool

	element *vectorPoolElement
}

func NewConstFixed[T any](typ types.Type, val T, length int, opts ...Options) *vectorWrapper {
	var (
		alloc *mpool.MPool
	)
	if len(opts) > 0 {
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}

	vc, err := vector.NewConstFixed[T](typ, val, length, alloc)
	if err != nil {
		panic(err)
	}
	vec := &vectorWrapper{
		wrapped: vc,
	}
	vec.mpool = alloc
	return vec
}

func NewConstBytes(typ types.Type, val []byte, length int, opts ...Options) *vectorWrapper {
	var (
		alloc *mpool.MPool
	)
	if len(opts) > 0 {
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}

	vc, err := vector.NewConstBytes(typ, val, length, alloc)
	if err != nil {
		panic(err)
	}
	vec := &vectorWrapper{
		wrapped: vc,
	}
	vec.mpool = alloc
	return vec
}

func NewConstNullVector(
	typ types.Type,
	length int,
	mp *mpool.MPool,
) *vectorWrapper {
	vec := &vectorWrapper{
		wrapped: vector.NewConstNull(typ, length, mp),
	}
	vec.mpool = mp
	return vec
}

func NewVector(typ types.Type, opts ...Options) *vectorWrapper {
	vec := &vectorWrapper{
		wrapped: vector.NewVec(typ),
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
		if err := vec.wrapped.PreExtend(capacity, vec.mpool); err != nil {
			panic(err)
		}
	}
	return vec
}

func (vec *vectorWrapper) ApproxSize() int {
	if vec.wrapped.NeedDup() {
		return 0
	} else {
		return vec.wrapped.Size()
	}
}

func (vec *vectorWrapper) PreExtend(length int) (err error) {
	vec.tryCOW()
	return vec.wrapped.PreExtend(length, vec.mpool)
}

func (vec *vectorWrapper) Get(i int) any {
	if vec.wrapped.IsConstNull() {
		return nil
	}
	if vec.GetType().IsVarlen() {
		if vec.IsNull(i) {
			var val []byte
			return val
		}
		bs := vec.ShallowGet(i).([]byte)
		ret := make([]byte, len(bs))
		copy(ret, bs)
		return any(ret)
	}

	return getNonNullValue(vec.wrapped, uint32(i))
}

func (vec *vectorWrapper) ShallowGet(i int) any {
	return getNonNullValue(vec.wrapped, uint32(i))
}

func (vec *vectorWrapper) Length() int {
	if vec.wrapped == nil {
		return 0
	}
	return vec.wrapped.Length()
}

func (vec *vectorWrapper) Append(v any, isNull bool) {
	// innsert AppendAny will check IsConst
	vec.tryCOW()

	var err error
	if isNull {
		err = vector.AppendAny(vec.wrapped, nil, true, vec.mpool)
	} else {
		err = vector.AppendAny(vec.wrapped, v, false, vec.mpool)
	}
	if err != nil {
		panic(err)
	}
}

func (vec *vectorWrapper) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec *vectorWrapper) IsNull(i int) bool {
	inner := vec.wrapped
	if inner.IsConstNull() {
		return true
	}
	if inner.IsConst() {
		return false
	}
	return nulls.Contains(inner.GetNulls(), uint64(i))
}

func (vec *vectorWrapper) NullMask() *nulls.Nulls {
	return vec.wrapped.GetNulls()
}

func (vec *vectorWrapper) GetType() *types.Type {
	return vec.wrapped.GetType()
}

func (vec *vectorWrapper) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *vectorWrapper) Update(i int, v any, isNull bool) {
	if vec.wrapped.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("update to const vectorWrapper"))
	}
	vec.tryCOW()
	UpdateValue(vec.wrapped, uint32(i), v, isNull, vec.mpool)
}

func (vec *vectorWrapper) WriteTo(w io.Writer) (n int64, err error) {
	var bs bytes.Buffer

	var size int64
	_, _ = bs.Write(types.EncodeInt64(&size))

	if err = vec.wrapped.MarshalBinaryWithBuffer(&bs); err != nil {
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

// WriteToV1 in version 1, vector.nulls.bitmap is v1
func (vec *vectorWrapper) WriteToV1(w io.Writer) (n int64, err error) {
	var bs bytes.Buffer

	var size int64
	_, _ = bs.Write(types.EncodeInt64(&size))

	if err = vec.wrapped.MarshalBinaryWithBufferV1(&bs); err != nil {
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

	// 1. Whole TN Vector
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = r.Read(buf); err != nil {
		return
	}

	n += int64(len(buf))

	t := vec.wrapped.GetType()
	vec.releaseWrapped()
	vec.wrapped = vector.NewVec(*t)
	if err = vec.wrapped.UnmarshalBinary(buf); err != nil {
		return
	}

	return
}

// ReadFromV1 in version 1, vector.nulls.bitmap is v1
func (vec *vectorWrapper) ReadFromV1(r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = r.Read(buf); err != nil {
		return
	}

	n += 8

	// 1. Whole TN Vector
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = r.Read(buf); err != nil {
		return
	}

	n += int64(len(buf))

	t := vec.wrapped.GetType()
	vec.releaseWrapped()
	vec.wrapped = vector.NewVec(*t)
	if err = vec.wrapped.UnmarshalBinaryV1(buf); err != nil {
		return
	}

	return
}

func (vec *vectorWrapper) HasNull() bool {
	if vec.wrapped.IsConstNull() {
		return true
	}
	if vec.wrapped.IsConst() {
		return false
	}
	return vec.NullMask() != nil && !vec.NullMask().IsEmpty()
}

func (vec *vectorWrapper) NullCount() int {
	if vec.wrapped.IsConstNull() {
		return vec.Length()
	}
	if vec.wrapped.IsConst() {
		return 0
	}
	if vec.NullMask() != nil {
		return vec.NullMask().GetCardinality()
	}
	return 0
}

func (vec *vectorWrapper) IsConst() bool {
	return vec.wrapped.IsConst()
}

func (vec *vectorWrapper) IsConstNull() bool {
	return vec.wrapped.IsConstNull()
}

// conver a const vectorWrapper to a normal one, getting ready to edit
func (vec *vectorWrapper) TryConvertConst() Vector {
	if vec.wrapped.IsConstNull() {
		ret := NewVector(*vec.GetType())
		ret.mpool = vec.mpool
		for i := 0; i < vec.Length(); i++ {
			ret.Append(nil, true)
		}
		return ret
	}

	if vec.wrapped.IsConst() {
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

func (vec *vectorWrapper) Foreach(op ItOp, sels *nulls.Bitmap) error {
	return vec.ForeachWindow(0, vec.wrapped.Length(), op, sels)
}

func (vec *vectorWrapper) ForeachWindow(offset, length int, op ItOp, sels *nulls.Bitmap) (err error) {
	return ForeachVectorWindow(vec, offset, length, nil, op, sels)
}

func (vec *vectorWrapper) Close() {
	if !vec.closed.CompareAndSwap(false, true) {
		return
	}
	if vec.element != nil {
		vec.element.put()
		vec.element = nil
		vec.wrapped = nil
		return
	}

	// if this wrapper is not get from a pool, we should release the wrapped vector
	vec.releaseWrapped()
}

func (vec *vectorWrapper) releaseWrapped() {
	if vec.wrapped == nil {
		return
	}
	if !vec.wrapped.NeedDup() {
		vec.wrapped.Free(vec.mpool)
	}
	vec.wrapped = nil
}

func (vec *vectorWrapper) Allocated() int {
	if vec.wrapped.NeedDup() {
		return 0
	}
	return vec.wrapped.Allocated()
}

// When a new Append() is happening on a SharedMemory vectorWrapper, we allocate the data[] from the mpool.
func (vec *vectorWrapper) tryCOW() {
	if !vec.wrapped.NeedDup() {
		return
	}

	newCnVector, err := vec.wrapped.Dup(vec.mpool)
	if err != nil {
		panic(err)
	}
	vec.wrapped = newCnVector
}

func (vec *vectorWrapper) Window(offset, length int) Vector {
	var err error
	win := new(vectorWrapper)
	win.mpool = vec.mpool
	win.wrapped, err = vec.wrapped.Window(offset, offset+length)
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
	v, err := vec.wrapped.CloneWindow(offset, offset+length, cloned.GetAllocator())
	if err != nil {
		panic(err)
	}
	cloned.setDownstreamVector(v)
	return cloned
}

func (vec *vectorWrapper) CloneWindowWithPool(offset, length int, pool *VectorPool) Vector {
	cloned := pool.GetVector(vec.GetType())
	if err := vec.wrapped.CloneWindowTo(cloned.wrapped, offset, offset+length, cloned.GetAllocator()); err != nil {
		panic(err)
	}
	return cloned
}

func (vec *vectorWrapper) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	if err := vec.extendWithOffset(src.GetDownstreamVector(), srcOff, srcLen); err != nil {
		panic(err)
	}
}

func (vec *vectorWrapper) ExtendVec(src *vector.Vector) (err error) {
	return vec.extendWithOffset(src, 0, src.Length())
}

func (vec *vectorWrapper) extendWithOffset(src *vector.Vector, srcOff, srcLen int) (err error) {
	if vec.wrapped.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("extend to const vectorWrapper"))
	}
	if srcLen <= 0 {
		return
	}

	if srcOff == 0 && srcLen == src.Length() {
		err = vector.GetUnionAllFunction(
			*vec.GetType(),
			vec.mpool,
		)(vec.wrapped, src)
		return
	}

	sels := make([]int64, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int64(j) + int64(srcOff)
	}
	err = vec.wrapped.Union(src, sels, vec.mpool)
	return
}

func (vec *vectorWrapper) CompactByBitmap(mask *nulls.Bitmap) {
	if mask.IsEmpty() {
		return
	}
	vec.tryCOW()

	dels := vec.mpool.GetSels()
	mask.Foreach(func(i uint64) bool {
		dels = append(dels, int64(i))
		return true
	})
	vec.wrapped.Shrink(dels, true)
	vec.mpool.PutSels(dels)
}

func (vec *vectorWrapper) Compact(deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	vec.tryCOW()

	dels := vec.mpool.GetSels()
	itr := deletes.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		dels = append(dels, int64(r))
	}

	vec.wrapped.Shrink(dels, true)
	vec.mpool.PutSels(dels)
}

func (vec *vectorWrapper) GetDownstreamVector() *vector.Vector {
	return vec.wrapped
}

func (vec *vectorWrapper) setDownstreamVector(dsVec *vector.Vector) {
	vec.wrapped = dsVec
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
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	_, _ = w.WriteString(fmt.Sprintf("[T=%s]%s", vec.GetType().String(), common.MoVectorToString(vec.GetDownstreamVector(), limit)))
	if vec.Length() > num {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(fmt.Sprintf("[%v]", vec.element != nil))
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
	typ := vec.wrapped.GetType()
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
