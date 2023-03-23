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
// 1. Nulls  - DN uses types.Nulls{}. It also uses isNullable. (Will be removed in future PR)
// 2. Window - DN uses sharedmemory Window
// 3. Mpool  - DN stores mpool reference within the vector
// 4. SharedMemory Logic - DN ResetWithData() doesn't allocate mpool memory unless Append() is called.
type vector[T any] struct {
	downstreamVector *cnVector.Vector

	// isNullable mainly used in Equals() & CloneWithBuffer(). Note:
	//1. We can't use cnVector.Nsp.Np to replace this flag, as this information will be lost in Marshalling/UnMarshalling.
	//2. It is also used in CloneWithBuffer() to avoid Data Race caused by using HasNull()
	isNullable bool

	// Used in Append()
	mpool *mpool.MPool

	// isOwner is used to implement the SharedMemory Logic from the previous DN vector implementation.
	isOwner bool
}

func NewVector[T any](typ types.Type, nullable bool, opts ...Options) *vector[T] {
	vec := &vector[T]{
		downstreamVector: cnVector.NewVec(typ),
		isNullable:       nullable,
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
	if isNull {
		_ = cnVector.AppendAny(vec.downstreamVector, types.DefaultVal[T](), true, vec.mpool)
	} else {
		_ = cnVector.AppendAny(vec.downstreamVector, v, false, vec.mpool)
	}
}

func (vec *vector[T]) Nullable() bool {
	return vec.isNullable
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
	//TODO: Upon update, we promote the memory.
	vec.tryCoW()
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec *vector[T]) Slice() any {
	return cnVector.MustFixedCol[T](vec.downstreamVector)
}

//func (vec *vector[T]) Bytes() *Bytes {
//	//TODO: get rid of Bytes type
//	return MoVecToBytes(vec.downstreamVector)
//}

func (vec *vector[T]) WriteTo(w io.Writer) (n int64, err error) {
	// 1. Nullable Flag [1 byte]
	buf := types.EncodeFixed(vec.Nullable())

	// 2. Vector bytes
	var vecBytes []byte
	if vecBytes, err = vec.downstreamVector.MarshalBinary(); err != nil {
		return 0, err
	}
	buf = append(buf, vecBytes...)

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

	// Whole DN Vector
	buf := make([]byte, length)
	if _, err = r.Read(buf); err != nil {
		return 0, err
	}

	n += int64(len(buf))

	// 1. Nullable flag [1 byte]
	isNullable := types.DecodeFixed[bool](buf[:1])
	vec.isNullable = isNullable

	//2. Vector
	buf = buf[1:]

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

//// ResetWithData is used by CloneWithBuffer to create a shallow copy of the srcVector.
//// When the shallowCopy undergoes an append, we allocate mpool memory. So we need isOwner flag.
//func (vec *vector[T]) ResetWithData(bs *Bytes, nulls *cnNulls.Nulls) {
//
//	newDownstream := newShallowCopyMoVecFromBytes(vec.GetType(), bs)
//	if nulls != nil {
//		cnNulls.Add(newDownstream.GetNulls(), nulls.ToArray()...)
//	}
//
//	vec.releaseDownstream()
//	vec.downstreamVector = newDownstream
//}

//func newShallowCopyMoVecFromBytes(typ types.Type, bs *Bytes) (mov *cnVector.Vector) {
//	if typ.IsVarlen() {
//		mov, _ = cnVector.FromDNVector(typ, bs.Header, bs.Storage, true)
//	} else {
//		mov, _ = cnVector.FromDNVector(typ, nil, bs.Storage, true)
//	}
//	return mov
//}

// When a new Append() is happening on a SharedMemory vector, we allocate the data[] from the mpool.
func (vec *vector[T]) tryCoW() {

	if !vec.isOwner {
		newCnVector, _ := vec.downstreamVector.Dup(vec.mpool)
		vec.downstreamVector = newCnVector
		vec.isOwner = true
	}
}

//func newDeepCopyMoVecFromBytes(typ types.Type, bs *Bytes, pool *mpool.MPool) (*cnVector.Vector, error) {
//
//	var mov *cnVector.Vector
//	if typ.IsVarlen() {
//
//		// 1. Mpool Allocate Header
//		var header []types.Varlena
//		headerByteArr := bs.HeaderBuf()
//
//		if len(headerByteArr) > 0 {
//			headerAllocated, err := pool.Alloc(len(headerByteArr))
//			if err != nil {
//				return nil, err
//			}
//			copy(headerAllocated, headerByteArr)
//
//			sz := typ.TypeSize()
//			header = unsafe.Slice((*types.Varlena)(unsafe.Pointer(&headerAllocated[0])), len(headerAllocated)/sz)
//		}
//
//		// 2. Mpool Allocate Storage
//		storageByteArr := bs.StorageBuf()
//		storageAllocated, err := pool.Alloc(len(storageByteArr))
//		if err != nil {
//			return nil, err
//		}
//		copy(storageAllocated, storageByteArr)
//
//		mov, _ = cnVector.FromDNVector(typ, header, storageAllocated, false)
//	} else {
//
//		// 1. Mpool Allocate Storage
//		storageByteArr := bs.StorageBuf()
//		storageAllocated, err := pool.Alloc(len(storageByteArr))
//		if err != nil {
//			return nil, err
//		}
//		copy(storageAllocated, storageByteArr)
//
//		mov, _ = cnVector.FromDNVector(typ, nil, storageAllocated, false)
//	}
//	return mov, nil
//}

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
	// TODO: Can we do it in CN vector?
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	cloned := NewVector[T](vec.GetType(), vec.Nullable(), opts)
	op := func(v any, _ int) error {
		cloned.Append(v)
		return nil
	}
	err := vec.ForeachWindowShallow(offset, length, op, nil)
	if err != nil {
		return nil
	}

	return cloned
}

//TODO: --- Below Functions Can be implemented in CN Vector.

func (vec *vector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	//TODO: CN vector include

	if srcLen <= 0 {
		return
	}

	sels := make([]int32, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int32(j) + int32(srcOff)
	}
	_ = vec.downstreamVector.Union(src.GetDownstreamVector(), sels, vec.GetAllocator())

	//// The downstream vector, ie CN vector needs isNull as argument.
	//// So, we can't directly call cn_vector.Append() without parsing the data.
	//// Hence, we are using src.Get(i) to retrieve the Null value as such from the src, and inserting
	//// it into the current CnVectorAdapter via this function.
	//for i := srcOff; i < srcOff+srcLen; i++ {
	//	vec.Append(src.Get(i))
	//}
}

func (vec *vector[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int, shallow bool) (err error) {

	if !vec.HasNull() {
		var v T
		if _, ok := any(v).([]byte); !ok {
			// Optimization for :- Vectors which are 1. not nullable & 2. not byte[]
			slice := vec.Slice().([]T)
			slice = slice[offset+bias : offset+length+bias]
			if sels == nil || sels.IsEmpty() {
				for i, elem := range slice {
					if err = op(elem, i+offset); err != nil {
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
					if err = op(slice[int(idx)-offset], int(idx)); err != nil {
						break
					}
				}
			}
			return
		}

	}
	if sels == nil || sels.IsEmpty() {
		for rowId := offset; rowId < offset+length; rowId++ {
			var elem any
			if shallow {
				elem = vec.ShallowGet(rowId + bias)
			} else {
				elem = vec.Get(rowId + bias)
			}
			if err = op(elem, rowId); err != nil {
				break
			}
		}
	} else {

		selsArray := sels.ToArray()
		end := offset + length
		for _, rowId := range selsArray {
			if int(rowId) < offset {
				continue
			} else if int(rowId) >= end {
				break
			}
			var elem any
			if shallow {
				elem = vec.ShallowGet(int(rowId) + bias)
			} else {
				elem = vec.Get(int(rowId) + bias)
			}
			if err = op(elem, int(rowId)); err != nil {
				break
			}
		}
	}
	return
}

// TODO: --- Need advise on the below functions

func (vec *vector[T]) Compact(deletes *roaring.Bitmap) {
	// TODO: Not doing tryCoW(). Is it ok XuPeng?
	//TODO: Do you have any other suggestion for converting []uint32 to []int64 using uns
	//var dels []int64
	//for i := range deletes.ToArray() {
	//	dels = append(dels, int64(i))
	//}

	vec.tryCoW()

	var sels []int64
	vecLen := uint32(vec.Length())
	for i := uint32(0); i < vecLen; i++ {
		if !deletes.Contains(i) {
			sels = append(sels, int64(i))
		}
	}

	vec.downstreamVector.Shrink(sels, false)
}

func (vec *vector[T]) GetDownstreamVector() *cnVector.Vector {
	return vec.downstreamVector
}

func (vec *vector[T]) SetDownstreamVector(dsVec *cnVector.Vector) {
	vec.isOwner = false
	vec.downstreamVector = dsVec
}

//Below functions are not used in critical path. Used mainly for testing

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
func (vec *vector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}
func (vec *vector[T]) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}
func (vec *vector[T]) Equals(o Vector) bool {

	if vec.Length() != o.Length() {
		return false
	}
	if vec.GetType() != o.GetType() {
		return false
	}
	if vec.Nullable() != o.Nullable() {
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
			if !bytes.Equal(vec.Get(i).([]byte), o.Get(i).([]byte)) {
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
