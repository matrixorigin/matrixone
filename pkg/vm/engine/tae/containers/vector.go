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
	"github.com/matrixorigin/matrixone/pkg/compress"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
	"unsafe"
)

// DN vector is different from CN vector by
// 1. Nulls  - DN uses types.Nulls{}. It also uses isNullable.
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
		downstreamVector: cnVector.New(typ),
		isNullable:       nullable,
	}

	// nullable
	if nullable {
		vec.downstreamVector.Nsp = cnNulls.NewWithSize(0)
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
	return GetValue(vec.downstreamVector, uint32(i))
}

func (vec *vector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *vector[T]) Append(v any) {
	vec.tryPromoting()

	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)
	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
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

func (vec *vector[T]) NullMask() *roaring64.Bitmap {
	if input := vec.downstreamVector.GetNulls().Np; input != nil {
		np := roaring64.New()
		np.AddMany(input.ToArray())
		return np
	}
	return nil
}

func (vec *vector[T]) GetType() types.Type {
	return vec.downstreamVector.GetType()
}

func (vec *vector[T]) String() string {
	s := fmt.Sprintf("DN Vector: Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]", vec.Length(), vec.Capacity(), vec.Allocated())

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

func (vec *vector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *vector[T]) Update(i int, v any) {
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec *vector[T]) Slice() any {
	return vec.downstreamVector.Col
}

func (vec *vector[T]) Bytes() *Bytes {
	return MoVecToBytes(vec.downstreamVector)
}

func (vec *vector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.Length(), op, sels)
}

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

func (vec *vector[T]) HasNull() bool {
	return vec.downstreamVector.Nsp != nil && vec.downstreamVector.Nsp.Any()
}

// TODO: --- We can remove below functions as they don't have any usage

func (vec *vector[T]) IsView() bool {
	panic("Soon Deprecated")
}

func (vec *vector[T]) GetView() VectorView {
	panic("Soon Deprecated")
}

func (vec *vector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}

func (vec *vector[T]) DataWindow(offset, length int) []byte {
	panic("Soon Deprecated")
}

func (vec *vector[T]) Data() []byte {
	panic("Soon Deprecated")
}

func (vec *vector[T]) SlicePtr() unsafe.Pointer {
	slice := vec.Slice().([]T)
	return unsafe.Pointer(&slice[0])
}

func (vec *vector[T]) AppendNoNulls(s any) {
	panic("Soon Deprecated")
}

func (vec *vector[T]) Reset() {
	panic("Soon Deprecated")
}

func (vec *vector[T]) Capacity() int {
	return vec.Length()
}

// TODO: --- We can remove below function as they are only used in Testcases.

func (vec *vector[T]) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}

func (vec *vector[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	stat := f.Stat()
	var n []byte
	var buf []byte
	var tmpNode []byte
	if stat.CompressAlgo() != compress.None {
		osize := int(stat.OriginSize())
		size := stat.Size()
		tmpNode, err = vec.GetAllocator().Alloc(int(size))
		if err != nil {
			return
		}
		defer vec.GetAllocator().Free(tmpNode)
		srcBuf := tmpNode[:size]
		if _, err = f.Read(srcBuf); err != nil {
			return
		}
		if buffer == nil {
			n, err = vec.GetAllocator().Alloc(osize)
			if err != nil {
				return
			}
			buf = n[:osize]
		} else {
			buffer.Reset()
			if osize > buffer.Cap() {
				buffer.Grow(osize)
			}
			buf = buffer.Bytes()[:osize]
		}
		if _, err = compress.Decompress(srcBuf, buf, compress.Lz4); err != nil {
			if n != nil {
				vec.GetAllocator().Free(n)
			}
			return nil
		}
	}

	_, err = vec.ReadFrom(bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	return nil
}

//TODO: --- Below Functions Can be implemented in CN Vector.

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
		if !vec.NullMask().Equals(o.NullMask()) {
			return false
		}
	}
	mask := vec.NullMask()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.ContainsInt(i) {
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
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := vec.Get(i).(types.Decimal128)
			od := vec.Get(i).(types.Decimal128)
			if d.Ne(od) {
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

func (vec *vector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	err = vec.forEachWindowWithBias(offset, length, op, sels, 0)
	return
}

func (vec *vector[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int) (err error) {
	if sels == nil || sels.IsEmpty() {
		for i := offset; i < offset+length; i++ {
			elem := vec.Get(i + bias)
			if err = op(elem, i); err != nil {
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
			elem := vec.Get(int(rowId) + bias)
			if err = op(elem, int(rowId)); err != nil {
				break
			}
		}
	}
	return
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

//TODO: --- Need advise on the below functions

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

func (vec *vector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {

	newDownstream := NewShallowCopyMoVecFromBytes(vec.GetType(), bs)

	if vec.Nullable() {
		//TODO: We can avoid cloning NSP when nulls is changed to a regular bitmap
		newNulls := cnNulls.NewWithSize(0)
		if nulls != nil && !nulls.IsEmpty() {
			cnNulls.Add(newNulls, nulls.ToArray()...)
		}

		newDownstream.Nsp = newNulls
	}

	vec.releaseDownstream()
	vec.downstreamVector = newDownstream
}

// When a new Append() is happening on a SharedMemory vector, we allocate the data[] from the mpool.
func (vec *vector[T]) tryPromoting() {

	if !vec.isOwner {
		src := vec.Bytes()

		// deep copy
		newDownstream, _ := NewDeepCopyMoVecFromBytes(vec.GetType(), src, vec.GetAllocator())
		newDownstream.Nsp = vec.downstreamVector.Nsp.Clone()

		vec.downstreamVector = newDownstream
		vec.isOwner = true
	}
}

func (vec *vector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {

	if srcLen <= 0 {
		return
	}

	// The downstream vector, ie CN vector needs isNull as argument.
	// So, we can't directly call cn_vector.Append() without parsing the data.
	// Hence, we are using src.Get(i) to retrieve the Null value as such from the src, and inserting
	// it into the current CnVectorAdapter via this function.
	for i := srcOff; i < srcOff+srcLen; i++ {
		vec.Append(src.Get(i))
	}
}

func (vec *vector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
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
	err := vec.ForeachWindow(offset, length, op, nil)
	if err != nil {
		return nil
	}

	return cloned
}

func (vec *vector[T]) Compact(deletes *roaring.Bitmap) {
	// TODO: Not doing tryPromoting()
	var sels []int64
	vecLen := uint32(vec.Length())
	for i := uint32(0); i < vecLen; i++ {
		if !deletes.Contains(i) {
			sels = append(sels, int64(i))
		}
	}
	cnVector.Shrink(vec.downstreamVector, sels)
}
