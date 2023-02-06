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

type CnTaeVector[T any] struct {
	downstreamVector     *cnVector.Vector
	mpool                *mpool.MPool
	isAllocatedFromMpool bool
}

func NewCnTaeVector[T any](typ types.Type, nullable bool, opts ...Options) *CnTaeVector[T] {
	vec := CnTaeVector[T]{
		downstreamVector: cnVector.New(typ),
	}

	// nullable
	if nullable {
		vec.downstreamVector.Nsp = cnNulls.NewWithSize(0)
	}

	// mpool
	var alloc *mpool.MPool
	if len(opts) > 0 {
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	vec.mpool = alloc

	// mpool allocated
	vec.isAllocatedFromMpool = true

	return &vec
}

func (vec *CnTaeVector[T]) Get(i int) any {
	return GetValue(vec.downstreamVector, uint32(i))
}

func (vec *CnTaeVector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *CnTaeVector[T]) Close() {
	vec.downstreamVector.Free(vec.mpool)
	vec.isAllocatedFromMpool = false
}

func (vec *CnTaeVector[T]) HasNull() bool {
	return vec.downstreamVector.GetNulls().Any()
}

func (vec *CnTaeVector[T]) Append(v any) {
	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)
	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
	}

}

func (vec *CnTaeVector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}

func (vec *CnTaeVector[T]) Nullable() bool {
	return vec.downstreamVector.Nsp.Np != nil
}

func (vec *CnTaeVector[T]) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec *CnTaeVector[T]) IsNull(i int) bool {
	return vec.downstreamVector.GetNulls() != nil && vec.downstreamVector.GetNulls().Contains(uint64(i))
}

func (vec *CnTaeVector[T]) NullMask() *roaring64.Bitmap {
	input := vec.downstreamVector.GetNulls().Np
	var np *roaring64.Bitmap
	if input != nil {
		np = roaring64.New()
		np.AddMany(input.ToArray())
		return np
	}
	return nil
}

func (vec *CnTaeVector[T]) GetType() types.Type {
	return vec.downstreamVector.GetType()
}

func (vec *CnTaeVector[T]) Compact(deletes *roaring.Bitmap) {
	var sels []int64
	for i := 0; i < vec.Length(); i++ {
		if !deletes.Contains(uint32(i)) {
			sels = append(sels, int64(i))
		}
	}
	cnVector.Shrink(vec.downstreamVector, sels)
}

func (vec *CnTaeVector[T]) String() string {
	// TODO: Replace with CN vector String
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

func (vec *CnTaeVector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *CnTaeVector[T]) Update(i int, v any) {
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec *CnTaeVector[T]) Reset() {
	if vec.Length() == 0 {
		return
	}

	vec.downstreamVector.Nsp.Np.Clear()
	cnVector.Reset(vec.downstreamVector)
	vec.isAllocatedFromMpool = false
}

func (vec *CnTaeVector[T]) Slice() any {
	return vec.downstreamVector.Col
}

func (vec *CnTaeVector[T]) Bytes() *Bytes {
	return MoVecToBytes(vec.downstreamVector)
}

func (vec *CnTaeVector[T]) Window(offset, length int) Vector {

	window := cnVector.New(vec.GetType())
	cnVector.Window(vec.downstreamVector, offset, offset+length, window)

	return &CnTaeVector[T]{
		downstreamVector:     window,
		mpool:                vec.GetAllocator(),
		isAllocatedFromMpool: false,
	}
}

func (vec *CnTaeVector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.Length(), op, sels)
}

func (vec *CnTaeVector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	// Create a clone
	cloned := NewVector[T](vec.GetType(), vec.Nullable(), opts)
	cloned.isAllocatedFromMpool = true

	// Create a duplicate of the current vector
	vecDup, _ := cnVector.Dup(vec.downstreamVector, opts.Allocator)

	// attach that duplicate to the window
	// and perform window operation
	cnVector.Window(vecDup, offset, offset+length, cloned.downstreamVector)

	return cloned
}

func (vec *CnTaeVector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {

	//TODO: ExtendWithOffset impl is having poor benchmark score.
	//Will migrate the DN implementation to CN once CN vector impl is completed.

	if srcLen <= 0 {
		return
	}

	for i := srcOff; i < srcOff+srcLen; i++ {
		vec.Append(src.Get(i))
	}

}

func (vec *CnTaeVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int

	output, _ := vec.downstreamVector.MarshalBinary()
	if nr, err = w.Write(output); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (vec *CnTaeVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	var marshalledByteArray []byte

	// isScalar 1
	scalar := make([]byte, 1)
	if _, err = r.Read(scalar); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, scalar...)

	// Length 8
	length := make([]byte, 8)
	if _, err = r.Read(length); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, length...)

	// Typ 20
	vecTyp := make([]byte, 20)
	if _, err = r.Read(vecTyp); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, vecTyp...)

	//1. Nsp Length 4
	nspLen := make([]byte, 4)
	if _, err = r.Read(nspLen); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, nspLen...)

	// Nsp [?]
	nspLenVal := types.DecodeUint32(nspLen)
	nsp := make([]byte, nspLenVal)
	if _, err = r.Read(nsp); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, nsp...)

	//2. Col Length 4
	colLen := make([]byte, 4)
	if _, err = r.Read(colLen); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, colLen...)

	// Col [?]
	colLenVal := types.DecodeUint32(colLen)
	col := make([]byte, colLenVal)
	if _, err = r.Read(col); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, col...)

	//3. Col Length 4
	areaLen := make([]byte, 4)
	if _, err = r.Read(areaLen); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, areaLen...)

	// Col [?]
	areaLenVal := types.DecodeUint32(areaLen)
	area := make([]byte, areaLenVal)
	if _, err = r.Read(area); err != nil {
		return
	}
	marshalledByteArray = append(marshalledByteArray, area...)

	n = int64(len(marshalledByteArray))

	err = vec.downstreamVector.UnmarshalBinary(marshalledByteArray)
	return
}

// TODO: --- We can remove below functions as they don't have any usage

func (vec *CnTaeVector[T]) IsView() bool {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) GetView() VectorView {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) DataWindow(offset, length int) []byte {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) Data() []byte {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) SlicePtr() unsafe.Pointer {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) AppendNoNulls(s any) {
	panic("Soon Deprecated")
}

// Delete TODO: used very rare.
func (vec *CnTaeVector[T]) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}

// TODO: --- We can remove below function as they are only used in Testcases.

func (vec *CnTaeVector[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	// No usage except in testcase

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

	err = vec.downstreamVector.UnmarshalBinary(buf)
	return err
}

// TODO: --- Below Functions Can be implemented in CN Vector.

func (vec *CnTaeVector[T]) PPString(num int) string {
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

func (vec *CnTaeVector[T]) Equals(o Vector) bool {

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

func (vec *CnTaeVector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {

	if sels == nil || sels.IsEmpty() {
		//TODO: When sel is Empty(), should we run it for all the entries or should we not perform at all.
		// In current DN impl, when sel is empty(), we run it on all the entries.
		for i := offset; i < offset+length; i++ {
			elem := vec.Get(i)
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
			elem := vec.Get(int(rowId))
			if err = op(elem, int(rowId)); err != nil {
				break
			}
		}
	}
	return
}

// TODO: --- I am not sure, if the below functions will work as expected

func (vec *CnTaeVector[T]) Allocated() int {

	if !vec.isAllocatedFromMpool {
		return 0
	}
	// Only VarLen is allocated using mpool.
	if vec.GetType().IsVarlen() {
		return vec.downstreamVector.Size()
	}

	// TODO: Not sure if the below part should return 0 or len(downstream.data)
	return 0
	//return vec.Length()
}

func (vec *CnTaeVector[T]) Capacity() int {
	// TODO: Can we use Length() instead of Capacity?
	// Not used much. Can we remove?

	// TODO: Capacity should be based on a number and not based on the Length. Fix it later.
	return vec.Length()
}

func (vec *CnTaeVector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {
	vec.Reset()

	src := NewMoVecFromBytesAndNulls(vec.GetType(), bs, nulls)
	vec.downstreamVector = src

	vec.isAllocatedFromMpool = false
	// TODO: Doesn't reset the capacity
}
