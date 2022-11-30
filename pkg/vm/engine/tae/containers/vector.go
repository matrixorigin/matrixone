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
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
)

type internalVector interface {
	Vector
	forEachWindowWithBias(offset, length int, op ItOp,
		sels *roaring.Bitmap, bias int) (err error)
}

type vector[T any] struct {
	stlvec    stl.Vector[T]
	impl      internalVector
	typ       types.Type
	nulls     *roaring64.Bitmap
	roStorage []byte
}

func NewVector[T any](typ types.Type, nullable bool, opts ...*Options) *vector[T] {
	vec := &vector[T]{
		stlvec: containers.NewVector[T](opts...),
		typ:    typ,
	}
	if nullable {
		vec.impl = newNullableVecImpl(vec)
	} else {
		vec.impl = newVecImpl(vec)
	}
	return vec
}

// func NewEmptyVector[T any](typ types.Type, opts ...*Options) *vector[T] {
// 	vec := new(vector[T])
// 	vec.typ = typ
// 	vec.stlvec = container.NewVector[T](opts...)
// 	return vec
// }

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
func (vec *vector[T]) IsView() bool                { return vec.impl.IsView() }
func (vec *vector[T]) Nullable() bool              { return vec.impl.Nullable() }
func (vec *vector[T]) IsNull(i int) bool           { return vec.impl.IsNull(i) }
func (vec *vector[T]) HasNull() bool               { return vec.impl.HasNull() }
func (vec *vector[T]) NullMask() *roaring64.Bitmap { return vec.impl.NullMask() }
func (vec *vector[T]) Bytes() *Bytes               { return vec.impl.Bytes() }
func (vec *vector[T]) Data() []byte                { return vec.impl.Data() }
func (vec *vector[T]) DataWindow(offset, length int) []byte {
	return vec.impl.DataWindow(offset, length)
}
func (vec *vector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := new(Options)
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}
	cloned := NewVector[T](vec.typ, vec.Nullable(), opts)
	if vec.nulls != nil {
		if offset == 0 || length == vec.Length() {
			cloned.nulls = vec.nulls.Clone()
			if length < vec.Length() {
				cloned.nulls.RemoveRange(uint64(length), uint64(vec.Length()))
			}
		} else {
			cloned.nulls = roaring64.New()
			for i := offset; i < offset+length; i++ {
				if vec.nulls.ContainsInt(i) {
					cloned.nulls.AddInt(i - offset)
				}
			}
		}
	}
	cloned.stlvec.Close()
	cloned.stlvec = vec.stlvec.Clone(offset, length, allocator...)
	return cloned
}
func (vec *vector[T]) fastSlice() []T {
	return vec.stlvec.Slice()
}
func (vec *vector[T]) Slice() any {
	return vec.stlvec.Slice()
}
func (vec *vector[T]) SlicePtr() unsafe.Pointer {
	return vec.stlvec.SlicePtr()
}

func (vec *vector[T]) Get(i int) (v any)                   { return vec.impl.Get(i) }
func (vec *vector[T]) Update(i int, v any)                 { vec.impl.Update(i, v) }
func (vec *vector[T]) Delete(i int)                        { vec.impl.Delete(i) }
func (vec *vector[T]) DeleteBatch(deletes *roaring.Bitmap) { vec.impl.DeleteBatch(deletes) }
func (vec *vector[T]) Append(v any)                        { vec.impl.Append(v) }
func (vec *vector[T]) AppendMany(vs ...any)                { vec.impl.AppendMany(vs...) }
func (vec *vector[T]) AppendNoNulls(s any)                 { vec.impl.AppendNoNulls(s) }
func (vec *vector[T]) Extend(o Vector)                     { vec.impl.Extend(o) }
func (vec *vector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {
	vec.impl.ExtendWithOffset(src, srcOff, srcLen)
}

// func (vec *vector[T]) ExtendView(o VectorView) { vec.impl.ExtendView(o) }
func (vec *vector[T]) Length() int    { return vec.impl.Length() }
func (vec *vector[T]) Capacity() int  { return vec.impl.Capacity() }
func (vec *vector[T]) Allocated() int { return vec.impl.Allocated() }

func (vec *vector[T]) GetAllocator() *mpool.MPool { return vec.stlvec.GetAllocator() }
func (vec *vector[T]) GetType() types.Type        { return vec.typ }
func (vec *vector[T]) String() string             { return vec.impl.String() }
func (vec *vector[T]) PPString(num int) string    { return vec.impl.PPString(num) }
func (vec *vector[T]) Close()                     { vec.impl.Close() }

func (vec *vector[T]) cow() {
	vec.stlvec = vec.stlvec.Clone(0, vec.stlvec.Length())
	vec.releaseRoStorage()
}
func (vec *vector[T]) releaseRoStorage() {
	if vec.roStorage != nil {
		vec.GetAllocator().Free(vec.roStorage)
	}
	vec.roStorage = nil
}

func (vec *vector[T]) Window(offset, length int) Vector {
	return &vectorWindow[T]{
		ref: vec,
		windowBase: &windowBase{
			offset: offset,
			length: length,
		},
	}
}

func (vec *vector[T]) Compact(deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	if vec.roStorage != nil {
		vec.cow()
	}
	vec.impl.DeleteBatch(deletes)
}

func (vec *vector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int
	var tmpn int64
	// 1. Vector type
	vt := vec.GetType()
	if nr, err = w.Write(types.EncodeType(&vt)); err != nil {
		return
	}
	n += int64(nr)
	// 2. Nullable
	if nr, err = w.Write(types.EncodeFixed(vec.Nullable())); err != nil {
		return
	}
	n += int64(nr)
	// 3. Vector data
	if tmpn, err = vec.stlvec.WriteTo(w); err != nil {
		return
	}
	n += tmpn
	if !vec.Nullable() {
		return
	}
	// 4. Nulls
	var nullBuf []byte
	if vec.nulls != nil {
		if nullBuf, err = vec.nulls.ToBytes(); err != nil {
			return
		}
	}
	if nr, err = w.Write(types.EncodeFixed(uint32(len(nullBuf)))); err != nil {
		return
	}
	n += int64(nr)
	if len(nullBuf) == 0 {
		return
	}
	if nr, err = w.Write(nullBuf); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (vec *vector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	vec.releaseRoStorage()
	var tmpn int64
	// 1. Vector type
	typeBuf := make([]byte, types.TSize)
	if _, err = r.Read(typeBuf); err != nil {
		return
	}
	vec.typ = types.DecodeType(typeBuf)
	n += int64(len(typeBuf))

	// 2. Nullable
	oneBuf := make([]byte, 1)
	if _, err = r.Read(oneBuf); err != nil {
		return
	}
	nullable := types.DecodeFixed[bool](oneBuf)
	n += 1

	if nullable {
		vec.impl = newNullableVecImpl(vec)
	} else {
		vec.impl = newVecImpl(vec)
	}

	// 3. Data
	if tmpn, err = vec.stlvec.ReadFrom(r); err != nil {
		return
	}
	n += tmpn

	// 4. Null
	if !nullable {
		return
	}
	fourBuf := make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = r.Read(fourBuf); err != nil {
		return
	}
	n += int64(len(fourBuf))
	nullSize := types.DecodeFixed[uint32](fourBuf)
	if nullSize == 0 {
		return
	}
	vec.nulls = roaring64.New()
	if tmpn, err = vec.nulls.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	return
}

func (vec *vector[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	vec.releaseRoStorage()
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
			return
		}
	}
	vec.typ = types.DecodeType(buf[:types.TSize])
	buf = buf[types.TSize:]

	nullable := types.DecodeFixed[bool](buf[:1])
	buf = buf[1:]

	if nullable {
		vec.impl = newNullableVecImpl(vec)
	} else {
		vec.impl = newVecImpl(vec)
	}
	var nr int64
	if nr, err = vec.stlvec.InitFromSharedBuf(buf); err != nil {
		if n != nil {
			vec.GetAllocator().Free(n)
		}
		return
	}
	buf = buf[nr:]
	if !nullable {
		vec.roStorage = n
		return
	}

	nullSize := types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	if nullSize > 0 {
		nullBuf := buf[:nullSize]
		nulls := roaring64.New()
		r := bytes.NewBuffer(nullBuf)
		if _, err = nulls.ReadFrom(r); err != nil {
			if n != nil {
				vec.GetAllocator().Free(n)
			}
			return
		}
		vec.nulls = nulls
	}
	vec.roStorage = n
	return
}

func (vec *vector[T]) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(0, vec.Length(), op, sels)
}

func (vec *vector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(offset, length, op, sels)
}

func (vec *vector[T]) GetView() (view VectorView) {
	return newVecView(vec)
}

func (vec *vector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {
	vec.releaseRoStorage()
	if vec.Nullable() {
		vec.nulls = nulls
	}
	vec.stlvec.ReadBytes(bs, true)
}

func (vec *vector[T]) Reset() {
	vec.releaseRoStorage()
	vec.stlvec.Reset()
	vec.nulls = nil
}
