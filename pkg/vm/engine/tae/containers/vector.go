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

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

// TAE works on *vector.Vector directly (Vector is an alias). The functions
// here are the TAE conveniences that used to be methods of a wrapper around
// it: they panic instead of returning errors, and take the vector's own pool
// from the vector.
//
// Ownership is the vector's (see pkg/container/vector): a vector with a pool
// owns its memory and is released with ReleaseVector; a read-only vector (no
// pool) references memory someone else keeps alive. The mutating functions
// here panic on a read-only vector instead of copying it: Dup it first.

func NewConstFixed[T any](typ types.Type, val T, length int, opts ...Options) Vector {
	vc, err := vector.NewConstFixed[T](typ, val, length, allocatorOf(opts))
	if err != nil {
		panic(err)
	}
	return vc
}

func NewConstBytes(typ types.Type, val []byte, length int, opts ...Options) Vector {
	vc, err := vector.NewConstBytes(typ, val, length, allocatorOf(opts))
	if err != nil {
		panic(err)
	}
	return vc
}

func NewConstNullVector(typ types.Type, length int, mp *mpool.MPool) Vector {
	return vector.NewConstNull(typ, length, mp)
}

func NewVector(typ types.Type, opts ...Options) Vector {
	alloc := allocatorOf(opts)
	vec := vector.NewVec(typ, alloc)
	if len(opts) > 0 && opts[0].Capacity > 0 {
		if err := vec.PreExtend(opts[0].Capacity, alloc); err != nil {
			panic(err)
		}
	}
	return vec
}

func allocatorOf(opts []Options) *mpool.MPool {
	if len(opts) > 0 && opts[0].Allocator != nil {
		return opts[0].Allocator
	}
	return common.DefaultAllocator
}

// mustBeWritable panics on a read-only vector, which the mutating functions
// here cannot change in place.
func mustBeWritable(vec Vector, op string) {
	if vec.IsReadOnly() {
		panic(moerr.NewInternalErrorNoCtxf("%s on a read-only vector: Dup it first", op))
	}
}

// ReleaseVector releases vec: a vector lent by a VectorPool goes back to its
// pool, any other vector is freed. Releasing a vector again, or the shared
// EMPTY_VECTOR, does nothing.
func ReleaseVector(vec Vector) {
	if vec == nil || vec == EMPTY_VECTOR {
		return
	}
	if returnToPool(vec) {
		return
	}
	vec.Free(vec.MPool())
}

// ApproxSize is the vector's data size, 0 for a read-only vector.
func ApproxSize(vec Vector) int {
	if vec.IsReadOnly() {
		return 0
	}
	return vec.Size()
}

// VectorAllocated is the memory vec owns, 0 for a read-only vector (whose
// referenced capacity vector.Vector.Allocated reports).
func VectorAllocated(vec Vector) int {
	if vec.IsReadOnly() {
		return 0
	}
	return vec.Allocated()
}

// GetValue returns row i as a value; a varlena value is a copy, and a NULL
// varlena value is a nil []byte.
func GetValue(vec Vector, i int) any {
	if vec.IsConstNull() {
		return nil
	}
	if vec.GetType().IsVarlen() {
		if IsNull(vec, i) {
			var val []byte
			return val
		}
		bs := ShallowGetValue(vec, i).([]byte)
		ret := make([]byte, len(bs))
		copy(ret, bs)
		return any(ret)
	}
	return getNonNullValue(vec, uint32(i))
}

// ShallowGetValue returns row i as a value without copying a varlena value
// and without looking at its NULL bit.
func ShallowGetValue(vec Vector, i int) any {
	return getNonNullValue(vec, uint32(i))
}

// CopyRow copies row srcRow of src over row dstRow of dst without converting
// it through an interface value.
func CopyRow(dst, src Vector, dstRow, srcRow int) {
	if err := dst.Copy(src, int64(dstRow), int64(srcRow), dst.MPool()); err != nil {
		panic(err)
	}
}

// AppendValue appends v, or a NULL.
func AppendValue(vec Vector, v any, isNull bool) {
	mustBeWritable(vec, "append")
	var err error
	if isNull {
		err = vector.AppendAny(vec, nil, true, vec.MPool())
	} else {
		err = vector.AppendAny(vec, v, false, vec.MPool())
	}
	if err != nil {
		panic(err)
	}
}

// AppendMany appends values, each NULL where isNulls says so.
func AppendMany(vec Vector, vs []any, isNulls []bool) {
	for i, v := range vs {
		AppendValue(vec, v, isNulls[i])
	}
}

func IsNull(vec Vector, i int) bool {
	return vec.IsNull(uint64(i))
}

// NullCount counts NULL rows, for const vectors too.
func NullCount(vec Vector) int {
	if vec.IsConstNull() {
		return vec.Length()
	}
	if vec.IsConst() {
		return 0
	}
	if nsp := vec.GetNulls(); nsp != nil {
		return nsp.Count()
	}
	return 0
}

// UpdateRow sets row i to v, or to NULL.
func UpdateRow(vec Vector, i int, v any, isNull bool) {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("update to const vector"))
	}
	mustBeWritable(vec, "update")
	UpdateValue(vec, uint32(i), v, isNull, vec.MPool())
}

func PreExtend(vec Vector, length int) error {
	mustBeWritable(vec, "pre-extend")
	return vec.PreExtend(length, vec.MPool())
}

// WriteVector writes vec as an 8-byte size followed by its binary encoding.
func WriteVector(vec Vector, w io.Writer) (n int64, err error) {
	return writeVector(vec, w, vec.MarshalBinaryWithBuffer)
}

// WriteVectorV1 is WriteVector in version 1, whose null bitmap is v1.
func WriteVectorV1(vec Vector, w io.Writer) (n int64, err error) {
	return writeVector(vec, w, vec.MarshalBinaryWithBufferV1)
}

func writeVector(vec Vector, w io.Writer, marshal func(*bytes.Buffer) error) (n int64, err error) {
	estimatedSize := ApproxSize(vec)
	if estimatedSize < 0 {
		estimatedSize = 0
	}
	estimatedSize += 8 // size header
	if estimatedSize < 256 {
		estimatedSize = 256
	}

	// Optimization: write directly into a *bytes.Buffer
	if buf, ok := w.(*bytes.Buffer); ok {
		if buf.Cap()-buf.Len() < estimatedSize {
			buf.Grow(estimatedSize)
		}
		sizeOffset := buf.Len()
		var sizePlaceholder int64
		if _, err = buf.Write(types.EncodeInt64(&sizePlaceholder)); err != nil {
			return
		}
		if err = marshal(buf); err != nil {
			return
		}
		size := int64(buf.Len() - sizeOffset - 8)
		copy(buf.Bytes()[sizeOffset:sizeOffset+8], types.EncodeInt64(&size))
		n = int64(buf.Len() - sizeOffset)
		return
	}

	var bs bytes.Buffer
	bs.Grow(estimatedSize)
	var size int64
	_, _ = bs.Write(types.EncodeInt64(&size))
	if err = marshal(&bs); err != nil {
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

// ReadVector reads what WriteVector wrote into vec: vec's memory is
// released, and vec becomes a read-only vector of its type over the bytes
// read.
func ReadVector(vec Vector, r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += 8
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += int64(len(buf))
	resetReadOnly(vec)
	err = vec.UnmarshalBinary(buf)
	return
}

// ReadVectorV1 reads what WriteVectorV1 wrote, like ReadVector.
func ReadVectorV1(vec Vector, r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += 8
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	resetReadOnly(vec)
	err = vec.UnmarshalBinaryV1(buf)
	return
}

// resetReadOnly releases vec's memory and makes it an empty read-only vector
// of the same type, ready to decode into.
func resetReadOnly(vec Vector) {
	t := *vec.GetType()
	vec.Free(vec.MPool())
	*vec = *vector.NewVec(t, nil)
}

// TryConvertConst returns a non-const copy of a const vector, ready to edit,
// or vec itself when it is not const.
func TryConvertConst(vec Vector) Vector {
	if !vec.IsConst() {
		return vec
	}
	mp := vec.MPool()
	if mp == nil {
		mp = common.DefaultAllocator
	}
	ret := NewVector(*vec.GetType(), Options{Allocator: mp})
	if vec.IsConstNull() {
		for i := 0; i < vec.Length(); i++ {
			AppendValue(ret, nil, true)
		}
		return ret
	}
	v := GetValue(vec, 0)
	for i := 0; i < vec.Length(); i++ {
		AppendValue(ret, v, false)
	}
	return ret
}

func Foreach(vec Vector, op ItOp, sels *nulls.Nulls) error {
	return ForeachWindow(vec, 0, vec.Length(), op, sels)
}

func ForeachWindow(vec Vector, offset, length int, op ItOp, sels *nulls.Nulls) error {
	return ForeachVectorWindow(vec, offset, length, nil, op, sels)
}

// Window returns a read-only view of rows [offset, offset+length) of vec.
func Window(vec Vector, offset, length int) Vector {
	win, err := vec.Window(offset, offset+length)
	if err != nil {
		panic(err)
	}
	return win
}

// CloneWindow copies rows [offset, offset+length) of vec into a new vector
// owned by mp.
func CloneWindow(vec Vector, offset, length int, mp *mpool.MPool) Vector {
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("CloneWindow needs a pool"))
	}
	cloned, err := vec.CloneWindow(offset, offset+length, mp)
	if err != nil {
		panic(err)
	}
	return cloned
}

// CloneWindowWithPool copies rows [offset, offset+length) of vec into a
// vector lent by pool.
func CloneWindowWithPool(vec Vector, offset, length int, pool *VectorPool) Vector {
	cloned := pool.GetVector(vec.GetType())
	if err := vec.CloneWindowTo(cloned, offset, offset+length, cloned.MPool()); err != nil {
		panic(err)
	}
	return cloned
}

// Extend appends every row of src to dst.
func Extend(dst, src Vector) {
	ExtendWithOffset(dst, src, 0, src.Length())
}

// ExtendWithOffset appends rows [srcOff, srcOff+srcLen) of src to dst.
func ExtendWithOffset(dst, src Vector, srcOff, srcLen int) {
	if err := ExtendVecWithOffset(dst, src, srcOff, srcLen); err != nil {
		panic(err)
	}
}

// ExtendVec appends every row of src to dst.
func ExtendVec(dst, src Vector) error {
	return ExtendVecWithOffset(dst, src, 0, src.Length())
}

func ExtendVecWithOffset(dst, src Vector, srcOff, srcLen int) error {
	if dst.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("extend to const vector"))
	}
	if srcLen <= 0 {
		return nil
	}
	return dst.UnionBatch(src, int64(srcOff), srcLen, nil, dst.MPool())
}

// CompactByBitmap removes the rows set in mask.
func CompactByBitmap(vec Vector, mask *nulls.Nulls) {
	if mask.IsEmpty() {
		return
	}
	mustBeWritable(vec, "compact")
	var dels []int64
	mask.Foreach(func(i uint64) bool {
		dels = append(dels, int64(i))
		return true
	})
	if err := vec.Shrink(dels, true, vec.MPool()); err != nil {
		panic(err)
	}
}

// Compact removes the rows in deletes.
func Compact(vec Vector, deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	mustBeWritable(vec, "compact")
	var dels []int64
	itr := deletes.Iterator()
	for itr.HasNext() {
		dels = append(dels, int64(itr.Next()))
	}
	if err := vec.Shrink(dels, true, vec.MPool()); err != nil {
		panic(err)
	}
}

/****** Below functions are not used in critical path. Used mainly for testing */

// VectorString formats vec with up to 100 values.
// Deprecated: Only use for test functions
func VectorString(vec Vector) string {
	s := fmt.Sprintf("DN Vector: Len=%d[Rows];Allocted:%d[Bytes]", vec.Length(), VectorAllocated(vec))
	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
	}
	data := "Vals=["
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, GetValue(vec, i))
	}
	if vec.Length() > end {
		s = fmt.Sprintf("%s %s...]", s, data)
	} else {
		s = fmt.Sprintf("%s %s]", s, data)
	}
	return s
}

// PPString pretty-prints up to num values of vec.
// Deprecated: Only use for test functions
func PPString(vec Vector, num int) string {
	var w bytes.Buffer
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	_, _ = w.WriteString(fmt.Sprintf("[T=%s]%s", vec.GetType().String(), common.MoVectorToString(vec, limit)))
	if vec.Length() > num {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(fmt.Sprintf("[%v]", isLent(vec)))
	return w.String()
}

// DeleteRow deletes one row.
// Deprecated: Only use for test functions
func DeleteRow(vec Vector, delRowId int) {
	Compact(vec, roaring.BitmapOf(uint32(delRowId)))
}

// VectorEquals compares two vectors.
// Deprecated: Only use for test functions
func VectorEquals(vec, o Vector) bool {
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
		if !vec.GetNulls().IsSame(o.GetNulls()) {
			return false
		}
	}
	mask := vec.GetNulls()
	typ := vec.GetType()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.Contains(uint64(i)) {
			continue
		}
		if typ.IsVarlen() {
			if !bytes.Equal(ShallowGetValue(vec, i).([]byte), ShallowGetValue(o, i).([]byte)) {
				return false
			}
		} else if typ.Oid == types.T_decimal64 {
			d := GetValue(vec, i).(types.Decimal64)
			od := GetValue(vec, i).(types.Decimal64)
			if d != od {
				return false
			}
		} else if typ.Oid == types.T_decimal128 {
			d := GetValue(vec, i).(types.Decimal128)
			od := GetValue(vec, i).(types.Decimal128)
			if d != od {
				return false
			}
		} else if typ.Oid == types.T_TS {
			d := GetValue(vec, i).(types.TS)
			od := GetValue(vec, i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if typ.Oid == types.T_Rowid {
			d := GetValue(vec, i).(types.Rowid)
			od := GetValue(vec, i).(types.Rowid)
			if types.CompareRowidRowidAligned(d, od) != 0 {
				return false
			}
		} else {
			if GetValue(vec, i) != GetValue(o, i) {
				return false
			}
		}
	}
	return true
}
