// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"sort"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
)

const (
	FLAT     = iota // flat vector represent a uncompressed vector
	CONSTANT        // const vector
	DIST            // dictionary vector
)

// Vector represent a column
type Vector struct {
	// vector's class
	class int
	// type represent the type of column
	typ types.Type

	// data of fixed length element, in case of varlen, the Varlena
	data []byte

	// area for holding large strings.
	area []byte

	length int

	nsp nulls.Nulls // nulls list
	gsp nulls.Nulls // grouping list

	cantFreeData bool
	cantFreeArea bool

	sorted bool // for some optimization

	// FIXME: Bad design! Will be deleted soon.
	isBin bool

	offHeap bool
}

func toSliceOfLengthNoTypeCheck[T any](vec *Vector, length int) []T {
	if length == 0 {
		return nil
	}
	checkTypeIfRaceDetectorEnabled[T](vec)
	return util.UnsafeSliceCastToLength[T](vec.data, length)
}

func ToSliceNoTypeCheck[T any](vec *Vector, ret *[]T) {
	if vec.IsConst() {
		*ret = toSliceOfLengthNoTypeCheck[T](vec, 1)
	} else {
		*ret = toSliceOfLengthNoTypeCheck[T](vec, vec.length)
	}
}

func ToSliceNoTypeCheck2[T any](vec *Vector) []T {
	if vec.IsConst() {
		return toSliceOfLengthNoTypeCheck[T](vec, 1)
	} else {
		return toSliceOfLengthNoTypeCheck[T](vec, vec.length)
	}
}

func ToSlice[T any](vec *Vector, ret *[]T) {
	checkType[T](vec)
	if vec.IsConst() {
		*ret = util.UnsafeSliceCastToLength[T](vec.data, 1)
	} else {
		*ret = util.UnsafeSliceCastToLength[T](vec.data, vec.length)
	}
}

func checkType[T any](vec *Vector) {
	if !typeCompatible[T](vec.typ) {
		panic(fmt.Sprintf("type mismatch: casting %v vector to %T", vec.typ.String(), []T{}))
	}
}

func (v *Vector) GetSorted() bool {
	return v.sorted
}

func (v *Vector) SetSorted(b bool) {
	v.sorted = b
}

// Reset update vector's fields with a specific type.
// we should redefine the value of capacity and values-ptr because of the possible change in type.
func (v *Vector) Reset(typ types.Type) {
	v.typ = typ

	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}

	v.length = 0
	v.nsp.Clear()
	v.gsp.Clear()
	v.sorted = false
}

func (v *Vector) ResetWithSameType() {
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.length = 0
	v.nsp.Reset()
	v.gsp.Reset()
	v.sorted = false
}

func (v *Vector) ResetArea() {
	v.area = v.area[:0]
}

// TODO: It is semantically same as Reset, need to merge them later.
func (v *Vector) ResetWithNewType(t *types.Type) {
	v.typ = *t
	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.nsp.Clear()
	v.gsp.Clear()
	v.length = 0
	v.sorted = false
}

func (v *Vector) UnsafeGetRawData() []byte {
	length := 1
	if !v.IsConst() {
		length = v.length
	}
	return v.data[:length*v.typ.TypeSize()]
}

func (v *Vector) Length() int {
	return v.length
}

func (v *Vector) Capacity() int {
	return cap(v.data) / v.typ.TypeSize()
}

// Allocated returns the total allocated memory size of the vector.
// it can be used to estimate the memory usage of the vector.
func (v *Vector) Allocated() int {
	return cap(v.data) + cap(v.area)
}

func (v *Vector) SetLength(n int) {
	v.length = n
}

// Size of data, I think this function is inherently broken.  This
// Size is not meaningful other than used in (approximate) memory accounting.
func (v *Vector) Size() int {
	return v.length*v.typ.TypeSize() + len(v.area)
}

func (v *Vector) GetType() *types.Type {
	return &v.typ
}

// Bug #23240
// This is very dangerous.   We changed vector type
// but did not change the underlying data.   So the length
// and capacity are all messed up.
func (v *Vector) SetType(typ types.Type) {
	v.typ = typ
}

// Bug #23240
// Neither this function, nor the SetType function are good
// Maybe we should just disallow.
func (v *Vector) SetTypeAndFixData(typ types.Type, mp *mpool.MPool) {
	if v.typ.IsVarlen() && typ.IsVarlen() {
		v.typ = typ
		return
	}

	if v.typ.IsVarlen() || typ.IsVarlen() {
		// this is a weird thing to do, we should not allow it.
		panic("SetTypeAndFixData is not allowed to change from/to varlen type")
	}

	v.typ = typ
	oldLength := v.length
	v.length = 0
	extend(v, oldLength, mp)
	v.length = oldLength
}

func (v *Vector) SetOffHeap(offHeap bool) {
	v.offHeap = offHeap
}

func (v *Vector) SetTypeScale(scale int32) {
	v.typ.Scale = scale
}

func (v *Vector) GetNulls() *nulls.Nulls {
	return &v.nsp
}

func (v *Vector) GetGrouping() *nulls.Nulls {
	return &v.gsp
}

func (v *Vector) SetNulls(nsp *nulls.Nulls) {
	v.nsp.Clear()
	if nsp == nil {
		return
	}
	v.nsp.Or(nsp)
}

func (v *Vector) SetAllNulls(length int) {
	v.nsp.InitWithSize(int(length))
	v.nsp.AddRange(0, uint64(length))
}

func (v *Vector) SetGrouping(gsp *nulls.Nulls) {
	v.gsp.Clear()
	if gsp == nil {
		return
	}
	v.gsp.Or(gsp)
}

func (v *Vector) HasNull() bool {
	return v.IsConstNull() || !v.nsp.IsEmpty()
}

func (v *Vector) HasGrouping() bool {
	return !v.gsp.IsEmpty()
}

func (v *Vector) AllNull() bool {
	return v.IsConstNull() || (v.length != 0 && v.nsp.Count() == v.length)
}

func (v *Vector) GetIsBin() bool {
	return v.isBin
}

func (v *Vector) SetIsBin(isBin bool) {
	v.isBin = isBin
}

func (v *Vector) NeedDup() bool {
	return v.cantFreeArea || v.cantFreeData
}

// make sure the type check is done before calling this function
func GetFixedAtNoTypeCheck[T any](v *Vector, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	var slice []T
	ToSliceNoTypeCheck(v, &slice)
	return slice[idx]
}

// Note:
// it is much inefficient than GetFixedAtNoTypeCheck
// if type check is done before calling this function, use GetFixedAtNoTypeCheck
func GetFixedAtWithTypeCheck[T any](v *Vector, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	var slice []T
	ToSlice(v, &slice)
	return slice[idx]
}

func (v *Vector) CloneBytesAt(i int) []byte {
	bs := v.GetBytesAt(i)
	ret := make([]byte, len(bs))
	copy(ret, bs)
	return ret
}

func (v *Vector) GetBytesAt(i int) []byte {
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].GetByteSlice(v.area)
}

func (v *Vector) GetBytesAt2(bs []types.Varlena, i int) []byte {
	if v.IsConst() {
		i = 0
	}
	return bs[i].GetByteSlice(v.area)
}

func (v *Vector) GetRawBytesAt(i int) []byte {
	if v.typ.IsVarlen() {
		return v.GetBytesAt(i)
	} else {
		if v.IsConst() {
			i = 0
		} else {
			i *= v.GetType().TypeSize()
		}
		return v.data[i : i+v.GetType().TypeSize()]
	}
}

func (v *Vector) CleanOnlyData() {
	if v.data != nil {
		v.length = 0
	}
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.nsp.Clear()
	v.gsp.Clear()
	v.sorted = false
}

// no copy. it is unsafe if the user cannot determine the vector's life
func (v *Vector) UnsafeGetStringAt(i int) string {
	if v.IsConst() {
		i = 0
	}
	// if !v.typ.Oid.IsFixedLen() {
	// 	panic(fmt.Sprintf("type mismatch: expect varlen type but actual %s", v.typ.String()))
	// }
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].UnsafeGetString(v.area)
}

// always copy
func (v *Vector) GetStringAt(i int) string {
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].GetString(v.area)
}

// GetArrayAt Returns []T at the specific index of the vector
func GetArrayAt[T types.RealNumbers](v *Vector, i int) []T {
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return types.GetArray[T](&bs[i], v.area)
}

func GetArrayAt2[T types.RealNumbers](v *Vector, bs []types.Varlena, i int) []T {
	if v.IsConst() {
		i = 0
	}
	return types.GetArray[T](&bs[i], v.area)
}

// WARNING: GetAny() return value with any type will cause memory escape to heap which will result in slow GC.
// If you know the actual type, better use the GetFixedAtWithTypeCheck() to get the values.
// Only use when you have no choice, e.g. you are dealing with column with any type that don't know in advanced.
func GetAny(vec *Vector, i int, deepCopy bool) any {
	switch vec.typ.Oid {
	case types.T_bool:
		return GetFixedAtNoTypeCheck[bool](vec, i)
	case types.T_bit:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_int8:
		return GetFixedAtNoTypeCheck[int8](vec, i)
	case types.T_int16:
		return GetFixedAtNoTypeCheck[int16](vec, i)
	case types.T_int32:
		return GetFixedAtNoTypeCheck[int32](vec, i)
	case types.T_int64:
		return GetFixedAtNoTypeCheck[int64](vec, i)
	case types.T_uint8:
		return GetFixedAtNoTypeCheck[uint8](vec, i)
	case types.T_uint16:
		return GetFixedAtNoTypeCheck[uint16](vec, i)
	case types.T_uint32:
		return GetFixedAtNoTypeCheck[uint32](vec, i)
	case types.T_uint64:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_float32:
		return GetFixedAtNoTypeCheck[float32](vec, i)
	case types.T_float64:
		return GetFixedAtNoTypeCheck[float64](vec, i)
	case types.T_date:
		return GetFixedAtNoTypeCheck[types.Date](vec, i)
	case types.T_datetime:
		return GetFixedAtNoTypeCheck[types.Datetime](vec, i)
	case types.T_time:
		return GetFixedAtNoTypeCheck[types.Time](vec, i)
	case types.T_timestamp:
		return GetFixedAtNoTypeCheck[types.Timestamp](vec, i)
	case types.T_year:
		return GetFixedAtNoTypeCheck[types.MoYear](vec, i)
	case types.T_enum:
		return GetFixedAtNoTypeCheck[types.Enum](vec, i)
	case types.T_decimal64:
		return GetFixedAtNoTypeCheck[types.Decimal64](vec, i)
	case types.T_decimal128:
		return GetFixedAtNoTypeCheck[types.Decimal128](vec, i)
	case types.T_uuid:
		return GetFixedAtNoTypeCheck[types.Uuid](vec, i)
	case types.T_TS:
		return GetFixedAtNoTypeCheck[types.TS](vec, i)
	case types.T_Rowid:
		return GetFixedAtNoTypeCheck[types.Rowid](vec, i)
	case types.T_Blockid:
		return GetFixedAtNoTypeCheck[types.Blockid](vec, i)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		ret := vec.GetBytesAt(i)
		if deepCopy {
			copied := make([]byte, len(ret))
			copy(copied, ret)
			ret = copied
		}
		return ret
	}
	return nil
}

func NewVec(typ types.Type) *Vector {
	vec := NewVecFromReuse()
	vec.typ = typ
	vec.class = FLAT
	return vec
}

func NewOffHeapVecWithType(typ types.Type) *Vector {
	vec := NewVec(typ)
	vec.offHeap = true
	return vec
}

func NewOffHeapVecWithTypeAndData(typ types.Type, data []byte, length, cap int) *Vector {
	vec := NewVec(typ)
	vec.offHeap = true
	vec.data = data
	vec.length = length
	return vec
}

func NewOffHeapVec() *Vector {
	vec := NewVecFromReuse()
	vec.offHeap = true
	return vec
}

func NewVecWithData(
	typ types.Type,
	length int,
	data []byte,
	area []byte,
) *Vector {
	vec := NewVec(typ)
	vec.length = length
	vec.data = data
	vec.area = area
	return vec
}

func NewConstNull(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT
	vec.length = length
	return vec
}

func NewRollupConst(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := NewVecFromReuse()
	vec.gsp.AddRange(0, uint64(length))
	vec.typ = typ
	vec.class = CONSTANT
	vec.length = length
	return vec
}

func NewConstFixed[T any](typ types.Type, val T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstFixed(vec, val, length, mp)
	}

	return vec, err
}

func NewConstBytes(typ types.Type, val []byte, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstBytes(vec, val, length, mp)
	}

	return vec, err
}

// NewConstArray Creates a Const_Array Vector
func NewConstArray[T types.RealNumbers](typ types.Type, val []T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstArray[T](vec, val, length, mp)
	}

	return vec, err
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

func (v *Vector) IsGrouping() bool {
	return v.length > 0 && v.length == v.gsp.Count()
}

func (v *Vector) SetClass(class int) {
	v.class = class
}

func (v *Vector) IsNull(i uint64) bool {
	if v.IsConstNull() {
		return true
	}
	if v.IsConst() {
		return false
	}
	return v.nsp.Contains(i)
}

func (v *Vector) SetNull(i uint64) {
	v.nsp.Add(i)
}

func (v *Vector) UnsetNull(i uint64) {
	v.nsp.Del(i)
}

// call this function if type already checked
func SetFixedAtNoTypeCheck[T types.FixedSizeT](v *Vector, idx int, t T) error {
	vacol := MustFixedColNoTypeCheck[T](v)
	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtxf("vector idx out of range: %d > %d", idx, len(vacol))
	}
	vacol[idx] = t
	return nil
}

// Note:
// it is 10x slower than SetFixedAtNoTypeCheck
func SetFixedAtWithTypeCheck[T types.FixedSizeT](v *Vector, idx int, t T) error {
	// Let it panic if v is not a varlena vec
	vacol := MustFixedColWithTypeCheck[T](v)

	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtxf("vector idx out of range: %d > %d", idx, len(vacol))
	}
	vacol[idx] = t
	return nil
}

func SetBytesAt(v *Vector, idx int, bs []byte, mp *mpool.MPool) error {
	var va types.Varlena
	err := BuildVarlenaFromByteSlice(v, &va, &bs, mp)
	if err != nil {
		return err
	}
	return SetFixedAtWithTypeCheck(v, idx, va)
}

func SetStringAt(v *Vector, idx int, bs string, mp *mpool.MPool) error {
	return SetBytesAt(v, idx, []byte(bs), mp)
}

func (v *Vector) SetRawBytesAt(i int, bs []byte, mp *mpool.MPool) error {
	if v.typ.IsVarlen() {
		return SetBytesAt(v, i, bs, mp)
	} else {
		copy(v.data[i*v.typ.TypeSize():i*v.typ.TypeSize()+v.typ.TypeSize()], bs)
		return nil
	}
}

// IsConstNull return true if the vector means a scalar Null.
// e.g.
//
//	a + Null, and the vector of right part will return true
func (v *Vector) IsConstNull() bool {
	if !v.IsConst() {
		return false
	}
	if len(v.data) == 0 {
		return true
	}

	return v.nsp.Count() > 0 && v.nsp.Contains(0)
}

func (v *Vector) GetArea() []byte {
	return v.area
}

func (v *Vector) GetData() []byte {
	return v.data
}

func GetPtrAt[T any](v *Vector, idx int64) *T {
	if v.IsConst() {
		idx = 0
	} else {
		idx *= int64(v.GetType().TypeSize())
	}
	return (*T)(unsafe.Pointer(&v.data[idx]))
}

func (v *Vector) Free(mp *mpool.MPool) {
	if v == nil {
		return
	}

	if !v.cantFreeData {
		mp.Free(v.data)
	}
	if !v.cantFreeArea {
		mp.Free(v.area)
	}
	v.class = FLAT
	v.data = nil
	v.area = nil
	v.length = 0
	v.cantFreeData = false
	v.cantFreeArea = false

	v.nsp.Reset()
	v.gsp.Reset()
	v.sorted = false
	v.isBin = false

	// if !v.OnUsed || v.OnPut {
	// 	panic("free vector which unalloc or in put list")
	// }
	// v.OnUsed = false
	// v.OnPut = false
	// if len(v.FreeMsg) > 20 {
	// 	v.FreeMsg = v.FreeMsg[1:]
	// }
	// v.FreeMsg = append(v.FreeMsg, time.Now().String()+" : typ="+v.typ.DescString()+" "+string(debug.Stack()))

	//reuse.Free[Vector](v, nil)
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := v.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (v *Vector) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {

	// write class
	buf.WriteByte(uint8(v.class))

	// write type
	data := types.EncodeType(&v.typ)
	buf.Write(data)

	// write length
	length := uint32(v.length)
	buf.Write(types.EncodeUint32(&length))

	// write dataLen, data
	dataLen := uint32(v.typ.TypeSize())
	if !v.IsConst() {
		dataLen *= uint32(v.length)
	} else if v.IsConstNull() {
		dataLen = 0
	}
	buf.Write(types.EncodeUint32(&dataLen))
	if dataLen > 0 {
		buf.Write(v.data[:dataLen])
	}

	// write areaLen, area
	areaLen := uint32(len(v.area))
	buf.Write(types.EncodeUint32(&areaLen))
	if areaLen > 0 {
		buf.Write(v.area)
	}

	// write nspLen, nsp
	nspData, err := v.nsp.Show()
	if err != nil {
		return err
	}
	nspLen := uint32(len(nspData))
	buf.Write(types.EncodeUint32(&nspLen))
	if nspLen > 0 {
		buf.Write(nspData)
	}

	buf.Write(types.EncodeBool(&v.sorted))

	return nil
}

func (v *Vector) UnmarshalBinary(data []byte) error {
	// read class
	v.class = int(data[0])
	data = data[1:]

	// read typ
	v.typ = types.DecodeType(data[:types.TSize])
	data = data[types.TSize:]

	// read length
	v.length = int(types.DecodeUint32(data[:4]))
	data = data[4:]

	// read data
	dataLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if dataLen > 0 {
		v.data = data[:dataLen]
		data = data[dataLen:]
	}

	// read area
	areaLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if areaLen > 0 {
		v.area = data[:areaLen]
		data = data[areaLen:]
	}

	// read nsp
	nspLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if nspLen > 0 {
		if err := v.nsp.ReadNoCopy(data[:nspLen]); err != nil {
			return err
		}
		data = data[nspLen:]
	} else {
		v.nsp.Reset()
	}

	v.sorted = types.DecodeBool(data[:1])
	//data = data[1:]

	v.cantFreeData = true
	v.cantFreeArea = true

	return nil
}

func (v *Vector) UnmarshalBinaryWithCopy(data []byte, mp *mpool.MPool) error {
	var err error

	// read class
	v.class = int(data[0])
	data = data[1:]

	// read typ
	v.typ = types.DecodeType(data[:types.TSize])
	data = data[types.TSize:]

	// read length
	v.length = int(types.DecodeUint32(data[:4]))
	data = data[4:]

	// read data
	dataLen := int(types.DecodeUint32(data[:4]))
	data = data[4:]
	if dataLen > 0 {
		v.data, err = mp.Alloc(dataLen, v.offHeap)
		if err != nil {
			return err
		}
		copy(v.data, data[:dataLen])
		data = data[dataLen:]
	}

	// read area
	areaLen := int(types.DecodeUint32(data[:4]))
	data = data[4:]
	if areaLen > 0 {
		v.area, err = mp.Alloc(areaLen, v.offHeap)
		if err != nil {
			return err
		}
		copy(v.area, data[:areaLen])
		data = data[areaLen:]
	}

	// read nsp
	nspLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if nspLen > 0 {
		if err := v.nsp.Read(data[:nspLen]); err != nil {
			return err
		}
		data = data[nspLen:]
	} else {
		v.nsp.Reset()
	}

	v.sorted = types.DecodeBool(data[:1])
	//data = data[1:]

	return nil
}

func (v *Vector) UnmarshalWithReader(r io.Reader, mp *mpool.MPool) error {
	var err error

	if v.class, err = types.ReadByteAsInt(r); err != nil {
		return err
	}

	if v.typ, err = types.ReadType(r); err != nil {
		return err
	}

	if v.length, err = types.ReadInt32AsInt(r); err != nil {
		return err
	}

	// read data
	dataLen, dataBuf, err := types.ReadSizeBytesMp(r, v.data, mp, v.offHeap)
	if err != nil {
		return err
	}
	if dataLen > 0 {
		v.data = dataBuf
	}

	// read area
	areaLen, areaBuf, err := types.ReadSizeBytesMp(r, v.area, mp, v.offHeap)
	if err != nil {
		return err
	}
	if areaLen > 0 {
		v.area = areaBuf
	}

	// read nsp, do not use mpool.  nspBuf is different because
	// it is not managed by vector.  In the following, it will
	// be unmarshalled into v.nsp
	nspLen, nspBuf, err := types.ReadSizeBytes(r)
	if err != nil {
		return err
	}
	if nspLen > 0 {
		v.nsp.Read(nspBuf)
	} else {
		v.nsp.Reset()
	}

	v.sorted, err = types.ReadBool(r)
	if err != nil {
		return err
	}

	return nil
}

func (v *Vector) ToConst() {
	v.class = CONSTANT
}

// PreExtend use to expand the capacity of the vector.
// PreExtend does not change the length of the vector.
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}

	return extend(v, rows, mp)
}

// PreExtendArea use to expand the mpool and area of vector
// extraAreaSize: the size of area to be extended
// mp: mpool
func (v *Vector) PreExtendWithArea(rows int, extraAreaSize int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}

	// pre-extend vector, the fixed len part
	if err := v.PreExtend(rows, mp); err != nil {
		return err
	}

	// check if required size is already satisfied
	area1 := v.GetArea()
	voff := len(area1)
	if voff+extraAreaSize <= cap(area1) {
		return nil
	}

	// grow area
	var err error
	oldSz := len(area1)
	area1, err = mp.Grow(area1, voff+extraAreaSize, v.offHeap)
	if err != nil {
		return err
	}
	area1 = area1[:oldSz] // This is important.

	// set area
	v.area = area1

	return nil
}

// Dup use to copy an identical vector
func (v *Vector) Dup(mp *mpool.MPool) (*Vector, error) {
	if v.IsConstNull() {
		return NewConstNull(v.typ, v.Length(), mp), nil
	}

	var err error

	w := NewVecFromReuse()
	w.class = v.class
	w.typ = v.typ
	w.length = v.length
	w.sorted = v.sorted
	w.GetNulls().InitWith(v.GetNulls())

	dataLen := v.typ.TypeSize()
	if v.IsConst() {
		if err := extend(w, 1, mp); err != nil {
			return nil, err
		}
	} else {
		if err := extend(w, v.length, mp); err != nil {
			return nil, err
		}
		dataLen *= v.length
	}
	copy(w.data, v.data[:dataLen])

	if len(v.area) > 0 {
		if w.area, err = mp.Alloc(len(v.area), v.offHeap); err != nil {
			return nil, err
		}
		copy(w.area, v.area)
	}
	return w, nil
}

// Shrink use to shrink vectors, sels must be guaranteed to be ordered
func (v *Vector) Shrink(sels []int64, negate bool) {

	shrinkSortedCheckIfRaceDetectorEnabled(sels)

	if v.IsConst() {
		if negate {
			v.length -= len(sels)
		} else {
			v.length = len(sels)
		}
		return
	}

	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixed[bool](v, sels, negate)
	case types.T_bit:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_int8:
		shrinkFixed[int8](v, sels, negate)
	case types.T_int16:
		shrinkFixed[int16](v, sels, negate)
	case types.T_int32:
		shrinkFixed[int32](v, sels, negate)
	case types.T_int64:
		shrinkFixed[int64](v, sels, negate)
	case types.T_uint8:
		shrinkFixed[uint8](v, sels, negate)
	case types.T_uint16:
		shrinkFixed[uint16](v, sels, negate)
	case types.T_uint32:
		shrinkFixed[uint32](v, sels, negate)
	case types.T_uint64:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_float32:
		shrinkFixed[float32](v, sels, negate)
	case types.T_float64:
		shrinkFixed[float64](v, sels, negate)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixed[types.Varlena](v, sels, negate)
	case types.T_date:
		shrinkFixed[types.Date](v, sels, negate)
	case types.T_datetime:
		shrinkFixed[types.Datetime](v, sels, negate)
	case types.T_time:
		shrinkFixed[types.Time](v, sels, negate)
	case types.T_timestamp:
		shrinkFixed[types.Timestamp](v, sels, negate)
	case types.T_year:
		shrinkFixed[types.MoYear](v, sels, negate)
	case types.T_enum:
		shrinkFixed[types.Enum](v, sels, negate)
	case types.T_decimal64:
		shrinkFixed[types.Decimal64](v, sels, negate)
	case types.T_decimal128:
		shrinkFixed[types.Decimal128](v, sels, negate)
	case types.T_uuid:
		shrinkFixed[types.Uuid](v, sels, negate)
	case types.T_TS:
		shrinkFixed[types.TS](v, sels, negate)
	case types.T_Rowid:
		shrinkFixed[types.Rowid](v, sels, negate)
	case types.T_Blockid:
		shrinkFixed[types.Blockid](v, sels, negate)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
}

func (v *Vector) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64) {
	if v.IsConst() {
		if negate {
			v.length -= sels.Count()
		} else {
			v.length = sels.Count()
		}
		return
	}

	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixedByMask[bool](v, sels, negate, offset)
	case types.T_bit:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_int8:
		shrinkFixedByMask[int8](v, sels, negate, offset)
	case types.T_int16:
		shrinkFixedByMask[int16](v, sels, negate, offset)
	case types.T_int32:
		shrinkFixedByMask[int32](v, sels, negate, offset)
	case types.T_int64:
		shrinkFixedByMask[int64](v, sels, negate, offset)
	case types.T_uint8:
		shrinkFixedByMask[uint8](v, sels, negate, offset)
	case types.T_uint16:
		shrinkFixedByMask[uint16](v, sels, negate, offset)
	case types.T_uint32:
		shrinkFixedByMask[uint32](v, sels, negate, offset)
	case types.T_uint64:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_float32:
		shrinkFixedByMask[float32](v, sels, negate, offset)
	case types.T_float64:
		shrinkFixedByMask[float64](v, sels, negate, offset)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixedByMask[types.Varlena](v, sels, negate, offset)
	case types.T_date:
		shrinkFixedByMask[types.Date](v, sels, negate, offset)
	case types.T_datetime:
		shrinkFixedByMask[types.Datetime](v, sels, negate, offset)
	case types.T_time:
		shrinkFixedByMask[types.Time](v, sels, negate, offset)
	case types.T_timestamp:
		shrinkFixedByMask[types.Timestamp](v, sels, negate, offset)
	case types.T_year:
		shrinkFixedByMask[types.MoYear](v, sels, negate, offset)
	case types.T_enum:
		shrinkFixedByMask[types.Enum](v, sels, negate, offset)
	case types.T_decimal64:
		shrinkFixedByMask[types.Decimal64](v, sels, negate, offset)
	case types.T_decimal128:
		shrinkFixedByMask[types.Decimal128](v, sels, negate, offset)
	case types.T_uuid:
		shrinkFixedByMask[types.Uuid](v, sels, negate, offset)
	case types.T_TS:
		shrinkFixedByMask[types.TS](v, sels, negate, offset)
	case types.T_Rowid:
		shrinkFixedByMask[types.Rowid](v, sels, negate, offset)
	case types.T_Blockid:
		shrinkFixedByMask[types.Blockid](v, sels, negate, offset)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
}

// Shuffle use to shrink vectors, sels can be disordered
func (v *Vector) Shuffle(sels []int64, mp *mpool.MPool) (err error) {
	if v.IsConst() {
		return nil
	}

	switch v.typ.Oid {
	case types.T_bool:
		err = shuffleFixedNoTypeCheck[bool](v, sels, mp)
	case types.T_bit:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_int8:
		err = shuffleFixedNoTypeCheck[int8](v, sels, mp)
	case types.T_int16:
		err = shuffleFixedNoTypeCheck[int16](v, sels, mp)
	case types.T_int32:
		err = shuffleFixedNoTypeCheck[int32](v, sels, mp)
	case types.T_int64:
		err = shuffleFixedNoTypeCheck[int64](v, sels, mp)
	case types.T_uint8:
		err = shuffleFixedNoTypeCheck[uint8](v, sels, mp)
	case types.T_uint16:
		err = shuffleFixedNoTypeCheck[uint16](v, sels, mp)
	case types.T_uint32:
		err = shuffleFixedNoTypeCheck[uint32](v, sels, mp)
	case types.T_uint64:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_float32:
		err = shuffleFixedNoTypeCheck[float32](v, sels, mp)
	case types.T_float64:
		err = shuffleFixedNoTypeCheck[float64](v, sels, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		err = shuffleFixedNoTypeCheck[types.Varlena](v, sels, mp)
	case types.T_date:
		err = shuffleFixedNoTypeCheck[types.Date](v, sels, mp)
	case types.T_datetime:
		err = shuffleFixedNoTypeCheck[types.Datetime](v, sels, mp)
	case types.T_time:
		err = shuffleFixedNoTypeCheck[types.Time](v, sels, mp)
	case types.T_timestamp:
		err = shuffleFixedNoTypeCheck[types.Timestamp](v, sels, mp)
	case types.T_year:
		err = shuffleFixedNoTypeCheck[types.MoYear](v, sels, mp)
	case types.T_enum:
		err = shuffleFixedNoTypeCheck[types.Enum](v, sels, mp)
	case types.T_decimal64:
		err = shuffleFixedNoTypeCheck[types.Decimal64](v, sels, mp)
	case types.T_decimal128:
		err = shuffleFixedNoTypeCheck[types.Decimal128](v, sels, mp)
	case types.T_uuid:
		err = shuffleFixedNoTypeCheck[types.Uuid](v, sels, mp)
	case types.T_TS:
		err = shuffleFixedNoTypeCheck[types.TS](v, sels, mp)
	case types.T_Rowid:
		err = shuffleFixedNoTypeCheck[types.Rowid](v, sels, mp)
	case types.T_Blockid:
		err = shuffleFixedNoTypeCheck[types.Blockid](v, sels, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}

	return err
}

// XXX Old Copy is FUBAR.
// Copy simply does v[vi] = w[wi]
func (v *Vector) Copy(w *Vector, vi, wi int64, mp *mpool.MPool) error {
	if w.class == CONSTANT {
		if w.IsConstNull() {
			v.nsp.Set(uint64(vi))
			return nil
		}
		wi = 0
	}
	if v.typ.IsFixedLen() {
		sz := v.typ.TypeSize()
		copy(v.data[vi*int64(sz):(vi+1)*int64(sz)], w.data[wi*int64(sz):(wi+1)*int64(sz)])
	} else {
		var err error
		vva := MustFixedColNoTypeCheck[types.Varlena](v)
		wva := MustFixedColNoTypeCheck[types.Varlena](w)
		if wva[wi].IsSmall() {
			vva[vi] = wva[wi]
		} else {
			bs := wva[wi].GetByteSlice(w.area)
			err = BuildVarlenaFromByteSlice(v, &vva[vi], &bs, mp)
			if err != nil {
				return err
			}
		}
	}

	if w.GetNulls().Contains(uint64(wi)) {
		v.GetNulls().Set(uint64(vi))
	} else {
		v.GetNulls().Unset(uint64(vi))
	}
	return nil
}

// GetUnionAllFunction: A more sensible function for copying vector,
// which avoids having to do type conversions and type judgements every time you append.
func GetUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	// a more simple and quickly union nsp but not good.
	unionNsp := func(dst *nulls.Nulls, more *nulls.Nulls, oldLength int, moreLength int) {
		u64offset := uint64(oldLength)
		u64Length := uint64(moreLength)

		moreNp := more.GetBitmap()
		if moreNp == nil || moreNp.EmptyByFlag() || moreLength == 0 {
			return
		}

		for i := u64Length - 1; i != 0; i-- {
			if moreNp.Contains(i) {
				dst.Set(i + u64offset)
			}
		}
		if moreNp.Contains(0) {
			dst.Set(u64offset)
		}
	}

	switch typ.Oid {
	case types.T_bool:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[bool](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_bit:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_int8:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int8](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_int16:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int16](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_int32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_int64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_uint8:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint8](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_uint16:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint16](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_uint32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_uint64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_float32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[float32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_float64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[float64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_date:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Date](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_year:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.MoYear](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_datetime:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Datetime](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_time:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Time](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_timestamp:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Timestamp](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_enum:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Enum](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_decimal64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Decimal64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_decimal128:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Decimal128](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_uuid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Uuid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_TS:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.TS](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_Rowid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Rowid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			ws := MustFixedColNoTypeCheck[types.Varlena](w)
			if w.IsConst() {
				if err := appendMultiBytes(v, ws[0].GetByteSlice(w.area), false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if sz := len(v.area) + len(w.area); sz > cap(v.area) {
				area, err := mp.Grow(v.area, sz, v.offHeap)
				if err != nil {
					return err
				}
				v.area = area[:len(v.area)]
			}

			var err error
			vs := toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+w.length)

			bm := w.nsp.GetBitmap()
			if bm != nil && !bm.EmptyByFlag() {
				for i := range ws {
					if w.gsp.Contains(uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					if bm.Contains(uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						err = BuildVarlenaFromVarlena(v, &vs[v.length], &ws[i], &w.area, mp)
						if err != nil {
							return err
						}
					}
					v.length++
				}
			} else {
				for i := range ws {
					err = BuildVarlenaFromVarlena(v, &vs[v.length], &ws[i], &w.area, mp)
					if err != nil {
						return err
					}
					v.length++
				}
			}
			return nil
		}
	case types.T_Blockid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Blockid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extend(v, w.length, mp); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.length += w.length
			return nil
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionFunction", typ))
	}
}

// GetConstSetFunction: A more sensible function for const vector set,
// which avoids having to do type conversions and type judgements every time you append.
func GetConstSetFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64, length int) error {
	switch typ.Oid {
	case types.T_bool:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[bool](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_bit:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int8:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int8](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint8:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint8](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return appendOneFixed(v, uint32(0), true, mp)
			}
			ws := MustFixedColNoTypeCheck[uint32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[float32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[float64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_date:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Date](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_year:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.MoYear](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_datetime:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Datetime](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_time:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Time](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_timestamp:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Timestamp](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_enum:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Enum](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_decimal64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Decimal64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_decimal128:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Decimal128](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uuid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Uuid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_TS:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.TS](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_Rowid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Rowid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text, types.T_array_float32, types.T_array_float64, types.T_datalink:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Varlena](w)
			v.area = v.area[:0]
			if w.IsConst() {
				return SetConstBytes(v, ws[0].GetByteSlice(w.area), length, mp)
			}
			return SetConstBytes(v, ws[sel].GetByteSlice(w.area), length, mp)
		}
	case types.T_Blockid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Blockid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetConstSetFunction", typ))
	}
}

func (v *Vector) UnionNull(mp *mpool.MPool) error {
	return appendOneFixed(v, 0, true, mp)
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionOne(w *Vector, sel int64, mp *mpool.MPool) error {
	if err := extend(v, 1, mp); err != nil {
		return err
	}

	oldLen := v.length
	v.length++
	if nulls.Contains(&w.gsp, uint64(sel)) {
		nulls.Add(&v.gsp, uint64(oldLen))
	}
	if w.IsConst() {
		if w.IsConstNull() {
			nulls.Add(&v.nsp, uint64(oldLen))
			return nil
		}
		sel = 0
	} else if nulls.Contains(&w.nsp, uint64(sel)) {
		nulls.Add(&v.nsp, uint64(oldLen))
		return nil
	}

	if v.GetType().IsVarlen() {
		var vs, ws []types.Varlena
		ToSliceNoTypeCheck(v, &vs)
		ToSliceNoTypeCheck(w, &ws)
		err := BuildVarlenaFromVarlena(v, &vs[oldLen], &ws[sel], &w.area, mp)
		if err != nil {
			return err
		}
	} else {
		tlen := v.GetType().TypeSize()
		switch tlen {
		case 8:
			p1 := unsafe.Pointer(&v.data[oldLen*8])
			p2 := unsafe.Pointer(&w.data[sel*8])
			*(*int64)(p1) = *(*int64)(p2)
		case 4:
			p1 := unsafe.Pointer(&v.data[oldLen*4])
			p2 := unsafe.Pointer(&w.data[sel*4])
			*(*int32)(p1) = *(*int32)(p2)
		case 2:
			p1 := unsafe.Pointer(&v.data[oldLen*2])
			p2 := unsafe.Pointer(&w.data[sel*2])
			*(*int16)(p1) = *(*int16)(p2)
		case 1:
			v.data[oldLen] = w.data[sel]
		default:
			copy(v.data[oldLen*tlen:(oldLen+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
		}
	}

	return nil
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionMulti(w *Vector, sel int64, cnt int, mp *mpool.MPool) error {
	if cnt == 0 {
		return nil
	}

	if err := extend(v, cnt, mp); err != nil {
		return err
	}

	oldLen := v.length
	v.length += cnt
	if nulls.Contains(&w.gsp, uint64(sel)) {
		nulls.AddRange(&v.gsp, uint64(oldLen), uint64(oldLen+cnt))
	}
	if w.IsConst() {
		if w.IsConstNull() {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+cnt))
			return nil
		}
		sel = 0
	} else if nulls.Contains(&w.nsp, uint64(sel)) {
		nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+cnt))
		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var va types.Varlena
		var ws []types.Varlena
		ToSliceNoTypeCheck(w, &ws)
		err = BuildVarlenaFromVarlena(v, &va, &ws[sel], &w.area, mp)
		if err != nil {
			return err
		}
		var col []types.Varlena
		ToSliceNoTypeCheck(v, &col)
		for i := oldLen; i < v.length; i++ {
			col[i] = va
		}
	} else {
		tlen := v.GetType().TypeSize()
		for i := oldLen; i < v.length; i++ {
			switch tlen {
			case 8:
				p1 := unsafe.Pointer(&v.data[i*8])
				p2 := unsafe.Pointer(&w.data[sel*8])
				*(*int64)(p1) = *(*int64)(p2)
			case 4:
				p1 := unsafe.Pointer(&v.data[i*4])
				p2 := unsafe.Pointer(&w.data[sel*4])
				*(*int32)(p1) = *(*int32)(p2)
			case 2:
				p1 := unsafe.Pointer(&v.data[i*2])
				p2 := unsafe.Pointer(&w.data[sel*2])
				*(*int16)(p1) = *(*int16)(p2)
			case 1:
				v.data[i] = w.data[sel]
			default:
				copy(v.data[i*tlen:(i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
			}
		}
	}

	return nil
}

func (v *Vector) Union(w *Vector, sels []int64, mp *mpool.MPool) error {
	return unionT[int64](v, w, sels, mp)
}
func (v *Vector) UnionInt32(w *Vector, sels []int32, mp *mpool.MPool) error {
	return unionT[int32](v, w, sels, mp)
}

func unionT[T int32 | int64](v, w *Vector, sels []T, mp *mpool.MPool) error {
	if len(sels) == 0 {
		return nil
	}

	if err := extend(v, len(sels), mp); err != nil {
		return err
	}

	oldLen := v.length
	v.length += len(sels)
	if w.IsConst() {
		if w.IsGrouping() {
			nulls.AddRange(&v.gsp, uint64(oldLen), uint64(oldLen+len(sels)))
		}
		if w.IsConstNull() {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+len(sels)))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			var ws []types.Varlena
			ToSliceNoTypeCheck(w, &ws)
			err = BuildVarlenaFromVarlena(v, &va, &ws[0], &w.area, mp)
			if err != nil {
				return err
			}
			var col []types.Varlena
			ToSliceNoTypeCheck(v, &col)
			for i := oldLen; i < v.length; i++ {
				col[i] = va
			}
		} else {
			tlen := v.GetType().TypeSize()
			for i := oldLen; i < v.length; i++ {
				copy(v.data[i*tlen:(i+1)*tlen], w.data[:tlen])
			}
		}

		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena
		ToSliceNoTypeCheck(v, &vCol)
		ToSliceNoTypeCheck(w, &wCol)
		if !w.GetNulls().EmptyByFlag() {
			for i, sel := range sels {
				if w.gsp.Contains(uint64(sel)) {
					nulls.Add(&v.gsp, uint64(oldLen+i))
				}
				if w.nsp.Contains(uint64(sel)) {
					nulls.Add(&v.nsp, uint64(oldLen+i))
					continue
				}
				err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp)
				if err != nil {
					return err
				}
			}
		} else {
			for i, sel := range sels {

				err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp)
				if err != nil {
					return err
				}
			}
		}
	} else {
		tlen := v.GetType().TypeSize()
		if !w.nsp.EmptyByFlag() {
			for i, sel := range sels {
				if w.gsp.Contains(uint64(sel)) {
					nulls.Add(&v.gsp, uint64(oldLen+i))
				}
				if w.nsp.Contains(uint64(sel)) {
					nulls.Add(&v.nsp, uint64(oldLen+i))
					continue
				}
				copy(v.data[(oldLen+i)*tlen:(oldLen+i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
			}
		} else {
			switch tlen {
			case 8:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*8])
					p2 := unsafe.Pointer(&w.data[int(sel)*8])
					*(*int64)(p1) = *(*int64)(p2)
				}
			case 4:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*4])
					p2 := unsafe.Pointer(&w.data[int(sel)*4])
					*(*int32)(p1) = *(*int32)(p2)
				}
			case 2:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*2])
					p2 := unsafe.Pointer(&w.data[int(sel)*2])
					*(*int16)(p1) = *(*int16)(p2)
				}
			case 1:
				for i, sel := range sels {
					v.data[(oldLen + i)] = w.data[int(sel)]
				}
			default:
				for i, sel := range sels {
					copy(v.data[(oldLen+i)*tlen:(oldLen+i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
				}
			}
		}
	}

	return nil
}

func (v *Vector) UnionBatch(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	addCnt := 0
	if flags == nil {
		addCnt = cnt
	} else {
		for i := range flags {
			addCnt += int(flags[i])
		}
	}

	if addCnt == 0 {
		return nil
	}

	if err := extend(v, addCnt, mp); err != nil {
		return err
	}

	if w.IsConst() {
		oldLen := v.length
		v.length += addCnt
		if w.IsGrouping() {
			nulls.AddRange(&v.gsp, uint64(oldLen), uint64(v.length))
		}
		if w.IsConstNull() {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(v.length))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			var ws []types.Varlena
			ToSliceNoTypeCheck(w, &ws)
			err = BuildVarlenaFromVarlena(v, &va, &ws[0], &w.area, mp)
			if err != nil {
				return err
			}
			var col []types.Varlena
			ToSliceNoTypeCheck(v, &col)
			for i := oldLen; i < v.length; i++ {
				col[i] = va
			}
		} else {
			tlen := v.GetType().TypeSize()
			for i := oldLen; i < v.length; i++ {
				copy(v.data[i*tlen:(i+1)*tlen], w.data[:tlen])
			}
		}

		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena

		vCol = toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+addCnt)
		ToSliceNoTypeCheck(w, &wCol)

		if !w.nsp.EmptyByFlag() {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
						if err != nil {
							return err
						}
					}
					v.length++
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
						if err != nil {
							return err
						}
					}
					v.length++
				}
			}
		} else {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
					if err != nil {
						return err
					}
					v.length++
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
					if err != nil {
						return err
					}
					v.length++
				}
			}
		}
	} else {
		tlen := v.GetType().TypeSize()
		if !w.nsp.EmptyByFlag() {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					}
					v.length++
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					}
					v.length++
				}
			}
		} else {
			if flags == nil {
				if w.nsp.Any() {
					for i := 0; i < cnt; i++ {
						if w.nsp.Contains(uint64(offset) + uint64(i)) {
							nulls.Add(&v.nsp, uint64(v.length))
						}
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
						v.length++
					}
				} else {
					copy(v.data[v.length*tlen:(v.length+cnt)*tlen], w.data[(int(offset))*tlen:(int(offset)+cnt)*tlen])
					v.length += cnt
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.gsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.gsp, uint64(v.length))
					}
					copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					v.length++
				}
			}
		}
	}

	return nil
}

// String function is used to visually display the vector,
// which is used to implement the Printf interface
func (v *Vector) String() string {
	switch v.typ.Oid {
	case types.T_bool:
		return vecToString[bool](v)
	case types.T_bit:
		return vecToString[uint64](v)
	case types.T_int8:
		return vecToString[int8](v)
	case types.T_int16:
		return vecToString[int16](v)
	case types.T_int32:
		return vecToString[int32](v)
	case types.T_int64:
		return vecToString[int64](v)
	case types.T_uint8:
		return vecToString[uint8](v)
	case types.T_uint16:
		return vecToString[uint16](v)
	case types.T_uint32:
		return vecToString[uint32](v)
	case types.T_uint64:
		return vecToString[uint64](v)
	case types.T_float32:
		return vecToString[float32](v)
	case types.T_float64:
		return vecToString[float64](v)
	case types.T_date:
		return vecToString[types.Date](v)
	case types.T_datetime:
		return vecToString[types.Datetime](v)
	case types.T_time:
		return vecToString[types.Time](v)
	case types.T_timestamp:
		return vecToString[types.Timestamp](v)
	case types.T_enum:
		return vecToString[types.Enum](v)
	case types.T_year:
		return vecToString[types.MoYear](v)
	case types.T_decimal64:
		return vecToString[types.Decimal64](v)
	case types.T_decimal128:
		return vecToString[types.Decimal128](v)
	case types.T_uuid:
		return vecToString[types.Uuid](v)
	case types.T_TS:
		return vecToString[types.TS](v)
	case types.T_Rowid:
		return vecToString[types.Rowid](v)
	case types.T_Blockid:
		return vecToString[types.Blockid](v)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text, types.T_datalink:
		col := InefficientMustStrCol(v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			} else {
				return col[0]
			}
		}
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
		} else {
			return fmt.Sprintf("%v", col)
		}
		//return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	case types.T_array_float32:
		//NOTE: Don't merge this with T_Varchar. We need to retrieve the Array and print the values.
		col := MustArrayCol[float32](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			} else {
				return types.ArrayToString[float32](col[0])
			}
		}

		str := types.ArraysToString[float32](col, types.DefaultArraysToStringSep)
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
		}
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_float64:
		//NOTE: Don't merge this with T_Varchar. We need to retrieve the Array and print the values.
		col := MustArrayCol[float64](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			} else {
				return types.ArrayToString[float64](col[0])
			}
		}
		str := types.ArraysToString[float64](col, types.DefaultArraysToStringSep)
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
		}
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	default:
		panic("vec to string unknown types.")
	}
}

func implFixedRowToString[T types.FixedSizeT](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", GetFixedAtNoTypeCheck[T](v, 0))
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return fmt.Sprintf("%v", GetFixedAtNoTypeCheck[T](v, idx))
	}
}

func implTimestampRowToString(v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	loc := time.Local
	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return GetFixedAtNoTypeCheck[types.Timestamp](v, 0).String2(loc, v.typ.Scale)
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return GetFixedAtNoTypeCheck[types.Timestamp](v, idx).String2(loc, v.typ.Scale)
	}
}

func implDatetimeRowToString(v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	var dt types.Datetime
	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			dt = GetFixedAtNoTypeCheck[types.Datetime](v, 0)
		}
	} else {
		if v.nsp.Contains(uint64(idx)) {
			return "null"
		} else {
			dt = GetFixedAtNoTypeCheck[types.Datetime](v, idx)
		}
	}

	return dt.String2(v.typ.Scale)
}

func implDecimalRowToString[T types.DecimalWithFormat](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return GetFixedAtNoTypeCheck[T](v, 0).Format(v.typ.Scale)
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return GetFixedAtNoTypeCheck[T](v, idx).Format(v.typ.Scale)
	}
}

func implArrayRowToString[T types.RealNumbers](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return types.ArrayToString(GetArrayAt[T](v, 0))
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return types.ArrayToString(GetArrayAt[T](v, idx))
	}
}

func (v *Vector) RowToString(idx int) string {
	switch v.typ.Oid {
	case types.T_bool:
		return implFixedRowToString[bool](v, idx)
	case types.T_bit:
		return implFixedRowToString[uint64](v, idx)
	case types.T_int8:
		return implFixedRowToString[int8](v, idx)
	case types.T_int16:
		return implFixedRowToString[int16](v, idx)
	case types.T_int32:
		return implFixedRowToString[int32](v, idx)
	case types.T_int64:
		return implFixedRowToString[int64](v, idx)
	case types.T_uint8:
		return implFixedRowToString[uint8](v, idx)
	case types.T_uint16:
		return implFixedRowToString[uint16](v, idx)
	case types.T_uint32:
		return implFixedRowToString[uint32](v, idx)
	case types.T_uint64:
		return implFixedRowToString[uint64](v, idx)
	case types.T_float32:
		return implFixedRowToString[float32](v, idx)
	case types.T_float64:
		return implFixedRowToString[float64](v, idx)
	case types.T_date:
		return implFixedRowToString[types.Date](v, idx)
	case types.T_year:
		return implFixedRowToString[types.MoYear](v, idx)
	case types.T_datetime:
		return implDatetimeRowToString(v, idx)
	case types.T_time:
		return implFixedRowToString[types.Time](v, idx)
	case types.T_timestamp:
		return implTimestampRowToString(v, idx)
	case types.T_enum:
		return implFixedRowToString[types.Enum](v, idx)
	case types.T_decimal64:
		return implDecimalRowToString[types.Decimal64](v, idx)
	case types.T_decimal128:
		return implDecimalRowToString[types.Decimal128](v, idx)
	case types.T_uuid:
		return implFixedRowToString[types.Uuid](v, idx)
	case types.T_TS:
		return implFixedRowToString[types.TS](v, idx)
	case types.T_Rowid:
		return implFixedRowToString[types.Rowid](v, idx)
	case types.T_Blockid:
		return implFixedRowToString[types.Blockid](v, idx)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text, types.T_datalink:
		col := MustFixedColNoTypeCheck[types.Varlena](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			} else {
				return col[0].UnsafeGetString(v.area)
			}
		}
		if v.nsp.Contains(uint64(idx)) {
			return "null"
		} else {
			return col[idx].UnsafeGetString(v.area)
		}
		//return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	case types.T_array_float32:
		return implArrayRowToString[float32](v, idx)
	case types.T_array_float64:
		return implArrayRowToString[float64](v, idx)
	default:
		panic("vec to string unknown types.")
	}
}

func SetConstNull(vec *Vector, length int, mp *mpool.MPool) error {
	if len(vec.data) > 0 {
		vec.data = vec.data[:0]
	}
	vec.class = CONSTANT
	vec.length = length
	return nil
}

func SetConstFixed[T any](vec *Vector, val T, length int, mp *mpool.MPool) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	vec.length = length

	col := toSliceOfLengthNoTypeCheck[T](vec, 1)
	col[0] = val
	return nil
}

func SetConstBytes(vec *Vector, val []byte, length int, mp *mpool.MPool) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	if err := BuildVarlenaFromByteSlice(vec, &col[0], &val, mp); err != nil {
		return err
	}
	vec.length = length
	return nil
}

func SetConstByteJson(vec *Vector, bj bytejson.ByteJson, length int, mp *mpool.MPool) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	if err := BuildVarlenaFromByteJson(vec, &col[0], bj, mp); err != nil {
		return err
	}
	vec.length = length
	return nil
}

// SetConstArray set current vector as Constant_Array vector of given length.
func SetConstArray[T types.RealNumbers](vec *Vector, val []T, length int, mp *mpool.MPool) error {
	var err error

	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	err = BuildVarlenaFromArray(vec, &col[0], &val, mp)
	if err != nil {
		return err
	}
	vec.length = length
	return nil
}

// WARNING: AppendAny() append value with any type will cause memory escape to heap which will result in slow GC.
// If you know the actual type, better use the AppendFixed() to append the values.
// Only use when you have no choice, e.g. you are dealing with column with any type that don't know in advanced.
func AppendAny(vec *Vector, val any, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}

	if isNull {
		return appendOneFixed(vec, 0, true, mp)
	}

	switch vec.typ.Oid {
	case types.T_bool:
		return appendOneFixed(vec, val.(bool), false, mp)
	case types.T_bit:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_int8:
		return appendOneFixed(vec, val.(int8), false, mp)
	case types.T_int16:
		return appendOneFixed(vec, val.(int16), false, mp)
	case types.T_int32:
		return appendOneFixed(vec, val.(int32), false, mp)
	case types.T_int64:
		return appendOneFixed(vec, val.(int64), false, mp)
	case types.T_uint8:
		return appendOneFixed(vec, val.(uint8), false, mp)
	case types.T_uint16:
		return appendOneFixed(vec, val.(uint16), false, mp)
	case types.T_uint32:
		return appendOneFixed(vec, val.(uint32), false, mp)
	case types.T_uint64:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_float32:
		return appendOneFixed(vec, val.(float32), false, mp)
	case types.T_float64:
		return appendOneFixed(vec, val.(float64), false, mp)
	case types.T_date:
		return appendOneFixed(vec, val.(types.Date), false, mp)
	case types.T_year:
		return appendOneFixed(vec, val.(types.MoYear), false, mp)
	case types.T_datetime:
		return appendOneFixed(vec, val.(types.Datetime), false, mp)
	case types.T_time:
		return appendOneFixed(vec, val.(types.Time), false, mp)
	case types.T_timestamp:
		return appendOneFixed(vec, val.(types.Timestamp), false, mp)
	case types.T_enum:
		return appendOneFixed(vec, val.(types.Enum), false, mp)
	case types.T_decimal64:
		return appendOneFixed(vec, val.(types.Decimal64), false, mp)
	case types.T_decimal128:
		return appendOneFixed(vec, val.(types.Decimal128), false, mp)
	case types.T_uuid:
		return appendOneFixed(vec, val.(types.Uuid), false, mp)
	case types.T_TS:
		return appendOneFixed(vec, val.(types.TS), false, mp)
	case types.T_Rowid:
		return appendOneFixed(vec, val.(types.Rowid), false, mp)
	case types.T_Blockid:
		return appendOneFixed(vec, val.(types.Blockid), false, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return appendOneBytes(vec, val.([]byte), false, mp)
	}
	return nil
}

func AppendNull(vec *Vector, mp *mpool.MPool) error {
	return appendOneFixed(vec, 0, true, mp)
}

func AppendFixed[T any](vec *Vector, val T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneFixed(vec, val, isNull, mp)
}

func AppendBytes(vec *Vector, val []byte, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneBytes(vec, val, isNull, mp)
}

func AppendByteJson(vec *Vector, bj bytejson.ByteJson, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneByteJson(vec, bj, isNull, mp)
}

// AppendArray mainly used in tests
func AppendArray[T types.RealNumbers](vec *Vector, val []T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneArray[T](vec, val, isNull, mp)
}

func AppendMultiFixed[T any](vec *Vector, vals T, isNull bool, cnt int, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMultiFixed(vec, vals, isNull, cnt, mp)
}

func AppendMultiBytes(vec *Vector, vals []byte, isNull bool, cnt int, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMultiBytes(vec, vals, isNull, cnt, mp)
}

func AppendFixedList[T any](vec *Vector, ws []T, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendList(vec, ws, isNulls, mp)
}

func AppendBytesList(vec *Vector, ws [][]byte, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendBytesList(vec, ws, isNulls, mp)
}

func AppendStringList(vec *Vector, ws []string, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendStringList(vec, ws, isNulls, mp)
}

// AppendArrayList mainly used in unit tests
func AppendArrayList[T types.RealNumbers](vec *Vector, ws [][]T, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendArrayList(vec, ws, isNulls, mp)
}

func appendOneFixed[T any](vec *Vector, val T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}

	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	length := vec.length
	vec.length++
	if isNull {
		nulls.Add(&vec.nsp, uint64(length))
	} else {
		var col []T
		ToSliceNoTypeCheck(vec, &col)
		col[length] = val
	}
	return nil
}

func appendOneBytes(vec *Vector, val []byte, isNull bool, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if isNull {
		return appendOneFixed(vec, va, true, mp)
	} else {
		err = BuildVarlenaFromByteSlice(vec, &va, &val, mp)
		if err != nil {
			return err
		}
		return appendOneFixed(vec, va, false, mp)
	}
}

func appendOneByteJson(vec *Vector, bj bytejson.ByteJson, isNull bool, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if isNull {
		return appendOneFixed(vec, va, true, mp)
	} else {
		err = BuildVarlenaFromByteJson(vec, &va, bj, mp)
		if err != nil {
			return err
		}
		return appendOneFixed(vec, va, false, mp)
	}
}

// appendOneArray mainly used for unit tests
func appendOneArray[T types.RealNumbers](vec *Vector, val []T, isNull bool, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if isNull {
		return appendOneFixed(vec, va, true, mp)
	} else {
		err = BuildVarlenaFromArray[T](vec, &va, &val, mp)
		if err != nil {
			return err
		}
		return appendOneFixed(vec, va, false, mp)
	}
}

func appendMultiFixed[T any](vec *Vector, val T, isNull bool, cnt int, mp *mpool.MPool) error {
	if err := extend(vec, cnt, mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += cnt
	if isNull {
		nulls.AddRange(&vec.nsp, uint64(length), uint64(length+cnt))
	} else if cnt > 0 {
		// XXX check cnt > 0 to avoid issue #23295
		var col []T
		ToSlice(vec, &col)
		for i := 0; i < cnt; i++ {
			col[length+i] = val
		}
	}
	return nil
}

func appendMultiBytes(vec *Vector, val []byte, isNull bool, cnt int, mp *mpool.MPool) error {
	var err error
	var va types.Varlena
	if err = extend(vec, cnt, mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += cnt
	if isNull {
		nulls.AddRange(&vec.nsp, uint64(length), uint64(length+cnt))
	} else {
		var col []types.Varlena
		ToSliceNoTypeCheck(vec, &col)
		err = BuildVarlenaFromByteSlice(vec, &va, &val, mp)
		if err != nil {
			return err
		}
		for i := 0; i < cnt; i++ {
			col[length+i] = va
		}
	}
	return nil
}

func appendList[T any](vec *Vector, vals []T, isNulls []bool, mp *mpool.MPool) error {
	if err := extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedColWithTypeCheck[T](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(vec *Vector, vals [][]byte, isNulls []bool, mp *mpool.MPool) error {
	var err error
	if err = extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &w, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func appendStringList(vec *Vector, vals []string, isNulls []bool, mp *mpool.MPool) error {
	var err error

	if err = extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			bs := []byte(w)
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// appendArrayList mainly used for unit tests
func appendArrayList[T types.RealNumbers](vec *Vector, vals [][]T, isNulls []bool, mp *mpool.MPool) error {
	var err error

	if err = extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			bs := w
			err = BuildVarlenaFromArray[T](vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func shrinkFixed[T types.FixedSizeT](v *Vector, sels []int64, negate bool) {
	vs := MustFixedColNoTypeCheck[T](v)
	if !negate {
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		nulls.Filter(&v.gsp, sels, false)
		nulls.Filter(&v.nsp, sels, false)
		v.length = len(sels)
	} else if len(sels) > 0 {
		for oldIdx, newIdx, selIdx, sel := 0, 0, 0, sels[0]; oldIdx < v.length; oldIdx++ {
			if oldIdx != int(sel) {
				vs[newIdx] = vs[oldIdx]
				newIdx++
			} else {
				selIdx++
				if selIdx >= len(sels) {
					for idx := oldIdx + 1; idx < v.length; idx++ {
						vs[newIdx] = vs[idx]
						newIdx++
					}
					break
				}
				sel = sels[selIdx]
			}
		}
		nulls.Filter(&v.gsp, sels, true)
		nulls.Filter(&v.nsp, sels, true)
		v.length -= len(sels)
	}
}

func shrinkFixedByMask[T types.FixedSizeT](v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64) {
	vs := MustFixedColNoTypeCheck[T](v)
	length := sels.Count()
	itr := sels.Iterator()
	if !negate {
		idx := 0
		for itr.HasNext() {
			vs[idx] = vs[itr.Next()+offset]
			idx++
		}
		nulls.FilterByMask(&v.gsp, sels, false)
		nulls.FilterByMask(&v.nsp, sels, false)
		v.length = length
	} else if length > 0 {
		sel := itr.Next() + offset
		for oldIdx, newIdx := 0, 0; oldIdx < v.length; oldIdx++ {
			if oldIdx != int(sel) {
				vs[newIdx] = vs[oldIdx]
				newIdx++
			} else {
				if !itr.HasNext() {
					for idx := oldIdx + 1; idx < v.length; idx++ {
						vs[newIdx] = vs[idx]
						newIdx++
					}
					break
				}
				sel = itr.Next() + offset
			}
		}
		nulls.FilterByMask(&v.gsp, sels, true)
		nulls.FilterByMask(&v.nsp, sels, true)
		v.length -= length
	}
}

// shuffleFixedNoTypeCheck is always used after type check. and we can use ToSliceNoTypeCheck here.
func shuffleFixedNoTypeCheck[T types.FixedSizeT](v *Vector, sels []int64, mp *mpool.MPool) error {
	sz := v.typ.TypeSize()
	olddata := v.data[:v.length*sz]
	ns := len(sels)
	var vs []T
	ToFixedColNoTypeCheck(v, &vs)
	data, err := mp.Alloc(ns*v.GetType().TypeSize(), v.offHeap)
	if err != nil {
		return err
	}
	v.data = data
	ws := toSliceOfLengthNoTypeCheck[T](v, ns)

	shuffle.FixedLengthShuffle(vs, ws, sels)
	nulls.Filter(&v.gsp, sels, false)
	nulls.Filter(&v.nsp, sels, false)
	// XXX We should never allow "half-owned" vectors later. And unowned vector should be strictly read-only.
	if v.cantFreeData {
		v.cantFreeData = false
	} else {
		mp.Free(olddata)
	}
	v.length = ns
	return nil
}

func vecToString[T types.FixedSizeT](v *Vector) string {
	col := MustFixedColWithTypeCheck[T](v)
	if len(col) == 1 {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	if v.nsp.Any() {
		return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	} else {
		return fmt.Sprintf("%v", col)
	}
}

// Window returns a "window" into the Vec.
// It selects a half-open range (i.e.[start, end)).
// The returned object is NOT allowed to be modified (
// TODO: Nulls are deep copied.
func (v *Vector) Window(start, end int) (*Vector, error) {
	if v.IsConstNull() {
		return NewConstNull(v.typ, end-start, nil), nil
	} else if v.IsConst() {
		vec := NewVec(v.typ)
		vec.class = v.class
		vec.data = v.data
		vec.area = v.area
		vec.length = end - start
		vec.cantFreeArea = true
		vec.cantFreeData = true
		vec.sorted = v.sorted
		return vec, nil
	}
	w := NewVec(v.typ)
	if start == end {
		return w, nil
	}
	nulls.Range(&v.nsp, uint64(start), uint64(end), uint64(start), &w.nsp)
	w.data = v.data[start*v.typ.TypeSize() : end*v.typ.TypeSize()]
	w.length = end - start
	if v.typ.IsVarlen() {
		w.area = v.area
	}
	w.cantFreeData = true
	w.cantFreeArea = true
	return w, nil
}

// CloneWindow Deep copies the content from start to end into another vector. Afterwise it's safe to destroy the original one.
func (v *Vector) CloneWindow(start, end int, mp *mpool.MPool) (*Vector, error) {
	if start == end {
		return NewVec(v.typ), nil
	}
	if end > v.Length() {
		panic(fmt.Sprintf("CloneWindow end %d >= length %d", end, v.Length()))
	}
	if v.IsConstNull() {
		return NewConstNull(v.typ, end-start, mp), nil
	} else if v.IsConst() {
		if v.typ.IsVarlen() {
			return NewConstBytes(v.typ, v.GetBytesAt(0), end-start, mp)
		} else {
			vec := NewVec(v.typ)
			vec.class = v.class
			vec.data = make([]byte, len(v.data))
			copy(vec.data, v.data)
			vec.length = end - start
			vec.cantFreeArea = true
			vec.cantFreeData = true
			vec.sorted = v.sorted
			return vec, nil
		}
	}
	w := NewVec(v.typ)
	if err := v.CloneWindowTo(w, start, end, mp); err != nil {
		return nil, err
	}
	return w, nil
}

func (v *Vector) CloneWindowTo(w *Vector, start, end int, mp *mpool.MPool) error {
	if start == end {
		return nil
	}
	if v.IsConstNull() {
		w.class = CONSTANT
		w.length = end - start
		w.data = nil
		return nil
	} else if v.IsConst() {
		if v.typ.IsVarlen() {
			w.class = CONSTANT
			SetConstBytes(v, v.GetBytesAt(0), end-start, mp)
			return nil
		} else {
			w.class = v.class
			w.data = make([]byte, len(v.data))
			copy(w.data, v.data)
			w.length = end - start
			w.cantFreeArea = true
			w.cantFreeData = true
			w.sorted = v.sorted
			return nil
		}
	}
	nulls.Range(&v.nsp, uint64(start), uint64(end), uint64(start), &w.nsp)
	length := (end - start) * v.typ.TypeSize()
	if mp == nil {
		w.data = make([]byte, length)
		copy(w.data, v.data[start*v.typ.TypeSize():end*v.typ.TypeSize()])
		w.length = end - start
		if v.typ.IsVarlen() {
			w.area = make([]byte, len(v.area))
			copy(w.area, v.area)
		}
		w.cantFreeData = true
		w.cantFreeArea = true
	} else {
		err := w.PreExtend(end-start, mp)
		if err != nil {
			return err
		}
		w.length = end - start
		if v.GetType().IsVarlen() {
			var vCol, wCol []types.Varlena
			ToSliceNoTypeCheck(v, &vCol)
			ToSliceNoTypeCheck(w, &wCol)
			for i := start; i < end; i++ {
				if !nulls.Contains(&v.nsp, uint64(i)) {
					bs := vCol[i].GetByteSlice(v.area)
					err = BuildVarlenaFromByteSlice(w, &wCol[i-start], &bs, mp)
					if err != nil {
						return err
					}
				}
			}
		} else {
			tlen := v.typ.TypeSize()
			copy(w.data[:length], v.data[start*tlen:end*tlen])
		}
	}

	return nil
}

// GetSumValue returns the sum value of the vector.
// if the length is 0 or all null or the vector is not numeric, return false
func (v *Vector) GetSumValue() (ok bool, sumv []byte) {
	if v.Length() == 0 || v.AllNull() || !v.typ.IsNumeric() {
		return
	}
	if v.typ.IsDecimal() && v.typ.Oid != types.T_decimal64 {
		return
	}
	ok = true
	switch v.typ.Oid {
	case types.T_bit:
		sumVal := IntegerGetSum[uint64, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_int8:
		sumVal := IntegerGetSum[int8, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int16:
		sumVal := IntegerGetSum[int16, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int32:
		sumVal := IntegerGetSum[int32, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int64:
		sumVal := IntegerGetSum[int64, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_uint8:
		sumVal := IntegerGetSum[uint8, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint16:
		sumVal := IntegerGetSum[uint16, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint32:
		sumVal := IntegerGetSum[uint32, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint64:
		sumVal := IntegerGetSum[uint64, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_float32:
		sumVal := FloatGetSum[float32](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_float64:
		sumVal := FloatGetSum[float64](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_decimal64:
		sumVal := Decimal64GetSum(v)
		sumv = types.EncodeDecimal64(&sumVal)
	default:
		panic(fmt.Sprintf("unsupported type %s", v.GetType().String()))
	}
	return
}

// GetMinMaxValue returns the min and max value of the vector.
// if the length is 0 or all null, return false
func (v *Vector) GetMinMaxValue() (ok bool, minv, maxv []byte) {
	if v.Length() == 0 || v.AllNull() {
		return
	}
	ok = true
	switch v.typ.Oid {
	case types.T_bool:
		var minVal, maxVal bool
		col := MustFixedColNoTypeCheck[bool](v)
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					minVal = minVal && col[i]
					maxVal = maxVal && col[i]
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				minVal = minVal && col[i]
				maxVal = maxVal && col[i]
			}
		}
		minv = types.EncodeBool(&minVal)
		maxv = types.EncodeBool(&maxVal)

	case types.T_bit:
		minVal, maxVal := OrderedGetMinAndMax[uint64](v)
		minv = types.EncodeUint64(&minVal)
		maxv = types.EncodeUint64(&maxVal)

	case types.T_int8:
		minVal, maxVal := OrderedGetMinAndMax[int8](v)
		minv = types.EncodeInt8(&minVal)
		maxv = types.EncodeInt8(&maxVal)

	case types.T_int16:
		minVal, maxVal := OrderedGetMinAndMax[int16](v)
		minv = types.EncodeInt16(&minVal)
		maxv = types.EncodeInt16(&maxVal)

	case types.T_int32:
		minVal, maxVal := OrderedGetMinAndMax[int32](v)
		minv = types.EncodeInt32(&minVal)
		maxv = types.EncodeInt32(&maxVal)

	case types.T_int64:
		minVal, maxVal := OrderedGetMinAndMax[int64](v)
		minv = types.EncodeInt64(&minVal)
		maxv = types.EncodeInt64(&maxVal)

	case types.T_uint8:
		minVal, maxVal := OrderedGetMinAndMax[uint8](v)
		minv = types.EncodeUint8(&minVal)
		maxv = types.EncodeUint8(&maxVal)

	case types.T_uint16:
		minVal, maxVal := OrderedGetMinAndMax[uint16](v)
		minv = types.EncodeUint16(&minVal)
		maxv = types.EncodeUint16(&maxVal)

	case types.T_uint32:
		minVal, maxVal := OrderedGetMinAndMax[uint32](v)
		minv = types.EncodeUint32(&minVal)
		maxv = types.EncodeUint32(&maxVal)

	case types.T_uint64:
		minVal, maxVal := OrderedGetMinAndMax[uint64](v)
		minv = types.EncodeUint64(&minVal)
		maxv = types.EncodeUint64(&maxVal)

	case types.T_float32:
		minVal, maxVal := OrderedGetMinAndMax[float32](v)
		minv = types.EncodeFloat32(&minVal)
		maxv = types.EncodeFloat32(&maxVal)

	case types.T_float64:
		minVal, maxVal := OrderedGetMinAndMax[float64](v)
		minv = types.EncodeFloat64(&minVal)
		maxv = types.EncodeFloat64(&maxVal)

	case types.T_date:
		minVal, maxVal := OrderedGetMinAndMax[types.Date](v)
		minv = types.EncodeDate(&minVal)
		maxv = types.EncodeDate(&maxVal)

	case types.T_year:
		minVal, maxVal := OrderedGetMinAndMax[types.MoYear](v)
		minv = types.EncodeMoYear(&minVal)
		maxv = types.EncodeMoYear(&maxVal)

	case types.T_datetime:
		minVal, maxVal := OrderedGetMinAndMax[types.Datetime](v)
		minv = types.EncodeDatetime(&minVal)
		maxv = types.EncodeDatetime(&maxVal)

	case types.T_time:
		minVal, maxVal := OrderedGetMinAndMax[types.Time](v)
		minv = types.EncodeTime(&minVal)
		maxv = types.EncodeTime(&maxVal)

	case types.T_timestamp:
		minVal, maxVal := OrderedGetMinAndMax[types.Timestamp](v)
		minv = types.EncodeTimestamp(&minVal)
		maxv = types.EncodeTimestamp(&maxVal)

	case types.T_enum:
		minVal, maxVal := OrderedGetMinAndMax[types.Enum](v)
		minv = types.EncodeEnum(&minVal)
		maxv = types.EncodeEnum(&maxVal)

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		var minVal, maxVal types.Decimal64
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Less(minVal) {
						minVal = col[i]
					}
					if maxVal.Less(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Less(minVal) {
					minVal = col[i]
				}
				if maxVal.Less(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeDecimal64(&minVal)
		maxv = types.EncodeDecimal64(&maxVal)

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		var minVal, maxVal types.Decimal128
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Less(minVal) {
						minVal = col[i]
					}
					if maxVal.Less(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Less(minVal) {
					minVal = col[i]
				}
				if maxVal.Less(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeDecimal128(&minVal)
		maxv = types.EncodeDecimal128(&maxVal)

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		var minVal, maxVal types.TS
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].LT(&minVal) {
						minVal = col[i]
					}
					if maxVal.LT(&col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].LT(&minVal) {
					minVal = col[i]
				}
				if maxVal.LT(&col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		var minVal, maxVal types.Uuid
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Lt(minVal) {
						minVal = col[i]
					}
					if maxVal.Lt(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Lt(minVal) {
					minVal = col[i]
				}
				if maxVal.Lt(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeUuid(&minVal)
		maxv = types.EncodeUuid(&maxVal)

	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		var minVal, maxVal types.Rowid
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].LT(&minVal) {
						minVal = col[i]
					}
					if maxVal.LT(&col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].LT(&minVal) {
					minVal = col[i]
				}
				if maxVal.LT(&col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		minv, maxv = VarlenGetMinMax(v)
	case types.T_array_float32:
		// Zone map Comparator should be consistent with the SQL Comparator for Array.
		// Hence, we are not using bytesComparator for Array.
		// [Update]: We won't be using the Min and Max inside the ZM. Vector index is going to be handled
		// outside the zonemap via indexing techniques like HNSW etc.
		// For Array ZM, we will mostly make it uninitialized or set theoretical min and max.
		_minv, _maxv := ArrayGetMinMax[float32](v)
		minv = types.ArrayToBytes[float32](_minv)
		maxv = types.ArrayToBytes[float32](_maxv)
	case types.T_array_float64:
		_minv, _maxv := ArrayGetMinMax[float64](v)
		minv = types.ArrayToBytes[float64](_minv)
		maxv = types.ArrayToBytes[float64](_maxv)
	default:
		panic(fmt.Sprintf("unsupported type %s", v.GetType().String()))
	}
	return
}

// InplaceSortAndCompact @todo optimization in the future
func (v *Vector) InplaceSortAndCompact() {
	cleanDataNotResetArea := func() {
		if v.data != nil {
			v.length = 0
		}
		v.nsp.Reset()
		v.sorted = true
	}

	switch v.GetType().Oid {
	case types.T_bool:
		col := MustFixedColNoTypeCheck[bool](v)
		sort.Slice(col, func(i, j int) bool {
			return !col[i] && col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})
		newCol := slices.CompactFunc(col, func(a, b types.Decimal64) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})
		newCol := slices.CompactFunc(col, func(a, b types.Decimal128) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].LT(&col[j])
		})
		newCol := slices.CompactFunc(col, func(a, b types.TS) bool {
			return a.Equal(&b)
		})
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Lt(col[j])
		})
		newCol := slices.CompactFunc(col, func(a, b types.Uuid) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}
	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].LT(&col[j])
		})
		newCol := slices.CompactFunc(col, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		})
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return bytes.Compare(col[i].GetByteSlice(area), col[j].GetByteSlice(area)) < 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return bytes.Equal(a.GetByteSlice(area), b.GetByteSlice(area))
		})

		if len(newCol) != len(col) {
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
		}

	case types.T_array_float32:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&col[i], area),
				types.GetArray[float32](&col[j], area),
			) < 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&a, area),
				types.GetArray[float32](&b, area),
			) == 0
		})
		if len(newCol) != len(col) {
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
		}

	case types.T_array_float64:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&col[i], area),
				types.GetArray[float64](&col[j], area),
			) < 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&a, area),
				types.GetArray[float64](&b, area),
			) == 0
		})
		if len(newCol) != len(col) {
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
		}
	}
}

func (v *Vector) InplaceSort() {
	switch v.GetType().Oid {
	case types.T_bool:
		col := MustFixedColNoTypeCheck[bool](v)
		sort.Slice(col, func(i, j int) bool {
			return !col[i] && col[j]
		})

	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].LT(&col[j])
		})

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Lt(col[j])
		})

	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		sort.Slice(col, func(i, j int) bool {
			return col[i].LT(&col[j])
		})

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return bytes.Compare(col[i].GetByteSlice(area), col[j].GetByteSlice(area)) < 0
		})

	case types.T_array_float32:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&col[i], area),
				types.GetArray[float32](&col[j], area),
			) < 0
		})
	case types.T_array_float64:
		col, area := MustVarlenaRawData(v)
		sort.Slice(col, func(i, j int) bool {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&col[i], area),
				types.GetArray[float64](&col[j], area),
			) < 0
		})
	}
}

func BuildVarlenaInline(v1, v2 *types.Varlena) {
	// use three dword operation to improve performance
	p1 := v1.UnsafePtr()
	p2 := v2.UnsafePtr()
	*(*int64)(p1) = *(*int64)(p2)
	*(*int64)(unsafe.Add(p1, 8)) = *(*int64)(unsafe.Add(p2, 8))
	*(*int64)(unsafe.Add(p1, 16)) = *(*int64)(unsafe.Add(p2, 16))
}

func BuildVarlenaNoInline(vec *Vector, v1 *types.Varlena, bs *[]byte, m *mpool.MPool) error {
	vlen := len(*bs)
	area1 := vec.GetArea()
	voff := len(area1)
	if voff+vlen <= cap(area1) || m == nil {
		area1 = append(area1, *bs...)
		v1.SetOffsetLen(uint32(voff), uint32(vlen))
		vec.area = area1
		return nil
	}
	var err error
	area1, err = m.Grow2(area1, *bs, voff+vlen, vec.offHeap)
	if err != nil {
		return err
	}
	v1.SetOffsetLen(uint32(voff), uint32(vlen))
	vec.area = area1
	return nil
}

func BuildVarlenaNoInlineFromByteJson(vec *Vector, v1 *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
	vlen := len(bj.Data) + 1
	area1 := vec.GetArea()
	voff := len(area1)

	var err error
	if voff+vlen > cap(area1) && m != nil {
		// Pass nil to Grow2, we can grow area1 to voff+vlen without
		// copy bytejson data.
		area1, err = m.Grow2(area1, nil, voff+vlen, vec.offHeap)
		if err != nil {
			return err
		}
		area1[voff] = byte(bj.Type)
		copy(area1[voff+1:voff+vlen], bj.Data)
	} else {
		area1 = append(area1, byte(bj.Type))
		area1 = append(area1, bj.Data...)
	}

	v1.SetOffsetLen(uint32(voff), uint32(vlen))
	vec.area = area1
	return nil
}

func BuildVarlenaFromVarlena(vec *Vector, v1, v2 *types.Varlena, area *[]byte, m *mpool.MPool) error {
	if (*v2)[0] <= types.VarlenaInlineSize {
		BuildVarlenaInline(v1, v2)
		return nil
	}
	voff, vlen := v2.OffsetLen()
	bs := (*area)[voff : voff+vlen]
	return BuildVarlenaNoInline(vec, v1, &bs, m)
}

func BuildVarlenaFromByteSlice(vec *Vector, v *types.Varlena, bs *[]byte, m *mpool.MPool) error {
	vlen := len(*bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], *bs)
		return nil
	}
	return BuildVarlenaNoInline(vec, v, bs, m)
}

func BuildVarlenaFromByteJson(vec *Vector, v *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
	vlen := len(bj.Data) + 1
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		v[1] = byte(bj.Type)
		copy(v[2:vlen+1], bj.Data)
		return nil
	}
	return BuildVarlenaNoInlineFromByteJson(vec, v, bj, m)
}

// BuildVarlenaFromArray convert array to Varlena so that it can be stored in the vector
func BuildVarlenaFromArray[T types.RealNumbers](vec *Vector, v *types.Varlena, array *[]T, m *mpool.MPool) error {
	_bs := types.ArrayToBytes[T](*array)
	bs := &_bs
	vlen := len(*bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], *bs)
		return nil
	}
	return BuildVarlenaNoInline(vec, v, bs, m)
}

// Intersection2VectorOrdered does a ∩ b ==> ret, keeps all item unique and sorted
// it assumes that a and b all sorted already
func Intersection2VectorOrdered[T types.OrderedT | types.Decimal128](
	a, b []T,
	ret *Vector,
	mp *mpool.MPool,
	cmp func(x, y T) int) (err error) {

	var preVal T
	var idxA, idxB int

	minAB := min(len(a), len(b))

	if err = ret.PreExtend(minAB, mp); err != nil {
		return err
	}

	for idxA < len(a) && idxB < len(b) {
		var cmpRet int

		if cmpRet = cmp(a[idxA], b[idxB]); cmpRet == 0 {
			if ret.Length() == 0 || cmp(preVal, a[idxA]) != 0 {
				if err = AppendFixed(ret, a[idxA], false, mp); err != nil {
					return err
				}

				preVal = a[idxA]
			}

			idxA++
			idxB++

		} else if cmpRet < 0 {
			idxA++

		} else {
			idxB++
		}
	}

	return nil
}

// Union2VectorOrdered does a ∪ b ==> ret, keeps all item unique and sorted
// it assumes that a and b all sorted already
func Union2VectorOrdered[T types.OrderedT | types.Decimal128](
	a, b []T,
	ret *Vector,
	mp *mpool.MPool,
	cmp func(x, y T) int) (err error) {

	var i, j int
	var prevVal T
	var lenA, lenB = len(a), len(b)

	if err = ret.PreExtend(lenA+lenB, mp); err != nil {
		return err
	}

	for i < lenA && j < lenB {
		if cmp(a[i], b[j]) <= 0 {
			if (i == 0 && j == 0) || cmp(prevVal, a[i]) != 0 {
				prevVal = a[i]
				if err = AppendFixed(ret, a[i], false, mp); err != nil {
					return err
				}
			}
			i++
		} else {
			if (i == 0 && j == 0) || cmp(prevVal, b[j]) != 0 {
				prevVal = b[j]
				if err = AppendFixed(ret, b[j], false, mp); err != nil {
					return err
				}
			}
			j++
		}
	}

	for ; i < lenA; i++ {
		if (i == 0 && j == 0) || cmp(prevVal, a[i]) != 0 {
			prevVal = a[i]
			if err = AppendFixed(ret, a[i], false, mp); err != nil {
				return err
			}
		}
	}

	for ; j < lenB; j++ {
		if (i == 0 && j == 0) || cmp(prevVal, b[j]) != 0 {
			prevVal = b[j]
			if err = AppendFixed(ret, b[j], false, mp); err != nil {
				return err
			}
		}
	}
	return nil
}

// Intersection2VectorVarlen does a ∩ b ==> ret, keeps all item unique and sorted
// it assumes that va and vb all sorted already
func Intersection2VectorVarlen(
	va, vb *Vector,
	ret *Vector,
	mp *mpool.MPool) (err error) {

	var preVal []byte
	var idxA, idxB int

	cola, areaa := MustVarlenaRawData(va)
	colb, areab := MustVarlenaRawData(vb)

	minAB := min(len(cola), len(colb))
	if err = ret.PreExtend(minAB, mp); err != nil {
		return err
	}

	for idxA < len(cola) && idxB < len(colb) {
		var cmpRet int

		bytesA := cola[idxA].GetByteSlice(areaa)
		bytesB := colb[idxB].GetByteSlice(areab)

		if cmpRet = bytes.Compare(bytesA, bytesB); cmpRet == 0 {
			if ret.Length() == 0 || !bytes.Equal(preVal, bytesA) {
				if err = AppendBytes(ret, bytesA, false, mp); err != nil {
					return err
				}

				preVal = bytesA
			}

			idxA++
			idxB++

		} else if cmpRet < 0 {
			idxA++

		} else {
			idxB++
		}
	}

	return nil
}

// Union2VectorValen does a ∪ b ==> ret, keeps all item unique and sorted
// it assumes that va and vb all sorted already
func Union2VectorValen(
	va, vb *Vector,
	ret *Vector,
	mp *mpool.MPool) (err error) {

	var i, j int
	var prevVal []byte

	cola, areaa := MustVarlenaRawData(va)
	colb, areab := MustVarlenaRawData(vb)

	var lenA, lenB = len(cola), len(colb)

	if err = ret.PreExtend(lenA+lenB, mp); err != nil {
		return err
	}

	for i < lenA && j < lenB {
		ba := cola[i].GetByteSlice(areaa)
		bb := colb[j].GetByteSlice(areab)

		if bytes.Compare(ba, bb) <= 0 {
			if (i == 0 && j == 0) || !bytes.Equal(prevVal, ba) {
				prevVal = ba
				if err = AppendBytes(ret, ba, false, mp); err != nil {
					return err
				}
			}
			i++
		} else {
			if (i == 0 && j == 0) || !bytes.Equal(prevVal, bb) {
				prevVal = bb
				if err = AppendBytes(ret, bb, false, mp); err != nil {
					return err
				}
			}
			j++
		}
	}

	for ; i < lenA; i++ {
		ba := cola[i].GetByteSlice(areaa)
		if (i == 0 && j == 0) || !bytes.Equal(prevVal, ba) {
			prevVal = ba
			if err = AppendBytes(ret, ba, false, mp); err != nil {
				return err
			}
		}
	}

	for ; j < lenB; j++ {
		bb := colb[j].GetByteSlice(areab)
		if (i == 0 && j == 0) || !bytes.Equal(prevVal, bb) {
			prevVal = bb
			if err = AppendBytes(ret, bb, false, mp); err != nil {
				return err
			}
		}
	}

	return nil
}

func (v *Vector) FillRawPtrLen(dest []uintptr) {
	dest[0], dest[1] = v.nsp.RawPtrLen()
	ds := v.UnsafeGetRawData()
	dest[2] = uintptr(unsafe.Pointer(&ds[0]))
	dest[3] = uintptr(len(ds))
	if len(v.area) == 0 {
		dest[4], dest[5] = 0, 0
	} else {
		dest[4] = uintptr(unsafe.Pointer(&v.area[0]))
		dest[5] = uintptr(len(v.area))
	}
}
