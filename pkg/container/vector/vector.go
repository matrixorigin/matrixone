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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	col  any
	data []byte

	// area for holding large strings.
	area []byte

	capacity int
	length   int

	nsp *nulls.Nulls // nulls list

	cantFreeData bool
	cantFreeArea bool

	// FIXME: Bad design! Will be deleted soon.
	isBin bool
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
	return v.capacity
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

func (v *Vector) SetType(typ types.Type) {
	v.typ = typ
}

func (v *Vector) GetNulls() *nulls.Nulls {
	return v.nsp
}

func (v *Vector) SetNulls(nsp *nulls.Nulls) {
	v.nsp = nsp
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

func GetFixedAt[T any](v *Vector, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	return v.col.([]T)[idx]
}

func (v *Vector) GetBytesAt(i int) []byte {
	if v.IsConst() {
		i = 0
	}
	bs := v.col.([]types.Varlena)
	return bs[i].GetByteSlice(v.area)
}

func (v *Vector) CleanOnlyData() {
	if v.data != nil {
		v.length = 0
	}
	if v.area != nil {
		v.area = v.area[:0]
	}
	if v.nsp != nil && v.nsp.Np != nil {
		v.nsp.Np.Clear()
	}
}

func (v *Vector) GetStringAt(i int) string {
	if v.IsConst() {
		i = 0
	}
	bs := v.col.([]types.Varlena)
	return bs[i].GetString(v.area)
}

func (v *Vector) TryExpandNulls(n int) {
	if v.nsp == nil {
		v.nsp = &nulls.Nulls{Np: bitmap.New(0)}
	}
	nulls.TryExpand(v.nsp, n)
}

func NewVec(typ types.Type) *Vector {
	vec := &Vector{
		typ:   typ,
		class: FLAT,
		nsp:   &nulls.Nulls{},
	}

	return vec
}

func NewConstNull(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := &Vector{
		typ:    typ,
		class:  CONSTANT,
		nsp:    &nulls.Nulls{},
		length: length,
	}

	return vec
}

func NewConstFixed[T any](typ types.Type, val T, length int, mp *mpool.MPool) *Vector {
	vec := &Vector{
		typ:   typ,
		class: CONSTANT,
		nsp:   &nulls.Nulls{},
	}

	if length > 0 {
		SetConstFixed(vec, val, length, mp)
	}

	return vec
}

func NewConstBytes(typ types.Type, val []byte, length int, mp *mpool.MPool) *Vector {
	vec := &Vector{
		typ:   typ,
		class: CONSTANT,
		nsp:   &nulls.Nulls{},
	}

	if length > 0 {
		SetConstBytes(vec, val, length, mp)
	}

	return vec
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

func (v *Vector) SetClass(class int) {
	v.class = class
}

func DecodeFixedCol[T types.FixedSizeT](v *Vector) []T {
	sz := int(v.typ.TypeSize())

	//if cap(v.data)%sz != 0 {
	//	panic(moerr.NewInternalErrorNoCtx("decode slice that is not a multiple of element size"))
	//}

	if cap(v.data) >= sz {
		return unsafe.Slice((*T)(unsafe.Pointer(&v.data[0])), cap(v.data)/sz)
	}
	return nil
}

func SetFixedAt[T types.FixedSizeT](v *Vector, idx int, t T) error {
	// Let it panic if v is not a varlena vec
	vacol := MustFixedCol[T](v)

	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtx("vector idx out of range: %d > %d", idx, len(vacol))
	}
	vacol[idx] = t
	return nil
}

func SetBytesAt(v *Vector, idx int, bs []byte, mp *mpool.MPool) error {
	var va types.Varlena
	var err error
	va, v.area, err = types.BuildVarlena(bs, v.area, mp)
	if err != nil {
		return err
	}
	return SetFixedAt(v, idx, va)
}

func SetStringAt(v *Vector, idx int, bs string, mp *mpool.MPool) error {
	return SetBytesAt(v, idx, []byte(bs), mp)
}

// IsConstNull return true if the vector means a scalar Null.
// e.g.
//
//	a + Null, and the vector of right part will return true
func (v *Vector) IsConstNull() bool {
	return v.IsConst() && len(v.data) == 0
}

func (v *Vector) GetArea() []byte {
	return v.area
}

func GetPtrAt(v *Vector, idx int64) unsafe.Pointer {
	if v.IsConst() {
		idx = 0
	} else {
		idx *= int64(v.GetType().TypeSize())
	}
	return unsafe.Pointer(&v.data[idx])
}

func (v *Vector) Free(mp *mpool.MPool) {
	if !v.cantFreeData {
		mp.Free(v.data)
	}
	if !v.cantFreeArea {
		mp.Free(v.area)
	}
	v.class = FLAT
	v.col = nil
	v.data = nil
	v.area = nil
	v.nsp = &nulls.Nulls{}
	v.capacity = 0
	v.length = 0
	v.cantFreeData = false
	v.cantFreeArea = false
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

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
		return nil, err
	}
	nspLen := uint32(len(nspData))
	buf.Write(types.EncodeUint32(&nspLen))
	if nspLen > 0 {
		buf.Write(nspData)
	}

	return buf.Bytes(), nil
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
		v.data = make([]byte, dataLen)
		copy(v.data, data[:dataLen])
		v.setupColFromData()
		data = data[dataLen:]
	}

	// read area
	areaLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if areaLen > 0 {
		v.area = make([]byte, areaLen)
		copy(v.area, data[:areaLen])
		data = data[areaLen:]
	}

	// read nsp
	v.nsp = &nulls.Nulls{}
	nspLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if nspLen > 0 {
		if err := v.nsp.Read(data[:nspLen]); err != nil {
			return err
		}
		//data = data[nspLen:]
	}

	v.cantFreeData = true
	v.cantFreeArea = true

	return nil
}

func (v *Vector) UnmarshalBinaryWithMpool(data []byte, mp *mpool.MPool) error {
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
		v.data, err = mp.Alloc(dataLen)
		if err != nil {
			return err
		}
		copy(v.data, data[:dataLen])
		v.setupColFromData()
		data = data[dataLen:]
	}

	// read area
	areaLen := int(types.DecodeUint32(data[:4]))
	data = data[4:]
	if areaLen > 0 {
		v.area, err = mp.Alloc(areaLen)
		if err != nil {
			return err
		}
		copy(v.area, data[:areaLen])
		data = data[areaLen:]
	}

	// read nsp
	v.nsp = &nulls.Nulls{}
	nspLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if nspLen > 0 {
		if err := v.nsp.Read(data[:nspLen]); err != nil {
			return err
		}
		//data = data[nspLen:]
	}

	return nil
}

func (v *Vector) ToConst(row, length int, mp *mpool.MPool) *Vector {
	if v.class == CONSTANT {
		if v.IsConstNull() {
			return NewConstNull(v.typ, length, mp)
		}
		return v
	}

	if nulls.Contains(v.nsp, uint64(row)) {
		return NewConstNull(v.typ, length, mp)
	}

	switch v.typ.Oid {
	case types.T_bool:
		return NewConstFixed(v.typ, v.col.([]bool)[row], length, mp)
	case types.T_int8:
		return NewConstFixed(v.typ, v.col.([]int8)[row], length, mp)
	case types.T_int16:
		return NewConstFixed(v.typ, v.col.([]int16)[row], length, mp)
	case types.T_int32:
		return NewConstFixed(v.typ, v.col.([]int32)[row], length, mp)
	case types.T_int64:
		return NewConstFixed(v.typ, v.col.([]int64)[row], length, mp)
	case types.T_uint8:
		return NewConstFixed(v.typ, v.col.([]uint8)[row], length, mp)
	case types.T_uint16:
		return NewConstFixed(v.typ, v.col.([]uint16)[row], length, mp)
	case types.T_uint32:
		return NewConstFixed(v.typ, v.col.([]uint32)[row], length, mp)
	case types.T_uint64:
		return NewConstFixed(v.typ, v.col.([]uint64)[row], length, mp)
	case types.T_float32:
		return NewConstFixed(v.typ, v.col.([]float32)[row], length, mp)
	case types.T_float64:
		return NewConstFixed(v.typ, v.col.([]float64)[row], length, mp)
	case types.T_date:
		return NewConstFixed(v.typ, v.col.([]types.Date)[row], length, mp)
	case types.T_datetime:
		return NewConstFixed(v.typ, v.col.([]types.Datetime)[row], length, mp)
	case types.T_time:
		return NewConstFixed(v.typ, v.col.([]types.Time)[row], length, mp)
	case types.T_timestamp:
		return NewConstFixed(v.typ, v.col.([]types.Timestamp)[row], length, mp)
	case types.T_decimal64:
		return NewConstFixed(v.typ, v.col.([]types.Decimal64)[row], length, mp)
	case types.T_decimal128:
		return NewConstFixed(v.typ, v.col.([]types.Decimal128)[row], length, mp)
	case types.T_uuid:
		return NewConstFixed(v.typ, v.col.([]types.Uuid)[row], length, mp)
	case types.T_TS:
		return NewConstFixed(v.typ, v.col.([]types.TS)[row], length, mp)
	case types.T_Rowid:
		return NewConstFixed(v.typ, v.col.([]types.Rowid)[row], length, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return NewConstBytes(v.typ, v.GetBytesAt(row), length, mp)
	}
	return nil
}

// PreExtend use to expand the capacity of the vector
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}

	return extend(v, rows, mp)
}

// Dup use to copy an identical vector
func (v *Vector) Dup(mp *mpool.MPool) (*Vector, error) {
	if v.IsConstNull() {
		return NewConstNull(v.typ, v.Length(), mp), nil
	}

	var err error

	w := &Vector{
		class:  v.class,
		typ:    v.typ,
		nsp:    v.nsp.Clone(),
		length: v.length,
	}

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
		if w.area, err = mp.Alloc(len(v.area)); err != nil {
			return nil, err
		}
		copy(w.area, v.area)
	}
	return w, nil
}

// Shrink use to shrink vectors, sels must be guaranteed to be ordered
func (v *Vector) Shrink(sels []int64, negate bool) {
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
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
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
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
}

// Shuffle use to shrink vectors, sels can be disordered
func (v *Vector) Shuffle(sels []int64, mp *mpool.MPool) error {
	if v.IsConst() {
		return nil
	}

	switch v.typ.Oid {
	case types.T_bool:
		shuffleFixed[bool](v, sels, mp)
	case types.T_int8:
		shuffleFixed[int8](v, sels, mp)
	case types.T_int16:
		shuffleFixed[int16](v, sels, mp)
	case types.T_int32:
		shuffleFixed[int32](v, sels, mp)
	case types.T_int64:
		shuffleFixed[int64](v, sels, mp)
	case types.T_uint8:
		shuffleFixed[uint8](v, sels, mp)
	case types.T_uint16:
		shuffleFixed[uint16](v, sels, mp)
	case types.T_uint32:
		shuffleFixed[uint32](v, sels, mp)
	case types.T_uint64:
		shuffleFixed[uint64](v, sels, mp)
	case types.T_float32:
		shuffleFixed[float32](v, sels, mp)
	case types.T_float64:
		shuffleFixed[float64](v, sels, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		shuffleFixed[types.Varlena](v, sels, mp)
	case types.T_date:
		shuffleFixed[types.Date](v, sels, mp)
	case types.T_datetime:
		shuffleFixed[types.Datetime](v, sels, mp)
	case types.T_time:
		shuffleFixed[types.Time](v, sels, mp)
	case types.T_timestamp:
		shuffleFixed[types.Timestamp](v, sels, mp)
	case types.T_decimal64:
		shuffleFixed[types.Decimal64](v, sels, mp)
	case types.T_decimal128:
		shuffleFixed[types.Decimal128](v, sels, mp)
	case types.T_uuid:
		shuffleFixed[types.Uuid](v, sels, mp)
	case types.T_TS:
		shuffleFixed[types.TS](v, sels, mp)
	case types.T_Rowid:
		shuffleFixed[types.Rowid](v, sels, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}

	return nil
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
		vva := MustFixedCol[types.Varlena](v)
		wva := MustFixedCol[types.Varlena](w)
		if wva[wi].IsSmall() {
			vva[vi] = wva[wi]
		} else {
			bs := wva[wi].GetByteSlice(w.area)
			vva[vi], v.area, err = types.BuildVarlena(bs, v.area, mp)
			if err != nil {
				return err
			}
		}
	}

	if w.nsp != nil {
		if v.nsp == nil {
			v.nsp = nulls.Build(v.Length())
		}
		if w.nsp.Contains(uint64(wi)) {
			v.nsp.Set(uint64(vi))
		} else if v.nsp.Contains(uint64(vi)) {
			v.nsp.Np.Remove(uint64(vi))
		}
	} else if v.nsp != nil {
		v.nsp.Np.Remove(uint64(vi))
	}
	return nil
}

// GetUnionOneFunction: A more sensible function for copying elements,
// which avoids having to do type conversions and type judgements every time you append.
func GetUnionOneFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64) error {
	switch typ.Oid {
	case types.T_bool:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[bool](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_int8:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[int8](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_int16:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[int16](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_int32:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[int32](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_int64:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[int64](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_uint8:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[uint8](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_uint16:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[uint16](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_uint32:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[uint32](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_uint64:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[uint64](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_float32:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[float32](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_float64:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[float64](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_date:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Date](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_datetime:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Datetime](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_time:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Time](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_timestamp:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Timestamp](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_decimal64:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Decimal64](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_decimal128:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Decimal128](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_uuid:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Uuid](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_TS:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.TS](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_Rowid:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Rowid](w)
			return appendOneFixed(v, ws[sel], nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return func(v, w *Vector, sel int64) error {
			ws := MustFixedCol[types.Varlena](w)
			return appendOneBytes(v, ws[sel].GetByteSlice(w.area), nulls.Contains(w.nsp, uint64(sel)), mp)
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionOneFunction", typ))
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
	if w.IsConst() {
		if w.IsConstNull() {
			nulls.Add(v.nsp, uint64(oldLen))
			return nil
		}
		sel = 0
	} else if nulls.Contains(w.nsp, uint64(sel)) {
		nulls.Add(v.nsp, uint64(oldLen))
		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		bs := w.col.([]types.Varlena)[sel].GetByteSlice(w.area)
		v.col.([]types.Varlena)[oldLen], v.area, err = types.BuildVarlena(bs, v.area, mp)
		if err != nil {
			return err
		}
	} else {
		tlen := v.GetType().TypeSize()
		copy(v.data[oldLen*tlen:(oldLen+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
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
	if w.IsConst() {
		if w.IsConstNull() {
			nulls.AddRange(v.nsp, uint64(oldLen), uint64(oldLen+cnt))
			return nil
		}
		sel = 0
	} else if nulls.Contains(w.nsp, uint64(sel)) {
		nulls.AddRange(v.nsp, uint64(oldLen), uint64(oldLen+cnt))
		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var va types.Varlena
		bs := w.col.([]types.Varlena)[sel].GetByteSlice(w.area)
		va, v.area, err = types.BuildVarlena(bs, v.area, mp)
		if err != nil {
			return err
		}
		col := v.col.([]types.Varlena)
		for i := oldLen; i < v.length; i++ {
			col[i] = va
		}
	} else {
		tlen := v.GetType().TypeSize()
		for i := oldLen; i < v.length; i++ {
			copy(v.data[i*tlen:(i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
		}
	}

	return nil
}

func (v *Vector) Union(w *Vector, sels []int32, mp *mpool.MPool) error {
	if len(sels) == 0 {
		return nil
	}

	if err := extend(v, len(sels), mp); err != nil {
		return err
	}

	oldLen := v.length
	v.length += len(sels)
	if w.IsConst() {
		if w.IsConstNull() {
			nulls.AddRange(v.nsp, uint64(oldLen), uint64(oldLen+len(sels)))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			bs := w.col.([]types.Varlena)[0].GetByteSlice(w.area)
			va, v.area, err = types.BuildVarlena(bs, v.area, mp)
			if err != nil {
				return err
			}
			col := v.col.([]types.Varlena)
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
		vCol := v.col.([]types.Varlena)
		wCol := w.col.([]types.Varlena)
		for i, sel := range sels {
			if nulls.Contains(w.GetNulls(), uint64(sel)) {
				nulls.Add(v.GetNulls(), uint64(oldLen+i))
				continue
			}
			bs := wCol[sel].GetByteSlice(w.area)
			vCol[oldLen+i], v.area, err = types.BuildVarlena(bs, v.area, mp)
			if err != nil {
				return err
			}
		}
	} else {
		tlen := v.GetType().TypeSize()
		for i, sel := range sels {
			if nulls.Contains(w.GetNulls(), uint64(sel)) {
				nulls.Add(v.GetNulls(), uint64(oldLen+i))
				continue
			}
			copy(v.data[(oldLen+i)*tlen:(oldLen+i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
		}
	}

	return nil
}

func (v *Vector) UnionBatch(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	if cnt == 0 {
		return nil
	}

	if err := extend(v, cnt, mp); err != nil {
		return err
	}

	if w.IsConst() {
		oldLen := v.length
		addCnt := 0
		for i := range flags {
			addCnt += int(flags[i])
		}
		v.length += addCnt
		if w.IsConstNull() {
			nulls.AddRange(v.GetNulls(), uint64(oldLen), uint64(v.length))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			bs := w.col.([]types.Varlena)[0].GetByteSlice(w.area)
			va, v.area, err = types.BuildVarlena(bs, v.area, mp)
			if err != nil {
				return err
			}
			col := v.col.([]types.Varlena)
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
		vCol := v.col.([]types.Varlena)
		wCol := w.col.([]types.Varlena)
		for i := range flags {
			if flags[i] == 0 {
				continue
			}
			if nulls.Contains(w.GetNulls(), uint64(offset)+uint64(i)) {
				nulls.Add(v.GetNulls(), uint64(v.length))
			} else {
				bs := wCol[int(offset)+i].GetByteSlice(w.area)
				vCol[v.length], v.area, err = types.BuildVarlena(bs, v.area, mp)
				if err != nil {
					return err
				}
			}
			v.length++
		}
	} else {
		tlen := v.GetType().TypeSize()
		for i := range flags {
			if flags[i] == 0 {
				continue
			}
			if nulls.Contains(w.GetNulls(), uint64(offset)+uint64(i)) {
				nulls.Add(v.GetNulls(), uint64(v.length))
			} else {
				copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
			}
			v.length++
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
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		col := MustStrCol(v)
		if len(col) == 1 {
			if nulls.Contains(v.nsp, 0) {
				return "null"
			} else {
				return col[0]
			}
		}
		return fmt.Sprintf("%v-%s", col, v.nsp)
	default:
		panic("vec to string unknown types.")
	}
}

func SetConstNull(vec *Vector, len int, mp *mpool.MPool) error {
	vec.Free(mp)
	vec.class = CONSTANT
	return nil
}

func SetConstFixed[T any](vec *Vector, val T, length int, mp *mpool.MPool) error {
	if vec.capacity == 0 {
		if err := extend(vec, 1, mp); err != nil {
			return err
		}
	}
	col := vec.col.([]T)
	col[0] = val
	vec.SetLength(length)
	return nil
}

func SetConstBytes(vec *Vector, val []byte, length int, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if vec.capacity == 0 {
		if err := extend(vec, 1, mp); err != nil {
			return err
		}
	}

	col := vec.col.([]types.Varlena)
	va, vec.area, err = types.BuildVarlena(val, vec.area, mp)
	if err != nil {
		return err
	}
	col[0] = va
	vec.SetLength(length)
	return nil
}

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
	case types.T_datetime:
		return appendOneFixed(vec, val.(types.Datetime), false, mp)
	case types.T_time:
		return appendOneFixed(vec, val.(types.Time), false, mp)
	case types.T_timestamp:
		return appendOneFixed(vec, val.(types.Timestamp), false, mp)
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
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return appendOneBytes(vec, val.([]byte), false, mp)
	}
	return nil
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

func appendOneFixed[T any](vec *Vector, val T, isNull bool, mp *mpool.MPool) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	length := vec.length
	vec.length++
	if isNull {
		nulls.Add(vec.nsp, uint64(length))
	} else {
		col := vec.col.([]T)
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
		va, vec.area, err = types.BuildVarlena(val, vec.area, mp)
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
		nulls.AddRange(vec.nsp, uint64(length), uint64(length+cnt))
	} else {
		col := vec.col.([]T)
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
		nulls.AddRange(vec.nsp, uint64(length), uint64(length+cnt))
	} else {
		col := vec.col.([]types.Varlena)
		va, vec.area, err = types.BuildVarlena(val, vec.area, mp)
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
	col := MustFixedCol[T](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(vec *Vector, vals [][]byte, isNulls []bool, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if err = extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedCol[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			va, vec.area, err = types.BuildVarlena(w, vec.area, mp)
			if err != nil {
				return err
			}
			col[length+i] = va
		}
	}
	return nil
}

func appendStringList(vec *Vector, vals []string, isNulls []bool, mp *mpool.MPool) error {
	var err error
	var va types.Varlena

	if err = extend(vec, len(vals), mp); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustFixedCol[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			va, vec.area, err = types.BuildVarlena([]byte(w), vec.area, mp)
			if err != nil {
				return err
			}
			col[length+i] = va
		}
	}
	return nil
}

func shrinkFixed[T types.FixedSizeT](v *Vector, sels []int64, negate bool) {
	vs := MustFixedCol[T](v)
	if !negate {
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.nsp = nulls.Filter(v.nsp, sels, false)
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
		v.nsp = nulls.Filter(v.nsp, sels, true)
		v.length -= len(sels)
	}
}

func shuffleFixed[T types.FixedSizeT](v *Vector, sels []int64, mp *mpool.MPool) error {
	sz := v.typ.TypeSize()
	olddata := v.data[:v.length*sz]
	ns := len(sels)
	vs := MustFixedCol[T](v)
	data, err := mp.Alloc(int(ns * v.GetType().TypeSize()))
	if err != nil {
		return err
	}
	v.data = data
	v.setupColFromData()
	ws := v.col.([]T)[:ns]
	shuffle.FixedLengthShuffle(vs, ws, sels)
	v.nsp = nulls.Filter(v.nsp, sels, false)
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
	col := MustFixedCol[T](v)
	if len(col) == 1 {
		if nulls.Contains(v.nsp, 0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	return fmt.Sprintf("%v-%s", col, v.nsp)
}

// CloneWindow Deep copies the content from start to end into another vector. Afterwise it's safe to destroy the original one.
func (v *Vector) CloneWindow(start, end int, mp *mpool.MPool) (*Vector, error) {
	w := NewVec(v.typ)
	if start == end {
		return w, nil
	}
	w.nsp = nulls.Range(v.nsp, uint64(start), uint64(end), uint64(start), w.nsp)
	length := (end - start) * v.typ.TypeSize()
	if mp == nil {
		w.data = make([]byte, length)
		copy(w.data, v.data[start*v.typ.TypeSize():end*v.typ.TypeSize()])
		w.length = end - start
		w.setupColFromData()
		if v.typ.IsString() {
			w.area = make([]byte, len(v.area))
			copy(w.area, v.area)
		}
		w.cantFreeData = true
		w.cantFreeArea = true
	} else {
		err := w.PreExtend(end-start, mp)
		if err != nil {
			return nil, err
		}
		w.length = end - start
		if v.GetType().IsVarlen() {
			var va types.Varlena
			vCol := v.col.([]types.Varlena)
			wCol := w.col.([]types.Varlena)
			for i := start; i < end; i++ {
				if !nulls.Contains(v.GetNulls(), uint64(i)) {
					bs := vCol[i].GetByteSlice(v.area)
					va, w.area, err = types.BuildVarlena(bs, w.area, mp)
					if err != nil {
						return nil, err
					}
					wCol[i-start] = va
				}
			}
		} else {
			tlen := v.typ.TypeSize()
			copy(w.data[:length], v.data[start*tlen:end*tlen])
		}
	}

	return w, nil
}
