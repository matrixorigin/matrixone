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
	"reflect"
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
	nsp *nulls.Nulls // nulls list

	// data of fixed length element, in case of varlen, the Varlena
	col  any
	data []byte

	// area for holding large strings.
	area []byte

	capacity int
	length   int

	// tag for distinguish '0x00..' and 0x... and 0x... is binary
	// TODO: check whether isBin should be changed into array/bitmap
	// now we assumpt that it can only be true in the case of only one data in vector
	isBin bool
}

func (v *Vector) UnsafeGetRawData() []byte {
	length := 1
	if !v.IsConst() {
		length = v.length
	}
	return v.data[:length*v.typ.TypeSize()]
}

func (v *Vector) SetIsBin(isBin bool) {
	v.isBin = isBin
}

func (v *Vector) GetIsBin() bool {
	return v.isBin
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

func (v *Vector) GetBytes(i int) []byte {
	if v.IsConst() {
		i = 0
	}
	bs := v.col.([]types.Varlena)
	return bs[i].GetByteSlice(v.area)
}

//func (v *Vector) GetRawData() []byte {
//	return (*(*[]byte)(unsafe.Pointer(&v.col)))[:v.length*v.typ.TypeSize()]
//}

func (v *Vector) GetString(i int) string {
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

func NewVector(typ types.Type) *Vector {
	vec := &Vector{
		typ:   typ,
		class: FLAT,
		nsp:   &nulls.Nulls{},
	}

	return vec
}

func NewConstNull(typ types.Type, len int, m *mpool.MPool) *Vector {
	vec := &Vector{
		typ:   typ,
		class: CONSTANT,
		nsp:   &nulls.Nulls{},
	}

	if len > 0 {
		SetConstNull(vec, len, m)
	}

	return vec
}

func NewConst[T any](typ types.Type, val T, len int, m *mpool.MPool) *Vector {
	vec := &Vector{
		typ:   typ,
		class: CONSTANT,
		nsp:   &nulls.Nulls{},
	}

	if len > 0 {
		SetConst(vec, val, len, m)
	}

	return vec
}

func NewConstBytes(typ types.Type, val []byte, len int, m *mpool.MPool) *Vector {
	vec := &Vector{
		typ:   typ,
		class: CONSTANT,
		nsp:   &nulls.Nulls{},
	}

	if len > 0 {
		SetConstBytes(vec, val, len, m)
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

func SetTAt[T types.FixedSizeT](v *Vector, idx int, t T) error {
	// Let it panic if v is not a varlena vec
	vacol := MustTCols[T](v)

	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtx("vector idx out of range")
	}
	vacol[idx] = t
	return nil
}

func SetBytesAt(v *Vector, idx int, bs []byte, m *mpool.MPool) error {
	var va types.Varlena
	var err error
	va, v.area, err = types.BuildVarlena(bs, v.area, m)
	if err != nil {
		return err
	}
	return SetTAt(v, idx, va)
}

func SetStringAt(v *Vector, idx int, bs string, m *mpool.MPool) error {
	return SetBytesAt(v, idx, []byte(bs), m)
}

// IsConstNull return true if the vector means a scalar Null.
// e.g.
//
//	a + Null, and the vector of right part will return true
func (v *Vector) IsConstNull() bool {
	return v.IsConst() && v.nsp != nil && nulls.Contains(v.nsp, 0)
}

func (v *Vector) GetArea() []byte {
	return v.area
}

func GetPtrAt(v *Vector, idx int64) unsafe.Pointer {
	return unsafe.Pointer(&v.data[idx*int64(v.GetType().TypeSize())])
}

func (v *Vector) Free(m *mpool.MPool) {
	// const vector's data & area allocate with nil,
	// so we can't free it by using mpool.
	m.Free(v.data)
	m.Free(v.area)
	v.col = nil
	v.data = nil
	v.area = nil
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(uint8(v.class))
	{ // write length
		length := int64(v.length)
		buf.Write(types.EncodeInt64(&length))
	}
	{ // write type
		data := types.EncodeType(&v.typ)
		buf.Write(data)
	}
	{ // write nspLen, nsp
		data, err := v.nsp.Show()
		if err != nil {
			return nil, err
		}
		length := uint32(len(data))
		buf.Write(types.EncodeUint32(&length))
		if len(data) > 0 {
			buf.Write(data)
		}
	}
	{ // write colLen, col
		data := v.data
		length := uint32(v.length * v.typ.TypeSize())
		if len(data[:length]) > 0 {
			buf.Write(data[:length])
		}
	}
	{ // write areaLen, area
		length := uint32(len(v.area))
		buf.Write(types.EncodeUint32(&length))
		if len(v.area) > 0 {
			buf.Write(v.area)
		}
	}
	return buf.Bytes(), nil
}

func (v *Vector) UnmarshalBinary(data []byte) error {
	if v.col == nil {
		v.col = new(reflect.SliceHeader)
	}
	{ // read class
		v.class = int(data[0])
		data = data[1:]
	}
	{ // read length
		v.length = int(types.DecodeInt64(data[:8]))
		data = data[8:]
	}
	{ // read typ
		v.typ = types.DecodeType(data[:types.TSize])
		data = data[types.TSize:]
	}
	{ // read nsp
		v.nsp = &nulls.Nulls{}
		size := types.DecodeUint32(data)
		data = data[4:]
		if size > 0 {
			if err := v.nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}
	}
	{ // read col
		length := v.length * v.typ.TypeSize()
		if length > 0 {
			v.data = make([]byte, length)
			copy(v.data, data[:length])
			v.setupColFromData()
			data = data[length:]
		}
	}
	{ // read area
		length := types.DecodeUint32(data)
		data = data[4:]
		if length > 0 {
			ndata := make([]byte, length)
			copy(ndata, data[:length])
			v.area = ndata
			//data = data[:length]
		}
	}
	return nil
}

func (v *Vector) UnmarshalBinaryWithMpool(data []byte, mp *mpool.MPool) error {
	var err error
	if v.col == nil {
		v.col = new(reflect.SliceHeader)
	}
	{ // read class
		v.class = int(data[0])
		data = data[1:]
	}
	{ // read length
		v.length = int(types.DecodeInt64(data[:8]))
		data = data[8:]
	}
	{ // read typ
		v.typ = types.DecodeType(data[:types.TSize])
		data = data[types.TSize:]
	}
	{ // read nsp
		v.nsp = &nulls.Nulls{}
		size := types.DecodeUint32(data)
		data = data[4:]
		if size > 0 {
			if err := v.nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}
	}
	{ // read col
		length := v.length * v.typ.TypeSize()
		if length > 0 {
			v.data, err = mp.Alloc(length)
			if err != nil {
				return err
			}
			copy(v.data, data[:length])
			v.setupColFromData()
			data = data[length:]
		}
	}
	{ // read area
		length := types.DecodeUint32(data)
		data = data[4:]
		if length > 0 {
			ndata, err := mp.Alloc(int(length))
			if err != nil {
				return err
			}
			copy(ndata, data[:length])
			v.area = ndata
			//data = data[:length]
		}
	}
	return nil
}

func (v *Vector) ToConst(row int, mp *mpool.MPool) *Vector {
	if v.class == CONSTANT {
		return v
	}
	switch v.typ.Oid {
	case types.T_bool:
		return toConstVector[bool](v, row, mp)
	case types.T_int8:
		return toConstVector[int8](v, row, mp)
	case types.T_int16:
		return toConstVector[int16](v, row, mp)
	case types.T_int32:
		return toConstVector[int32](v, row, mp)
	case types.T_int64:
		return toConstVector[int64](v, row, mp)
	case types.T_uint8:
		return toConstVector[uint8](v, row, mp)
	case types.T_uint16:
		return toConstVector[uint16](v, row, mp)
	case types.T_uint32:
		return toConstVector[uint32](v, row, mp)
	case types.T_uint64:
		return toConstVector[uint64](v, row, mp)
	case types.T_float32:
		return toConstVector[float32](v, row, mp)
	case types.T_float64:
		return toConstVector[float64](v, row, mp)
	case types.T_date:
		return toConstVector[types.Date](v, row, mp)
	case types.T_datetime:
		return toConstVector[types.Datetime](v, row, mp)
	case types.T_time:
		return toConstVector[types.Time](v, row, mp)
	case types.T_timestamp:
		return toConstVector[types.Timestamp](v, row, mp)
	case types.T_decimal64:
		return toConstVector[types.Decimal64](v, row, mp)
	case types.T_decimal128:
		return toConstVector[types.Decimal128](v, row, mp)
	case types.T_uuid:
		return toConstVector[types.Uuid](v, row, mp)
	case types.T_TS:
		return toConstVector[types.TS](v, row, mp)
	case types.T_Rowid:
		return toConstVector[types.Rowid](v, row, mp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		if nulls.Contains(v.nsp, uint64(row)) {
			return NewConstNull(v.typ, 1, mp)
		}
		bs := v.GetBytes(row)
		vec := NewConstBytes(v.typ, bs, 1, mp)
		return vec
	}
	return nil
}

func toConstVector[T types.FixedSizeT](v *Vector, row int, m *mpool.MPool) *Vector {
	if nulls.Contains(v.nsp, uint64(row)) {
		return NewConstNull(v.typ, 1, m)
	} else {
		val := v.col.([]T)[row]
		return NewConst(v.typ, val, 1, m)
	}
}

// PreExtend use to expand the capacity of the vector
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}

	return extend(v, rows, mp)
}

// Dup use to copy an identical vector
func (v *Vector) Dup(m *mpool.MPool) (*Vector, error) {
	if v.IsConstNull() {
		return NewConstNull(v.typ, v.Length(), m), nil
	}

	var err error

	w := &Vector{
		class:  v.class,
		typ:    v.typ,
		nsp:    v.nsp.Clone(),
		length: v.length,
		isBin:  v.isBin,
	}

	if v.IsConst() {
		if err := extend(w, 1, m); err != nil {
			return nil, err
		}
	} else {
		if err := extend(w, v.length, m); err != nil {
			return nil, err
		}
	}
	copy(w.data, v.data)

	if len(v.area) > 0 {
		if w.area, err = m.Alloc(len(v.area)); err != nil {
			return nil, err
		}
		copy(w.area, v.area)
	}
	return w, nil
}

// Shrink use to shrink vectors, sels must be guaranteed to be ordered
func (v *Vector) Shrink(sels []int64) {
	if v.class == FLAT {
		switch v.typ.Oid {
		case types.T_bool:
			shrinkFixed[bool](v, sels)
		case types.T_int8:
			shrinkFixed[int8](v, sels)
		case types.T_int16:
			shrinkFixed[int16](v, sels)
		case types.T_int32:
			shrinkFixed[int32](v, sels)
		case types.T_int64:
			shrinkFixed[int64](v, sels)
		case types.T_uint8:
			shrinkFixed[uint8](v, sels)
		case types.T_uint16:
			shrinkFixed[uint16](v, sels)
		case types.T_uint32:
			shrinkFixed[uint32](v, sels)
		case types.T_uint64:
			shrinkFixed[uint64](v, sels)
		case types.T_float32:
			shrinkFixed[float32](v, sels)
		case types.T_float64:
			shrinkFixed[float64](v, sels)
		case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
			// XXX shrink varlena, but did not shrink area.  For our vector, this
			// may well be the right thing.  If want to shrink area as well, we
			// have to copy each varlena value and swizzle pointer.
			shrinkFixed[types.Varlena](v, sels)
		case types.T_date:
			shrinkFixed[types.Date](v, sels)
		case types.T_datetime:
			shrinkFixed[types.Datetime](v, sels)
		case types.T_time:
			shrinkFixed[types.Time](v, sels)
		case types.T_timestamp:
			shrinkFixed[types.Timestamp](v, sels)
		case types.T_decimal64:
			shrinkFixed[types.Decimal64](v, sels)
		case types.T_decimal128:
			shrinkFixed[types.Decimal128](v, sels)
		case types.T_uuid:
			shrinkFixed[types.Uuid](v, sels)
		case types.T_TS:
			shrinkFixed[types.TS](v, sels)
		case types.T_Rowid:
			shrinkFixed[types.Rowid](v, sels)
		default:
			panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
		}
	}
	v.length = len(sels)
}

// Shuffle use to shrink vectors, sels can be disordered
func (v *Vector) Shuffle(sels []int64, m *mpool.MPool) error {
	if v.class == FLAT {
		switch v.typ.Oid {
		case types.T_bool:
			shuffleFixed[bool](v, sels, m)
		case types.T_int8:
			shuffleFixed[int8](v, sels, m)
		case types.T_int16:
			shuffleFixed[int16](v, sels, m)
		case types.T_int32:
			shuffleFixed[int32](v, sels, m)
		case types.T_int64:
			shuffleFixed[int64](v, sels, m)
		case types.T_uint8:
			shuffleFixed[uint8](v, sels, m)
		case types.T_uint16:
			shuffleFixed[uint16](v, sels, m)
		case types.T_uint32:
			shuffleFixed[uint32](v, sels, m)
		case types.T_uint64:
			shuffleFixed[uint64](v, sels, m)
		case types.T_float32:
			shuffleFixed[float32](v, sels, m)
		case types.T_float64:
			shuffleFixed[float64](v, sels, m)
		case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
			shuffleFixed[types.Varlena](v, sels, m)
		case types.T_date:
			shuffleFixed[types.Date](v, sels, m)
		case types.T_datetime:
			shuffleFixed[types.Datetime](v, sels, m)
		case types.T_time:
			shuffleFixed[types.Time](v, sels, m)
		case types.T_timestamp:
			shuffleFixed[types.Timestamp](v, sels, m)
		case types.T_decimal64:
			shuffleFixed[types.Decimal64](v, sels, m)
		case types.T_decimal128:
			shuffleFixed[types.Decimal128](v, sels, m)
		case types.T_uuid:
			shuffleFixed[types.Uuid](v, sels, m)
		case types.T_TS:
			shuffleFixed[types.TS](v, sels, m)
		case types.T_Rowid:
			shuffleFixed[types.Rowid](v, sels, m)
		default:
			panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
		}
	}
	return nil
}

// Copy simply does v[vi] = w[wi]
func (v *Vector) Copy(w *Vector, vi, wi int64, m *mpool.MPool) error {
	if w.class == CONSTANT {
		wi = 0
	}
	if v.typ.IsFixedLen() {
		sz := v.typ.TypeSize()
		vdata := (*(*[]byte)(unsafe.Pointer(&v.col)))[:v.length*sz]
		wdata := (*(*[]byte)(unsafe.Pointer(&w.col)))[:v.length*sz]
		copy(vdata[vi*int64(sz):(vi+1)*int64(sz)], wdata[wi*int64(sz):(wi+1)*int64(sz)])
	} else {
		var err error
		vva := MustTCols[types.Varlena](v)
		wva := MustTCols[types.Varlena](w)
		if wva[wi].IsSmall() {
			vva[vi] = wva[wi]
		} else {
			bs := wva[wi].GetByteSlice(w.area)
			vva[vi], v.area, err = types.BuildVarlena(bs, v.area, m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionOne(w *Vector, sel int64, mp *mpool.MPool) error {
	if w.class == CONSTANT {
		sel = 0
	}

	if w.nsp.Contains((uint64(sel))) {
		return Append(v, 0, true, mp)
	}

	switch v.typ.Oid {
	case types.T_bool:
		return Append(v, MustTCols[bool](w)[sel], false, mp)
	case types.T_int8:
		return Append(v, MustTCols[int8](w)[sel], false, mp)
	case types.T_int16:
		return Append(v, MustTCols[int16](w)[sel], false, mp)
	case types.T_int32:
		return Append(v, MustTCols[int32](w)[sel], false, mp)
	case types.T_int64:
		return Append(v, MustTCols[int64](w)[sel], false, mp)
	case types.T_uint8:
		return Append(v, MustTCols[uint8](w)[sel], false, mp)
	case types.T_uint16:
		return Append(v, MustTCols[uint16](w)[sel], false, mp)
	case types.T_uint32:
		return Append(v, MustTCols[uint32](w)[sel], false, mp)
	case types.T_uint64:
		return Append(v, MustTCols[uint64](w)[sel], false, mp)
	case types.T_float32:
		return Append(v, MustTCols[float32](w)[sel], false, mp)
	case types.T_float64:
		return Append(v, MustTCols[float64](w)[sel], false, mp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		ws := MustTCols[types.Varlena](w)
		return AppendBytes(v, ws[sel].GetByteSlice(v.area), false, mp)
	case types.T_date:
		return Append(v, MustTCols[types.Date](w)[sel], false, mp)
	case types.T_datetime:
		return Append(v, MustTCols[types.Datetime](w)[sel], false, mp)
	case types.T_time:
		return Append(v, MustTCols[types.Time](w)[sel], false, mp)
	case types.T_timestamp:
		return Append(v, MustTCols[types.Timestamp](w)[sel], false, mp)
	case types.T_decimal64:
		return Append(v, MustTCols[types.Decimal64](w)[sel], false, mp)
	case types.T_decimal128:
		return Append(v, MustTCols[types.Decimal128](w)[sel], false, mp)
	case types.T_uuid:
		return Append(v, MustTCols[types.Uuid](w)[sel], false, mp)
	case types.T_TS:
		return Append(v, MustTCols[types.TS](w)[sel], false, mp)
	case types.T_Rowid:
		return Append(v, MustTCols[types.Rowid](w)[sel], false, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionMulti(w *Vector, sel int64, cnt int, mp *mpool.MPool) error {
	if w.class == CONSTANT {
		sel = 0
	}

	if w.nsp.Contains((uint64(sel))) {
		return AppendMulti(v, 0, true, cnt, mp)
	}

	switch v.typ.Oid {
	case types.T_bool:
		return AppendMulti(v, MustTCols[bool](w)[sel], false, cnt, mp)
	case types.T_int8:
		return AppendMulti(v, MustTCols[int8](w)[sel], false, cnt, mp)
	case types.T_int16:
		return AppendMulti(v, MustTCols[int16](w)[sel], false, cnt, mp)
	case types.T_int32:
		return AppendMulti(v, MustTCols[int32](w)[sel], false, cnt, mp)
	case types.T_int64:
		return AppendMulti(v, MustTCols[int64](w)[sel], false, cnt, mp)
	case types.T_uint8:
		return AppendMulti(v, MustTCols[uint8](w)[sel], false, cnt, mp)
	case types.T_uint16:
		return AppendMulti(v, MustTCols[uint16](w)[sel], false, cnt, mp)
	case types.T_uint32:
		return AppendMulti(v, MustTCols[uint32](w)[sel], false, cnt, mp)
	case types.T_uint64:
		return AppendMulti(v, MustTCols[uint64](w)[sel], false, cnt, mp)
	case types.T_float32:
		return AppendMulti(v, MustTCols[float32](w)[sel], false, cnt, mp)
	case types.T_float64:
		return AppendMulti(v, MustTCols[float64](w)[sel], false, cnt, mp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		ws := MustTCols[types.Varlena](w)
		return AppendMultiBytes(v, ws[sel].GetByteSlice(v.area), false, cnt, mp)
	case types.T_date:
		return AppendMulti(v, MustTCols[types.Date](w)[sel], false, cnt, mp)
	case types.T_datetime:
		return AppendMulti(v, MustTCols[types.Datetime](w)[sel], false, cnt, mp)
	case types.T_time:
		return AppendMulti(v, MustTCols[types.Time](w)[sel], false, cnt, mp)
	case types.T_timestamp:
		return AppendMulti(v, MustTCols[types.Timestamp](w)[sel], false, cnt, mp)
	case types.T_decimal64:
		return AppendMulti(v, MustTCols[types.Decimal64](w)[sel], false, cnt, mp)
	case types.T_decimal128:
		return AppendMulti(v, MustTCols[types.Decimal128](w)[sel], false, cnt, mp)
	case types.T_uuid:
		return AppendMulti(v, MustTCols[types.Uuid](w)[sel], false, cnt, mp)
	case types.T_TS:
		return AppendMulti(v, MustTCols[types.TS](w)[sel], false, cnt, mp)
	case types.T_Rowid:
		return AppendMulti(v, MustTCols[types.Rowid](w)[sel], false, cnt, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}
}

func (v *Vector) Union(w *Vector, sel []int64, mp *mpool.MPool) error {
	var err error
	err = nil
	for i := range sel {
		err1 := v.UnionOne(w, sel[i], mp)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

func UnionNull(v, _ *Vector, m *mpool.MPool) error {
	if v.GetType().IsTuple() {
		panic(moerr.NewInternalErrorNoCtx("unionnull of tuple vector"))
	}

	pos := uint64(v.Length() - 1)
	nulls.Add(v.GetNulls(), pos)
	return nil
}

func UnionBatch(v, w *Vector, offset int64, cnt int, flags []uint8, m *mpool.MPool) (err error) {
	if err = v.PreExtend(cnt, m); err != nil {
		return err
	}

	for i := range flags {
		if flags[i] > 0 {
			err1 := v.UnionOne(w, offset+int64(i), m)
			if err1 != nil {
				err = err1
			}
		}
	}
	return err
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
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		col := MustStrCols(v)
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

func SetConstNull(vec *Vector, len int, m *mpool.MPool) error {
	nulls.Add(vec.nsp, uint64(0))
	vec.SetLength(len)
	return nil
}

func SetConst[T any](vec *Vector, val T, length int, m *mpool.MPool) error {
	if vec.capacity == 0 {
		if err := extend(vec, 1, m); err != nil {
			return err
		}
	}
	col := vec.col.([]T)
	col[0] = val
	vec.SetLength(length)
	return nil
}

func SetConstBytes(vec *Vector, val []byte, length int, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if vec.capacity == 0 {
		if err := extend(vec, 1, m); err != nil {
			return err
		}
	}

	col := vec.col.([]types.Varlena)
	va, vec.area, err = types.BuildVarlena(val, vec.area, m)
	if err != nil {
		return err
	}
	col[0] = va
	vec.SetLength(length)
	return nil
}

func Append[T any](vec *Vector, val T, isNull bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOne(vec, val, isNull, m)
}

func AppendBytes(vec *Vector, val []byte, isNull bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneBytes(vec, val, isNull, m)
}

func AppendMulti[T any](vec *Vector, vals T, isNull bool, cnt int, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMulti(vec, vals, isNull, cnt, m)
}

func AppendMultiBytes(vec *Vector, vals []byte, isNull bool, cnt int, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMultiBytes(vec, vals, isNull, cnt, m)
}

func AppendList[T any](v *Vector, ws []T, isNulls []bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendList(v, ws, isNulls, m)
}

func AppendBytesList(v *Vector, ws [][]byte, isNulls []bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendBytesList(v, ws, isNulls, m)
}

func AppendStringList(v *Vector, ws []string, isNulls []bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendStringList(v, ws, isNulls, m)
}

func appendOne[T any](vec *Vector, val T, isNull bool, m *mpool.MPool) error {
	if err := extend(vec, 1, m); err != nil {
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

func appendOneBytes(vec *Vector, val []byte, isNull bool, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if isNull {
		return appendOne(vec, va, true, m)
	} else {
		va, vec.area, err = types.BuildVarlena(val, vec.area, m)
		if err != nil {
			return err
		}
		return appendOne(vec, va, false, m)
	}
}

func appendMulti[T any](vec *Vector, val T, isNull bool, cnt int, m *mpool.MPool) error {
	if err := extend(vec, 1, m); err != nil {
		return err
	}
	length := vec.length
	vec.length += cnt
	col := vec.col.([]T)
	if isNull {
		nulls.AddRange(vec.nsp, uint64(length), uint64(length+cnt))
	} else {
		for i := 0; i < cnt; i++ {
			col[length+i] = val
		}
	}
	return nil
}

func appendMultiBytes(vec *Vector, val []byte, isNull bool, cnt int, m *mpool.MPool) error {
	var err error
	var va types.Varlena
	if err = extend(vec, 1, m); err != nil {
		return err
	}
	length := vec.length
	vec.length += cnt
	col := vec.col.([]types.Varlena)
	if isNull {
		nulls.AddRange(vec.nsp, uint64(length), uint64(length+cnt))
	} else {
		va, vec.area, err = types.BuildVarlena(val, vec.area, m)
		if err != nil {
			return err
		}
		for i := 0; i < cnt; i++ {
			col[length+i] = va
		}
	}
	return nil
}

func appendList[T any](vec *Vector, vals []T, isNulls []bool, m *mpool.MPool) error {
	if err := extend(vec, len(vals), m); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustTCols[T](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(vec *Vector, vals [][]byte, isNulls []bool, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if err = extend(vec, len(vals), m); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustTCols[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			va, vec.area, err = types.BuildVarlena(w, vec.area, m)
			if err != nil {
				return err
			}
			col[length+i] = va
		}
	}
	return nil
}

func appendStringList(vec *Vector, vals []string, isNulls []bool, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if err = extend(vec, len(vals), m); err != nil {
		return err
	}
	length := vec.length
	vec.length += len(vals)
	col := MustTCols[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(vec.nsp, uint64(length+i))
		} else {
			va, vec.area, err = types.BuildVarlena([]byte(w), vec.area, m)
			if err != nil {
				return err
			}
			col[length+i] = va
		}
	}
	return nil
}

func shrinkFixed[T types.FixedSizeT](v *Vector, sels []int64) {
	vs := MustTCols[T](v)
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	v.nsp = nulls.Filter(v.nsp, sels)
}

func shuffleFixed[T types.FixedSizeT](v *Vector, sels []int64, m *mpool.MPool) error {
	sz := v.typ.TypeSize()
	olddata := v.data[:v.length*sz]
	ns := len(sels)
	vs := MustTCols[T](v)
	data, err := m.Alloc(int(ns * v.GetType().TypeSize()))
	if err != nil {
		return err
	}
	v.data = data
	v.setupColFromData()
	ws := v.col.([]T)[:ns]
	shuffle.FixedLengthShuffle(vs, ws, sels)
	v.nsp = nulls.Filter(v.nsp, sels)
	m.Free(olddata)
	v.length = ns
	return nil
}

func vecToString[T types.FixedSizeT](v *Vector) string {
	col := MustTCols[T](v)
	if len(col) == 1 {
		if nulls.Contains(v.nsp, 0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	return fmt.Sprintf("%v-%s", col, v.nsp)
}

func GetInitConstVal(typ types.Type) any {
	switch typ.Oid {
	case types.T_bool:
		return false
	case types.T_int8:
		return int8(0)
	case types.T_int16:
		return int16(0)
	case types.T_int32:
		return int32(0)
	case types.T_int64:
		return int64(0)
	case types.T_uint8:
		return uint8(0)
	case types.T_uint16:
		return uint16(0)
	case types.T_uint32:
		return uint32(0)
	case types.T_uint64:
		return uint64(0)
	case types.T_float32:
		return float32(0)
	case types.T_float64:
		return float64(0)
	case types.T_date:
		return types.Date(0)
	case types.T_time:
		return types.Time(0)
	case types.T_datetime:
		return types.Datetime(0)
	case types.T_timestamp:
		return types.Timestamp(0)
	case types.T_decimal64:
		return types.Decimal64{}
	case types.T_decimal128:
		return types.Decimal128{}
	case types.T_uuid:
		var emptyUuid [16]byte
		return emptyUuid[:]
	case types.T_TS:
		var emptyTs [types.TxnTsSize]byte
		return emptyTs[:]
	case types.T_Rowid:
		var emptyRowid [types.RowidSize]byte
		return emptyRowid[:]
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		var emptyVarlena [types.VarlenaSize]byte
		return emptyVarlena[:]
	default:
		//T_any T_star T_tuple T_interval
		return int64(0)
	}
}

func CopyConst(toVec, fromVec *Vector, length int, m *mpool.MPool) error {
	typ := fromVec.GetType()
	switch typ.Oid {
	case types.T_bool:
		item := MustTCols[bool](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_int8:
		item := MustTCols[int8](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_int16:
		item := MustTCols[int16](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_int32:
		item := MustTCols[int32](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_int64:
		item := MustTCols[int64](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_uint8:
		item := MustTCols[uint8](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_uint16:
		item := MustTCols[uint16](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_uint32:
		item := MustTCols[uint32](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_uint64:
		item := MustTCols[uint64](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_float32:
		item := MustTCols[float32](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_float64:
		item := MustTCols[float64](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		item := MustBytesCols(fromVec)[0]
		for i := 0; i < length; i++ {
			AppendBytes(toVec, item, false, m)
		}

	case types.T_date:
		item := MustTCols[types.Date](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_datetime:
		item := MustTCols[types.Datetime](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_time:
		item := MustTCols[types.Time](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_timestamp:
		item := MustTCols[types.Timestamp](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_decimal64:
		item := MustTCols[types.Decimal64](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_decimal128:
		item := MustTCols[types.Decimal128](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	case types.T_uuid:
		item := MustTCols[types.Uuid](fromVec)[0]
		for i := 0; i < length; i++ {
			Append(toVec, item, false, m)
		}

	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("vec %v can not copy", fromVec))
	}

	return nil
}
