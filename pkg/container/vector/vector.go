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

	// area for holding large strings.
	area []byte

	length int

	// tag for distinguish '0x00..' and 0x... and 0x... is binary
	// TODO: check whether isBin should be changed into array/bitmap
	// now we assumpt that it can only be true in the case of only one data in vector
	isBin bool
	// data of fixed length element, in case of varlen, the Varlena
	col *reflect.SliceHeader
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

func (v *Vector) SetLength(n int) {
	v.length = n
}

// Size of data, I think this function is inherently broken.  This
// Size is not meaningful other than used in (approximate) memory accounting.
func (v *Vector) Size() int {
	return v.length*v.typ.TypeSize() + len(v.area)
}

func (v *Vector) GetType() types.Type {
	return v.typ
}

func (v *Vector) GetNulls() *nulls.Nulls {
	return v.nsp
}

func (v *Vector) SetNulls(nsp *nulls.Nulls) {
	v.nsp = nsp
}

func (v *Vector) GetRawData() []byte {
	return (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length*v.typ.TypeSize()]
}

func (v *Vector) TryExpandNulls(n int) {
	if v.nsp == nil {
		v.nsp = &nulls.Nulls{Np: bitmap.New(0)}
	}
	nulls.TryExpand(v.nsp, n)
}

func New(class int, typ types.Type) *Vector {
	return &Vector{
		typ:   typ,
		class: class,
		nsp:   &nulls.Nulls{},
		col:   new(reflect.SliceHeader),
	}
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

// IsConstNull return true if the vector means a scalar Null.
// e.g.
//
//	a + Null, and the vector of right part will return true
func (v *Vector) IsConstNull() bool {
	return v.IsConst() && v.nsp != nil && nulls.Contains(v.nsp, 0)
}

func (v *Vector) Free(m *mpool.MPool) {
	// const vector's data & area allocate with nil,
	// so we can't free it by using mpool.
	if v.col != nil {
		m.Free((*(*[]byte)(unsafe.Pointer(v.col))))
	}
	if v.area != nil {
		m.Free(v.area)
	}
	v.col = nil
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
		data := (*(*[]byte)(unsafe.Pointer(v.col)))
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
			ndata := make([]byte, length)
			copy(ndata, data[:length])
			*(*[]byte)(unsafe.Pointer(v.col)) = ndata
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
			data = data[:length]
		}
	}
	return nil
}

func (v *Vector) UnmarshalBinaryWithMpool(data []byte, mp *mpool.MPool) error {
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
			ndata, err := mp.Alloc(length)
			if err != nil {
				return err
			}
			copy(ndata, data[:length])
			*(*[]byte)(unsafe.Pointer(v.col)) = ndata
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
			data = data[:length]
		}
	}
	return nil
}

// PreExtend use to expand the capacity of the vector
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}
	switch v.typ.Oid {
	case types.T_bool:
		return extend[bool](v, rows, mp)
	case types.T_int8:
		return extend[int8](v, rows, mp)
	case types.T_int16:
		return extend[int16](v, rows, mp)
	case types.T_int32:
		return extend[int32](v, rows, mp)
	case types.T_int64:
		return extend[int64](v, rows, mp)
	case types.T_uint8:
		return extend[uint8](v, rows, mp)
	case types.T_uint16:
		return extend[uint16](v, rows, mp)
	case types.T_uint32:
		return extend[uint32](v, rows, mp)
	case types.T_uint64:
		return extend[uint64](v, rows, mp)
	case types.T_float32:
		return extend[float32](v, rows, mp)
	case types.T_float64:
		return extend[float64](v, rows, mp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		return extend[types.Varlena](v, rows, mp)
	case types.T_date:
		return extend[types.Date](v, rows, mp)
	case types.T_datetime:
		return extend[types.Datetime](v, rows, mp)
	case types.T_time:
		return extend[types.Time](v, rows, mp)
	case types.T_timestamp:
		return extend[types.Timestamp](v, rows, mp)
	case types.T_decimal64:
		return extend[types.Decimal64](v, rows, mp)
	case types.T_decimal128:
		return extend[types.Decimal128](v, rows, mp)
	case types.T_uuid:
		return extend[types.Uuid](v, rows, mp)
	case types.T_TS:
		return extend[types.TS](v, rows, mp)
	case types.T_Rowid:
		return extend[types.Rowid](v, rows, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.PreExtend", v.typ))
	}
}

// Dup use to copy an identical vector
func (v *Vector) Dup(m *mpool.MPool) (*Vector, error) {
	var err error

	w := &Vector{
		typ:    v.typ,
		isBin:  v.isBin,
		class:  v.class,
		length: v.length,
		nsp:    v.nsp.Clone(),
		col:    new(reflect.SliceHeader),
	}

	sz := v.typ.TypeSize()
	data := (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length/sz]
	// Copy v.data, note that this should work for Varlena type
	// as because we will copy area next and offset len will stay
	// valid for long varlena.
	if len(data) > 0 {
		var ndata []byte

		if ndata, err = m.Alloc(int(len(data))); err != nil {
			return nil, err
		}
		copy(ndata, data)
		*(*[]byte)(unsafe.Pointer(w.col)) = ndata
	}
	if len(v.area) > 0 {
		if w.area, err = m.Alloc(int(len(v.area))); err != nil {
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
		vdata := (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length*sz]
		wdata := (*(*[]byte)(unsafe.Pointer(w.col)))[:v.length*sz]
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
func (v *Vector) UnionOne(w *Vector, sel int64, isNull bool, mp *mpool.MPool) error {
	if w.class == CONSTANT {
		sel = 0
	}
	switch v.typ.Oid {
	case types.T_bool:
		if isNull {
			return Append(v, false, true, mp)
		}
		return Append(v, MustTCols[bool](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_int8:
		if isNull {
			return Append(v, int8(0), true, mp)
		}
		return Append(v, MustTCols[int8](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_int16:
		if isNull {
			return Append(v, int16(0), true, mp)
		}
		return Append(v, MustTCols[int16](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_int32:
		if isNull {
			return Append(v, int32(0), true, mp)
		}
		return Append(v, MustTCols[int32](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_int64:
		if isNull {
			return Append(v, int64(0), true, mp)
		}
		return Append(v, MustTCols[int64](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_uint8:
		if isNull {
			return Append(v, uint8(0), true, mp)
		}
		return Append(v, MustTCols[uint8](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_uint16:
		if isNull {
			return Append(v, uint16(0), true, mp)
		}
		return Append(v, MustTCols[uint16](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_uint32:
		if isNull {
			return Append(v, uint32(0), true, mp)
		}
		return Append(v, MustTCols[uint32](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_uint64:
		if isNull {
			return Append(v, uint64(0), true, mp)
		}
		return Append(v, MustTCols[uint64](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_float32:
		if isNull {
			return Append(v, float32(0), true, mp)
		}
		return Append(v, MustTCols[float32](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_float64:
		if isNull {
			return Append(v, float64(0), true, mp)
		}
		return Append(v, MustTCols[float64](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		if isNull {
			return Append(v, types.Varlena{}, true, mp)
		}
		ws := MustTCols[types.Varlena](w)
		return AppendBytes(v, ws[sel].GetByteSlice(v.area), v.nsp.Contains(uint64(sel)), mp)
	case types.T_date:
		if isNull {
			return Append(v, types.Date(0), true, mp)
		}
		return Append(v, MustTCols[types.Date](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_datetime:
		if isNull {
			return Append(v, types.Datetime(0), true, mp)
		}
		return Append(v, MustTCols[types.Datetime](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_time:
		if isNull {
			return Append(v, types.Time(0), true, mp)
		}
		return Append(v, MustTCols[types.Time](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_timestamp:
		if isNull {
			return Append(v, types.Timestamp(0), true, mp)
		}
		return Append(v, MustTCols[types.Timestamp](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_decimal64:
		if isNull {
			return Append(v, types.Decimal64{}, true, mp)
		}
		return Append(v, MustTCols[types.Decimal64](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_decimal128:
		if isNull {
			return Append(v, types.Decimal128{}, true, mp)
		}
		return Append(v, MustTCols[types.Decimal128](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_uuid:
		if isNull {
			return Append(v, types.Uuid{}, true, mp)
		}
		return Append(v, MustTCols[types.Uuid](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_TS:
		if isNull {
			return Append(v, types.TS{}, true, mp)
		}
		return Append(v, MustTCols[types.TS](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	case types.T_Rowid:
		if isNull {
			return Append(v, types.Rowid{}, true, mp)
		}
		return Append(v, MustTCols[types.Rowid](w)[sel], v.nsp.Contains(uint64(sel)), mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}
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

func Append[T any](v *Vector, w T, isNull bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOne(v, w, isNull, m)
}

func AppendBytes(v *Vector, w []byte, isNull bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if isNull {
		return appendOneBytes(v, nil, true, m)
	}
	return appendOneBytes(v, w, false, m)
}

func AppendList[T any](v *Vector, ws []T, isNulls []bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendList(v, ws, isNulls, m)
}

func AppendBytesList(v *Vector, ws [][]byte, isNulls []bool, m *mpool.MPool) error {
	if m == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendBytesList(v, ws, isNulls, m)
}

func appendOne[T any](v *Vector, w T, isNull bool, m *mpool.MPool) error {
	if err := extend[T](v, 1, m); err != nil {
		return err
	}
	length := v.length
	v.length++
	col := MustTCols[T](v)
	if isNull {
		nulls.Add(v.nsp, uint64(v.length))
	} else {
		col[length] = w
	}
	return nil
}

func appendOneBytes(v *Vector, bs []byte, isNull bool, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if isNull {
		return appendOne(v, va, true, m)
	} else {
		va, v.area, err = types.BuildVarlena(bs, v.area, m)
		if err != nil {
			return err
		}
		return appendOne(v, va, false, m)
	}
}

func appendList[T any](v *Vector, ws []T, isNulls []bool, m *mpool.MPool) error {
	if err := extend[T](v, len(ws), m); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := MustTCols[T](v)
	for i, w := range ws {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(v.nsp, uint64(length+i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(v *Vector, ws [][]byte, isNulls []bool, m *mpool.MPool) error {
	var err error
	var va types.Varlena

	if err = extend[types.Varlena](v, len(ws), m); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := MustTCols[types.Varlena](v)
	for i, w := range ws {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(v.nsp, uint64(length+i))
		} else {
			va, v.area, err = types.BuildVarlena(w, v.area, m)
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
	olddata := (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length*sz]
	ns := len(sels)
	vs := MustTCols[T](v)
	data, err := m.Alloc(int(ns * v.GetType().TypeSize()))
	if err != nil {
		return err
	}
	*(*[]byte)(unsafe.Pointer(v.col)) = data
	ws := (*(*[]T)(unsafe.Pointer(v.col)))[:ns]
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
