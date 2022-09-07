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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func MustTCols[T types.FixedSizeT](v *Vector) []T {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any {
		return nil
	}

	if t, ok := v.Col.([]T); ok {
		return t
	}
	panic("unexpected parameter types were received")
}

func MustBytesCols(v *Vector) [][]byte {
	varcol := MustTCols[types.Varlena](v)
	ret := make([][]byte, len(varcol))
	for i := range varcol {
		ret[i] = (&varcol[i]).GetByteSlice(v.area)
	}
	return ret
}

func MustStrCols(v *Vector) []string {
	varcol := MustTCols[types.Varlena](v)
	ret := make([]string, len(varcol))
	for i := range varcol {
		ret[i] = (&varcol[i]).GetString(v.area)
	}
	return ret
}

func (v *Vector) colFromData() {
	if v.Typ.Oid == types.T_tuple || v.Typ.Oid == types.T_any {
		// No op
	} else if v.GetType().IsVarlen() {
		v.Col = DecodeFixedCol[types.Varlena](v, types.VarlenaSize)
	} else {
		// The followng switch attach the correct type to v.Col
		// even though v.Col is only an interface.
		tlen := v.GetType().TypeSize()
		switch v.Typ.Oid {
		case types.T_bool:
			v.Col = DecodeFixedCol[bool](v, tlen)
		case types.T_int8:
			v.Col = DecodeFixedCol[int8](v, tlen)
		case types.T_int16:
			v.Col = DecodeFixedCol[int16](v, tlen)
		case types.T_int32:
			v.Col = DecodeFixedCol[int32](v, tlen)
		case types.T_int64:
			v.Col = DecodeFixedCol[int64](v, tlen)
		case types.T_uint8:
			v.Col = DecodeFixedCol[uint8](v, tlen)
		case types.T_uint16:
			v.Col = DecodeFixedCol[uint16](v, tlen)
		case types.T_uint32:
			v.Col = DecodeFixedCol[uint32](v, tlen)
		case types.T_uint64:
			v.Col = DecodeFixedCol[uint64](v, tlen)
		case types.T_float32:
			v.Col = DecodeFixedCol[float32](v, tlen)
		case types.T_float64:
			v.Col = DecodeFixedCol[float64](v, tlen)
		case types.T_decimal64:
			v.Col = DecodeFixedCol[types.Decimal64](v, tlen)
		case types.T_decimal128:
			v.Col = DecodeFixedCol[types.Decimal128](v, tlen)
		case types.T_date:
			v.Col = DecodeFixedCol[types.Date](v, tlen)
		case types.T_datetime:
			v.Col = DecodeFixedCol[types.Datetime](v, tlen)
		case types.T_timestamp:
			v.Col = DecodeFixedCol[types.Timestamp](v, tlen)
		case types.T_TS:
			v.Col = DecodeFixedCol[types.TS](v, tlen)
		case types.T_Rowid:
			v.Col = DecodeFixedCol[types.Rowid](v, tlen)
		default:
			panic("unknown type")
		}
	}
}

func (v *Vector) setupColFromData(start, end int) {
	if v.Typ.Oid == types.T_tuple {
		vec := v.Col.([][]interface{})
		v.Col = vec[start:end]
	} else if v.GetType().IsVarlen() {
		v.Col = DecodeFixedCol[types.Varlena](v, types.VarlenaSize)[start:end]
	} else {
		// The followng switch attach the correct type to v.Col
		// even though v.Col is only an interface.
		tlen := v.GetType().TypeSize()
		switch v.Typ.Oid {
		case types.T_bool:
			v.Col = DecodeFixedCol[bool](v, tlen)[start:end]
		case types.T_int8:
			v.Col = DecodeFixedCol[int8](v, tlen)[start:end]
		case types.T_int16:
			v.Col = DecodeFixedCol[int16](v, tlen)[start:end]
		case types.T_int32:
			v.Col = DecodeFixedCol[int32](v, tlen)[start:end]
		case types.T_int64:
			v.Col = DecodeFixedCol[int64](v, tlen)[start:end]
		case types.T_uint8:
			v.Col = DecodeFixedCol[uint8](v, tlen)[start:end]
		case types.T_uint16:
			v.Col = DecodeFixedCol[uint16](v, tlen)[start:end]
		case types.T_uint32:
			v.Col = DecodeFixedCol[uint32](v, tlen)[start:end]
		case types.T_uint64:
			v.Col = DecodeFixedCol[uint64](v, tlen)[start:end]
		case types.T_float32:
			v.Col = DecodeFixedCol[float32](v, tlen)[start:end]
		case types.T_float64:
			v.Col = DecodeFixedCol[float64](v, tlen)[start:end]
		case types.T_decimal64:
			v.Col = DecodeFixedCol[types.Decimal64](v, tlen)[start:end]
		case types.T_decimal128:
			v.Col = DecodeFixedCol[types.Decimal128](v, tlen)[start:end]
		case types.T_date:
			v.Col = DecodeFixedCol[types.Date](v, tlen)[start:end]
		case types.T_datetime:
			v.Col = DecodeFixedCol[types.Datetime](v, tlen)[start:end]
		case types.T_timestamp:
			v.Col = DecodeFixedCol[types.Timestamp](v, tlen)[start:end]
		case types.T_TS:
			v.Col = DecodeFixedCol[types.TS](v, tlen)[start:end]
		case types.T_Rowid:
			v.Col = DecodeFixedCol[types.Rowid](v, tlen)[start:end]
		default:
			panic("unknown type")
		}
	}
}

func (v *Vector) encodeColToByteSlice() []byte {
	switch v.GetType().Oid {
	case types.T_bool:
		return types.EncodeBoolSlice(v.Col.([]bool))
	case types.T_int8:
		return types.EncodeInt8Slice(v.Col.([]int8))
	case types.T_int16:
		return types.EncodeInt16Slice(v.Col.([]int16))
	case types.T_int32:
		return types.EncodeInt32Slice(v.Col.([]int32))
	case types.T_int64:
		return types.EncodeInt64Slice(v.Col.([]int64))
	case types.T_uint8:
		return types.EncodeUint8Slice(v.Col.([]uint8))
	case types.T_uint16:
		return types.EncodeUint16Slice(v.Col.([]uint16))
	case types.T_uint32:
		return types.EncodeUint32Slice(v.Col.([]uint32))
	case types.T_uint64:
		return types.EncodeUint64Slice(v.Col.([]uint64))
	case types.T_float32:
		return types.EncodeFloat32Slice(v.Col.([]float32))
	case types.T_float64:
		return types.EncodeFloat64Slice(v.Col.([]float64))
	case types.T_decimal64:
		return types.EncodeDecimal64Slice(v.Col.([]types.Decimal64))
	case types.T_decimal128:
		return types.EncodeDecimal128Slice(v.Col.([]types.Decimal128))
	case types.T_date:
		return types.EncodeDateSlice(v.Col.([]types.Date))
	case types.T_datetime:
		return types.EncodeDatetimeSlice(v.Col.([]types.Datetime))
	case types.T_timestamp:
		return types.EncodeTimestampSlice(v.Col.([]types.Timestamp))
	case types.T_TS:
		return types.EncodeFixedSlice(v.Col.([]types.TS), types.TxnTsSize)
	case types.T_Rowid:
		return types.EncodeFixedSlice(v.Col.([]types.Rowid), types.RowidSize)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json:
		return types.EncodeVarlenaSlice(v.Col.([]types.Varlena))
	case types.T_tuple:
		bs, _ := types.Encode(v.Col.([][]interface{}))
		return bs
	default:
		panic("unknow type when encode vector column")
	}
}

// XXX extend will entend the vector's Data to accormordate rows more entry.
// XXX we do not fix null map, Huh?
func (v *Vector) extend(rows int, m *mheap.Mheap) error {
	origSz := len(v.Data)
	growSz := rows * v.GetType().TypeSize()
	tgtSz := origSz + growSz
	if tgtSz <= cap(v.Data) {
		// XXX v.Data can hold data, just grow.
		// XXX do not use Grow, because this case it will still malloc and copy
		v.Data = v.Data[:tgtSz]
	} else if v.Data == nil {
		// XXX mheap Relloc is broken, cannot handle nil, so we Alloc here.
		// XXX The interface on size, int/int64 u, FUBAR.
		data, err := mheap.Alloc(m, int64(tgtSz))
		if err != nil {
			return err
		}
		v.Data = data[:tgtSz]
	} else {
		data, err := mheap.Grow(m, v.Data, int64(tgtSz))
		if err != nil {
			return err
		}
		mheap.Free(m, v.Data)
		v.Data = data[:tgtSz]
	}
	// Setup v.Col
	v.setupColFromData(0, tgtSz/v.GetType().TypeSize())
	return nil
}
