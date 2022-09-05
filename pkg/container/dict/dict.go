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

package dict

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

var (
	ErrTypeMismatch = errors.New("the type of data is mismatch with dict")
)

type Dict struct {
	typ types.Type

	m      *mheap.Mheap
	idx    reverseIndex
	unique *vector.Vector
}

func New(typ types.Type, m *mheap.Mheap) (*Dict, error) {
	// typ cannot be `T_decimal128`, `T_json`
	d := &Dict{
		typ: typ,
		m:   m,
	}

	var idx reverseIndex
	var err error

	if d.fixed() { // check whether the type of data is fixed-length or variable-length
		if idx, err = newFixedReverseIndex(m); err != nil {
			return nil, err
		}
		d.unique = vector.New(types.Type{Oid: types.T_uint64})
	} else {
		if idx, err = newVarReverseIndex(m); err != nil {
			return nil, err
		}
		d.unique = vector.New(types.Type{Oid: types.T_varchar})
	}

	d.idx = idx
	return d, nil
}

func (d *Dict) Typ() types.Type { return d.typ }

func (d *Dict) GetUnique() *vector.Vector { return d.unique }

func (d *Dict) InsertBatch(data *vector.Vector) ([]uint64, error) {
	if d.Typ().Oid != data.Typ.Oid {
		return nil, ErrTypeMismatch
	}

	var ks any
	if d.fixed() {
		ks = d.encodeFixedData(data)
	} else {
		ks = d.encodeVarData(data)
	}

	ips /*insertion points*/, err := d.idx.insert(ks)
	if err != nil {
		return nil, err
	}
	for i := range ips {
		if int(ips[i]) > d.unique.Count() {
			if d.fixed() {
				err = d.unique.Append(ks.([]uint64)[i], d.m)
			} else {
				err = d.unique.Append(ks.([][]byte)[i], d.m)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return ips, nil
}

func (d *Dict) FindBatch(data *vector.Vector) ([]uint64, error) {
	if d.Typ().Oid != data.Typ.Oid {
		return nil, ErrTypeMismatch
	}

	var ks any
	if d.fixed() {
		ks = d.encodeFixedData(data)
	} else {
		ks = d.encodeVarData(data)
	}
	return d.idx.find(ks), nil
}

func (d *Dict) FindData(pos uint64) *vector.Vector {
	if d.fixed() {
		return d.decodeFixedData(d.getFixedData(int(pos)))
	} else {
		data := d.getVarData(int(pos))
		v := vector.NewConst(d.typ, 1)
		v.Data = data
		col := v.Col.(*types.Bytes)
		col.Data = data
		col.Offsets[0] = 0
		col.Lengths[0] = uint32(len(data))
		return v
	}
}

func (d *Dict) fixed() bool { return !d.typ.IsString() }

func (d *Dict) encodeFixedData(data *vector.Vector) []uint64 {
	us := make([]uint64, data.Count())
	switch d.typ.TypeSize() {
	case 1:
		slice := types.DecodeUint8Slice(data.Data)
		for i := range slice {
			us[i] = uint64(slice[i])
		}
	case 2:
		slice := types.DecodeUint16Slice(data.Data)
		for i := range slice {
			us[i] = uint64(slice[i])
		}
	case 4:
		slice := types.DecodeUint32Slice(data.Data)
		for i := range slice {
			us[i] = uint64(slice[i])
		}
	case 8:
		copy(us, types.DecodeUint64Slice(data.Data))
	}
	return us
}

func (d *Dict) encodeVarData(data *vector.Vector) [][]byte {
	bs := make([][]byte, data.Count())
	for i := 0; i < data.Count(); i++ {
		col := data.Col.(*types.Bytes)
		bs[i] = col.Data[col.Offsets[i] : col.Offsets[i]+col.Lengths[i]]
	}
	return bs
}

func (d *Dict) decodeFixedData(data uint64) *vector.Vector {
	v := vector.NewConst(d.typ, 1)
	switch d.Typ().Oid {
	case types.T_bool:
		val := false
		if data == 1 {
			val = true
		}
		v.Col.([]bool)[0] = val
	case types.T_int8:
		v.Col.([]int8)[0] = int8(data)
	case types.T_int16:
		v.Col.([]int16)[0] = int16(data)
	case types.T_int32:
		v.Col.([]int32)[0] = int32(data)
	case types.T_int64:
		v.Col.([]int64)[0] = int64(data)
	case types.T_uint8:
		v.Col.([]uint8)[0] = uint8(data)
	case types.T_uint16:
		v.Col.([]uint16)[0] = uint16(data)
	case types.T_uint32:
		v.Col.([]uint32)[0] = uint32(data)
	case types.T_uint64:
		v.Col.([]uint64)[0] = uint64(data)
	case types.T_float32:
		v.Col.([]float32)[0] = float32(data)
	case types.T_float64:
		v.Col.([]float64)[0] = float64(data)
	case types.T_decimal64:
		val := types.DecodeDecimal64(types.EncodeUint64(data))
		v.Col.([]types.Decimal64)[0] = val
	case types.T_date:
		v.Col.([]types.Date)[0] = types.Date(data)
	case types.T_datetime:
		v.Col.([]types.Datetime)[0] = types.Datetime(data)
	case types.T_timestamp:
		v.Col.([]types.Timestamp)[0] = types.Timestamp(data)
	}
	return v
}

func (d *Dict) getFixedData(n int) uint64 {
	return d.unique.Col.([]uint64)[n-1]
}

func (d *Dict) getVarData(n int) []byte {
	col := d.unique.Col.(*types.Bytes)
	start, end := col.Offsets[n-1], col.Offsets[n-1]+col.Lengths[n-1]
	return append([]byte{}, col.Data[start:end]...)
}
