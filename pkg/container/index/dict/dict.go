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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Dict struct {
	typ types.Type

	m      *mpool.MPool
	idx    reverseIndex
	unique *vector.Vector

	ref int
}

func New(typ types.Type, m *mpool.MPool) (*Dict, error) {
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
		d.unique = vector.New(types.T_uint64.ToType())
	} else {
		if idx, err = newVarReverseIndex(m); err != nil {
			return nil, err
		}
		d.unique = vector.New(types.T_varchar.ToType())
	}

	d.idx = idx
	d.ref = 1
	return d, nil
}

func (d *Dict) GetUnique() *vector.Vector {
	return d.unique
}

func (d *Dict) Cardinality() uint64 {
	return uint64(d.unique.Length())
}

func (d *Dict) Dup() *Dict {
	d.ref++
	return d
}

func (d *Dict) InsertBatch(data *vector.Vector) ([]uint16, error) {
	var ks any
	if d.fixed() {
		ks = d.encodeFixedData(data)
	} else {
		ks = d.encodeVarData(data)
	}

	values, err := d.idx.insert(ks)
	if err != nil {
		return nil, err
	}
	ips /* insertion points */ := make([]uint16, len(values))
	for i, v := range values {
		if int(v) > d.unique.Length() {
			if d.fixed() {
				err = d.unique.Append(ks.([]uint64)[i], false, d.m)
			} else {
				err = d.unique.Append(ks.([][]byte)[i], false, d.m)
			}
			if err != nil {
				return nil, err
			}
		}
		ips[i] = uint16(v)
	}
	return ips, nil
}

func (d *Dict) FindBatch(data *vector.Vector) []uint16 {
	var ks any
	if d.fixed() {
		ks = d.encodeFixedData(data)
	} else {
		ks = d.encodeVarData(data)
	}
	values := d.idx.find(ks)

	poses := make([]uint16, len(values))
	for i, v := range values {
		poses[i] = uint16(v)
	}
	return poses
}

func (d *Dict) FindData(pos uint16) *vector.Vector {
	if d.fixed() {
		return d.findFixedData(int(pos))
	} else {
		return d.findVarData(int(pos))
	}
}

func (d *Dict) Free() {
	if d.ref == 0 {
		return
	}
	d.ref--
	if d.ref > 0 {
		return
	}

	if d.unique != nil {
		d.unique.Free(d.m)
	}
	if d.idx != nil {
		d.idx.free()
	}
}

func (d *Dict) fixed() bool { return !d.typ.IsString() }

func (d *Dict) encodeFixedData(data *vector.Vector) []uint64 {
	us := make([]uint64, data.Length())
	switch d.typ.Oid {
	case types.T_bool:
		col := vector.MustTCols[bool](data)
		for i, v := range col {
			if v {
				us[i] = 1
			}
		}
	case types.T_int32:
		col := vector.MustTCols[int32](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_int64:
		col := vector.MustTCols[int64](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_uint32:
		col := vector.MustTCols[uint32](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_uint64:
		copy(us, vector.MustTCols[uint64](data))
	case types.T_float32:
		col := vector.MustTCols[float32](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_float64:
		col := vector.MustTCols[float64](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_decimal64:
		col := vector.MustTCols[types.Decimal64](data)
		for i, v := range col {
			us[i] = types.DecodeUint64(types.EncodeDecimal64(&v))
		}
	case types.T_date:
		col := vector.MustTCols[types.Date](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_datetime:
		col := vector.MustTCols[types.Datetime](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	case types.T_timestamp:
		col := vector.MustTCols[types.Timestamp](data)
		for i, v := range col {
			us[i] = uint64(v)
		}
	}
	return us
}

func (d *Dict) encodeVarData(data *vector.Vector) [][]byte {
	return vector.GetBytesVectorValues(data)
}

func (d *Dict) findFixedData(pos int) *vector.Vector {
	v := vector.NewConst(d.typ, 1)
	data := d.getFixedData(pos)
	switch d.typ.Oid {
	case types.T_bool:
		val := false
		if data == 1 {
			val = true
		}
		vector.MustTCols[bool](v)[0] = val
	case types.T_int32:
		vector.MustTCols[int32](v)[0] = int32(data)
	case types.T_int64:
		vector.MustTCols[int64](v)[0] = int64(data)
	case types.T_uint32:
		vector.MustTCols[uint32](v)[0] = uint32(data)
	case types.T_uint64:
		vector.MustTCols[uint64](v)[0] = uint64(data)
	case types.T_float32:
		vector.MustTCols[float32](v)[0] = float32(data)
	case types.T_float64:
		vector.MustTCols[float64](v)[0] = float64(data)
	case types.T_decimal64:
		val := types.DecodeDecimal64(types.EncodeUint64(&data))
		vector.MustTCols[types.Decimal64](v)[0] = val
	case types.T_date:
		vector.MustTCols[types.Date](v)[0] = types.Date(data)
	case types.T_datetime:
		vector.MustTCols[types.Datetime](v)[0] = types.Datetime(data)
	case types.T_timestamp:
		vector.MustTCols[types.Timestamp](v)[0] = types.Timestamp(data)
	}
	return v
}

func (d *Dict) findVarData(pos int) *vector.Vector {
	return vector.NewConstBytes(d.typ, 1, d.getVarData(pos), d.m)
}

func (d *Dict) getFixedData(n int) uint64 {
	return vector.MustTCols[uint64](d.unique)[n-1]
}

func (d *Dict) getVarData(n int) []byte {
	return d.unique.GetBytes(int64(n - 1))
}
