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

package index

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
)

type ZoneMap struct {
	typ      types.Type
	min, max any
	inited   bool
}

func NewZoneMap(typ types.Type) *ZoneMap {
	zm := &ZoneMap{typ: typ}
	return zm
}

func LoadZoneMapFrom(data []byte) (zm *ZoneMap, err error) {
	zm = new(ZoneMap)
	err = zm.Unmarshal(data)
	return
}

func (zm *ZoneMap) GetType() types.Type {
	return zm.typ
}

func (zm *ZoneMap) Update(v any) (err error) {
	if !zm.inited {
		zm.min = v
		zm.max = v
		zm.inited = true
		return
	}
	if compute.CompareGeneric(v, zm.max, zm.typ) > 0 {
		zm.max = v
	} else if compute.CompareGeneric(v, zm.min, zm.typ) < 0 {
		zm.min = v
	}
	return
}

func (zm *ZoneMap) BatchUpdate(KeysCtx *KeysCtx) error {
	if !zm.typ.Eq(KeysCtx.Keys.Typ) {
		return ErrWrongType
	}
	update := func(v any, _ uint32) error {
		return zm.Update(v)
	}
	if err := compute.ProcessVector(KeysCtx.Keys, KeysCtx.Start, KeysCtx.Count, update, nil); err != nil {
		return err
	}
	return nil
}

func (zm *ZoneMap) Contains(key any) (ok bool) {
	if !zm.inited {
		return
	}
	if compute.CompareGeneric(key, zm.max, zm.typ) > 0 || compute.CompareGeneric(key, zm.min, zm.typ) < 0 {
		return
	}
	ok = true
	return
}

func (zm *ZoneMap) ContainsAny(keys *vector.Vector) (visibility *roaring.Bitmap, ok bool) {
	if !zm.inited {
		return
	}
	visibility = roaring.NewBitmap()
	row := uint32(0)
	process := func(key any, _ uint32) (err error) {
		if compute.CompareGeneric(key, zm.max, zm.typ) <= 0 && compute.CompareGeneric(key, zm.min, zm.typ) >= 0 {
			visibility.Add(row)
		}
		row++
		return
	}
	if err := compute.ProcessVector(keys, 0, uint32(vector.Length(keys)), process, nil); err != nil {
		panic(err)
	}
	if visibility.GetCardinality() != 0 {
		ok = true
	}
	return
}

func (zm *ZoneMap) SetMax(v any) {
	if !zm.inited {
		zm.min = v
		zm.max = v
		zm.inited = true
		return
	}
	if compute.CompareGeneric(v, zm.max, zm.typ) > 0 {
		zm.max = v
	}
}

func (zm *ZoneMap) GetMax() any {
	return zm.max
}

func (zm *ZoneMap) SetMin(v any) {
	if !zm.inited {
		zm.min = v
		zm.max = v
		zm.inited = true
		return
	}
	if compute.CompareGeneric(v, zm.min, zm.typ) < 0 {
		zm.min = v
	}
}

func (zm *ZoneMap) GetMin() any {
	return zm.min
}

// func (zm *ZoneMap) Print() string {
// 	// default int32
// 	s := "<ZM>\n["
// 	s += strconv.Itoa(int(zm.min.(int32)))
// 	s += ","
// 	s += strconv.Itoa(int(zm.max.(int32)))
// 	s += "]\n"
// 	s += "</ZM>"
// 	return s
// }

func (zm *ZoneMap) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if _, err = w.Write(encoding.EncodeType(zm.typ)); err != nil {
		return
	}
	if !zm.inited {
		if _, err = w.Write(encoding.EncodeInt8(0)); err != nil {
			return
		}
		buf = w.Bytes()
		return
	}
	if _, err = w.Write(encoding.EncodeInt8(1)); err != nil {
		return
	}
	switch zm.typ.Oid {
	case types.T_bool:
		if _, err = w.Write(encoding.EncodeBool(zm.min.(bool))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeBool(zm.max.(bool))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_int8:
		if _, err = w.Write(encoding.EncodeInt8(zm.min.(int8))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeInt8(zm.max.(int8))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_int16:
		if _, err = w.Write(encoding.EncodeInt16(zm.min.(int16))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeInt16(zm.max.(int16))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_int32:
		if _, err = w.Write(encoding.EncodeInt32(zm.min.(int32))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeInt32(zm.max.(int32))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_int64:
		if _, err = w.Write(encoding.EncodeInt64(zm.min.(int64))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeInt64(zm.max.(int64))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_uint8:
		if _, err = w.Write(encoding.EncodeUint8(zm.min.(uint8))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeUint8(zm.max.(uint8))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_uint16:
		if _, err = w.Write(encoding.EncodeUint16(zm.min.(uint16))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeUint16(zm.max.(uint16))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_uint32:
		if _, err = w.Write(encoding.EncodeUint32(zm.min.(uint32))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeUint32(zm.max.(uint32))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_uint64:
		if _, err = w.Write(encoding.EncodeUint64(zm.min.(uint64))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeUint64(zm.max.(uint64))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_float32:
		if _, err = w.Write(encoding.EncodeFloat32(zm.min.(float32))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeFloat32(zm.max.(float32))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_float64:
		if _, err = w.Write(encoding.EncodeFloat64(zm.min.(float64))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeFloat64(zm.max.(float64))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_date:
		if _, err = w.Write(encoding.EncodeDate(zm.min.(types.Date))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeDate(zm.max.(types.Date))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_datetime:
		if _, err = w.Write(encoding.EncodeDatetime(zm.min.(types.Datetime))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeDatetime(zm.max.(types.Datetime))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_timestamp:
		if _, err = w.Write(encoding.EncodeTimestamp(zm.min.(types.Timestamp))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeTimestamp(zm.max.(types.Timestamp))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_decimal64:
		if _, err = w.Write(encoding.EncodeDecimal64(zm.min.(types.Decimal64))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeDecimal64(zm.max.(types.Decimal64))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_decimal128:
		if _, err = w.Write(encoding.EncodeDecimal128(zm.min.(types.Decimal128))); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeDecimal128(zm.max.(types.Decimal128))); err != nil {
			return
		}
		buf = w.Bytes()
		return
	case types.T_char, types.T_varchar, types.T_json:
		minv := zm.min.([]byte)
		maxv := zm.max.([]byte)
		if _, err = w.Write(encoding.EncodeInt16(int16(len(minv)))); err != nil {
			return
		}
		if _, err = w.Write(minv); err != nil {
			return
		}
		if _, err = w.Write(encoding.EncodeInt16(int16(len(maxv)))); err != nil {
			return
		}
		if _, err = w.Write(maxv); err != nil {
			return
		}
		buf = w.Bytes()
		return
	}
	panic("unsupported")
}

func (zm *ZoneMap) Unmarshal(buf []byte) error {
	zm.typ = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	init := encoding.DecodeInt8(buf[:1])
	buf = buf[1:]
	if init == 0 {
		zm.inited = false
		return nil
	}
	zm.inited = true
	switch zm.typ.Oid {
	case types.T_bool:
		zm.min = encoding.DecodeBool(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeBool(buf[:1])
		return nil
	case types.T_int8:
		zm.min = encoding.DecodeInt8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeInt8(buf[:1])
		return nil
	case types.T_int16:
		zm.min = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeInt16(buf[:2])
		return nil
	case types.T_int32:
		zm.min = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeInt32(buf[:4])
		return nil
	case types.T_int64:
		zm.min = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeInt64(buf[:8])
		return nil
	case types.T_uint8:
		zm.min = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeUint8(buf[:1])
		return nil
	case types.T_uint16:
		zm.min = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeUint16(buf[:2])
		return nil
	case types.T_uint32:
		zm.min = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeUint32(buf[:4])
		return nil
	case types.T_uint64:
		zm.min = encoding.DecodeUint64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeUint64(buf[:8])
		return nil
	case types.T_float32:
		zm.min = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeFloat32(buf[:4])
		return nil
	case types.T_float64:
		zm.min = encoding.DecodeFloat64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeFloat64(buf[:8])
		return nil
	case types.T_date:
		zm.min = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		return nil
	case types.T_datetime:
		zm.min = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		return nil
	case types.T_timestamp:
		zm.min = encoding.DecodeTimestamp(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeTimestamp(buf[:8])
		buf = buf[8:]
		return nil
	case types.T_decimal64:
		zm.min = encoding.DecodeDecimal64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeDecimal64(buf[:8])
		buf = buf[8:]
		return nil
	case types.T_decimal128:
		zm.min = encoding.DecodeDecimal128(buf[:16])
		buf = buf[16:]
		zm.max = encoding.DecodeDecimal128(buf[:16])
		buf = buf[16:]
		return nil
	case types.T_char, types.T_varchar, types.T_json:
		lenminv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		minBuf := make([]byte, int(lenminv))
		copy(minBuf, buf[:int(lenminv)])
		buf = buf[int(lenminv):]

		lenmaxv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		maxBuf := make([]byte, int(lenmaxv))
		copy(maxBuf, buf[:int(lenmaxv)])
		zm.min = minBuf
		zm.max = maxBuf
		return nil
	}
	return nil
}

func (zm *ZoneMap) GetMemoryUsage() uint64 {
	buf, err := zm.Marshal()
	if err != nil {
		panic(err)
	}
	return uint64(len(buf))
}
