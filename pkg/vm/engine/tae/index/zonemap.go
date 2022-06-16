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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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
	if err := compute.ApplyOpToColumnWithOffset(KeysCtx.Keys, KeysCtx.Start, KeysCtx.Count, update, nil); err != nil {
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
	if err := compute.ApplyOpToColumn(keys, process, nil); err != nil {
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
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		minv := zm.min.([]byte)
		maxv := zm.max.([]byte)
		if _, err = types.WriteValues(
			&w,
			int16(len(minv)),
			minv,
			int16(len(maxv)),
			maxv); err != nil {
			return
		}
		buf = w.Bytes()
	default:
		if _, err = types.WriteValues(&w, zm.min, zm.max); err != nil {
			return
		}
		buf = w.Bytes()
	}
	return
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
	case types.Type_BOOL:
		zm.min = encoding.DecodeBool(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeBool(buf[:1])
		return nil
	case types.Type_INT8:
		zm.min = encoding.DecodeInt8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeInt8(buf[:1])
		return nil
	case types.Type_INT16:
		zm.min = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeInt16(buf[:2])
		return nil
	case types.Type_INT32:
		zm.min = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeInt32(buf[:4])
		return nil
	case types.Type_INT64:
		zm.min = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeInt64(buf[:8])
		return nil
	case types.Type_UINT8:
		zm.min = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeUint8(buf[:1])
		return nil
	case types.Type_UINT16:
		zm.min = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeUint16(buf[:2])
		return nil
	case types.Type_UINT32:
		zm.min = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeUint32(buf[:4])
		return nil
	case types.Type_UINT64:
		zm.min = encoding.DecodeUint64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeUint64(buf[:8])
		return nil
	case types.Type_FLOAT32:
		zm.min = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeFloat32(buf[:4])
		return nil
	case types.Type_FLOAT64:
		zm.min = encoding.DecodeFloat64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeFloat64(buf[:8])
		return nil
	case types.Type_DATE:
		zm.min = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		return nil
	case types.Type_DATETIME:
		zm.min = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		return nil
	case types.Type_TIMESTAMP:
		zm.min = encoding.DecodeTimestamp(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeTimestamp(buf[:8])
		buf = buf[8:]
		return nil
	case types.Type_DECIMAL64:
		zm.min = encoding.DecodeDecimal64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeDecimal64(buf[:8])
		buf = buf[8:]
		return nil
	case types.Type_DECIMAL128:
		zm.min = encoding.DecodeDecimal128(buf[:16])
		buf = buf[16:]
		zm.max = encoding.DecodeDecimal128(buf[:16])
		buf = buf[16:]
		return nil
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
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
