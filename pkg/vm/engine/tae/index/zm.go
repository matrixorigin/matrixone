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
	"fmt"
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	ZMSize = 64
)

// [0,...29, 30, 31,...60, 61, 62, 63]
//
//	-------  --  --------  --  --  --
//	  min     |     max    |    |   |
//	       len(min)    len(max) |   |
//	                       reserved |
//	                              type
type ZM []byte

// TODO: remove me later
func NewZoneMap(typ types.Type) *ZM {
	return NewZM(typ.Oid)
}

func NewZM(t types.T) *ZM {
	zm := ZM(make([]byte, ZMSize))
	zm.SetType(t)
	return &zm
}

func BuildZM(t types.T, v []byte) ZM {
	zm := ZM(make([]byte, ZMSize))
	zm.SetType(t)
	zm.doInit(v)
	return zm
}

func (zm ZM) doInit(v []byte) {
	if zm.IsString() {
		zm.updateMinString(v)
		zm.updateMaxString(v)
	} else {
		zm.updateMinFixed(v)
		zm.updateMaxFixed(v)
	}
	zm.setInited()
}

func (zm ZM) String() string {
	var b strings.Builder
	_, _ = b.WriteString(fmt.Sprintf("ZM(%s)[%v,%v]",
		zm.GetType().String(), zm.GetMin(), zm.GetMax()))
	if zm.MaxTruncated() {
		_ = b.WriteByte('+')
	}
	return b.String()
}

func (zm ZM) GetType() types.T {
	return types.T(zm[63])
}

func (zm ZM) IsString() bool {
	return zm.GetType().FixedLength() < 0
}

func (zm ZM) SetType(t types.T) {
	zm[63] &= 0x00
	zm[63] |= byte(t)
}

func (zm ZM) GetMin() any {
	return zm.getValue(true)
}

func (zm ZM) GetMax() any {
	return zm.getValue(false)
}

func (zm ZM) GetMinBuf() []byte {
	return zm[0 : zm[30]&0x1f]
}

func (zm ZM) GetMaxBuf() []byte {
	return zm[31 : 31+zm[61]&0x1f]
}

func (zm ZM) GetBuf() []byte {
	return zm
}

func (zm ZM) MaxTruncated() bool {
	return zm[61]&0x80 != 0
}

func (zm ZM) Encode() []byte {
	return zm[:]
}

func (zm ZM) Marshal() ([]byte, error) {
	buf := make([]byte, ZMSize)
	copy(buf, zm[:])
	return buf, nil
}

func (zm ZM) Unmarshal(buf []byte) (err error) {
	copy(zm[:], buf[:ZMSize])
	return
}

// TODO: remove me later
func (zm ZM) Update(v any) (err error) {
	UpdateZMAny(&zm, v, zm.GetType().ToType())
	return
}

func (zm ZM) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	if !zm.IsInited() {
		return
	}
	visibility = roaring.New()
	var op containers.ItOpT[[]byte]
	if zm.IsString() {
		op = func(key []byte, isNull bool, row int) (err error) {
			if isNull || zm.ContainsString(key) {
				visibility.AddInt(row)
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op)
	} else {
		op = func(key []byte, isNull bool, row int) (err error) {
			if isNull || zm.ContainsFixed(key) {
				visibility.AddInt(row)
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op)
	}
	ok = !visibility.IsEmpty()
	return
}

func (zm ZM) FastContainsAny(keys containers.Vector) (ok bool) {
	if !zm.IsInited() {
		return false
	}
	var op containers.ItOpT[[]byte]
	if zm.IsString() {
		op = func(key []byte, isNull bool, _ int) (err error) {
			if isNull || zm.ContainsString(key) {
				err = moerr.GetOkExpectedEOB()
				ok = true
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op)
	} else {
		op = func(key []byte, isNull bool, _ int) (err error) {
			if isNull || zm.ContainsFixed(key) {
				err = moerr.GetOkExpectedEOB()
				ok = true
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op)
	}
	return
}

// Optimize me later
func (zm ZM) ContainsFixed(k []byte) bool {
	t := types.T(zm[63])
	return compute.Compare(k, zm.GetMinBuf(), t) >= 0 &&
		compute.Compare(k, zm.GetMaxBuf(), t) <= 0
}

func (zm ZM) ContainsString(k []byte) bool {
	if zm.MaxTruncated() {
		return true
	}
	return compute.CompareBytes(k, zm.GetMinBuf()) >= 0 &&
		compute.CompareBytes(k, zm.GetMaxBuf()) <= 0
}

// TODO: remove me later
func (zm ZM) Contains(k any) bool {
	if !zm.IsInited() {
		return false
	}
	if zm.IsString() {
		return zm.ContainsString(k.([]byte))
	}

	t := types.T(zm[63])
	v := types.EncodeValue(k, t.ToType())
	return zm.ContainsFixed(v)
}

func (zm ZM) ContainsKey(k []byte) bool {
	if !zm.IsInited() {
		return false
	}
	if zm.IsString() {
		return zm.ContainsString(k)
	}
	t := types.T(zm[63])
	return compute.Compare(k, zm.GetMinBuf(), t) >= 0 &&
		compute.Compare(k, zm.GetMaxBuf(), t) <= 0
}

func (zm ZM) IsInited() bool {
	return zm[62]&0x80 != 0
}

func (zm ZM) setInited() {
	zm[62] |= 0x80
}

func (zm ZM) getValue(min bool) any {
	offset := 0
	if !min {
		offset = 31
	}
	switch types.T(zm[63]) {
	case types.T_bool:
		return types.DecodeFixed[bool](zm[offset : offset+1])
	case types.T_int8:
		return types.DecodeFixed[int8](zm[offset : offset+1])
	case types.T_int16:
		return types.DecodeFixed[int16](zm[offset : offset+2])
	case types.T_int32:
		return types.DecodeFixed[int32](zm[offset : offset+4])
	case types.T_int64:
		return types.DecodeFixed[int64](zm[offset : offset+8])
	case types.T_uint8:
		return types.DecodeFixed[uint8](zm[offset : offset+1])
	case types.T_uint16:
		return types.DecodeFixed[uint16](zm[offset : offset+2])
	case types.T_uint32:
		return types.DecodeFixed[uint32](zm[offset : offset+4])
	case types.T_uint64:
		return types.DecodeFixed[uint64](zm[offset : offset+8])
	case types.T_float32:
		return types.DecodeFixed[float32](zm[offset : offset+4])
	case types.T_float64:
		return types.DecodeFixed[float64](zm[offset : offset+8])
	case types.T_date:
		return types.DecodeFixed[types.Date](zm[offset : offset+types.DateSize])
	case types.T_time:
		return types.DecodeFixed[types.Time](zm[offset : offset+types.TimeSize])
	case types.T_datetime:
		return types.DecodeFixed[types.Datetime](zm[offset : offset+types.DatetimeSize])
	case types.T_timestamp:
		return types.DecodeFixed[types.Timestamp](zm[offset : offset+types.TimestampSize])
	case types.T_decimal64:
		return types.DecodeFixed[types.Decimal64](zm[offset : offset+types.Decimal64Size])
	case types.T_decimal128:
		return types.DecodeFixed[types.Decimal128](zm[offset : offset+types.Decimal128Size])
	case types.T_uuid:
		return types.DecodeFixed[types.Uuid](zm[offset : offset+types.UuidSize])
	case types.T_TS:
		return types.DecodeFixed[types.TS](zm[offset : offset+types.TxnTsSize])
	case types.T_Rowid:
		return types.DecodeFixed[types.Rowid](zm[offset : offset+types.RowidSize])
	case types.T_Blockid:
		return types.DecodeFixed[types.Rowid](zm[offset : offset+types.BlockidSize])
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		length := int(zm[offset+30] & 0x01f)
		return []byte(zm)[offset : offset+length]
	default:
		panic(fmt.Sprintf("unsupported type: %v", zm.GetType()))
	}
}

func (zm ZM) updateMinString(v []byte) {
	size := len(v)
	if size > 30 {
		size = 30
	}
	copy(zm[:], v[:size])
	zm[30] = byte(size)
}

func (zm ZM) updateMaxString(v []byte) {
	size := len(v)
	var flag byte
	if size > 30 {
		size = 30
		copy(zm[31:], v[:size])
		if hasMaxPrefix(v) {
			flag |= 0x80
		} else {
			adjustBytes(zm[31:61])
		}
	} else {
		copy(zm[31:], v[:size])
	}
	flag |= byte(size)
	zm[61] = flag
}

func (zm ZM) updateMinFixed(v []byte) {
	copy(zm[:], v)
	zm[30] = byte(len(v))
}

func (zm ZM) updateMaxFixed(v []byte) {
	copy(zm[31:], v)
	zm[61] = byte(len(v))
}

func hasMaxPrefix(bs []byte) bool {
	for i := 0; i < 3; i++ {
		if types.DecodeFixed[uint64](bs[i*8:(i+1)*8]) != math.MaxUint64 {
			return false
		}
	}
	if types.DecodeFixed[uint32](bs[24:28]) != math.MaxUint32 {
		return false
	}
	return types.DecodeFixed[uint16](bs[28:30]) == math.MaxUint16
}

func adjustBytes(bs []byte) {
	for i := len(bs) - 1; i >= 0; i-- {
		bs[i] += 1
		if bs[i] != 0 {
			break
		}
	}
}

func BatchUpdateZM(zm *ZM, vs containers.Vector) (err error) {
	op := func(v []byte, isNull bool, _ int) (err error) {
		if isNull {
			return
		}
		UpdateZM(zm, v)
		return
	}
	containers.ForeachWindowBytes(vs, 0, vs.Length(), op)
	return
}

func UpdateZM(zm *ZM, v []byte) {
	if !zm.IsInited() {
		zm.doInit(v)
	}
	if zm.IsString() {
		if compute.CompareBytes(v, zm.GetMinBuf()) < 0 {
			zm.updateMinString(v)
		} else if compute.CompareBytes(v, zm.GetMaxBuf()) > 0 {
			zm.updateMaxString(v)
		}
		return
	}
	t := zm.GetType()
	if compute.Compare(v, zm.GetMinBuf(), t) < 0 {
		zm.updateMinFixed(v)
	} else if compute.Compare(v, zm.GetMaxBuf(), t) > 0 {
		zm.updateMaxFixed(v)
	}
}

func UpdateZMAny(zm *ZM, v any, typ types.Type) {
	vv := types.EncodeValue(v, typ)
	UpdateZM(zm, vv)
}

func EncodeZM(zm *ZM) []byte {
	return *zm
}

func DecodeZM(buf []byte) ZM {
	return buf[:ZMSize]
}
