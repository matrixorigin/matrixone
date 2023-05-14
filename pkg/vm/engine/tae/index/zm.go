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
	"fmt"
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	ZMSize = 64
)

var MaxBytesValue []byte
var zeroZM = make([]byte, ZMSize)

func init() {
	MaxBytesValue = bytes.Repeat([]byte{0xff}, 31)
}

// [0,...29, 30, 31,...60, 61, 62, 63]
//
//	-------  --  --------  --  --  --
//	  min     |     max    |    |   |
//	       len(min)    len(max) |   |
//	                       reserved |
//	                              type
type ZM []byte

func NewZM(t types.T, scale int32) ZM {
	zm := ZM(make([]byte, ZMSize))
	zm.SetType(t)
	zm.SetScale(scale)
	return zm
}

func BuildZM(t types.T, v []byte) ZM {
	zm := ZM(make([]byte, ZMSize))
	zm.SetType(t)
	zm.doInit(v)
	return zm
}

func (zm ZM) ResetMinMax() {
	t := zm.GetType()
	scale := zm.GetScale()
	copy(zm[:], zeroZM)
	zm.SetType(t)
	zm.SetScale(scale)
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
	if zm.IsString() {
		_, _ = b.WriteString(fmt.Sprintf("ZM(%s)%d[%v,%v]",
			zm.GetType().String(), zm.GetScale(), string(zm.GetMinBuf()), string(zm.GetMaxBuf())))
	} else {
		_, _ = b.WriteString(fmt.Sprintf("ZM(%s)%d[%v,%v]",
			zm.GetType().String(), zm.GetScale(), zm.GetMin(), zm.GetMax()))
	}
	if zm.MaxTruncated() {
		_ = b.WriteByte('+')
	}
	if !zm.IsInited() {
		_, _ = b.WriteString("--")
	}
	return b.String()
}

func (zm ZM) Clone() ZM {
	cloned := make([]byte, ZMSize)
	copy(cloned[:], zm[:])
	return cloned
}

func (zm ZM) GetType() types.T {
	return types.T(zm[63])
}

func (zm ZM) IsString() bool {
	return zm.GetType().FixedLength() < 0
}

func (zm ZM) Valid() bool {
	return len(zm) == ZMSize && zm.IsInited()
}

func (zm ZM) SetType(t types.T) {
	zm[63] &= 0x00
	zm[63] |= byte(t)
	sz := t.FixedLength()
	if sz <= 0 {
		return
	}
	zm[61] = byte(sz)
	zm[30] = byte(sz)
}

func (zm ZM) SetScale(scale int32) {
	sz := 0x3f & byte(scale)
	zm[62] &= 0xc0
	zm[62] |= sz
}

func (zm ZM) GetScale() int32 {
	sz := 0x3f & zm[62]
	return int32(sz)
}

func (zm ZM) GetMin() any {
	if !zm.IsInited() {
		return nil
	}
	buf := zm.GetMinBuf()
	return zm.getValue(buf)
}

func (zm ZM) GetMax() any {
	if !zm.IsInited() {
		return nil
	}
	buf := zm.GetMaxBuf()
	return zm.getValue(buf)
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
	UpdateZMAny(zm, v)
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
			if isNull || zm.containsString(key) {
				visibility.AddInt(row)
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil)
	} else {
		op = func(key []byte, isNull bool, row int) (err error) {
			if isNull || zm.containsBytes(key) {
				visibility.AddInt(row)
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil)
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
			if isNull || zm.containsString(key) {
				err = moerr.GetOkExpectedEOB()
				ok = true
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil)
	} else {
		op = func(key []byte, isNull bool, _ int) (err error) {
			if isNull || zm.containsBytes(key) {
				err = moerr.GetOkExpectedEOB()
				ok = true
			}
			return
		}
		containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil)
	}
	return
}

// Optimize me later
func (zm ZM) containsBytes(k []byte) bool {
	t := types.T(zm[63])
	return compute.Compare(k, zm.GetMinBuf(), t, 0, 0) >= 0 &&
		compute.Compare(k, zm.GetMaxBuf(), t, 0, 0) <= 0
}

func (zm ZM) containsString(k []byte) bool {
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
		return zm.containsString(k.([]byte))
	}

	t := types.T(zm[63])
	v := types.EncodeValue(k, t)
	return zm.containsBytes(v)
}

func (zm ZM) ContainsKey(k []byte) bool {
	if !zm.IsInited() {
		return false
	}
	if zm.IsString() {
		return zm.containsString(k)
	}
	t := types.T(zm[63])
	return compute.Compare(k, zm.GetMinBuf(), t, 0, 0) >= 0 &&
		compute.Compare(k, zm.GetMaxBuf(), t, 0, 0) <= 0
}

func (zm ZM) IsInited() bool {
	return zm[62]&0x80 != 0
}

func (zm ZM) setInited() {
	zm[62] |= 0x80
}

func (zm ZM) getValue(buf []byte) any {
	switch types.T(zm[63]) {
	case types.T_bool:
		return types.DecodeFixed[bool](buf)
	case types.T_int8:
		return types.DecodeFixed[int8](buf)
	case types.T_int16:
		return types.DecodeFixed[int16](buf)
	case types.T_int32:
		return types.DecodeFixed[int32](buf)
	case types.T_int64:
		return types.DecodeFixed[int64](buf)
	case types.T_uint8:
		return types.DecodeFixed[uint8](buf)
	case types.T_uint16:
		return types.DecodeFixed[uint16](buf)
	case types.T_uint32:
		return types.DecodeFixed[uint32](buf)
	case types.T_uint64:
		return types.DecodeFixed[uint64](buf)
	case types.T_float32:
		return types.DecodeFixed[float32](buf)
	case types.T_float64:
		return types.DecodeFixed[float64](buf)
	case types.T_date:
		return types.DecodeFixed[types.Date](buf)
	case types.T_time:
		return types.DecodeFixed[types.Time](buf)
	case types.T_datetime:
		return types.DecodeFixed[types.Datetime](buf)
	case types.T_timestamp:
		return types.DecodeFixed[types.Timestamp](buf)
	case types.T_decimal64:
		return types.DecodeFixed[types.Decimal64](buf)
	case types.T_decimal128:
		return types.DecodeFixed[types.Decimal128](buf)
	case types.T_uuid:
		return types.DecodeFixed[types.Uuid](buf)
	case types.T_TS:
		return types.DecodeFixed[types.TS](buf)
	case types.T_Rowid:
		return types.DecodeFixed[types.Rowid](buf)
	case types.T_Blockid:
		return types.DecodeFixed[types.Rowid](buf)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		return buf
	}
	panic(fmt.Sprintf("unsupported type: %v", zm.GetType()))
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

func (zm ZM) compareCheck(o ZM) (ok bool) {
	if !zm.IsInited() || !o.IsInited() {
		return false
	}
	return zm.GetType() == o.GetType() || (zm.IsString() && o.IsString())
}

func (zm ZM) AnyGT(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	// zm.max > o.min
	ok = true
	res = compute.Compare(zm.GetMaxBuf(), o.GetMinBuf(), zm.GetType(), zm.GetScale(), o.GetScale()) > 0
	return
}

func (zm ZM) AnyGE(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	// zm.max >= o.min
	ok = true
	res = compute.Compare(zm.GetMaxBuf(), o.GetMinBuf(), zm.GetType(), zm.GetScale(), o.GetScale()) >= 0
	return
}

func (zm ZM) AnyLT(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	// zm.min < o.max
	ok = true
	res = compute.Compare(zm.GetMinBuf(), o.GetMaxBuf(), zm.GetType(), zm.GetScale(), o.GetScale()) < 0
	return
}

func (zm ZM) AnyLE(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	// zm.min <= o.max
	ok = true
	res = compute.Compare(zm.GetMinBuf(), o.GetMaxBuf(), zm.GetType(), zm.GetScale(), o.GetScale()) <= 0
	return
}

func (zm ZM) FastIntersect(o ZM) (res bool) {
	t := zm.GetType()
	// zm.max >= o.min && zm.min <= v2.max
	res = compute.Compare(zm.GetMaxBuf(), o.GetMinBuf(), t, zm.GetScale(), o.GetScale()) >= 0 &&
		compute.Compare(zm.GetMinBuf(), o.GetMaxBuf(), t, zm.GetScale(), o.GetScale()) <= 0
	return
}

func (zm ZM) Intersect(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	ok = true
	res = zm.FastIntersect(o)
	return
}

// both zm should be of type bool, otherwise, ok is false
// res is true only when zm.min == true and o.min == true
func (zm ZM) And(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	t := zm.GetType()
	if t != types.T_bool {
		ok = false
		return
	}
	ok = true
	if !types.DecodeBool(zm.GetMinBuf()) {
		return
	}
	res = types.DecodeBool(o.GetMinBuf())
	return
}

// both zm should be of type bool, otherwise, ok is false
// res is false only when zm.max == false and o.max == false
func (zm ZM) Or(o ZM) (res bool, ok bool) {
	if !zm.compareCheck(o) {
		ok = false
		return
	}
	t := zm.GetType()
	if t != types.T_bool {
		ok = false
		return
	}
	res, ok = true, true
	if !types.DecodeBool(zm.GetMaxBuf()) && !types.DecodeBool(o.GetMaxBuf()) {
		res = false
	}
	return
}

// max = v1.max+v2.max
// min = v1.min+v2.min
func ZMPlus(v1, v2 ZM) (res ZM, ok bool) {
	if !v1.compareCheck(v2) {
		ok = false
		return
	}
	// check supported type
	res = NewZM(v1.GetType(), v1.GetScale())
	ok = applyArithmetic(v1, v2, res, '+', v1.GetScale(), v2.GetScale())
	return
}

// max = v1.max-v2.min
// min = v1.max-v2.min
func ZMMinus(v1, v2 ZM) (res ZM, ok bool) {
	if !v1.compareCheck(v2) {
		ok = false
		return
	}
	// check supported type
	res = NewZM(v1.GetType(), v1.GetScale())
	ok = applyArithmetic(v1, v2, res, '-', v1.GetScale(), v2.GetScale())
	return
}

// v1 product v2 => p[r0,r1,r2,r3]
// min,max = Min(p),Max(p)
func ZMMulti(v1, v2 ZM) (res ZM, ok bool) {
	if !v1.compareCheck(v2) {
		ok = false
		return
	}
	// check supported type
	res = NewZM(v1.GetType(), v2.GetScale())
	ok = applyArithmetic(v1, v2, res, '*', v1.GetScale(), v2.GetScale())
	return
}

func applyArithmetic(v1, v2, res ZM, op byte, scale1, scale2 int32) (ok bool) {
	ok = true
	switch v1.GetType() {
	case types.T_int8:
		var minv, maxv int8
		switch op {
		case '+':
			maxv = types.DecodeInt8(v1.GetMaxBuf()) + types.DecodeInt8(v2.GetMaxBuf())
			minv = types.DecodeInt8(v1.GetMinBuf()) + types.DecodeInt8(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeInt8(v1.GetMaxBuf()) - types.DecodeInt8(v2.GetMinBuf())
			minv = types.DecodeInt8(v1.GetMinBuf()) - types.DecodeInt8(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeInt8(v1.GetMinBuf()), types.DecodeInt8(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeInt8(v2.GetMinBuf()), types.DecodeInt8(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeInt8(&minv))
		UpdateZM(res, types.EncodeInt8(&maxv))
	case types.T_int16:
		var minv, maxv int16
		switch op {
		case '+':
			maxv = types.DecodeInt16(v1.GetMaxBuf()) + types.DecodeInt16(v2.GetMaxBuf())
			minv = types.DecodeInt16(v1.GetMinBuf()) + types.DecodeInt16(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeInt16(v1.GetMaxBuf()) - types.DecodeInt16(v2.GetMinBuf())
			minv = types.DecodeInt16(v1.GetMinBuf()) - types.DecodeInt16(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeInt16(v1.GetMinBuf()), types.DecodeInt16(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeInt16(v2.GetMinBuf()), types.DecodeInt16(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeInt16(&minv))
		UpdateZM(res, types.EncodeInt16(&maxv))
	case types.T_int32:
		var minv, maxv int32
		switch op {
		case '+':
			maxv = types.DecodeInt32(v1.GetMaxBuf()) + types.DecodeInt32(v2.GetMaxBuf())
			minv = types.DecodeInt32(v1.GetMinBuf()) + types.DecodeInt32(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeInt32(v1.GetMaxBuf()) - types.DecodeInt32(v2.GetMinBuf())
			minv = types.DecodeInt32(v1.GetMinBuf()) - types.DecodeInt32(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeInt32(v1.GetMinBuf()), types.DecodeInt32(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeInt32(v2.GetMinBuf()), types.DecodeInt32(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeInt32(&minv))
		UpdateZM(res, types.EncodeInt32(&maxv))
	case types.T_int64:
		var minv, maxv int64
		switch op {
		case '+':
			maxv = types.DecodeInt64(v1.GetMaxBuf()) + types.DecodeInt64(v2.GetMaxBuf())
			minv = types.DecodeInt64(v1.GetMinBuf()) + types.DecodeInt64(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeInt64(v1.GetMaxBuf()) - types.DecodeInt64(v2.GetMinBuf())
			minv = types.DecodeInt64(v1.GetMinBuf()) - types.DecodeInt64(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeInt64(v1.GetMinBuf()), types.DecodeInt64(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeInt64(v2.GetMinBuf()), types.DecodeInt64(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeInt64(&minv))
		UpdateZM(res, types.EncodeInt64(&maxv))
	case types.T_uint8:
		var minv, maxv uint8
		switch op {
		case '+':
			maxv = types.DecodeUint8(v1.GetMaxBuf()) + types.DecodeUint8(v2.GetMaxBuf())
			minv = types.DecodeUint8(v1.GetMinBuf()) + types.DecodeUint8(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeUint8(v1.GetMaxBuf()) - types.DecodeUint8(v2.GetMinBuf())
			minv = types.DecodeUint8(v1.GetMinBuf()) - types.DecodeUint8(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeUint8(v1.GetMinBuf()), types.DecodeUint8(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeUint8(v2.GetMinBuf()), types.DecodeUint8(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeUint8(&minv))
		UpdateZM(res, types.EncodeUint8(&maxv))
	case types.T_uint16:
		var minv, maxv uint16
		switch op {
		case '+':
			maxv = types.DecodeUint16(v1.GetMaxBuf()) + types.DecodeUint16(v2.GetMaxBuf())
			minv = types.DecodeUint16(v1.GetMinBuf()) + types.DecodeUint16(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeUint16(v1.GetMaxBuf()) - types.DecodeUint16(v2.GetMinBuf())
			minv = types.DecodeUint16(v1.GetMinBuf()) - types.DecodeUint16(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeUint16(v1.GetMinBuf()), types.DecodeUint16(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeUint16(v2.GetMinBuf()), types.DecodeUint16(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeUint16(&minv))
		UpdateZM(res, types.EncodeUint16(&maxv))
	case types.T_uint32:
		var minv, maxv uint32
		switch op {
		case '+':
			maxv = types.DecodeUint32(v1.GetMaxBuf()) + types.DecodeUint32(v2.GetMaxBuf())
			minv = types.DecodeUint32(v1.GetMinBuf()) + types.DecodeUint32(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeUint32(v1.GetMaxBuf()) - types.DecodeUint32(v2.GetMinBuf())
			minv = types.DecodeUint32(v1.GetMinBuf()) - types.DecodeUint32(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeUint32(v1.GetMinBuf()), types.DecodeUint32(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeUint32(v2.GetMinBuf()), types.DecodeUint32(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeUint32(&minv))
		UpdateZM(res, types.EncodeUint32(&maxv))
	case types.T_uint64:
		var minv, maxv uint64
		switch op {
		case '+':
			maxv = types.DecodeUint64(v1.GetMaxBuf()) + types.DecodeUint64(v2.GetMaxBuf())
			minv = types.DecodeUint64(v1.GetMinBuf()) + types.DecodeUint64(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeUint64(v1.GetMaxBuf()) - types.DecodeUint64(v2.GetMinBuf())
			minv = types.DecodeUint64(v1.GetMinBuf()) - types.DecodeUint64(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeUint64(v1.GetMinBuf()), types.DecodeUint64(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeUint64(v2.GetMinBuf()), types.DecodeUint64(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeUint64(&minv))
		UpdateZM(res, types.EncodeUint64(&maxv))
	case types.T_float32:
		var minv, maxv float32
		switch op {
		case '+':
			maxv = types.DecodeFloat32(v1.GetMaxBuf()) + types.DecodeFloat32(v2.GetMaxBuf())
			minv = types.DecodeFloat32(v1.GetMinBuf()) + types.DecodeFloat32(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeFloat32(v1.GetMaxBuf()) - types.DecodeFloat32(v2.GetMinBuf())
			minv = types.DecodeFloat32(v1.GetMinBuf()) - types.DecodeFloat32(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeFloat32(v1.GetMinBuf()), types.DecodeFloat32(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeFloat32(v2.GetMinBuf()), types.DecodeFloat32(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeFloat32(&minv))
		UpdateZM(res, types.EncodeFloat32(&maxv))
	case types.T_float64:
		var minv, maxv float64
		switch op {
		case '+':
			maxv = types.DecodeFloat64(v1.GetMaxBuf()) + types.DecodeFloat64(v2.GetMaxBuf())
			minv = types.DecodeFloat64(v1.GetMinBuf()) + types.DecodeFloat64(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeFloat64(v1.GetMaxBuf()) - types.DecodeFloat64(v2.GetMinBuf())
			minv = types.DecodeFloat64(v1.GetMinBuf()) - types.DecodeFloat64(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeFloat64(v1.GetMinBuf()), types.DecodeFloat64(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeFloat64(v2.GetMinBuf()), types.DecodeFloat64(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeFloat64(&minv))
		UpdateZM(res, types.EncodeFloat64(&maxv))
	case types.T_date:
		var minv, maxv types.Date
		switch op {
		case '+':
			maxv = types.DecodeDate(v1.GetMaxBuf()) + types.DecodeDate(v2.GetMaxBuf())
			minv = types.DecodeDate(v1.GetMinBuf()) + types.DecodeDate(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeDate(v1.GetMaxBuf()) - types.DecodeDate(v2.GetMinBuf())
			minv = types.DecodeDate(v1.GetMinBuf()) - types.DecodeDate(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeDate(v1.GetMinBuf()), types.DecodeDate(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeDate(v2.GetMinBuf()), types.DecodeDate(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeDate(&minv))
		UpdateZM(res, types.EncodeDate(&maxv))
	case types.T_datetime:
		var minv, maxv types.Datetime
		switch op {
		case '+':
			maxv = types.DecodeDatetime(v1.GetMaxBuf()) + types.DecodeDatetime(v2.GetMaxBuf())
			minv = types.DecodeDatetime(v1.GetMinBuf()) + types.DecodeDatetime(v2.GetMinBuf())
		case '-':
			maxv = types.DecodeDatetime(v1.GetMaxBuf()) - types.DecodeDatetime(v2.GetMinBuf())
			minv = types.DecodeDatetime(v1.GetMinBuf()) - types.DecodeDatetime(v2.GetMaxBuf())
		case '*':
			v1_0, v1_1 := types.DecodeDatetime(v1.GetMinBuf()), types.DecodeDatetime(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeDatetime(v2.GetMinBuf()), types.DecodeDatetime(v2.GetMaxBuf())
			minv, maxv = compute.GetOrderedMinAndMax(v1_0*v2_0, v1_0*v2_1, v1_1*v2_0, v1_1*v2_1)
		default:
			ok = false
			return
		}
		UpdateZM(res, types.EncodeDatetime(&minv))
		UpdateZM(res, types.EncodeDatetime(&maxv))
	case types.T_decimal64:
		var minv, maxv types.Decimal64
		var newScale int32
		var err error
		switch op {
		case '+':
			maxv = types.DecodeDecimal64(v1.GetMaxBuf())
			if maxv, newScale, err = maxv.Add(types.DecodeDecimal64(v2.GetMaxBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
			minv = types.DecodeDecimal64(v1.GetMinBuf())
			if minv, _, err = minv.Add(types.DecodeDecimal64(v2.GetMinBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
		case '-':
			maxv = types.DecodeDecimal64(v1.GetMaxBuf())
			if maxv, newScale, err = maxv.Sub(types.DecodeDecimal64(v2.GetMinBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
			minv = types.DecodeDecimal64(v1.GetMinBuf())
			if minv, _, err = minv.Sub(types.DecodeDecimal64(v2.GetMaxBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
		case '*':
			rs := make([]types.Decimal64, 4)
			v1_0, v1_1 := types.DecodeDecimal64(v1.GetMinBuf()), types.DecodeDecimal64(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeDecimal64(v2.GetMinBuf()), types.DecodeDecimal64(v2.GetMaxBuf())
			if rs[0], newScale, err = v1_0.Mul(v2_0, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[1], _, err = v1_1.Mul(v2_0, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[2], _, err = v1_0.Mul(v2_1, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[3], _, err = v1_1.Mul(v2_1, scale1, scale2); err != nil {
				ok = false
				return
			}
			minv, maxv = compute.GetDecimal64MinAndMax(rs)
		default:
			ok = false
			return
		}
		res.SetScale(newScale)
		UpdateZM(res, types.EncodeDecimal64(&minv))
		UpdateZM(res, types.EncodeDecimal64(&maxv))
	case types.T_decimal128:
		var newScale int32
		var (
			err        error
			minv, maxv types.Decimal128
		)
		switch op {
		case '+':
			maxv = types.DecodeDecimal128(v1.GetMaxBuf())
			if maxv, newScale, err = maxv.Add(types.DecodeDecimal128(v2.GetMaxBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
			minv = types.DecodeDecimal128(v1.GetMinBuf())
			if minv, _, err = minv.Add(types.DecodeDecimal128(v2.GetMinBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
		case '-':
			maxv = types.DecodeDecimal128(v1.GetMaxBuf())
			if maxv, newScale, err = maxv.Sub(types.DecodeDecimal128(v2.GetMinBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
			minv = types.DecodeDecimal128(v1.GetMinBuf())
			if minv, _, err = minv.Sub(types.DecodeDecimal128(v2.GetMaxBuf()), scale1, scale2); err != nil {
				ok = false
				return
			}
		case '*':
			rs := make([]types.Decimal128, 4)
			v1_0, v1_1 := types.DecodeDecimal128(v1.GetMinBuf()), types.DecodeDecimal128(v1.GetMaxBuf())
			v2_0, v2_1 := types.DecodeDecimal128(v2.GetMinBuf()), types.DecodeDecimal128(v2.GetMaxBuf())
			if rs[0], newScale, err = v1_0.Mul(v2_0, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[1], _, err = v1_1.Mul(v2_0, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[2], _, err = v1_0.Mul(v2_1, scale1, scale2); err != nil {
				ok = false
				return
			}
			if rs[3], _, err = v1_1.Mul(v2_1, scale1, scale2); err != nil {
				ok = false
				return
			}
			minv, maxv = compute.GetDecimal128MinAndMax(rs)
		default:
			ok = false
			return
		}
		res.SetScale(newScale)
		UpdateZM(res, types.EncodeDecimal128(&minv))
		UpdateZM(res, types.EncodeDecimal128(&maxv))
	}
	return
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

func BatchUpdateZM(zm ZM, vs containers.Vector) (err error) {
	op := func(v []byte, isNull bool, _ int) (err error) {
		if isNull {
			return
		}
		UpdateZM(zm, v)
		return
	}
	containers.ForeachWindowBytes(vs, 0, vs.Length(), op, nil)
	return
}

func UpdateZM(zm ZM, v []byte) {
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
	scale := zm.GetScale()
	if compute.Compare(v, zm.GetMinBuf(), t, scale, scale) < 0 {
		zm.updateMinFixed(v)
	} else if compute.Compare(v, zm.GetMaxBuf(), t, scale, scale) > 0 {
		zm.updateMaxFixed(v)
	}
}

func UpdateZMAny(zm ZM, v any) {
	vv := types.EncodeValue(v, zm.GetType())
	UpdateZM(zm, vv)
}

func EncodeZM(zm *ZM) []byte {
	return *zm
}

func DecodeZM(buf []byte) ZM {
	return buf[:ZMSize]
}

func BoolToZM(v bool) ZM {
	zm := NewZM(types.T_bool, 0)
	buf := types.EncodeBool(&v)
	UpdateZM(zm, buf)
	return zm
}

func MustZMToVector(zm ZM, m *mpool.MPool) (vec *vector.Vector) {
	var err error
	if vec, err = ZMToVector(zm, m); err != nil {
		t := zm.GetType().ToType()
		t.Scale = zm.GetScale()
		vec = vector.NewConstNull(t, 2, m)
	}
	return vec
}

// if zm is not initialized, return a const null vector
// if zm is of type varlen and truncated, the max value is null
func ZMToVector(zm ZM, m *mpool.MPool) (vec *vector.Vector, err error) {
	t := zm.GetType().ToType()
	t.Scale = zm.GetScale()
	if !zm.IsInited() {
		vec = vector.NewConstNull(t, 2, m)
		return
	}

	vec = vector.NewVec(t)
	appendFn := vector.MakeAppendBytesFunc(vec)
	if err = appendFn(zm.GetMinBuf(), false, m); err != nil {
		vec.Free(m)
		vec = nil
		return
	}

	null := false
	if t.IsVarlen() && zm.MaxTruncated() {
		null = true
	}
	if err = appendFn(zm.GetMaxBuf(), null, m); err != nil {
		vec.Free(m)
		vec = nil
	}
	return
}

// if zm is not of length 2, return not initilized zm
func VectorToZM(vec *vector.Vector) (zm ZM) {
	t := vec.GetType()
	zm = NewZM(t.Oid, t.Scale)
	if vec.Length() != 2 {
		return
	}
	if vec.IsConstNull() || vec.GetNulls().Count() == 2 {
		return
	}
	if t.IsVarlen() {
		UpdateZM(zm, vec.GetBytesAt(0))
		nsp := vec.GetNulls()
		if nsp.Contains(1) {
			zm.updateMaxString(MaxBytesValue)
		} else {
			UpdateZM(zm, vec.GetBytesAt(1))
		}
	} else {
		data := vec.UnsafeGetRawData()
		if vec.IsConst() {
			UpdateZM(zm, data)
		} else {
			UpdateZM(zm, data[:len(data)/2])
			UpdateZM(zm, data[len(data)/2:])
		}
	}
	return
}
