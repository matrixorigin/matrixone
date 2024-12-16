/*
 * tuple.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Portions of this file are additionally subject to the following
 * copyright.
 *
 * Copyright (C) 2022 Matrix Origin.
 *
 * Modified the behavior of the tuple.
 */

package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

/*
 * Tuple type is used for encoding multiColumns to single column
 * for example:
 * we create table (a int8, b int8, primary key(a, b))
 * we need to create composite primary key to combine a and b
 * we have one method to generate the primary key([]byte):
 *    var a int8 = 1, var b int8 = 1
 *    packer := newPacker()
 *    packer.EncodeInt8(a)
 *    packer.EncodeInt8(b)
 *    var byteArr []byte
 *    byteArr = packer.GetBuf()
 * we have one method recover from []byte to tuple
 *    var tuple Tuple
 *    tuple, err = Unpack(byteArr)
 *    tuple[0] = 1
 *    tuple[1] = 1
 *
 * in the composite_primary_key_util.go, we default use method2 to encode tupleElement
 */

type TupleElement any

type Tuple []TupleElement

func (tp Tuple) String() string {
	return printTuple(tp)
}

func (tp Tuple) ErrString(scales []int32) string {
	var res strings.Builder
	if len(tp) > 1 {
		res.WriteString("(")
	}
	for i, t := range tp {
		switch t := t.(type) {
		case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			res.WriteString(fmt.Sprintf("%v", t))
		case []byte:
			res.WriteString(*(*string)(unsafe.Pointer(&t)))
		case Date:
			res.WriteString(fmt.Sprintf("%v", t.String()))
		case Time:
			res.WriteString(fmt.Sprintf("%v", t.String()))
		case Datetime:
			res.WriteString(fmt.Sprintf("%v", t.String()))
		case Timestamp:
			res.WriteString(fmt.Sprintf("%v", t.String()))
		case Decimal64:
			res.WriteString(fmt.Sprintf("%v", t.Format(scales[i])))
		case Decimal128:
			res.WriteString(fmt.Sprintf("%v", t.Format(scales[i])))
		default:
			res.WriteString(fmt.Sprintf("%v", t))
		}
		if i != len(tp)-1 {
			res.WriteString(",")
		}
	}
	if len(tp) > 1 {
		res.WriteString(")")
	}
	return res.String()
}

func (tp Tuple) SQLStrings(scales []int32) []string {
	res := make([]string, 0, len(tp))
	for i, t := range tp {
		switch t := t.(type) {
		case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			res = append(res, fmt.Sprintf("%v", t))
		case []byte:
			s := *(*string)(unsafe.Pointer(&t))
			res = append(res, strconv.Quote(s))
		case Uuid:
			res = append(res, fmt.Sprintf("'%v'", t.String()))
		case Date:
			res = append(res, fmt.Sprintf("'%v'", t.String()))
		case Time:
			res = append(res, fmt.Sprintf("'%v'", t.String()))
		case Datetime:
			res = append(res, fmt.Sprintf("'%v'", t.String()))
		case Timestamp:
			res = append(res, fmt.Sprintf("'%v'", t.String()))
		case Decimal64:
			res = append(res, fmt.Sprintf("%v", t.Format(scales[i])))
		case Decimal128:
			res = append(res, fmt.Sprintf("%v", t.Format(scales[i])))
		default:
			res = append(res, fmt.Sprintf("%v", t))
		}
	}
	return res
}

func printTuple(tuple Tuple) string {
	var res strings.Builder
	for i, t := range tuple {
		switch t := t.(type) {
		case bool:
			res.WriteString(fmt.Sprintf("(bool: %v)", t))
		case int8:
			res.WriteString(fmt.Sprintf("(int8: %v)", t))
		case int16:
			res.WriteString(fmt.Sprintf("(int16: %v)", t))
		case int32:
			res.WriteString(fmt.Sprintf("(int32: %v)", t))
		case int64:
			res.WriteString(fmt.Sprintf("(int64: %v)", t))
		case uint8:
			res.WriteString(fmt.Sprintf("(uint8: %v)", t))
		case uint16:
			res.WriteString(fmt.Sprintf("(uint16: %v)", t))
		case uint32:
			res.WriteString(fmt.Sprintf("(uint32: %v)", t))
		case uint64:
			res.WriteString(fmt.Sprintf("(uint64: %v)", t))
		case Date:
			res.WriteString(fmt.Sprintf("(date: %v)", t.String()))
		case Time:
			res.WriteString(fmt.Sprintf("(time: %v)", t.String()))
		case Datetime:
			res.WriteString(fmt.Sprintf("(datetime: %v)", t.String()))
		case Timestamp:
			res.WriteString(fmt.Sprintf("(timestamp: %v)", t.String()))
		case Decimal64:
			res.WriteString(fmt.Sprintf("(decimal64: %v)", t.Format(0)))
		case Decimal128:
			res.WriteString(fmt.Sprintf("(decimal128: %v)", t.Format(0)))
		case []byte:
			res.WriteString(fmt.Sprintf("([]byte: %v --str--: %v)", t, string(t)))
		case float32:
			res.WriteString(fmt.Sprintf("(float32: %v)", t))
		case float64:
			res.WriteString(fmt.Sprintf("(float64: %v)", t))
		default:
			res.WriteString(fmt.Sprintf("(unorganizedType: %v)", t))
		}
		if i != len(tuple)-1 {
			res.WriteString(",")
		}
	}
	res.WriteString(")")
	return res.String()
}

const (
	nilCode        = 0x00
	bytesCode      = 0x01
	bytesMaxCode   = 0x02
	intZeroCode    = 0x14
	float32Code    = 0x20
	float64Code    = 0x21
	falseCode      = 0x26
	trueCode       = 0x27
	int8Code       = 0x28
	int16Code      = 0x29
	int32Code      = 0x3a
	int64Code      = 0x3b
	uint8Code      = 0x3c
	uint16Code     = 0x3d
	uint32Code     = 0x3e
	uint64Code     = 0x40
	dateCode       = 0x41
	datetimeCode   = 0x42
	timestampCode  = 0x43
	decimal64Code  = 0x44
	decimal128Code = 0x45
	stringTypeCode = 0x46
	timeCode       = 0x47
	enumCode       = 0x50 // TODO: reorder the list to put timeCode next to date type code?
	bitCode        = 0x51
	uuidCode       = 0x52
)

var sizeLimits = []uint64{
	1<<(0*8) - 1,
	1<<(1*8) - 1,
	1<<(2*8) - 1,
	1<<(3*8) - 1,
	1<<(4*8) - 1,
	1<<(5*8) - 1,
	1<<(6*8) - 1,
	1<<(7*8) - 1,
	1<<(8*8) - 1,
}

func bisectLeft(u uint64) int {
	var n int
	for sizeLimits[n] < u {
		n++
	}
	return n
}

func adjustFloatBytes(b []byte, encode bool) {
	if (encode && b[0]&0x80 != 0x00) || (!encode && b[0]&0x80 == 0x00) {
		// Negative numbers: flip all of the bytes.
		for i := 0; i < len(b); i++ {
			b[i] = b[i] ^ 0xff
		}
	} else {
		// Positive number: flip just the sign bit.
		b[0] = b[0] ^ 0x80
	}
}

func findTerminator(b []byte) int {
	bp := b
	var length int

	for {
		idx := bytes.IndexByte(bp, 0x00)
		length += idx
		if idx+1 == len(bp) || bp[idx+1] != 0xFF {
			break
		}
		length += 2
		bp = bp[idx+2:]
	}
	if length == -1 {
		return len(b)
	}

	return length
}

func decodeBytes(b []byte) ([]byte, int) {
	idx := findTerminator(b[1:])
	return bytes.ReplaceAll(b[1:idx+1], []byte{0x00, 0xFF}, []byte{0x00}), idx + 2
}

func decodeInt(code byte, b []byte) (interface{}, int) {
	if b[0] == intZeroCode {
		switch code {
		case int8Code:
			return int8(0), 1
		case int16Code:
			return int16(0), 1
		case int32Code:
			return int32(0), 1
		case dateCode:
			return Date(0), 1
		case datetimeCode:
			return Datetime(0), 1
		case timestampCode:
			return Timestamp(0), 1
		default:
			return int64(0), 1
		}
	}

	var neg bool

	n := int(b[0]) - intZeroCode
	if n < 0 {
		n = -n
		neg = true
	}

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret int64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	if neg {
		switch code {
		case int8Code:
			return int8(ret - int64(sizeLimits[n])), n + 1
		case int16Code:
			return int16(ret - int64(sizeLimits[n])), n + 1
		case int32Code:
			return int32(ret - int64(sizeLimits[n])), n + 1
		case dateCode:
			return Date(ret - int64(sizeLimits[n])), n + 1
		case datetimeCode:
			return Datetime(ret - int64(sizeLimits[n])), n + 1
		case timestampCode:
			return Timestamp(ret - int64(sizeLimits[n])), n + 1
		default:
			return ret - int64(sizeLimits[n]), n + 1
		}
	}
	switch code {
	case int8Code:
		return int8(ret), n + 1
	case int16Code:
		return int16(ret), n + 1
	case int32Code:
		return int32(ret), n + 1
	case dateCode:
		return Date(ret), n + 1
	case datetimeCode:
		return Datetime(ret), n + 1
	case timestampCode:
		return Timestamp(ret), n + 1
	case enumCode:
		return Enum(ret), n + 1
	default:
		return ret, n + 1
	}
}

func decodeUint(code byte, b []byte) (interface{}, int) {
	if b[0] == intZeroCode {
		switch code {
		case uint8Code:
			return uint8(0), 1
		case uint16Code:
			return uint16(0), 1
		case uint32Code:
			return uint32(0), 1
		}
		return uint64(0), 1
	}
	n := int(b[0]) - intZeroCode

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret uint64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	switch code {
	case uint8Code:
		return uint8(ret), n + 1
	case uint16Code:
		return uint16(ret), n + 1
	case uint32Code:
		return uint32(ret), n + 1
	default:
		return ret, n + 1
	}
}

func decodeFloat32(b []byte) (float32, int) {
	bp := make([]byte, 4)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float32
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return ret, 5
}

func decodeFloat64(b []byte) (float64, int) {
	bp := make([]byte, 8)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return ret, 9
}

func decodeDecimal64(b []byte) (Decimal64, int) {
	bp := make([]byte, 8)
	copy(bp, b[:])
	bp[0] ^= 0x80
	for i := 0; i < 4; i++ {
		bp[i] ^= bp[7-i]
		bp[7-i] ^= bp[i]
		bp[i] ^= bp[7-i]
	}
	ret := *(*Decimal64)(unsafe.Pointer(&bp[0]))
	return ret, 9
}

func decodeDecimal128(b []byte) (Decimal128, int) {
	bp := make([]byte, 16)
	copy(bp, b[:])
	bp[0] ^= 0x80
	for i := 0; i < 8; i++ {
		bp[i] ^= bp[15-i]
		bp[15-i] ^= bp[i]
		bp[i] ^= bp[15-i]
	}
	ret := *(*Decimal128)(unsafe.Pointer(&bp[0]))
	return ret, 17
}

func decodeUuid(b []byte) (Uuid, int) {
	var ret Uuid
	copy(ret[:], b[1:])
	return ret, 17
}

var DecodeTuple = decodeTuple

func decodeTuple(b []byte) (Tuple, int, []T, error) {
	var t Tuple

	var i int
	schema := make([]T, 0)
	for i < len(b) {
		var el interface{}
		var off int

		switch {
		case b[i] == nilCode:
			schema = append(schema, T_any)
			el = nil
			off = 1
		case b[i] == int8Code:
			schema = append(schema, T_int8)
			el, off = decodeInt(int8Code, b[i+1:])
			off += 1
		case b[i] == int16Code:
			schema = append(schema, T_int16)
			el, off = decodeInt(int16Code, b[i+1:])
			off += 1
		case b[i] == int32Code:
			schema = append(schema, T_int32)
			el, off = decodeInt(int32Code, b[i+1:])
			off += 1
		case b[i] == int64Code:
			schema = append(schema, T_int64)
			el, off = decodeInt(int64Code, b[i+1:])
			off += 1
		case b[i] == uint8Code:
			schema = append(schema, T_uint8)
			el, off = decodeUint(uint8Code, b[i+1:])
			off += 1
		case b[i] == uint16Code:
			schema = append(schema, T_uint16)
			el, off = decodeUint(uint16Code, b[i+1:])
			off += 1
		case b[i] == uint32Code:
			schema = append(schema, T_uint32)
			el, off = decodeUint(uint32Code, b[i+1:])
			off += 1
		case b[i] == uint64Code:
			schema = append(schema, T_uint64)
			el, off = decodeUint(uint64Code, b[i+1:])
			off += 1
		case b[i] == trueCode:
			schema = append(schema, T_bool)
			el = true
			off = 1
		case b[i] == falseCode:
			schema = append(schema, T_bool)
			el = false
			off = 1
		case b[i] == float32Code:
			schema = append(schema, T_float32)
			el, off = decodeFloat32(b[i:])
		case b[i] == float64Code:
			schema = append(schema, T_float64)
			el, off = decodeFloat64(b[i:])
		case b[i] == dateCode:
			schema = append(schema, T_date)
			el, off = decodeInt(dateCode, b[i+1:])
			off += 1
		case b[i] == datetimeCode:
			schema = append(schema, T_datetime)
			el, off = decodeInt(datetimeCode, b[i+1:])
			off += 1
		case b[i] == timestampCode:
			schema = append(schema, T_timestamp)
			el, off = decodeInt(timestampCode, b[i+1:])
			off += 1
		case b[i] == timeCode:
			schema = append(schema, T_time)
			el, off = decodeInt(timeCode, b[i+1:])
			off += 1
		case b[i] == decimal64Code:
			schema = append(schema, T_decimal64)
			el, off = decodeDecimal64(b[i+1:])
		case b[i] == decimal128Code:
			schema = append(schema, T_decimal128)
			el, off = decodeDecimal128(b[i+1:])
		case b[i] == stringTypeCode:
			schema = append(schema, T_varchar)
			el, off = decodeBytes(b[i+1:])
			off += 1
		case b[i] == bitCode:
			schema = append(schema, T_bit)
			el, off = decodeUint(uint64Code, b[i+1:])
			off += 1
		case b[i] == enumCode:
			schema = append(schema, T_enum)
			// TODO: need to verify @YANGGMM
			el, off = decodeUint(uint16Code, b[i+1:])
			off += 1
		case b[i] == uuidCode:
			schema = append(schema, T_uuid)
			el, off = decodeUuid(b[i:])
			// off += 1
		default:
			return nil, i, nil, moerr.NewInternalErrorNoCtxf("unable to decode tuple element with unknown typecode %02x", b[i])
		}
		t = append(t, el)
		i += off
	}

	return t, i, schema, nil
}

func Unpack(b []byte) (Tuple, error) {
	t, _, _, err := decodeTuple(b)
	return t, err
}

func UnpackWithSchema(b []byte) (Tuple, []T, error) {
	t, _, schema, err := decodeTuple(b)
	return t, schema, err
}

func StringifyTuple(b []byte, types []plan.Type) ([]string, error) {
	items := make([]string, len(types))

	offset := 0
	for i := 0; i < len(items) && offset < len(b); i++ {
		var (
			item    string
			itemLen int
		)

		switch {
		case b[offset] == nilCode:
			item = "null"
			itemLen = 1
		case b[offset] == int8Code:
			item, itemLen = stringifyInt(int8Code, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == int16Code:
			item, itemLen = stringifyInt(int16Code, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == int32Code:
			item, itemLen = stringifyInt(int32Code, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == int64Code:
			item, itemLen = stringifyInt(int64Code, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == uint8Code:
			item, itemLen = stringifyUint(uint8Code, b[offset+1:])
			itemLen += 1
		case b[offset] == uint16Code:
			item, itemLen = stringifyUint(uint16Code, b[offset+1:])
			itemLen += 1
		case b[offset] == uint32Code:
			item, itemLen = stringifyUint(uint32Code, b[offset+1:])
			itemLen += 1
		case b[offset] == uint64Code:
			item, itemLen = stringifyUint(uint64Code, b[offset+1:])
			itemLen += 1
		case b[offset] == trueCode:
			item = "true"
			itemLen = 1
		case b[offset] == falseCode:
			item = "false"
			itemLen = 1
		case b[offset] == float32Code:
			item, itemLen = stringifyFloat32(b[offset:])
		case b[offset] == float64Code:
			item, itemLen = stringifyFloat64(b[offset:])
		case b[offset] == dateCode:
			item, itemLen = stringifyInt(dateCode, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == datetimeCode:
			item, itemLen = stringifyInt(datetimeCode, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == timestampCode:
			item, itemLen = stringifyInt(timestampCode, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == timeCode:
			item, itemLen = stringifyInt(timeCode, b[offset+1:], types[i].Scale)
			itemLen += 1
		case b[offset] == decimal64Code:
			item, itemLen = stringifyDecimal64(b[offset+1:], types[i].Scale)
		case b[offset] == decimal128Code:
			item, itemLen = stringifyDecimal128(b[offset+1:], types[i].Scale)
		case b[offset] == stringTypeCode:
			item, itemLen = stringifyBytes(b[offset+1:])
			itemLen += 1
		case b[offset] == bitCode:
			item, itemLen = stringifyUint(uint64Code, b[offset+1:])
			itemLen += 1
		case b[offset] == enumCode:
			// TODO: need to verify @YANGGMM
			item, itemLen = stringifyUint(uint16Code, b[offset+1:])
			itemLen += 1
		case b[offset] == uuidCode:
			item, itemLen = stringifyUuid(b[offset:])
			// off += 1
		default:
			return nil, moerr.NewInternalErrorNoCtxf("unable to decode tuple element with unknown typecode %02x", b[offset])
		}

		items[i] = item
		offset += itemLen
	}

	return items, nil
}

func stringifyBytes(b []byte) (string, int) {
	idx := findTerminator(b[1:])
	return string(bytes.ReplaceAll(b[1:idx+1], []byte{0x00, 0xFF}, []byte{0x00})), idx + 2
}

func stringifyInt(code byte, b []byte, scale int32) (string, int) {
	loc := time.Local
	if b[0] == intZeroCode {
		switch code {
		case dateCode:
			return Date(0).String(), 1
		case timeCode:
			return Time(0).String2(scale), 1
		case datetimeCode:
			return Datetime(0).String2(scale), 1
		case timestampCode:
			return Timestamp(0).String2(loc, scale), 1
		default:
			return "0", 1
		}
	}

	var neg bool

	n := int(b[0]) - intZeroCode
	if n < 0 {
		n = -n
		neg = true
	}

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret int64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	if neg {
		switch code {
		case dateCode:
			return Date(ret - int64(sizeLimits[n])).String(), n + 1
		case timeCode:
			return Time(ret - int64(sizeLimits[n])).String2(scale), n + 1
		case datetimeCode:
			return Datetime(ret - int64(sizeLimits[n])).String2(scale), n + 1
		case timestampCode:
			return Timestamp(ret-int64(sizeLimits[n])).String2(loc, scale), n + 1
		default:
			return strconv.FormatInt(ret-int64(sizeLimits[n]), 10), n + 1
		}
	}
	switch code {
	case dateCode:
		return Date(ret).String(), n + 1
	case timeCode:
		return Time(ret).String2(scale), n + 1
	case datetimeCode:
		return Datetime(ret).String2(scale), n + 1
	case timestampCode:
		return Timestamp(ret).String2(loc, scale), n + 1
	//case enumCode:
	//	return Enum(ret), n + 1
	default:
		return strconv.FormatInt(ret, 10), n + 1
	}
}

func stringifyUint(code byte, b []byte) (string, int) {
	if b[0] == intZeroCode {
		return "0", 1
	}
	n := int(b[0]) - intZeroCode

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret uint64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	return strconv.FormatUint(ret, 10), n + 1
}

func stringifyFloat32(b []byte) (string, int) {
	bp := make([]byte, 4)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float32
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return strconv.FormatFloat(float64(ret), 'G', -1, 32), 5
}

func stringifyFloat64(b []byte) (string, int) {
	bp := make([]byte, 8)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return strconv.FormatFloat(ret, 'G', -1, 64), 9
}

func stringifyDecimal64(b []byte, scale int32) (string, int) {
	bp := make([]byte, 8)
	copy(bp, b[:])
	bp[0] ^= 0x80
	for i := 0; i < 4; i++ {
		bp[i] ^= bp[7-i]
		bp[7-i] ^= bp[i]
		bp[i] ^= bp[7-i]
	}
	ret := *(*Decimal64)(unsafe.Pointer(&bp[0]))
	return ret.Format(scale), 9
}

func stringifyDecimal128(b []byte, scale int32) (string, int) {
	bp := make([]byte, 16)
	copy(bp, b[:])
	bp[0] ^= 0x80
	for i := 0; i < 8; i++ {
		bp[i] ^= bp[15-i]
		bp[15-i] ^= bp[i]
		bp[i] ^= bp[15-i]
	}
	ret := *(*Decimal128)(unsafe.Pointer(&bp[0]))
	return ret.Format(scale), 17
}

func stringifyUuid(b []byte) (string, int) {
	var ret Uuid
	copy(ret[:], b[1:])
	return ret.String(), 17
}
