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
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func (tp Tuple) ErrString() string {
	res := ""
	if len(tp) != 1 {
		res = "("
	}
	for i, t := range tp {
		switch t := t.(type) {
		case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			res += fmt.Sprintf("%v", t)
		case []byte:
			res += *(*string)(unsafe.Pointer(&t))
		case Date:
			res += fmt.Sprintf("%v", t.String())
		case Time:
			res += fmt.Sprintf("%v", t.String())
		case Datetime:
			res += fmt.Sprintf("%v", t.String())
		case Timestamp:
			res += fmt.Sprintf("%v", t.String())
		case Decimal64:
			res += fmt.Sprintf("%v", t.Format(0))
		case Decimal128:
			res += fmt.Sprintf("%v", t.Format(0))
		default:
			res += fmt.Sprintf("%v", t)
		}
		if i != len(tp)-1 {
			res += ","
		}
	}
	if len(tp) != 1 {
		res += ")"
	}
	return res
}

func printTuple(tuple Tuple) string {
	res := "("
	for i, t := range tuple {
		switch t := t.(type) {
		case bool:
			res += fmt.Sprintf("(bool: %v)", t)
		case int8:
			res += fmt.Sprintf("(int8: %v)", t)
		case int16:
			res += fmt.Sprintf("(int16: %v)", t)
		case int32:
			res += fmt.Sprintf("(int32: %v)", t)
		case int64:
			res += fmt.Sprintf("(int64: %v)", t)
		case uint8:
			res += fmt.Sprintf("(uint8: %v)", t)
		case uint16:
			res += fmt.Sprintf("(uint16: %v)", t)
		case uint32:
			res += fmt.Sprintf("(uint32: %v)", t)
		case uint64:
			res += fmt.Sprintf("(uint64: %v)", t)
		case Date:
			res += fmt.Sprintf("(date: %v)", t.String())
		case Time:
			res += fmt.Sprintf("(time: %v)", t.String())
		case Datetime:
			res += fmt.Sprintf("(datetime: %v)", t.String())
		case Timestamp:
			res += fmt.Sprintf("(timestamp: %v)", t.String())
		case Decimal64:
			res += fmt.Sprintf("(decimal64: %v)", t.Format(0))
		case Decimal128:
			res += fmt.Sprintf("(decimal128: %v)", t.Format(0))
		case []byte:
			res += fmt.Sprintf("([]byte: %v)", t)
		case float32:
			res += fmt.Sprintf("(float32: %v)", t)
		case float64:
			res += fmt.Sprintf("(float64: %v)", t)
		default:
			res += fmt.Sprintf("(unorganizedType: %v)", t)
		}
		if i != len(tuple)-1 {
			res += ","
		}
	}
	res += ")"
	return res
}

const nilCode = 0x00
const bytesCode = 0x01
const intZeroCode = 0x14
const float32Code = 0x20
const float64Code = 0x21
const falseCode = 0x26
const trueCode = 0x27
const int8Code = 0x28
const int16Code = 0x29
const int32Code = 0x3a
const int64Code = 0x3b
const uint8Code = 0x3c
const uint16Code = 0x3d
const uint32Code = 0x3e
const uint64Code = 0x40
const dateCode = 0x41
const datetimeCode = 0x42
const timestampCode = 0x43
const decimal64Code = 0x44
const decimal128Code = 0x45
const stringTypeCode = 0x46
const timeCode = 0x47 // TODO: reorder the list to put timeCode next to date type code?

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

const PackerMemUnit = 64

type Packer struct {
	buf      []byte
	size     int
	capacity int
	mp       *mpool.MPool
}

func NewPacker(mp *mpool.MPool) *Packer {
	bytes, err := mp.Alloc(PackerMemUnit)
	if err != nil {
		panic(err)
	}
	return &Packer{
		buf:      bytes,
		size:     0,
		capacity: PackerMemUnit,
		mp:       mp,
	}
}

func NewPackerArray(length int, mp *mpool.MPool) []*Packer {
	packerArr := make([]*Packer, length)
	for num := range packerArr {
		bytes, err := mp.Alloc(PackerMemUnit)
		if err != nil {
			panic(err)
		}
		packerArr[num] = &Packer{
			buf:      bytes,
			size:     0,
			capacity: PackerMemUnit,
			mp:       mp,
		}
	}
	return packerArr
}

func (p *Packer) FreeMem() {
	if p.buf != nil {
		p.mp.Free(p.buf)
		p.size = 0
		p.capacity = 0
		p.buf = nil
	}
}

func (p *Packer) Reset() {
	p.size = 0
}

func (p *Packer) putByte(b byte) {
	if p.size < p.capacity {
		p.buf[p.size] = b
		p.size++
	} else {
		p.buf, _ = p.mp.Grow(p.buf, p.capacity+PackerMemUnit)
		p.capacity += PackerMemUnit
		p.buf[p.size] = b
		p.size++
	}
}

func (p *Packer) putBytes(bs []byte) {
	if p.size+len(bs) < p.capacity {
		for _, b := range bs {
			p.buf[p.size] = b
			p.size++
		}
	} else {
		incrementSize := ((len(bs) / PackerMemUnit) + 1) * PackerMemUnit
		p.buf, _ = p.mp.Grow(p.buf, p.capacity+incrementSize)
		p.capacity += incrementSize
		for _, b := range bs {
			p.buf[p.size] = b
			p.size++
		}
	}
}

func (p *Packer) putBytesNil(b []byte, i int) {
	for i >= 0 {
		p.putBytes(b[:i+1])
		p.putByte(0xFF)
		b = b[i+1:]
		i = bytes.IndexByte(b, 0x00)
	}
	p.putBytes(b)
}

func (p *Packer) encodeBytes(code byte, b []byte) {
	p.putByte(code)
	if i := bytes.IndexByte(b, 0x00); i >= 0 {
		p.putBytesNil(b, i)
	} else {
		p.putBytes(b)
	}
	p.putByte(0x00)
}

func (p *Packer) encodeUint(i uint64) {
	if i == 0 {
		p.putByte(intZeroCode)
		return
	}

	n := bisectLeft(i)
	var scratch [8]byte

	p.putByte(byte(intZeroCode + n))
	binary.BigEndian.PutUint64(scratch[:], i)

	p.putBytes(scratch[8-n:])
}

func (p *Packer) encodeInt(i int64) {
	if i >= 0 {
		p.encodeUint(uint64(i))
		return
	}

	n := bisectLeft(uint64(-i))
	var scratch [8]byte

	p.putByte(byte(intZeroCode - n))
	offsetEncoded := int64(sizeLimits[n]) + i
	binary.BigEndian.PutUint64(scratch[:], uint64(offsetEncoded))

	p.putBytes(scratch[8-n:])
}

func (p *Packer) encodeFloat32(f float32) {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], math.Float32bits(f))
	adjustFloatBytes(scratch[:], true)

	p.putByte(float32Code)
	p.putBytes(scratch[:])
}

func (p *Packer) encodeFloat64(d float64) {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], math.Float64bits(d))
	adjustFloatBytes(scratch[:], true)

	p.putByte(float64Code)
	p.putBytes(scratch[:])
}

func (p *Packer) EncodeInt8(e int8) {
	p.putByte(int8Code)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeInt16(e int16) {
	p.putByte(int16Code)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeInt32(e int32) {
	p.putByte(int32Code)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeInt64(e int64) {
	p.putByte(int64Code)
	p.encodeInt(e)
}

func (p *Packer) EncodeUint8(e uint8) {
	p.putByte(uint8Code)
	p.encodeUint(uint64(e))
}

func (p *Packer) EncodeUint16(e uint16) {
	p.putByte(uint16Code)
	p.encodeUint(uint64(e))
}

func (p *Packer) EncodeUint32(e uint32) {
	p.putByte(uint32Code)
	p.encodeUint(uint64(e))
}

func (p *Packer) EncodeUint64(e uint64) {
	p.putByte(uint64Code)
	p.encodeUint(e)
}

func (p *Packer) EncodeFloat32(e float32) {
	p.encodeFloat32(e)
}

func (p *Packer) EncodeFloat64(e float64) {
	p.encodeFloat64(e)
}

func (p *Packer) EncodeBool(e bool) {
	if e {
		p.putByte(trueCode)
	} else {
		p.putByte(falseCode)
	}
}

func (p *Packer) EncodeDate(e Date) {
	p.putByte(dateCode)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeTime(e Time) {
	p.putByte(timeCode)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeDatetime(e Datetime) {
	p.putByte(datetimeCode)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeTimestamp(e Timestamp) {
	p.putByte(timestampCode)
	p.encodeInt(int64(e))
}

func (p *Packer) EncodeDecimal64(e Decimal64) {
	p.putByte(decimal64Code)
	b := *(*[8]byte)(unsafe.Pointer(&e))
	b[0] ^= 0x80
	p.putBytes(b[:])
}

func (p *Packer) EncodeDecimal128(e Decimal128) {
	p.putByte(decimal128Code)
	b := *(*[16]byte)(unsafe.Pointer(&e))
	for i := 0; i < 8; i++ {
		b[i] ^= b[i+8]
		b[i+8] ^= b[i]
		b[i] ^= b[i+8]
	}
	b[0] ^= 0x80
	p.putBytes(b[:])
}

func (p *Packer) EncodeStringType(e []byte) {
	p.putByte(stringTypeCode)
	p.encodeBytes(bytesCode, e)
}

func (p *Packer) GetBuf() []byte {
	return p.buf[:p.size]
}

func (p *Packer) Bytes() []byte {
	ret := make([]byte, p.size)
	copy(ret, p.buf[:p.size])
	return ret
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
	copy(bp, b[1:])
	bp[0] ^= 0x80
	ret := *(*Decimal64)(unsafe.Pointer(&bp))
	return ret, 9
}

func decodeDecimal128(b []byte) (Decimal128, int) {
	bp := make([]byte, 16)
	copy(bp, b[1:])
	bp[0] ^= 0x80
	for i := 0; i < 8; i++ {
		bp[i] ^= bp[i+8]
		bp[i+8] ^= bp[i]
		bp[i] ^= bp[i+8]
	}
	ret := *(*Decimal128)(unsafe.Pointer(&bp))
	return ret, 17
}

var DecodeTuple = decodeTuple

func decodeTuple(b []byte) (Tuple, int, error) {
	var t Tuple

	var i int

	for i < len(b) {
		var el interface{}
		var off int

		switch {
		case b[i] == nilCode:
			el = nil
			off = 1
		case b[i] == int8Code:
			el, off = decodeInt(int8Code, b[i+1:])
			off += 1
		case b[i] == int16Code:
			el, off = decodeInt(int16Code, b[i+1:])
			off += 1
		case b[i] == int32Code:
			el, off = decodeInt(int32Code, b[i+1:])
			off += 1
		case b[i] == int64Code:
			el, off = decodeInt(int64Code, b[i+1:])
			off += 1
		case b[i] == uint8Code:
			el, off = decodeUint(uint8Code, b[i+1:])
			off += 1
		case b[i] == uint16Code:
			el, off = decodeUint(uint16Code, b[i+1:])
			off += 1
		case b[i] == uint32Code:
			el, off = decodeUint(uint32Code, b[i+1:])
			off += 1
		case b[i] == uint64Code:
			el, off = decodeUint(uint64Code, b[i+1:])
			off += 1
		case b[i] == trueCode:
			el = true
			off = 1
		case b[i] == falseCode:
			el = false
			off = 1
		case b[i] == float32Code:
			el, off = decodeFloat32(b[i:])
		case b[i] == float64Code:
			el, off = decodeFloat64(b[i:])
		case b[i] == dateCode:
			el, off = decodeInt(dateCode, b[i+1:])
			off += 1
		case b[i] == datetimeCode:
			el, off = decodeInt(datetimeCode, b[i+1:])
			off += 1
		case b[i] == timestampCode:
			el, off = decodeInt(timestampCode, b[i+1:])
			off += 1
		case b[i] == timeCode:
			el, off = decodeInt(timeCode, b[i+1:])
			off += 1
		case b[i] == decimal64Code:
			el, off = decodeDecimal64(b[i+1:])
		case b[i] == decimal128Code:
			el, off = decodeDecimal128(b[i+1:])
		case b[i] == stringTypeCode:
			el, off = decodeBytes(b[i+1:])
			off += 1
		default:
			return nil, i, moerr.NewInternalErrorNoCtx("unable to decode tuple element with unknown typecode %02x", b[i])
		}
		t = append(t, el)
		i += off
	}

	return t, i, nil
}

func Unpack(b []byte) (Tuple, error) {
	t, _, err := decodeTuple(b)
	return t, err
}
