// Copyright 2024 Matrix Origin
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

package types

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"slices"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

type Packer struct {
	buffer            []byte
	bufferDeallocator malloc.Deallocator
}

var packerAllocator = malloc.NewShardedAllocator(
	runtime.GOMAXPROCS(0),
	func() malloc.Allocator {
		return malloc.NewClassAllocator(
			malloc.NewFixedSizeSyncPoolAllocator,
		)
	},
)

func NewPacker() *Packer {
	bs, dec, err := packerAllocator.Allocate(4096, malloc.NoClear)
	if err != nil {
		panic(err)
	}
	return &Packer{
		buffer:            bs[:0],
		bufferDeallocator: dec,
	}
}

func NewPackerArray(length int) []*Packer {
	ret := make([]*Packer, 0, length)
	for i := 0; i < length; i++ {
		ret = append(ret, NewPacker())
	}
	return ret
}

func (p *Packer) Close() {
	if p.bufferDeallocator != nil {
		p.bufferDeallocator.Deallocate(malloc.NoHints)
	}
	*p = Packer{}
}

func (p *Packer) Reset() {
	p.buffer = p.buffer[:0]
}

func (p *Packer) ensureSize(n int) {
	if len(p.buffer)+n <= cap(p.buffer) {
		return
	}
	p.ensureSizeSlow(n)
}

func (p *Packer) ensureSizeSlow(n int) {
	newBuffer, newDec, err := packerAllocator.Allocate(uint64(cap(p.buffer)+n), malloc.NoClear)
	if err != nil {
		panic(err)
	}
	newBuffer = newBuffer[:len(p.buffer)]
	copy(newBuffer, p.buffer)
	if p.bufferDeallocator != nil {
		p.bufferDeallocator.Deallocate(malloc.NoHints)
	}
	p.buffer = newBuffer
	p.bufferDeallocator = newDec
}

func (p *Packer) putByte(b byte) {
	p.ensureSize(1)
	p.buffer = append(p.buffer, b)
}

func (p *Packer) putBytes(bs []byte) {
	p.ensureSize(len(bs))
	p.buffer = append(p.buffer, bs...)
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

func (p *Packer) EncodeNull() {
	p.putByte(nilCode)
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

func (p *Packer) EncodeEnum(e Enum) {
	p.putByte(enumCode)
	p.EncodeUint16(uint16(e))
}

func (p *Packer) EncodeDecimal64(e Decimal64) {
	p.putByte(decimal64Code)
	b := *(*[8]byte)(unsafe.Pointer(&e))
	b[7] ^= 0x80
	for i := 7; i >= 0; i-- {
		p.putByte(b[i])
	}
}

func (p *Packer) EncodeDecimal128(e Decimal128) {
	p.putByte(decimal128Code)
	b := *(*[16]byte)(unsafe.Pointer(&e))
	b[15] ^= 0x80
	for i := 15; i >= 0; i-- {
		p.putByte(b[i])
	}
}

func (p *Packer) EncodeStringType(e []byte) {
	p.putByte(stringTypeCode)
	p.encodeBytes(bytesCode, e)
}

var stringMax = [2]byte{stringTypeCode, bytesMaxCode}

func PackerStringMax() []byte {
	return stringMax[:]
}

func (p *Packer) EncodeBit(e uint64) {
	p.putByte(bitCode)
	p.encodeUint(e)
}

func (p *Packer) EncodeUuid(e Uuid) {
	p.putByte(uuidCode)
	p.putBytes(e[:])
}

func (p *Packer) GetBuf() []byte {
	return p.buffer
}

func (p *Packer) Bytes() []byte {
	return slices.Clone(p.buffer)
}
