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

package bsi

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/encoding"

	"github.com/RoaringBitmap/roaring"
)

func NewNumericBSI(bitSize int, valType ValueType) BitSlicedIndex {
	slices := make([]*roaring.Bitmap, bitSize+1)
	for i := 0; i <= bitSize; i++ {
		slices[i] = roaring.NewBitmap()
	}
	return &NumericBSI{
		rowCount: 0,
		valType:  valType,
		bitSize:  bitSize,
		slices:   slices,
	}
}

func (b *NumericBSI) Clone() BitSlicedIndex {
	slices := make([]*roaring.Bitmap, b.bitSize+1)
	for i := 0; i <= b.bitSize; i++ {
		slices[i] = b.slices[i].Clone()
	}
	return &NumericBSI{
		rowCount: b.rowCount,
		valType:  b.valType,
		bitSize:  b.bitSize,
		slices:   slices,
	}
}

func (b *NumericBSI) Marshal() ([]byte, error) {
	var body []byte
	var buf bytes.Buffer

	os := make([]uint32, 0, len(b.slices))
	buf.WriteByte(byte(b.valType & 0xFF))
	buf.WriteByte(byte(b.bitSize & 0xFF))
	for _, m := range b.slices {
		data, err := marshallRB(m)
		if err != nil {
			return nil, err
		}
		os = append(os, uint32(len(body)))
		body = append(body, data...)
	}
	{
		data := encoding.EncodeUint32Slice(os)
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
	}
	buf.Write(body)
	return buf.Bytes(), nil
}

func (b *NumericBSI) Unmarshal(data []byte) error {
	b.valType = ValueType(data[0])
	b.bitSize = int(data[1])
	data = data[2:]
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	os := encoding.DecodeUint32Slice(data[:n])
	data = data[n:]
	b.slices = make([]*roaring.Bitmap, b.bitSize+1)
	for i := 0; i <= b.bitSize; i++ {
		b.slices[i] = roaring.NewBitmap()
		if i < b.bitSize {
			if err := b.slices[i].UnmarshalBinary(data[os[i]:os[i+1]]); err != nil {
				return err
			}
		} else {
			if err := b.slices[i].UnmarshalBinary(data[os[i]:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *NumericBSI) NotNull(filter *roaring.Bitmap) *roaring.Bitmap {
	if filter == nil {
		return b.subMap(bsiExistsBit).Clone()
	} else {
		return roaring.And(filter, b.subMap(bsiExistsBit))
	}
}

func (b *NumericBSI) Count(filter *roaring.Bitmap) uint64 {
	return b.NotNull(filter).GetCardinality()
}

func (b *NumericBSI) NullCount(filter *roaring.Bitmap) uint64 {
	if filter == nil {
		return uint64(b.rowCount) - b.subMap(bsiExistsBit).GetCardinality()
	} else {
		return roaring.AndNot(filter, b.subMap(bsiExistsBit)).GetCardinality()
	}
}

func (b *NumericBSI) Min(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return nil, 0
	}
	return b.min(filter)
}

func (b *NumericBSI) Max(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return nil, 0
	}
	return b.max(filter)
}

func (b *NumericBSI) Sum(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.NotNull(filter)
	count := filter.GetCardinality()

	switch b.valType {
	case UnsignedInt:
		if count == 0 {
			return uint64(0), 0
		}

		var sum uint64

		for i, j := 0, b.bitSize; i < j; i++ {
			bits := b.subMap(uint64(bsiOffsetBit + i))
			sum += (1 << i) * bits.AndCardinality(filter)
		}
		return sum, count

	case SignedInt:
		if count == 0 {
			return int64(0), 0
		}

		var sum int64

		negCount := count - filter.AndCardinality(b.subMap(uint64(bsiOffsetBit+b.bitSize-1)))
		sum += int64(negCount) * (-1 << (b.bitSize - 1))

		for i, j := 0, b.bitSize-1; i < j; i++ {
			bits := b.subMap(uint64(bsiOffsetBit + i))
			sum += (1 << i) * int64(bits.AndCardinality(filter))
		}
		return sum, count

	default:
		return nil, 0
	}
}

func (b *NumericBSI) Get(k uint64) (interface{}, bool) {
	if !b.bit(bsiExistsBit, k) {
		return 0, false
	}

	var v uint64
	for i, j := 0, b.bitSize; i < j; i++ {
		if b.bit(uint64(bsiOffsetBit+i), k) {
			v |= 1 << i
		}
	}
	return b.recoveryFromUint64(v), true
}

func (b *NumericBSI) Set(k uint64, e interface{}) error {
	v, _ := b.normalizeToUint64(e)
	for i, j := 0, b.bitSize; i < j; i++ {
		if v&(1<<i) != 0 {
			b.setBit(uint64(bsiOffsetBit+i), k)
		} else {
			b.clearBit(uint64(bsiOffsetBit+i), k)
		}
	}
	b.setBit(bsiExistsBit, k)
	if int(k) >= b.rowCount {
		b.rowCount = int(k) + 1
	}
	return nil
}

func (b *NumericBSI) Del(k uint64) error {
	b.clearBit(bsiExistsBit, k)
	return nil
}

func (b *NumericBSI) normalizeToUint64(v interface{}) (uint64, error) {
	switch b.valType {
	case UnsignedInt:
		sz := b.bitSize
		if sz > 0 && sz <= 8 {
			return uint64(v.(uint8)), nil
		} else if sz > 8 && sz <= 16 {
			return uint64(v.(uint16)), nil
		} else if sz > 16 && sz <= 32 {
			return uint64(v.(uint32)), nil
		} else {
			return uint64(v.(uint64)), nil
		}
		//return v.(uint64), nil

	case SignedInt:
		highBit := int64(-1) << (b.bitSize - 1)
		sz := b.bitSize
		if sz > 0 && sz <= 8 {
			return uint64(int64(v.(int8)) ^ highBit), nil
		} else if sz > 8 && sz <= 16 {
			return uint64(int64(v.(int16)) ^ highBit), nil
		} else if sz > 16 && sz <= 32 {
			return uint64(int64(v.(int32)) ^ highBit), nil
		} else {
			return uint64(int64(v.(int64)) ^ highBit), nil
		}
		//return uint64(v.(int64) ^ highBit), nil

	case Float:
		if b.bitSize == 32 {
			vfloat := v.(float32)
			vint := *(*uint32)(unsafe.Pointer(&vfloat))
			highBit := uint32(1) << 31

			if highBit&vint != 0 {
				return uint64(^vint), nil
			} else {
				return uint64(highBit ^ vint), nil
			}
		} else /*if b.bitSize == 64*/ {
			vfloat := v.(float64)
			vint := *(*uint64)(unsafe.Pointer(&vfloat))
			highBit := uint64(1) << 63

			if highBit&vint != 0 {
				return ^vint, nil
			} else {
				return highBit ^ vint, nil
			}
		}

	default:
		return 0, fmt.Errorf("type not supported")
	}
}

func (b *NumericBSI) recoveryFromUint64(v uint64) interface{} {
	switch b.valType {
	case UnsignedInt:
		sz := b.bitSize
		if sz > 0 && sz <= 8 {
			return uint8(v)
		} else if sz > 8 && sz <= 16 {
			return uint16(v)
		} else if sz > 16 && sz <= 32 {
			return uint32(v)
		} else {
			return uint64(v)
		}
		//return v

	case SignedInt:
		highBit := int64(-1) << (b.bitSize - 1)
		sz := b.bitSize
		if sz > 0 && sz <= 8 {
			return int8(int64(v) + highBit)
		} else if sz > 8 && sz <= 16 {
			return int16(int64(v) + highBit)
		} else if sz > 16 && sz <= 32 {
			return int32(int64(v) + highBit)
		} else {
			return int64(int64(v) + highBit)
		}
		//return int64(v) + highBit

	case Float:
		if b.bitSize == 32 {
			vint := uint32(v)
			highBit := uint32(1) << 31
			if highBit&vint != 0 {
				vint ^= highBit
			} else {
				vint = ^vint
			}
			return *(*float32)(unsafe.Pointer(&vint))
		} else /*if b.bitSize == 64*/ {
			highBit := uint64(1) << 63
			if highBit&v != 0 {
				v ^= highBit
			} else {
				v = ^v
			}
			return *(*float64)(unsafe.Pointer(&v))
		}

	default:
		return nil
	}
}

func (b *NumericBSI) Eq(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	filter = b.NotNull(filter)
	for i := b.bitSize - 1; i >= 0; i-- {
		if filter.IsEmpty() {
			return filter, nil
		}
		if v&(1<<i) != 0 {
			filter.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	return filter, nil
}

func (b *NumericBSI) Ne(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return filter, nil
	}
	equals := filter.Clone()
	for i := b.bitSize - 1; i >= 0; i-- {
		if equals.IsEmpty() {
			return filter, nil
		}
		if v&(1<<i) != 0 {
			equals.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			equals.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	filter.AndNot(equals)
	return filter, nil
}

func (b *NumericBSI) Lt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.lt(v, b.NotNull(filter), false), nil
}

func (b *NumericBSI) Le(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.lt(v, b.NotNull(filter), true), nil
}

func (b *NumericBSI) Gt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.gt(v, b.NotNull(filter), false), nil
}

func (b *NumericBSI) Ge(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.gt(v, b.NotNull(filter), true), nil
}

func (b *NumericBSI) Top(k uint64, filter *roaring.Bitmap) *roaring.Bitmap {
	filter = b.NotNull(filter)
	if k > filter.GetCardinality() {
		return filter
	}

	res := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		ones := roaring.And(filter, b.subMap(uint64(bsiOffsetBit+i)))
		probe := roaring.Or(res, ones)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			if count == k || i == 0 {
				return probe
			}
			filter = ones
		}
	}
	res.Or(filter)
	return res
}

func (b *NumericBSI) Bottom(k uint64, filter *roaring.Bitmap) *roaring.Bitmap {
	filter = b.NotNull(filter)
	if k > filter.GetCardinality() {
		return filter
	}

	res := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		zeros := roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i)))
		probe := roaring.Or(res, zeros)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			if count == k || i == 0 {
				return probe
			}
			filter = zeros
		}
	}
	res.Or(filter)
	return res
}

func (b *NumericBSI) min(filter *roaring.Bitmap) (interface{}, uint64) {
	var min uint64
	var count uint64

	for i := b.bitSize - 1; i >= 0; i-- {
		mp := roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i)))
		count = mp.GetCardinality()
		if count > 0 {
			filter = mp
		} else {
			min += 1 << i
			if i == 0 {
				count = filter.GetCardinality()
			}
		}
	}
	return b.recoveryFromUint64(min), count
}

func (b *NumericBSI) max(filter *roaring.Bitmap) (interface{}, uint64) {
	var max uint64
	var count uint64

	for i := b.bitSize - 1; i >= 0; i-- {
		mp := roaring.And(b.subMap(uint64(bsiOffsetBit+i)), filter)
		count = mp.GetCardinality()
		if count > 0 {
			max += 1 << i
			filter = mp
		} else if i == 0 {
			count = filter.GetCardinality()
		}
	}
	return b.recoveryFromUint64(max), count
}

func (b *NumericBSI) lt(v uint64, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	zflg := true // leading zero flag
	keep := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		bit := (v >> i) & 1
		if zflg {
			if bit == 0 {
				filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
				continue
			} else {
				zflg = false
			}
		}
		if i == 0 && !eq {
			if bit == 0 {
				return keep
			}
			filter.AndNot(roaring.AndNot(b.subMap(uint64(bsiOffsetBit+i)), keep))
			return filter
		}
		if bit == 0 {
			filter.AndNot(roaring.AndNot(b.subMap(uint64(bsiOffsetBit+i)), keep))
			continue
		}
		if i > 0 {
			keep.Or(roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i))))
		}
	}
	return filter
}

func (b *NumericBSI) gt(v uint64, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	keep := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		bit := (v >> i) & 1
		if i == 0 && !eq {
			if bit == 1 {
				return keep
			}
			filter.AndNot(roaring.AndNot(roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i))), keep))
			return filter
		}
		if bit == 1 {
			filter.AndNot(roaring.AndNot(roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i))), keep))
			continue
		}
		if i > 0 {
			keep.Or(roaring.And(filter, b.subMap(uint64(bsiOffsetBit+i))))
		}
	}
	return filter
}

// x is the bit offset, y is row id
func (b *NumericBSI) setBit(x, y uint64)   { b.slices[x].Add(uint32(y)) }
func (b *NumericBSI) clearBit(x, y uint64) { b.slices[x].Remove(uint32(y)) }
func (b *NumericBSI) bit(x, y uint64) bool { return b.slices[x].Contains(uint32(y)) }

func (b *NumericBSI) subMap(x uint64) *roaring.Bitmap { return b.slices[x] }
