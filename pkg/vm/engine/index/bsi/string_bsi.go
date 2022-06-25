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

	"github.com/matrixorigin/matrixone/pkg/encoding"

	"github.com/RoaringBitmap/roaring"
)

func NewStringBSI(charWidth, charSize int) BitSlicedIndex {
	bitSize := charWidth * charSize
	slices := make([]*roaring.Bitmap, bitSize+1)
	for i := 0; i <= bitSize; i++ {
		slices[i] = roaring.NewBitmap()
	}
	return &StringBSI{
		rowCount:  0,
		charWidth: charWidth,
		charSize:  charSize,
		slices:    slices,
	}
}

func (b *StringBSI) Clone() BitSlicedIndex {
	slices := make([]*roaring.Bitmap, len(b.slices))
	for i, s := range b.slices {
		slices[i] = s.Clone()
	}
	return &StringBSI{
		rowCount:  b.rowCount,
		charWidth: b.charWidth,
		charSize:  b.charSize,
		slices:    slices,
	}
}

func (b *StringBSI) Marshal() ([]byte, error) {
	var body []byte
	var buf bytes.Buffer

	os := make([]uint32, 0, len(b.slices))
	buf.WriteByte(byte(b.charWidth & 0xFF))
	buf.WriteByte(byte(b.charSize & 0xFF))
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

func (b *StringBSI) Unmarshal(data []byte) error {
	b.charWidth = int(data[0])
	b.charSize = int(data[1])
	data = data[2:]
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	os := encoding.DecodeUint32Slice(data[:n])
	data = data[n:]

	bitSize := b.charWidth * b.charSize
	b.slices = make([]*roaring.Bitmap, bitSize+1)
	for i := 0; i <= bitSize; i++ {
		b.slices[i] = roaring.NewBitmap()
		if i < b.charSize {
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

func (b *StringBSI) NotNull(filter *roaring.Bitmap) *roaring.Bitmap {
	if filter == nil {
		return b.subMap(bsiExistsBit).Clone()
	} else {
		return roaring.And(filter, b.subMap(bsiExistsBit))
	}
}

func (b *StringBSI) Count(filter *roaring.Bitmap) uint64 {
	return b.NotNull(filter).GetCardinality()
}

func (b *StringBSI) NullCount(filter *roaring.Bitmap) uint64 {
	if filter == nil {
		return uint64(b.rowCount) - b.subMap(bsiExistsBit).GetCardinality()
	} else {
		return roaring.AndNot(filter, b.subMap(bsiExistsBit)).GetCardinality()
	}
}

func (b *StringBSI) Min(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return nil, 0
	}
	return b.min(filter)
}

func (b *StringBSI) Max(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return nil, 0
	}
	return b.max(filter)
}

func (b *StringBSI) Sum(filter *roaring.Bitmap) (interface{}, uint64) {
	return nil, 0
}

func (b *StringBSI) Get(k uint64) (interface{}, bool) {
	if !b.bit(bsiExistsBit, k) {
		return nil, false
	}

	bitSize := b.charWidth * b.charSize
	v := make([]byte, bitSize/8)
	zBits := 0
	for i, j := 0, bitSize; i < j; i++ {
		charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
		byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
		if b.bit(uint64(bsiOffsetBit+i), k) {
			v[byteNum] |= 1 << bytePos
		} else {
			zBits++
			if zBits == b.charWidth {
				return v[:(i+1-b.charWidth)/8], true
			}
		}
		if (i+1)%b.charWidth == 0 {
			zBits = 0
		}
	}
	return v, true
}

func (b *StringBSI) Set(k uint64, e interface{}) error {
	v := e.([]byte)
	for i, j := 0, len(v)*8; i < j; i++ {
		charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
		byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
		if (v[byteNum]>>bytePos)&1 == 1 {
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

func (b *StringBSI) Del(k uint64) error {
	b.clearBit(bsiExistsBit, k)
	return nil
}

func (b *StringBSI) Eq(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return filter, nil
	}

	v := e.([]byte)
	var bit byte
	vlen := len(v) * 8
	loopEnd := vlen
	if loopEnd < b.charWidth*b.charSize {
		loopEnd += b.charWidth
	}
	for i := 0; i < loopEnd; i++ {
		if i < vlen {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			bit = (v[byteNum] >> bytePos) & 1
		} else {
			bit = 0
		}
		if bit == 1 {
			filter.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
		if filter.IsEmpty() {
			return filter, nil
		}
	}
	return filter, nil
}

func (b *StringBSI) Ne(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	filter = b.NotNull(filter)
	if filter.IsEmpty() {
		return filter, nil
	}

	equals := filter.Clone()
	v := e.([]byte)
	var bit byte
	vlen := len(v) * 8
	loopEnd := vlen
	if loopEnd < b.charWidth*b.charSize {
		loopEnd += b.charWidth
	}
	for i := 0; i < loopEnd; i++ {
		if i < vlen {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			bit = (v[byteNum] >> bytePos) & 1
		} else {
			bit = 0
		}
		if bit == 1 {
			equals.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			equals.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
		if equals.IsEmpty() {
			return filter, nil
		}
	}
	filter.AndNot(equals)
	return filter, nil
}

func (b *StringBSI) Lt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v := e.([]byte)
	return b.lt(v, b.NotNull(filter), false), nil
}

func (b *StringBSI) Le(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v := e.([]byte)
	return b.lt(v, b.NotNull(filter), true), nil
}

func (b *StringBSI) Gt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v := e.([]byte)
	return b.gt(v, b.NotNull(filter), false), nil
}

func (b *StringBSI) Ge(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v := e.([]byte)
	return b.gt(v, b.NotNull(filter), true), nil
}

func (b *StringBSI) lt(v []byte, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	zflg := true // leading zero flag
	keep := roaring.NewBitmap()

	var bit byte
	vlen := len(v) * 8
	loopEnd := vlen
	if loopEnd < b.charWidth*b.charSize {
		loopEnd += b.charWidth
	}
	for i := 0; i < loopEnd; i++ {
		if i < vlen {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			bit = (v[byteNum] >> bytePos) & 1
		} else {
			bit = 0
		}
		if zflg {
			if bit == 0 {
				filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
				continue
			} else {
				zflg = false
			}
		}
		if i == loopEnd-1 && !eq {
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
		if i < loopEnd-1 {
			keep.Or(roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i))))
		}
	}
	return filter
}

func (b *StringBSI) gt(v []byte, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	keep := roaring.NewBitmap()

	var bit byte
	vlen := len(v) * 8
	loopEnd := vlen
	if loopEnd < b.charWidth*b.charSize {
		loopEnd += b.charWidth
	}
	for i := 0; i < loopEnd; i++ {
		if i < vlen {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			bit = (v[byteNum] >> bytePos) & 1
		} else {
			bit = 0
		}
		if i == loopEnd-1 && !eq {
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
		if i < loopEnd-1 {
			keep.Or(roaring.And(filter, b.subMap(uint64(bsiOffsetBit+i))))
		}
	}
	return filter
}

func (b *StringBSI) Top(k uint64, filter *roaring.Bitmap) *roaring.Bitmap {
	filter = b.NotNull(filter)
	if k > filter.GetCardinality() {
		return filter
	}

	res := roaring.NewBitmap()
	for i, j := 0, b.charWidth*b.charSize; i < j; i++ {
		ones := roaring.And(filter, b.subMap(uint64(bsiOffsetBit+i)))
		probe := roaring.Or(res, ones)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			if count == k || i == j-1 {
				return probe
			}
			filter = ones
		}
	}
	res.Or(filter)
	return res
}

func (b *StringBSI) Bottom(k uint64, filter *roaring.Bitmap) *roaring.Bitmap {
	filter = b.NotNull(filter)
	if k > filter.GetCardinality() {
		return filter
	}

	res := roaring.NewBitmap()
	for i, j := 0, b.charWidth*b.charSize; i < j; i++ {
		zeros := roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i)))
		probe := roaring.Or(res, zeros)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			if count == k || i == j-1 {
				return probe
			}
			filter = zeros
		}
	}
	res.Or(filter)
	return res
}

func (b *StringBSI) min(filter *roaring.Bitmap) (interface{}, uint64) {
	var count uint64
	bitSize := b.charWidth * b.charSize
	min := make([]byte, bitSize/8)
	zBits := 0

	for i, j := 0, bitSize; i < j; i++ {
		mp := roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i)))
		count = mp.GetCardinality()
		if count > 0 {
			zBits++
			if zBits == b.charWidth {
				return min[:(i+1-b.charWidth)/8], count
			}
			filter = mp
		} else {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			min[byteNum] += 1 << bytePos
			if i == j-1 {
				count = filter.GetCardinality()
			}
		}
		if (i+1)%b.charWidth == 0 {
			zBits = 0
		}
	}
	return min, count
}

func (b *StringBSI) max(filter *roaring.Bitmap) (interface{}, uint64) {
	var count uint64
	bitSize := b.charWidth * b.charSize
	max := make([]byte, bitSize/8)
	zBits := 0

	for i, j := 0, b.charWidth*b.charSize; i < j; i++ {
		mp := roaring.And(b.subMap(uint64(bsiOffsetBit+i)), filter)
		count = mp.GetCardinality()
		if count > 0 {
			charNum, charPos := i/b.charWidth, b.charWidth-i%b.charWidth-1
			byteNum, bytePos := charNum*(b.charWidth/8)+charPos/8, charPos%8
			max[byteNum] += 1 << bytePos
			filter = mp
		} else {
			zBits++
			if zBits == b.charWidth {
				return max[:(i+1-b.charWidth)/8], filter.GetCardinality()
			}
			if i == j-1 {
				count = filter.GetCardinality()
			}
		}
		if (i+1)%b.charWidth == 0 {
			zBits = 0
		}
	}
	return max, count
}

// x is the bit offset, y is row id
func (b *StringBSI) setBit(x, y uint64)   { b.slices[x].Add(uint32(y)) }
func (b *StringBSI) clearBit(x, y uint64) { b.slices[x].Remove(uint32(y)) }
func (b *StringBSI) bit(x, y uint64) bool { return b.slices[x].Contains(uint32(y)) }

func (b *StringBSI) subMap(x uint64) *roaring.Bitmap { return b.slices[x] }
