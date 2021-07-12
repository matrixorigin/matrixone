package bsi

import (
	"bytes"
	"fmt"
	"unsafe"

	"matrixone/pkg/encoding"

	"github.com/RoaringBitmap/roaring"
)

func New(bitSize int, valType ValueType) *BitSlicedIndex {
	slices := make([]*roaring.Bitmap, bitSize+1)
	for i := 0; i <= bitSize; i++ {
		slices[i] = roaring.NewBitmap()
	}
	return &BitSlicedIndex{
		bitSize: bitSize,
		valType: valType,
		slices:  slices,
	}
}

func (b *BitSlicedIndex) Map() *roaring.Bitmap {
	return b.subMap(bsiExistsBit)
}

func (b *BitSlicedIndex) Clone() *BitSlicedIndex {
	slices := make([]*roaring.Bitmap, b.bitSize+1)
	for i := 0; i <= b.bitSize; i++ {
		slices[i] = b.slices[i].Clone()
	}
	return &BitSlicedIndex{
		bitSize: b.bitSize,
		valType: b.valType,
		slices:  slices,
	}
}

func (b *BitSlicedIndex) Marshall() ([]byte, error) {
	var body []byte
	var buf bytes.Buffer

	os := make([]uint32, 0, len(b.slices))
	buf.WriteByte(byte(b.bitSize & 0xFF))
	buf.WriteByte(byte(b.valType & 0xFF))
	for _, m := range b.slices {
		data, err := show(m)
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

func (b *BitSlicedIndex) Unmarshall(data []byte) error {
	b.bitSize = int(data[0])
	b.valType = ValueType(data[1])
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

func (b *BitSlicedIndex) filterByNulls(extFilter *roaring.Bitmap) *roaring.Bitmap {
	if extFilter == nil {
		return b.subMap(bsiExistsBit).Clone()
	} else {
		return roaring.And(extFilter, b.subMap(bsiExistsBit))
	}
}

func (b *BitSlicedIndex) Count(filter *roaring.Bitmap) uint64 {
	return b.filterByNulls(filter).GetCardinality()
}

func (b *BitSlicedIndex) Min(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.filterByNulls(filter)
	if filter.IsEmpty() {
		return 0, 0
	}
	return b.min(filter)
}

func (b *BitSlicedIndex) Max(filter *roaring.Bitmap) (interface{}, uint64) {
	filter = b.filterByNulls(filter)
	if filter.IsEmpty() {
		return 0, 0
	}
	return b.max(filter)
}

func (b *BitSlicedIndex) Sum(filter *roaring.Bitmap) (interface{}, uint64) {
	switch b.valType {
	case UnsignedInt:
		var sum uint64

		filter = b.filterByNulls(filter)
		count := filter.GetCardinality()

		for i, j := 0, b.bitSize; i < j; i++ {
			bits := b.subMap(uint64(bsiOffsetBit + i))
			sum += (1 << i) * bits.AndCardinality(filter)
		}
		return sum, count

	case SignedInt:
		var sum int64

		filter = b.filterByNulls(filter)
		count := filter.GetCardinality()
		negCount := count - filter.AndCardinality(b.subMap(uint64(b.bitSize)))
		sum += int64(negCount) * (-1 << (b.bitSize - 1))

		for i := b.bitSize - 2; i >= 0; i-- {
			bits := b.subMap(uint64(bsiOffsetBit + i))
			sum += (1 << i) * int64(bits.AndCardinality(filter))
		}
		return sum, count

	default:
		return 0, 0
	}
}

func (b *BitSlicedIndex) Get(k uint64) (interface{}, bool) {
	var v uint64

	if !b.bit(bsiExistsBit, k) {
		return 0, false
	}
	for i, j := uint(0), uint(b.bitSize); i < j; i++ {
		if b.bit(uint64(bsiOffsetBit+i), k) {
			v |= (1 << i)
		}
	}
	return b.recoveryFromUint64(v), true
}

func (b *BitSlicedIndex) normalizeToUint64(v interface{}) (uint64, error) {
	switch b.valType {
	case UnsignedInt:
		return v.(uint64), nil

	case SignedInt:
		highBit := int64(-1) << (b.bitSize - 1)
		return uint64(v.(int64) ^ highBit), nil

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
	}

	return 0, fmt.Errorf("type not supported")
}

func (b *BitSlicedIndex) recoveryFromUint64(v uint64) interface{} {
	switch b.valType {
	case UnsignedInt:
		return v
	case SignedInt:
		highBit := int64(-1) << (b.bitSize - 1)
		return int64(v) + highBit
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
	}

	return fmt.Errorf("type not supported")
}

func (b *BitSlicedIndex) Set(k uint64, e interface{}) error {
	v, _ := b.normalizeToUint64(e)
	for i, j := uint(0), uint(b.bitSize); i < j; i++ {
		if v&(1<<i) != 0 {
			b.setBit(uint64(bsiOffsetBit+i), k)
		} else {
			b.clearBit(uint64(bsiOffsetBit+i), k)
		}
	}
	b.setBit(bsiExistsBit, k)
	return nil
}

func (b *BitSlicedIndex) Del(k uint64) error {
	b.clearBit(bsiExistsBit, k)
	return nil
}

func (b *BitSlicedIndex) Eq(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	filter = b.filterByNulls(filter)
	for i := b.bitSize - 1; i >= 0; i-- {
		if filter.IsEmpty() {
			return filter, nil
		}
		if (v>>uint(i))&1 == 1 {
			filter.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			filter.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	return filter, nil
}

func (b *BitSlicedIndex) Ne(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	filter = b.filterByNulls(filter)
	if filter.IsEmpty() {
		return filter, nil
	}
	equals := filter.Clone()
	for i := b.bitSize - 1; i >= 0; i-- {
		if equals.IsEmpty() {
			return filter, nil
		}
		if (v>>uint(i))&1 == 1 {
			equals.And(b.subMap(uint64(bsiOffsetBit + i)))
		} else {
			equals.AndNot(b.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	filter.AndNot(equals)
	return filter, nil
}

func (b *BitSlicedIndex) Lt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.lt(v, b.filterByNulls(filter), false), nil
}

func (b *BitSlicedIndex) Le(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.lt(v, b.filterByNulls(filter), true), nil
}

func (b *BitSlicedIndex) Gt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.gt(v, b.filterByNulls(filter), false), nil
}

func (b *BitSlicedIndex) Ge(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	v, _ := b.normalizeToUint64(e)
	return b.gt(v, b.filterByNulls(filter), true), nil
}

func (b *BitSlicedIndex) Top(k uint64, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	filter = b.filterByNulls(filter)
	if k > filter.GetCardinality() {
		return filter, nil
	}

	res := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		ones := roaring.And(filter, b.subMap(uint64(i)))
		probe := roaring.Or(res, ones)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.AndNot(b.subMap(uint64(i)))
		} else {
			if count == k || i == 0 {
				return probe, nil
			}
			filter = ones
		}
	}
	res.Or(filter)
	return res, nil
}

func (b *BitSlicedIndex) Bottom(k uint64, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	filter = b.filterByNulls(filter)
	if k > filter.GetCardinality() {
		return filter, nil
	}

	res := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		zeros := roaring.AndNot(filter, b.subMap(uint64(i)))
		probe := roaring.Or(res, zeros)
		count := probe.GetCardinality()

		if count < k {
			res = probe
			filter.And(b.subMap(uint64(i)))
		} else {
			if count == k || i == 0 {
				return probe, nil
			}
			filter = zeros
		}
	}
	res.Or(filter)
	return res, nil
}

func (b *BitSlicedIndex) min(filter *roaring.Bitmap) (interface{}, uint64) {
	var min uint64
	var count uint64

	for i := b.bitSize - 1; i >= 0; i-- {
		mp := roaring.AndNot(filter, b.subMap(uint64(bsiOffsetBit+i)))
		count = mp.GetCardinality()
		if count > 0 {
			filter = mp
		} else {
			min += (1 << uint(i))
			if i == 0 {
				count = filter.GetCardinality()
			}
		}
	}
	return b.recoveryFromUint64(min), count
}

func (b *BitSlicedIndex) max(filter *roaring.Bitmap) (interface{}, uint64) {
	var max uint64
	var count uint64

	for i := b.bitSize - 1; i >= 0; i-- {
		mp := roaring.And(b.subMap(uint64(bsiOffsetBit+i)), filter)
		count = mp.GetCardinality()
		if count > 0 {
			max += (1 << uint(i))
			filter = mp
		} else if i == 0 {
			count = filter.GetCardinality()
		}
	}
	return b.recoveryFromUint64(max), count
}

func (b *BitSlicedIndex) lt(v uint64, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	zflg := true // leading zero flag
	keep := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		bit := (v >> uint(i)) & 1
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

func (b *BitSlicedIndex) gt(v uint64, filter *roaring.Bitmap, eq bool) *roaring.Bitmap {
	keep := roaring.NewBitmap()
	for i := b.bitSize - 1; i >= 0; i-- {
		bit := (v >> uint(i)) & 1
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
func (b *BitSlicedIndex) setBit(x, y uint64)   { b.slices[x].Add(uint32(y)) }
func (b *BitSlicedIndex) clearBit(x, y uint64) { b.slices[x].Remove(uint32(y)) }
func (b *BitSlicedIndex) bit(x, y uint64) bool { return b.slices[x].Contains(uint32(y)) }

func (b *BitSlicedIndex) subMap(x uint64) *roaring.Bitmap { return b.slices[x] }

func show(mp *roaring.Bitmap) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := mp.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
