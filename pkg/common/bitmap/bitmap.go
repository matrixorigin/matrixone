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

package bitmap

import (
	"bytes"
	"encoding"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

//
// In case len is not multiple of 64, many of these code following assumes the trailing
// bits of last uint64 are zero.   This may well be true in all our usage.  So let's
// leave as it is for now.
//

type bitmask = uint64

/*
 * Array giving the position of the right-most set bit for each possible
 * byte value. count the right-most position as the 0th bit, and the
 * left-most the 7th bit.  The 0th entry of the array should not be used.
 * e.g. 2 = 0x10 ==> rightmost_one_pos_8[2] = 1, 3 = 0x11 ==> rightmost_one_pos_8[3] = 0
 */
var rightmost_one_pos_8 = [256]uint8{
	0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
}

func New() Bitmap {
	return Bitmap{}
}

func (n *Bitmap) InitWith(other *Bitmap) {
	n.len = other.len
	n.emptyFlag.Store(other.emptyFlag.Load())
	n.data = append([]uint64(nil), other.data...)
}

func (n *Bitmap) InitWithSize(len int64) {
	n.len = len
	n.emptyFlag.Store(kEmptyFlagEmpty)
	n.data = make([]uint64, (len+63)/64)
}

func (n *Bitmap) Clone() *Bitmap {
	if n == nil {
		return nil
	}
	var ret Bitmap
	ret.InitWith(n)
	return &ret
}

func (n *Bitmap) Iterator() Iterator {
	// When initialization, the itr.i is set to the first rightmost_one position.
	itr := BitmapIterator{i: 0, bm: n}
	if first_1_pos, has_next := itr.hasNext(0); has_next {
		itr.i = first_1_pos
		itr.has_next = true
		return &itr
	}
	itr.has_next = false
	return &itr
}

func rightmost_one_pos_64(word uint64) uint64 {
	// find out the rightmost_one position.
	// Firstly, use eight bits as a group to quickly determine whether there is a 1 in it.
	// if not, then rightmost_one exists in next group, add up the distance with result and shift the word
	// if rightmost_one exists in this group, get the distance directly from a pre-made hash table
	var result uint64
	for {
		if (word & 0xFF) == 0 {
			word >>= 8
			result += 8
		} else {
			break
		}
	}
	result += uint64(rightmost_one_pos_8[word&255])
	return result
}

func (itr *BitmapIterator) hasNext(i uint64) (uint64, bool) {
	// if the uint64 is 0, move forward to next word
	// if the uint64 is not 0, then calculate the rightest_one position in a word, add up prev result and return.
	// when there is 1 in bitmap, return true, otherwise bitmap is empty and return false.
	// either case loop over words not bits
	nwords := (itr.bm.len + 63) / 64
	current_word := i >> 6
	mask := (^(bitmask)(0)) << (i & 0x3F) // ignore bits check before
	var result uint64

	for ; current_word < uint64(nwords); current_word++ {
		word := itr.bm.data[current_word]
		word &= mask

		if word != 0 {
			result = rightmost_one_pos_64(word) + current_word*64
			return result, true
		}
		mask = (^(bitmask)(0)) // in subsequent words, consider all bits
	}
	return result, false
}

func (itr *BitmapIterator) HasNext() bool {
	// maintain a bool var to avoid unnecessary calculations.
	return itr.has_next
}

func (itr *BitmapIterator) PeekNext() uint64 {
	if itr.has_next {
		return itr.i
	}
	return 0
}

func (itr *BitmapIterator) Next() uint64 {
	// When a iterator is initialized, the itr.i is set to the first rightmost_one pos.
	// so current itr.i is a rightmost_one pos, cal the next one pos and return current pos.
	pos := itr.i
	if next, has_next := itr.hasNext(itr.i + 1); has_next { // itr.i + 1 to ignore bits check before
		itr.i = next
		itr.has_next = true
		return pos
	}
	itr.has_next = false
	return pos
}

// Reset set n.data to nil
func (n *Bitmap) Reset() {
	n.len = 0
	// if n.emptyFlag == nil {
	// 	panic("emptyFlag is nil")
	// }
	n.emptyFlag.Store(kEmptyFlagEmpty)
	n.data = nil
}

// Len returns the number of bits in the Bitmap.
func (n *Bitmap) Len() int64 {
	return n.len
}

// Size return number of bytes in n.data
// XXX WTF Note that this size is not the same as InitWithSize.
func (n *Bitmap) Size() int {
	return len(n.data) * 8
}

func (n *Bitmap) Ptr() *uint64 {
	if n == nil || len(n.data) == 0 {
		return nil
	}
	return &n.data[0]
}

// EmptyByFlag is a quick and dirty way to check if the bitmap is empty.
// If it retruns true, the bitmap is empty.  Otherwise, it may or may not be empty.
func (n *Bitmap) EmptyByFlag() bool {
	return n == nil || n.emptyFlag.Load() == kEmptyFlagEmpty || len(n.data) == 0
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	// if n.emptyFlag == nil {
	// 	panic("emptyFlag is nil")
	// }
	flag := n.emptyFlag.Load()
	if flag == kEmptyFlagEmpty {
		return true
	} else if flag == kEmptyFlagNotEmpty {
		return false
	}
	for i := 0; i < len(n.data); i++ {
		if n.data[i] != 0 {
			n.emptyFlag.Store(kEmptyFlagNotEmpty)
			return false
		}
	}
	n.emptyFlag.Store(kEmptyFlagEmpty)
	return true
}

// We always assume that bitmap has been extended to at least row.
func (n *Bitmap) Add(row uint64) {
	n.data[row>>6] |= 1 << (row & 0x3F)
	n.emptyFlag.Store(kEmptyFlagNotEmpty)
}

func (n *Bitmap) AddMany(rows []uint64) {
	for _, row := range rows {
		n.data[row>>6] |= 1 << (row & 0x3F)
	}
	n.emptyFlag.Store(kEmptyFlagNotEmpty)
}

func (n *Bitmap) Remove(row uint64) {
	if row >= uint64(n.len) {
		return
	}
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
	n.emptyFlag.CompareAndSwap(kEmptyFlagNotEmpty, kEmptyFlagUnknown)
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	if row >= uint64(n.len) {
		return false
	}
	idx := row >> 6
	return (n.data[idx] & (1 << (row & 0x3F))) != 0
}

func (n *Bitmap) AddRange(start, end uint64) {
	if start >= end {
		return
	}
	i, j := start>>6, (end-1)>>6
	if i == j {
		n.data[i] |= (^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) & 0x3F))
		n.emptyFlag.Store(kEmptyFlagNotEmpty)
		return
	}
	n.data[i] |= (^uint64(0) << uint(start&0x3F))
	for k := i + 1; k < j; k++ {
		n.data[k] = ^uint64(0)
	}
	n.data[j] |= (^uint64(0) >> (uint(-end) & 0x3F))

	n.emptyFlag.Store(kEmptyFlagNotEmpty)
}

func (n *Bitmap) RemoveRange(start, end uint64) {
	if end > uint64(n.len) {
		end = uint64(n.len)
	}
	if start >= end {
		return
	}
	i, j := start>>6, (end-1)>>6
	if i == j {
		n.data[i] &= ^((^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) % 0x3F)))
		n.emptyFlag.CompareAndSwap(kEmptyFlagNotEmpty, kEmptyFlagUnknown)
		return
	}
	n.data[i] &= ^(^uint64(0) << uint(start&0x3F))
	for k := i + 1; k < j; k++ {
		n.data[k] = 0
	}
	n.data[j] &= ^(^uint64(0) >> (uint(-end) & 0x3F))
	n.emptyFlag.CompareAndSwap(kEmptyFlagNotEmpty, kEmptyFlagUnknown)
}

func (n *Bitmap) IsSame(m *Bitmap) bool {
	//if n.len != m.len ||
	if len(m.data) != len(n.data) {
		return false
	}
	for i := 0; i < len(n.data); i++ {
		if n.data[i] != m.data[i] {
			return false
		}
	}
	return true
}

func (n *Bitmap) Or(m *Bitmap) {
	n.TryExpand(m)
	size := (int(m.len) + 63) / 64
	for i := 0; i < size; i++ {
		n.data[i] |= m.data[i]
	}
	n.emptyFlag.CompareAndSwap(kEmptyFlagEmpty, kEmptyFlagUnknown)
}

func (n *Bitmap) And(m *Bitmap) {
	n.TryExpand(m)
	size := (int(m.len) + 63) / 64
	for i := 0; i < size; i++ {
		n.data[i] &= m.data[i]
	}
	for i := size; i < len(n.data); i++ {
		n.data[i] = 0
	}
	n.emptyFlag.CompareAndSwap(kEmptyFlagNotEmpty, kEmptyFlagUnknown)
}

func (n *Bitmap) Negate() {
	nBlock, nTail := int(n.len)/64, int(n.len)%64
	for i := 0; i < nBlock; i++ {
		n.data[i] = ^n.data[i]
	}
	if nTail > 0 {
		mask := (uint64(1) << nTail) - 1
		n.data[nBlock] ^= mask
	}
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	n.TryExpandWithSize(int(m.len))
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if int(n.len) >= size {
		return
	}
	newCap := (size + 63) / 64
	n.len = int64(size)
	if newCap > cap(n.data) {
		data := make([]uint64, newCap)
		copy(data, n.data)
		n.data = data
		return
	}
	if len(n.data) < newCap {
		n.data = n.data[:newCap]
	}
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	var m Bitmap
	m.InitWithSize(n.len)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			m.Add(uint64(i))
		}
	}
	return &m
}

func (n *Bitmap) Count() int {
	var cnt int
	if n.emptyFlag.Load() == kEmptyFlagEmpty { //must be empty
		return 0
	}
	for i := int64(0); i < n.len/64; i++ {
		cnt += bits.OnesCount64(n.data[i])
	}
	if offset := n.len % 64; offset > 0 {
		start := (n.len / 64) * 64
		for i, j := start, start+offset; i < j; i++ {
			if n.Contains(uint64(i)) {
				cnt++
			}
		}
	}
	if cnt > 0 {
		n.emptyFlag.Store(kEmptyFlagNotEmpty)
	} else {
		n.emptyFlag.Store(kEmptyFlagEmpty)
	}
	return cnt
}

func (n *Bitmap) ToArray() []uint64 {
	var rows []uint64
	if n.EmptyByFlag() {
		return rows
	}

	itr := n.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		rows = append(rows, r)
	}
	return rows
}

func (n *Bitmap) ToI64Arrary() []int64 {
	var rows []int64
	if n.EmptyByFlag() {
		return rows
	}

	itr := n.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		rows = append(rows, int64(r))
	}
	return rows
}

func (n *Bitmap) Marshal() []byte {
	var buf bytes.Buffer
	flag := n.emptyFlag.Load()
	u1 := uint64(n.len)
	u2 := uint64(len(n.data) * 8)
	buf.Write(types.EncodeInt32(&flag))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(n.data))
	return buf.Bytes()
}

func (n *Bitmap) Unmarshal(data []byte) {
	n.emptyFlag.Store(types.DecodeInt32(data[:4]))
	data = data[4:]
	n.len = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

func (n *Bitmap) UnmarshalNoCopy(data []byte) {
	n.emptyFlag.Store(types.DecodeInt32(data[:4]))
	data = data[4:]
	n.len = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), size/8)
	}
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}

var _ encoding.BinaryMarshaler = new(Bitmap)

func (n *Bitmap) MarshalBinary() ([]byte, error) {
	return n.Marshal(), nil
}

var _ encoding.BinaryUnmarshaler = new(Bitmap)

func (n *Bitmap) UnmarshalBinary(data []byte) error {
	n.Unmarshal(data)
	return nil
}
