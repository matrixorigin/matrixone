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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math/bits"
	"unsafe"
)

func (n *BitmapV2) InitWith(other *BitmapV2) {
	n.len = other.len
	n.count = other.count
	n.data = append([]uint64(nil), other.data...)
}

func (n *BitmapV2) InitWithSize(len int64) {
	n.len = len
	n.count = 0
	n.data = make([]uint64, (len+63)/64)
}

func (n *BitmapV2) Clone() *BitmapV2 {
	if n == nil {
		return nil
	}
	var ret BitmapV2
	ret.InitWith(n)
	return &ret
}

func (n *BitmapV2) Iterator() Iterator {
	// When initialization, the itr.i is set to the first rightmost_one position.
	itr := BitmapV2Iterator{i: 0, bm: n}
	if first_1_pos, has_next := itr.hasNext(0); has_next {
		itr.i = first_1_pos
		itr.has_next = true
		return &itr
	}
	itr.has_next = false
	return &itr
}

func (itr *BitmapV2Iterator) hasNext(i uint64) (uint64, bool) {
	// if the uint64 is 0, move forward to next word
	// if the uint64 is not 0, then calculate the rightest_one position in a word, add up prev result and return.
	// when there is 1 in BitmapV2, return true, otherwise BitmapV2 is empty and return false.
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

func (itr *BitmapV2Iterator) HasNext() bool {
	// maintain a bool var to avoid unnecessary calculations.
	return itr.has_next
}

func (itr *BitmapV2Iterator) PeekNext() uint64 {
	if itr.has_next {
		return itr.i
	}
	return 0
}

func (itr *BitmapV2Iterator) Next() uint64 {
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
func (n *BitmapV2) Reset() {
	n.len = 0
	n.count = 0
	n.data = nil
}

// Len returns the number of bits in the BitmapV2.
func (n *BitmapV2) Len() int64 {
	return n.len
}

// Size return number of bytes in n.data
// XXX WTF Note that this size is not the same as InitWithSize.
func (n *BitmapV2) Size() int {
	return len(n.data) * 8
}

func (n *BitmapV2) Ptr() *uint64 {
	if n == nil || len(n.data) == 0 {
		return nil
	}
	return &n.data[0]
}

// EmptyByFlag is a quick and dirty way to check if the BitmapV2 is empty.
// If it retruns true, the BitmapV2 is empty.  Otherwise, it may or may not be empty.
func (n *BitmapV2) EmptyByFlag() bool {
	return n == nil || n.count == 0 || len(n.data) == 0
}

// IsEmpty returns true if no bit in the BitmapV2 is set, otherwise it will return false.
func (n *BitmapV2) IsEmpty() bool {
	return n.count == 0
}

// We always assume that BitmapV2 has been extended to at least row.
func (n *BitmapV2) Add(row uint64) {
	if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
		n.count++
	}
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *BitmapV2) Add2(row uint64) {
	if n.count == -1 {
		return
	}
	if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
		n.count++
	}
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *BitmapV2) AddMany(rows []uint64) {
	for _, row := range rows {
		if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
			n.count++
		}
		n.data[row>>6] |= 1 << (row & 0x3F)
	}
}

func (n *BitmapV2) Remove(row uint64) {
	if row >= uint64(n.len) {
		return
	}
	if n.data[row>>6]&(1<<(row&0x3F)) != 0 {
		n.count--
	}
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
}

// Contains returns true if the row is contained in the BitmapV2
func (n *BitmapV2) Contains(row uint64) bool {
	if row >= uint64(n.len) {
		return false
	}
	idx := row >> 6
	return (n.data[idx] & (1 << (row & 0x3F))) != 0
}

func (n *BitmapV2) AddRange(start, end uint64) {
	if start >= end {
		return
	}
	i, j := start>>6, (end-1)>>6
	count := 0
	if i == j {
		mask := (^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) & 0x3F))
		count = bits.OnesCount64(mask &^ n.data[i])
		n.data[i] |= mask
		n.count += int64(count)
		return
	}
	mask := ^uint64(0) << uint(start&0x3F)
	count += bits.OnesCount64(mask &^ n.data[i])
	n.data[i] |= mask
	for k := i + 1; k < j; k++ {
		count += bits.OnesCount64(^n.data[k])
		n.data[k] = ^uint64(0)
	}
	mask = ^uint64(0) >> (uint(-end) & 0x3F)
	count += bits.OnesCount64(mask &^ n.data[j])
	n.data[j] |= mask
	n.count += int64(count)
}

func (n *BitmapV2) RemoveRange(start, end uint64) {
	if end > uint64(n.len) {
		end = uint64(n.len)
	}
	if start >= end {
		return
	}
	count := 0
	i, j := start>>6, (end-1)>>6
	if i == j {
		mask := (^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) % 0x3F))
		count = bits.OnesCount64(n.data[i] & mask)
		n.data[i] &= ^mask
		n.count -= int64(count)
		return
	}
	mask := ^uint64(0) << uint(start&0x3F)
	count += bits.OnesCount64(n.data[i] & mask)
	n.data[i] &= ^mask
	for k := i + 1; k < j; k++ {
		count += bits.OnesCount64(n.data[k])
		n.data[k] = 0
	}
	mask = ^uint64(0) >> (uint(-end) & 0x3F)
	count += bits.OnesCount64(n.data[j] & mask)
	n.data[j] &= ^mask
	n.count -= int64(count)
}

func (n *BitmapV2) IsSame(m *BitmapV2) bool {
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

func (n *BitmapV2) Or(m *BitmapV2) {
	n.TryExpand(m)
	size := (int(m.len) + 63) / 64
	for i := 0; i < size; i++ {
		if n.data[i] == 0 && m.data[i] != 0 {
			n.count++
		}
		n.data[i] |= m.data[i]
	}
}

func (n *BitmapV2) And(m *BitmapV2) {
	n.TryExpand(m)
	size := (int(m.len) + 63) / 64
	for i := 0; i < size; i++ {
		if n.data[i] != 0 && m.data[i] == 0 {
			n.count--
		}
		n.data[i] &= m.data[i]
	}
	for i := size; i < len(n.data); i++ {
		n.count -= int64(bits.OnesCount64(n.data[i]))
		n.data[i] = 0
	}
}

func (n *BitmapV2) Negate() {
	nBlock, nTail := int(n.len)/64, int(n.len)%64
	for i := 0; i < nBlock; i++ {
		n.data[i] = ^n.data[i]
		cnt := bits.OnesCount64(n.data[i])
		n.count += int64(cnt - (64 - cnt))
	}
	if nTail > 0 {
		mask := (uint64(1) << nTail) - 1
		n.data[nBlock] ^= mask
		cnt := bits.OnesCount64(n.data[nBlock] & mask)
		n.count += int64(cnt - (nTail - cnt))
	}
}

func (n *BitmapV2) TryExpand(m *BitmapV2) {
	n.TryExpandWithSize(int(m.len))
}

func (n *BitmapV2) TryExpandWithSize(size int) {
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

func (n *BitmapV2) Filter(sels []int64) *BitmapV2 {
	var m BitmapV2
	m.InitWithSize(n.len)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			m.Add(uint64(i))
		}
	}
	return &m
}

func (n *BitmapV2) Count() int {
	return int(n.count)
}

func (n *BitmapV2) ToArray() []uint64 {
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

func (n *BitmapV2) ToI64Arrary() []int64 {
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

func (n *BitmapV2) Marshal() []byte {
	var buf bytes.Buffer
	u1 := uint64(n.len)
	u2 := uint64(len(n.data) * 8)
	buf.Write(types.EncodeInt64(&n.count))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(n.data))
	return buf.Bytes()
}

func (n *BitmapV2) Unmarshal(data []byte) {
	n.count = types.DecodeInt64(data[:8])
	data = data[8:]
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

func (n *BitmapV2) UnmarshalNoCopy(data []byte) {
	n.count = types.DecodeInt64(data[:8])
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

func (n *BitmapV2) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}

var _ encoding.BinaryMarshaler = new(BitmapV2)

func (n *BitmapV2) MarshalBinary() ([]byte, error) {
	return n.Marshal(), nil
}

var _ encoding.BinaryUnmarshaler = new(BitmapV2)

func (n *BitmapV2) UnmarshalBinary(data []byte) error {
	n.Unmarshal(data)
	return nil
}

func (n *BitmapV2) Clear() {
	n.count = 0
	for i := range n.data {
		n.data[i] = 0
	}
}
