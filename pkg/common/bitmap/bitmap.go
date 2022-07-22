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
	"fmt"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/encoding"
)

//
// In case len is not multiple of 64, many of these code following assumes the trailing
// bits of last uint64 are zero.   This may well be true in all our usage.  So let's
// leave as it is for now.
//

func New(n int) *Bitmap {
	return &Bitmap{
		len:  n,
		data: make([]uint64, (n-1)/64+1),
	}
}

func (n *Bitmap) Clone() *Bitmap {
	var ret Bitmap
	ret.len = n.len
	ret.data = make([]uint64, len(n.data))
	copy(ret.data, n.data)
	return &ret
}

func (n *Bitmap) Iterator() Iterator {
	return &BitmapIterator{i: 0, bm: n}
}

func (itr *BitmapIterator) HasNext() bool {
	for ; itr.i < uint64(itr.bm.len); itr.i++ {
		if itr.bm.Contains(itr.i) {
			return true
		}
	}
	return false
}

func (itr *BitmapIterator) PeekNext() uint64 {
	for i := itr.i; i < uint64(itr.bm.len); i++ {
		if itr.bm.Contains(i) {
			return i
		}
	}
	return 0
}

func (itr *BitmapIterator) Next() uint64 {
	return itr.i
}

func (n *Bitmap) Clear() {
	n.data = make([]uint64, (n.len-1)/64+1)
}

func (n *Bitmap) Len() int {
	return n.len
}

func (n *Bitmap) Size() int {
	return len(n.data) * 8
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	for i := 0; i < len(n.data); i++ {
		if n.data[i] != 0 {
			return false
		}
	}
	return true
}

func (n *Bitmap) Add(row uint64) {
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *Bitmap) AddMany(rows []uint64) {
	for _, row := range rows {
		n.data[row>>6] |= 1 << (row & 0x3F)
	}
}

func (n *Bitmap) Remove(row uint64) {
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	return (n.data[row>>6] & (1 << (row & 0x3F))) != 0
}

func (n *Bitmap) AddRange(start, end uint64) {
	if start >= end {
		return
	}
	i, j := start>>6, (end-1)>>6
	if i == j {
		n.data[i] |= (^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) & 0x3F))
		return
	}
	n.data[i] |= (^uint64(0) << uint(start&0x3F))
	for k := i + 1; k < j; k++ {
		n.data[k] = ^uint64(0)
	}
	n.data[j] |= (^uint64(0) >> (uint(-end) & 0x3F))
}

func (n *Bitmap) RemoveRange(start, end uint64) {
	if start >= end {
		return
	}
	i, j := start>>6, (end-1)>>6
	if i == j {
		n.data[i] &= ^((^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) % 0x3F)))
		return
	}
	n.data[i] &= ^(^uint64(0) << uint(start&0x3F))
	for k := i + 1; k < j; k++ {
		n.data[k] = 0
	}
	n.data[j] &= ^(^uint64(0) >> (uint(-end) & 0x3F))
}

func (n *Bitmap) IsSame(m *Bitmap) bool {
	if n.len != m.len || len(m.data) != len(n.data) {
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
	for i := 0; i < len(n.data); i++ {
		n.data[i] |= m.data[i]
	}
}

func (n *Bitmap) And(m *Bitmap) {
	n.TryExpand(m)
	for i := 0; i < len(n.data); i++ {
		n.data[i] &= m.data[i]
	}
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	if n.len < m.len {
		n.Expand(m.len)
	}
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if n.len < size {
		n.Expand(size)
	}
}

func (n *Bitmap) Expand(size int) {
	data := make([]uint64, (size-1)/64+1)
	copy(data, n.data)
	n.len = size
	n.data = data
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	m := New(n.len)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			m.Add(uint64(i))
		}
	}
	return m
}

func (n *Bitmap) Count() int {
	var cnt int

	for i := 0; i < len(n.data); i++ {
		cnt += bits.OnesCount64(n.data[i])
	}
	return cnt
}

func (n *Bitmap) ToArray() []uint64 {
	var rows []uint64

	for i := uint64(0); i < uint64(n.len); i++ {
		if (n.data[i>>6] & (1 << (i & 0x3F))) != 0 {
			rows = append(rows, i)
		}
	}
	return rows
}

func (n *Bitmap) Marshal() []byte {
	var buf bytes.Buffer

	buf.Write(encoding.EncodeUint64(uint64(n.len)))
	buf.Write(encoding.EncodeUint64(uint64(len(n.data) * 8)))
	buf.Write(encoding.EncodeFixedSlice(n.data, 8))
	return buf.Bytes()
}

func (n *Bitmap) Unmarshal(data []byte) {
	n.len = int(encoding.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(encoding.DecodeUint64(data[:8]))
	data = data[8:]
	n.data = encoding.DecodeFixedSlice[uint64](data[:size], 8)
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}
