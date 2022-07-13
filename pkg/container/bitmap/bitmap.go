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

	"github.com/matrixorigin/matrixone/pkg/encoding"
)

func New(n int) *Bitmap {
	return &Bitmap{
		Len:  n,
		Any:  false,
		Data: make([]byte, (n-1)/8+1),
	}
}

func (n *Bitmap) Iterator() Iterator {
	return &BitmapIterator{i: 0, bm: n}
}

func (itr *BitmapIterator) HasNext() bool {
	for ; itr.i < uint64(itr.bm.Len); itr.i++ {
		if itr.bm.Contains(itr.i) {
			return true
		}
	}
	return false
}

func (itr *BitmapIterator) PeekNext() uint64 {
	for i := itr.i; i < uint64(itr.bm.Len); i++ {
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
	n.Any = false
	n.Data = make([]byte, (n.Len-1)/8+1)
}

func (n *Bitmap) Size() int {
	return len(n.Data)
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	return !n.Any
}

func (n *Bitmap) Add(row uint64) {
	n.Any = true
	n.Data[row>>3] |= 1 << (row & 0x7)
}

func (n *Bitmap) AddMany(rows []uint64) {
	for _, row := range rows {
		n.Data[row>>3] |= 1 << (row & 0x7)
	}
}

func (n *Bitmap) Remove(row uint64) {
	n.Data[row>>3] &= ^(uint8(1) << (row & 0x7))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	return (n.Data[row>>3] & (1 << (row & 0x7))) != 0
}

func (n *Bitmap) RemoveRange(start, end uint64) {
	for ; start < end; start++ {
		if n.Contains(start) {
			n.Remove(start)
		}
	}
}

func (n *Bitmap) Or(m *Bitmap) {
	n.TryExpand(m)
	for i, j := uint64(0), uint64(m.Len); i < j; i++ {
		if m.Contains(i) {
			n.Add(i)
		}
	}
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	if n.Len < m.Len {
		n.Expand(m.Len)
	}
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if n.Len < size {
		n.Expand(size)
	}
}

func (n *Bitmap) Expand(size int) {
	data := make([]byte, (size-1)/8+1)
	copy(data, n.Data)
	n.Len = size
	n.Data = data
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	m := New(n.Len)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			m.Add(uint64(i))
		}
	}
	return m
}

func (n *Bitmap) Numbers() int {
	var cnt int

	for i, j := uint64(0), uint64(n.Len); i < j; i++ {
		if n.Contains(i) {
			cnt++
		}
	}
	return cnt
}

func (n *Bitmap) ToArray() []uint64 {
	var rows []uint64

	for i, j := uint64(0), uint64(n.Len); i < j; i++ {
		if n.Contains(i) {
			rows = append(rows, i)
		}
	}
	return rows
}

func (n *Bitmap) Show() []byte {
	var buf bytes.Buffer

	if n.Any {
		buf.WriteByte('1')
	} else {
		buf.WriteByte('0')
	}
	buf.Write(encoding.EncodeUint64(uint64(n.Len)))
	buf.Write(encoding.EncodeUint64(uint64(len(n.Data))))
	buf.Write(n.Data)
	return buf.Bytes()
}

func (n *Bitmap) Read(data []byte) error {
	if data[0] == '1' {
		n.Any = true
	} else {
		n.Any = false
	}
	data = data[1:]
	n.Len = int(encoding.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(encoding.DecodeUint64(data[:8]))
	data = data[8:]
	n.Data = data[:size]
	return nil
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}
