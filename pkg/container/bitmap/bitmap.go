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

import "fmt"

func New(n int) *Bitmap {
	return &Bitmap{
		Len:  n,
		Any:  false,
		Data: make([]byte, (n-1)/8+1),
	}
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	return !n.Any
}

func (n *Bitmap) Add(row uint64) {
	n.Data[row>>3] |= 1 << (row & 0x7)
	n.Any = true
}

func (n *Bitmap) Del(row uint64) {
	n.Data[row>>3] &= ^(uint8(1) << (row & 0x7))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	return (n.Data[row>>3] & (1 << (row & 0x7))) != 0
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

func (n *Bitmap) Rows() []uint64 {
	var rows []uint64

	for i, j := uint64(0), uint64(n.Len); i < j; i++ {
		if n.Contains(i) {
			rows = append(rows, i)
		}
	}
	return rows
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.Rows())
}
