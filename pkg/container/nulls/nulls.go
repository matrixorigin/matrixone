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

// Package nulls wrap up functions for the manipulation of bitmap library roaring.
// MatrixOne uses nulls to store all NULL values in a column.
// You can think of Nulls as a bitmap.
package nulls

import (
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
)

type Nulls struct {
	Np *bitmap.Bitmap
}

func (n *Nulls) Clone() *Nulls {
	if n == nil {
		return nil
	}
	if n.Np == nil {
		return &Nulls{Np: nil}
	}
	return &Nulls{
		Np: n.Np.Clone(),
	}
}

// Or performs union operation on Nulls n,m and store the result in r
func Or(n, m, r *Nulls) {
	if Ptr(n) == nil && Ptr(m) == nil {
		r.Np = nil
		return
	}

	r.Np = bitmap.New(0)
	if Ptr(n) != nil {
		r.Np.Or(n.Np)
	}
	if Ptr(m) != nil {
		r.Np.Or(m.Np)
	}
}

func Reset(n *Nulls) {
	if n.Np != nil {
		n.Np.Clear()
	}
}

func NewWithSize(size int) *Nulls {
	return &Nulls{
		Np: bitmap.New(size),
	}
}

func Build(size int, rows ...uint64) *Nulls {
	n := NewWithSize(size)
	Add(n, rows...)
	return n
}

// XXX this is so broken,
func New(n *Nulls, size int) {
	n.Np = bitmap.New(size)
}

// Any returns true if any bit in the Nulls is set, otherwise it will return false.
func Any(n *Nulls) bool {
	if n == nil || n.Np == nil {
		return false
	}
	return !n.Np.IsEmpty()
}

func Ptr(n *Nulls) *uint64 {
	if n == nil {
		return nil
	}
	return n.Np.Ptr()
}

// Size estimates the memory usage of the Nulls.
func Size(n *Nulls) int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.Size())
}

// Length returns the number of integers contained in the Nulls
func Length(n *Nulls) int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.Count())
}

func String(n *Nulls) string {
	if n.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", n.Np.ToArray())
}

func TryExpand(n *Nulls, size int) {
	if n.Np == nil {
		n.Np = bitmap.New(size)
		return
	}
	n.Np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
func Contains(n *Nulls, row uint64) bool {
	return n != nil && n.Np != nil && n.Np.Contains(row)
}

func Add(n *Nulls, rows ...uint64) {
	if len(rows) == 0 {
		return
	}
	if n == nil {
		n = &Nulls{}
	}
	TryExpand(n, int(rows[len(rows)-1])+1)
	n.Np.AddMany(rows)
}

func AddRange(n *Nulls, start, end uint64) {
	TryExpand(n, int(end+1))
	n.Np.AddRange(start, end)
}

func Del(n *Nulls, rows ...uint64) {
	if n.Np == nil {
		return
	}
	for _, row := range rows {
		n.Np.Remove(row)
	}
}

// Set performs union operation on Nulls n,m and store the result in n
func Set(n, m *Nulls) {
	if m != nil && m.Np != nil {
		if n.Np == nil {
			n.Np = bitmap.New(0)
		}
		n.Np.Or(m.Np)
	}
}

// FilterCount returns the number count that appears in both n and sel
func FilterCount(n *Nulls, sels []int64) int {
	var cnt int

	if n.Np == nil {
		return cnt
	}
	if len(sels) == 0 {
		return cnt
	}
	var sp []uint64
	if len(sels) > 0 {
		sp = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	}
	for _, sel := range sp {
		if n.Np.Contains(sel) {
			cnt++
		}
	}
	return cnt
}

func RemoveRange(n *Nulls, start, end uint64) {
	if n.Np != nil {
		n.Np.RemoveRange(start, end)
	}
}

// Range adds the numbers in n starting at start and ending at end to m.
// `bias` represents the starting offset used for the Range Output
// Return the result
func Range(n *Nulls, start, end, bias uint64, m *Nulls) *Nulls {
	switch {
	case n.Np == nil && m.Np == nil:
	case n.Np != nil && m.Np == nil:
		m.Np = bitmap.New(int(end + 1 - bias))
		for ; start < end; start++ {
			if n.Np.Contains(start) {
				m.Np.Add(start - bias)
			}
		}
	case n.Np != nil && m.Np != nil:
		m.Np = bitmap.New(int(end + 1 - bias))
		for ; start < end; start++ {
			if n.Np.Contains(start) {
				m.Np.Add(start - bias)
			}
		}
	}
	return m
}

func Filter(n *Nulls, sels []int64) *Nulls {
	if n.Np == nil {
		return n
	}
	if len(sels) == 0 {
		return n
	}
	var sp []uint64
	if len(sels) > 0 {
		sp = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	}
	np := bitmap.New(len(sels))
	upperLimit := uint64(n.Np.Len())
	for i, sel := range sp {
		if sel >= upperLimit {
			continue
		}
		if n.Np.Contains(sel) {
			np.Add(uint64(i))
		}
	}
	n.Np = np
	return n
}

func (n *Nulls) Any() bool {
	if n == nil || n.Np == nil {
		return false
	}
	return !n.Np.IsEmpty()
}

func (n *Nulls) Set(row uint64) {
	TryExpand(n, int(row)+1)
	n.Np.Add(row)
}

func (n *Nulls) Contains(row uint64) bool {
	return n != nil && n.Np != nil && n.Np.Contains(row)
}

func (n *Nulls) Show() ([]byte, error) {
	if n.Np == nil {
		return nil, nil
	}
	return n.Np.Marshal(), nil
}

func (n *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	n.Np = bitmap.New(0)
	n.Np.Unmarshal(data)
	return nil
}

func (n *Nulls) Or(m *Nulls) *Nulls {
	switch {
	case m == nil:
		return n
	case n.Np == nil && m.Np == nil:
		return n
	case n.Np != nil && m.Np == nil:
		return n
	case n.Np == nil && m.Np != nil:
		return m
	default:
		n.Np.Or(m.Np)
		return n
	}
}
