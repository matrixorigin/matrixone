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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
)

type Nulls struct {
	np bitmap.Bitmap
}

func (nsp *Nulls) InitWithSize(n int) {
	nsp.np.InitWithSize(n)
}

func NewWithSize(n int) *Nulls {
	var nsp Nulls
	nsp.InitWithSize(n)
	return &nsp
}

func (nsp *Nulls) Init(other *Nulls) {
	nsp.np.Init(&other.np)
}

func (nsp *Nulls) SetBitmap(bm *bitmap.Bitmap) {
	nsp.np = *bm
}

func (nsp *Nulls) Clone() *Nulls {
	var n Nulls
	n.SetBitmap(nsp.np.Clone())
	return &n
}

// Or performs union operation on Nulls nsp,m and store the result in r
func Or(nsp, m, r *Nulls) {
	if nsp.IsEmpty() && m.IsEmpty() {
		r.Reset()
		return
	}

	if !nsp.IsEmpty() {
		r.np.Or(&nsp.np)
	}
	if !m.IsEmpty() {
		r.np.Or(&m.np)
	}
}

func (nsp *Nulls) Free() {
	nsp.np.Free()
}

func (nsp *Nulls) Reset() {
	nsp.np.Reset()
}

func (nsp *Nulls) Clear() {
	nsp.np.Clear()
}

func Build(size int, rows ...uint64) *Nulls {
	var nsp Nulls
	nsp.Build(size, rows...)
	return &nsp
}

func (nsp *Nulls) Build(size int, rows ...uint64) {
	nsp.InitWithSize(size)
	Add(nsp, rows...)
}

// Any returns true if any bit in the Nulls is set, otherwise it will return false.
func Any(nsp *Nulls) bool {
	return !nsp.IsEmpty()
}

func (nsp *Nulls) IsEmpty() bool {
	if nsp == nil {
		return true
	}
	return nsp.np.IsEmpty()
}

// Size estimates the memory usage of the Nulls.
func Size(nsp *Nulls) int {
	if nsp == nil {
		return 0
	}
	return int(nsp.np.Size())
}

func TryExpand(nsp *Nulls, size int) {
	nsp.np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
func Contains(nsp *Nulls, row uint64) bool {
	return nsp.Contains(row)
}

// Add adds the integer to the Nulls
// Note: rows must be in ascending order
func Add(nsp *Nulls, rows ...uint64) {
	if len(rows) == 0 {
		return
	}
	TryExpand(nsp, int(rows[len(rows)-1])+1)
	nsp.np.AddMany(rows)
}

// Why do we have this two conventions?
func (n *Nulls) AddRange(start, end uint64) {
	AddRange(n, start, end)
}
func AddRange(nsp *Nulls, start, end uint64) {
	TryExpand(nsp, int(end+1))
	nsp.np.AddRange(start, end)
}

func Del(nsp *Nulls, rows ...uint64) {
	if !nsp.np.IsEmpty() {
		for _, row := range rows {
			nsp.np.Remove(row)
		}
	}
}

// Set performs union operation on Nulls nsp,m and store the result in nsp
// XXX: It is a union, not set!
func Set(nsp, m *Nulls) {
	if !m.IsEmpty() {
		nsp.np.Or(&m.np)
	}
}

// FilterCount returns the number count that appears in both nsp and sel
func FilterCount(nsp *Nulls, sels []int64) int {
	if nsp.IsEmpty() || len(sels) == 0 {
		return 0
	}

	var cnt int
	// WTF is this?   cannt we just use sels directly with a correct type?
	// var sp []uint64
	// if len(sels) > 0 {
	// sp = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	// }
	for _, sel := range sels {
		if nsp.Contains(uint64(sel)) {
			cnt++
		}
	}
	return cnt
}

func RemoveRange(nsp *Nulls, start, end uint64) {
	if !nsp.IsEmpty() {
		nsp.np.RemoveRange(start, end)
	}
}

// Range adds the numbers in nsp starting at start and ending at end to m.
// `bias` represents the starting offset used for the Range Output
func Range(nsp *Nulls, start, end, bias uint64, m *Nulls) {
	if !nsp.IsEmpty() {
		m.InitWithSize(int(end + 1 - bias))
		for ; start < end; start++ {
			if nsp.np.Contains(start) {
				m.np.Add(start - bias)
			}
		}
	}
}

func Filter(nsp *Nulls, sels []int64, negate bool) {
	if nsp.IsEmpty() || len(sels) == 0 {
		return
	}

	if negate {
		// create a new bitmap
		oldLen := nsp.np.Len()
		np := bitmap.New(oldLen)

		// iterate over the old bitmap and set the new bitmap
		for oldIdx, newIdx, selIdx, sel := 0, 0, 0, sels[0]; oldIdx < oldLen; oldIdx++ {
			if oldIdx != int(sel) {
				if nsp.np.Contains(uint64(oldIdx)) {
					np.Add(uint64(newIdx))
				}
				newIdx++
			} else {
				selIdx++
				if selIdx >= len(sels) {
					for idx := oldIdx + 1; idx < oldLen; idx++ {
						if nsp.np.Contains(uint64(idx)) {
							np.Add(uint64(newIdx))
						}
						newIdx++
					}
					break
				}
				sel = sels[selIdx]
			}
		}
		nsp.SetBitmap(np)
	} else {
		np := bitmap.New(len(sels))
		upperLimit := int64(nsp.np.Len())
		for i, sel := range sels {
			if sel >= upperLimit {
				continue
			}
			if nsp.np.Contains(uint64(sel)) {
				np.Add(uint64(i))
			}
		}
		nsp.SetBitmap(np)
	}
}

func (nsp *Nulls) Any() bool {
	return !nsp.IsEmpty()
}

func (nsp *Nulls) Set(row uint64) {
	TryExpand(nsp, int(row)+1)
	nsp.np.Add(row)
}

func (nsp *Nulls) Unset(row uint64) {
	if !nsp.IsEmpty() {
		nsp.np.Remove(row)
	}
}

func (nsp *Nulls) Contains(row uint64) bool {
	return !nsp.IsEmpty() && nsp.np.Contains(row)
}

func Count(nsp *Nulls) int {
	return nsp.Count()
}

func (nsp *Nulls) Count() int {
	if nsp.IsEmpty() {
		return 0
	}
	return nsp.np.Count()
}

// XXX: Show marshals bitmap, copies data.
func (nsp *Nulls) Show() ([]byte, error) {
	if nsp.Count() == 0 {
		return nil, nil
	}
	return nsp.np.Marshal(), nil
}

// XXX: Read unmarshals, copies data.
func (nsp *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.Unmarshal(data)
	return nil
}

func (nsp *Nulls) Or(m *Nulls) *Nulls {
	if m.IsEmpty() {
		return nsp
	}
	if nsp.IsEmpty() {
		return m
	}

	nsp.np.Or(&m.np)
	return nsp
}

func (nsp *Nulls) IsSame(m *Nulls) bool {
	if nsp == m {
		return true
	}
	if nsp.IsEmpty() && m.IsEmpty() {
		return true
	}
	if nsp.IsEmpty() != m.IsEmpty() {
		return false
	}
	return nsp.np.IsSame(&m.np)
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.np.ToArray()
}

func String(nsp *Nulls) string {
	return nsp.String()
}

func (nsp *Nulls) String() string {
	if nsp == nil {
		return "[]"
	}
	return nsp.np.String()
}

func (nsp *Nulls) Ptr() *uint64 {
	return nsp.np.Ptr()
}

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.np.Ptr()
}

func (nsp *Nulls) Iterator() bitmap.Iterator {
	if nsp == nil {
		return nil
	}
	return nsp.np.Iterator()
}
