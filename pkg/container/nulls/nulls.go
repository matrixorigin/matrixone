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

func (nsp *Nulls) Clone() *Nulls {
	if nsp == nil {
		return nil
	}
	if nsp.Np == nil {
		return &Nulls{Np: nil}
	}
	return &Nulls{
		Np: nsp.Np.Clone(),
	}
}

// Or performs union operation on Nulls nsp,m and store the result in r
func Or(nsp, m, r *Nulls) {
	if Ptr(nsp) == nil && Ptr(m) == nil {
		r.Np = nil
		return
	}

	r.Np = bitmap.New(0)
	if Ptr(nsp) != nil {
		r.Np.Or(nsp.Np)
	}
	if Ptr(m) != nil {
		r.Np.Or(m.Np)
	}
}

func Reset(nsp *Nulls) {
	if nsp.Np != nil {
		nsp.Np.Clear()
	}
}

func NewWithSize(size int) *Nulls {
	return &Nulls{
		Np: bitmap.New(size),
	}
}

func Build(size int, rows ...uint64) *Nulls {
	nsp := NewWithSize(size)
	Add(nsp, rows...)
	return nsp
}

// XXX this is so broken,
func New(nsp *Nulls, size int) {
	nsp.Np = bitmap.New(size)
}

// Any returns true if any bit in the Nulls is set, otherwise it will return false.
func Any(nsp *Nulls) bool {
	if nsp == nil || nsp.Np == nil {
		return false
	}
	return !nsp.Np.IsEmpty()
}

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.Np.Ptr()
}

// Size estimates the memory usage of the Nulls.
func Size(nsp *Nulls) int {
	if nsp.Np == nil {
		return 0
	}
	return int(nsp.Np.Size())
}

// Length returns the number of integers contained in the Nulls
func Length(nsp *Nulls) int {
	if nsp == nil || nsp.Np == nil {
		return 0
	}
	return int(nsp.Np.Count())
}

func String(nsp *Nulls) string {
	if nsp.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", nsp.Np.ToArray())
}

func TryExpand(nsp *Nulls, size int) {
	if nsp.Np == nil {
		nsp.Np = bitmap.New(size)
		return
	}
	nsp.Np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
func Contains(nsp *Nulls, row uint64) bool {
	return nsp != nil && nsp.Np != nil && nsp.Np.Contains(row)
}

func Add(nsp *Nulls, rows ...uint64) {
	if len(rows) == 0 {
		return
	}
	if nsp == nil {
		nsp = &Nulls{}
	}
	TryExpand(nsp, int(rows[len(rows)-1])+1)
	nsp.Np.AddMany(rows)
}

func AddRange(nsp *Nulls, start, end uint64) {
	TryExpand(nsp, int(end+1))
	nsp.Np.AddRange(start, end)
}

func Del(nsp *Nulls, rows ...uint64) {
	if nsp.Np == nil {
		return
	}
	for _, row := range rows {
		nsp.Np.Remove(row)
	}
}

// Set performs union operation on Nulls nsp,m and store the result in nsp
func Set(nsp, m *Nulls) {
	if m != nil && m.Np != nil {
		if nsp.Np == nil {
			nsp.Np = bitmap.New(0)
		}
		nsp.Np.Or(m.Np)
	}
}

// FilterCount returns the number count that appears in both nsp and sel
func FilterCount(nsp *Nulls, sels []int64) int {
	var cnt int

	if nsp.Np == nil {
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
		if nsp.Np.Contains(sel) {
			cnt++
		}
	}
	return cnt
}

func RemoveRange(nsp *Nulls, start, end uint64) {
	if nsp.Np != nil {
		nsp.Np.RemoveRange(start, end)
	}
}

// Range adds the numbers in nsp starting at start and ending at end to m.
// `bias` represents the starting offset used for the Range Output
// Return the result
func Range(nsp *Nulls, start, end, bias uint64, m *Nulls) *Nulls {
	switch {
	case nsp.Np == nil && m.Np == nil:
	case nsp.Np != nil && m.Np == nil:
		m.Np = bitmap.New(int(end + 1 - bias))
		for ; start < end; start++ {
			if nsp.Np.Contains(start) {
				m.Np.Add(start - bias)
			}
		}
	case nsp.Np != nil && m.Np != nil:
		m.Np = bitmap.New(int(end + 1 - bias))
		for ; start < end; start++ {
			if nsp.Np.Contains(start) {
				m.Np.Add(start - bias)
			}
		}
	}
	return m
}

func Filter(nsp *Nulls, sels []int64, negate bool) *Nulls {
	if nsp == nil || nsp.Np == nil || len(sels) == 0 {
		return nsp
	}

	if negate {
		oldLen := nsp.Np.Len()
		np := bitmap.New(oldLen)
		for oldIdx, newIdx, selIdx, sel := 0, 0, 0, sels[0]; oldIdx < oldLen; oldIdx++ {
			if oldIdx != int(sel) {
				if nsp.Np.Contains(uint64(oldIdx)) {
					np.Add(uint64(newIdx))
				}
				newIdx++
			} else {
				selIdx++
				if selIdx >= len(sels) {
					for idx := oldIdx + 1; idx < oldLen; idx++ {
						if nsp.Np.Contains(uint64(idx)) {
							np.Add(uint64(newIdx))
						}
						newIdx++
					}
					break
				}
				sel = sels[selIdx]
			}
		}
		nsp.Np = np
		return nsp
	} else {
		np := bitmap.New(len(sels))
		upperLimit := int64(nsp.Np.Len())
		for i, sel := range sels {
			if sel >= upperLimit {
				continue
			}
			if nsp.Np.Contains(uint64(sel)) {
				np.Add(uint64(i))
			}
		}
		nsp.Np = np
		return nsp
	}
}

func (nsp *Nulls) Any() bool {
	if nsp == nil || nsp.Np == nil {
		return false
	}
	return !nsp.Np.IsEmpty()
}

func (nsp *Nulls) Set(row uint64) {
	TryExpand(nsp, int(row)+1)
	nsp.Np.Add(row)
}

func (nsp *Nulls) Contains(row uint64) bool {
	return nsp != nil && nsp.Np != nil && nsp.Np.Contains(row)
}

func (nsp *Nulls) Count() int {
	if nsp == nil || nsp.Np == nil {
		return 0
	}
	return nsp.Np.Count()
}

func (nsp *Nulls) Show() ([]byte, error) {
	if nsp == nil || nsp.Np == nil {
		return nil, nil
	}
	return nsp.Np.Marshal(), nil
}

func (nsp *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.Np = bitmap.New(0)
	nsp.Np.Unmarshal(data)
	return nil
}

func (nsp *Nulls) Or(m *Nulls) *Nulls {
	switch {
	case m == nil:
		return nsp
	case m.Np == nil:
		return nsp
	case nsp.Np == nil && m.Np != nil:
		return m
	default:
		nsp.Np.Or(m.Np)
		return nsp
	}
}

func (nsp *Nulls) IsSame(m *Nulls) bool {
	switch {
	case nsp == nil && m == nil:
		return true
	case nsp.Np == nil && m.Np == nil:
		return true
	case nsp.Np != nil && m.Np != nil:
		return nsp.Np.IsSame(m.Np)
	default:
		return false
	}
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp.Np == nil {
		return []uint64{}
	}
	return nsp.Np.ToArray()
}

func (nsp *Nulls) GetCardinality() int {
	if nsp.Np == nil {
		return 0
	}
	return nsp.Np.Count()
}
