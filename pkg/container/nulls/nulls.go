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
	"golang.org/x/exp/constraints"
)

type Bitmap = Nulls

type Nulls struct {
	np bitmap.Bitmap
}

func (nsp *Nulls) Clone() *Nulls {
	if nsp == nil {
		return nil
	}
	var n Nulls
	n.InitWith(nsp)
	return &n
}

func (nsp *Nulls) InitWith(n *Nulls) {
	nsp.np.InitWith(&n.np)
}

func (nsp *Nulls) InitWithSize(size int) {
	nsp.np.InitWithSize(int64(size))
}

func NewWithSize(size int) *Nulls {
	var n Nulls
	n.InitWithSize(size)
	return &n
}

func (nsp *Nulls) Reset() {
	nsp.np.Reset()
}

func (nsp *Nulls) GetBitmap() *bitmap.Bitmap {
	return &nsp.np
}

// Or performs union operation on Nulls nsp,m and store the result in r
func Or(nsp, m, r *Nulls) {
	if nsp.EmptyByFlag() && m.EmptyByFlag() {
		r.Reset()
	}

	if !nsp.EmptyByFlag() {
		r.np.Or(&nsp.np)
	}

	if !m.EmptyByFlag() {
		r.np.Or(&m.np)
	}
}

func (nsp *Nulls) Build(size int, rows ...uint64) {
	nsp.InitWithSize(size)
	Add(nsp, rows...)
}

func Build(size int, rows ...uint64) *Nulls {
	var n Nulls
	n.Build(size, rows...)
	return &n
}

// Any returns true if any bit in the Nulls is set, otherwise it will return false.
func Any(nsp *Nulls) bool {
	if nsp == nil {
		return false
	}
	return !nsp.np.IsEmpty()
}

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.np.Ptr()
}

// Size estimates the memory usage of the Nulls.
func Size(nsp *Nulls) int {
	if nsp == nil {
		return 0
	}
	return nsp.np.Size()
}

func String(nsp *Nulls) string {
	if nsp == nil || nsp.np.EmptyByFlag() {
		return "[]"
	}
	return fmt.Sprintf("%v", nsp.np.ToArray())
}

func TryExpand(nsp *Nulls, size int) {
	nsp.np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
func (nsp *Nulls) Contains(row uint64) bool {
	return nsp != nil && !nsp.np.EmptyByFlag() && nsp.np.Contains(row)
}

func Contains(nsp *Nulls, row uint64) bool {
	return nsp.Contains(row)
}

func (nsp *Nulls) Add(sels ...uint64) {
	if nsp == nil || len(sels) == 0 {
		return
	}
	TryExpand(nsp, int(sels[len(sels)-1])+1)
	nsp.np.AddMany(sels)
}

func Add(nsp *Nulls, sels ...uint64) {
	nsp.Add(sels...)
}

func (nsp *Nulls) AddRange(start, end uint64) {
	if nsp != nil {
		TryExpand(nsp, int(end+1))
		nsp.np.AddRange(start, end)
	}
}

func AddRange(nsp *Nulls, start, end uint64) {
	nsp.AddRange(start, end)
}

func (nsp *Nulls) Del(sels ...uint64) {
	if nsp != nil {
		for _, sel := range sels {
			nsp.np.Remove(sel)
		}
	}
}

func (nsp *Nulls) DelI64(rows ...int64) {
	if nsp != nil {
		for _, row := range rows {
			nsp.np.Remove(uint64(row))
		}
	}
}

func Del(nsp *Nulls, sels ...uint64) {
	nsp.Del(sels...)
}

// Set performs union operation on Nulls nsp,m and store the result in nsp
func Set(nsp, other *Nulls) {
	if !other.np.EmptyByFlag() {
		nsp.np.Or(&other.np)
	}
}

// FilterCount returns the number count that appears in both nsp and sel
func FilterCount(nsp *Nulls, sels []int64) int {
	var count int
	if nsp.np.EmptyByFlag() || len(sels) == 0 {
		return 0
	}

	// XXX WTF is this?  convert int64 to uint64?
	var idxs []uint64
	if len(sels) > 0 {
		idxs = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	}

	for _, idx := range idxs {
		if nsp.np.Contains(idx) {
			count++
		}
	}
	return count
}

func RemoveRange(nsp *Nulls, start, end uint64) {
	if !nsp.np.EmptyByFlag() {
		nsp.np.RemoveRange(start, end)
	}
}

// Range adds the numbers in nsp starting at start and ending at end to m.
// `bias` represents the starting offset used for the Range Output
// Always update in place.
func Range(nsp *Nulls, start, end, bias uint64, b *Nulls) {
	if nsp.np.EmptyByFlag() {
		return
	}

	b.np.InitWithSize(int64(end + 1 - bias))
	for ; start < end; start++ {
		if nsp.np.Contains(start) {
			b.np.Add(start - bias)
		}
	}
}

// XXX old API returns nsp, which is broken -- we update in place.
func Filter(nsp *Nulls, sels []int64, negate bool) {
	if nsp.np.EmptyByFlag() {
		return
	}

	if negate {
		oldLen := nsp.np.Len()
		var bm bitmap.Bitmap
		bm.InitWithSize(oldLen)
		for oldIdx, newIdx, selIdx, sel := int64(0), 0, 0, sels[0]; oldIdx < oldLen; oldIdx++ {
			if oldIdx != sel {
				if nsp.np.Contains(uint64(oldIdx)) {
					bm.Add(uint64(newIdx))
				}
				newIdx++
			} else {
				selIdx++
				if selIdx >= len(sels) {
					for idx := oldIdx + 1; idx < oldLen; idx++ {
						if nsp.np.Contains(uint64(idx)) {
							bm.Add(uint64(newIdx))
						}
						newIdx++
					}
					break
				}
				sel = sels[selIdx]
			}
		}
		nsp.np.InitWith(&bm)
	} else {
		var b bitmap.Bitmap
		b.InitWithSize(int64(len(sels)))
		upperLimit := int64(nsp.np.Len())
		for i, sel := range sels {
			if sel >= upperLimit {
				continue
			}
			if nsp.np.Contains(uint64(sel)) {
				b.Add(uint64(i))
			}
		}
		nsp.np.InitWith(&b)
	}
}

func FilterByMask(nsp *Nulls, sels bitmap.Mask, negate bool) {
	if nsp.np.EmptyByFlag() {
		return
	}
	length := sels.Count()
	itr := sels.Iterator()
	if negate {
		oldLen := nsp.np.Len()
		var bm bitmap.Bitmap
		bm.InitWithSize(oldLen)
		sel := itr.Next()
		for oldIdx, newIdx, selIdx := int64(0), 0, 0; oldIdx < oldLen; oldIdx++ {
			if uint64(oldIdx) != sel {
				if nsp.np.Contains(uint64(oldIdx)) {
					bm.Add(uint64(newIdx))
				}
				newIdx++
			} else {
				selIdx++
				if !itr.HasNext() {
					for idx := oldIdx + 1; idx < oldLen; idx++ {
						if nsp.np.Contains(uint64(idx)) {
							bm.Add(uint64(newIdx))
						}
						newIdx++
					}
					break
				}
				sel = itr.Next()
			}
		}
		nsp.np.InitWith(&bm)
	} else {
		var bm bitmap.Bitmap
		bm.InitWithSize(int64(length))
		upperLimit := nsp.np.Len()
		idx := 0
		for itr.HasNext() {
			sel := itr.Next()
			if sel >= uint64(upperLimit) {
				continue
			}
			if nsp.np.Contains(sel) {
				bm.Add(uint64(idx))
			}
			idx++
		}
		nsp.np.InitWith(&bm)
	}
}

// XXX This emptyFlag thing is broken -- it simply cannot be used concurrently.
// Make any an alias of EmptyByFlag, otherwise there will be hell lots of race conditions.
func (nsp *Nulls) Any() bool {
	return nsp != nil && !nsp.np.EmptyByFlag()
}

func (nsp *Nulls) IsEmpty() bool {
	return nsp == nil || nsp.np.IsEmpty()
}

func (nsp *Nulls) EmptyByFlag() bool {
	return nsp == nil || nsp.np.EmptyByFlag()
}

func (nsp *Nulls) Set(row uint64) {
	TryExpand(nsp, int(row)+1)
	nsp.np.Add(row)
}

// Call it unset to match set.   Clear or reset are taken.
func (nsp *Nulls) Unset(row uint64) {
	if nsp != nil {
		nsp.np.Remove(row)
	}
}

// pop count
func (nsp *Nulls) Count() int {
	if nsp == nil {
		return 0
	}
	return nsp.np.Count()
}

func (nsp *Nulls) Show() ([]byte, error) {
	if nsp.np.EmptyByFlag() {
		return nil, nil
	}
	return nsp.np.Marshal(), nil
}

// ShowV1 in version 1, bitmap is v1
func (nsp *Nulls) ShowV1() ([]byte, error) {
	if nsp.np.EmptyByFlag() {
		return nil, nil
	}
	return nsp.np.MarshalV1(), nil
}

func (nsp *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		// don't we need to reset?   Or we always, Read into a blank Nulls?
		// nsp.np.Reset()
		return nil
	}
	nsp.np.Unmarshal(data)
	return nil
}

func (nsp *Nulls) ReadNoCopy(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopy(data)
	return nil
}

func (nsp *Nulls) ReadNoCopyV1(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopyV1(data)
	return nil
}

// Or the m Nulls into nsp.
func (nsp *Nulls) Or(m *Nulls) {
	if m != nil && !m.np.EmptyByFlag() {
		nsp.np.Or(&m.np)
	}
}

func (nsp *Nulls) IsSame(m *Nulls) bool {
	if nsp == m {
		return true
	}
	if nsp == nil || m == nil {
		return false
	}

	return nsp.np.IsSame(&m.np)
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp == nil || nsp.np.EmptyByFlag() {
		return []uint64{}
	}
	return nsp.np.ToArray()
}

func (nsp *Nulls) ToI64Arrary() []int64 {
	if nsp == nil || nsp.np.EmptyByFlag() {
		return []int64{}
	}
	return nsp.np.ToI64Arrary()
}

func (nsp *Nulls) GetCardinality() int {
	return nsp.Count()
}

func (nsp *Nulls) Foreach(fn func(uint64) bool) {
	if nsp.IsEmpty() {
		return
	}
	itr := nsp.np.Iterator()
	for itr.HasNext() {
		row := itr.Next()
		if !fn(row) {
			break
		}
	}
}

func (nsp *Nulls) Merge(other *Nulls) {
	if other.Count() == 0 {
		return
	}
	nsp.np.Or(&other.np)
}

func (nsp *Nulls) String() string {
	if nsp.IsEmpty() {
		return fmt.Sprintf("%v", []uint64{})
	}
	return nsp.np.String()
}

func ToArray[T constraints.Integer](nsp *Nulls) []T {
	if nsp.IsEmpty() {
		return []T{}
	}
	ret := make([]T, 0, nsp.Count())
	it := nsp.np.Iterator()
	for it.HasNext() {
		r := it.Next()
		ret = append(ret, T(r))
	}
	return ret
}
