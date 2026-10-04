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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"golang.org/x/exp/constraints"
)

type Bitmap = Nulls

type Nulls struct {
	np bitmap.Bitmap

	// Arrow validity uses the inverse convention: bit 1 means valid. Keep the
	// immutable source representation as a leased view and materialize the MO
	// null bitmap only at a legacy or mutating boundary.
	validity       []byte
	validityOffset int
	validityLength int
	validityNulls  int
	validityLease  bufferlease.BufferLease
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
	nsp.Reset()
	if n != nil && n.validityLease != nil {
		if !n.validityLease.Retain() {
			panic("retain released null validity lease")
		}
		nsp.validity = n.validity
		nsp.validityOffset = n.validityOffset
		nsp.validityLength = n.validityLength
		nsp.validityNulls = n.validityNulls
		nsp.validityLease = n.validityLease
		return
	}
	nsp.np.InitWith(&n.np)
}

func (nsp *Nulls) InitWithSize(size int) {
	nsp.releaseValidity()
	nsp.np.InitWithSize(int64(size))
}

func NewWithSize(size int) *Nulls {
	var n Nulls
	n.InitWithSize(size)
	return &n
}

func (nsp *Nulls) Reset() {
	nsp.releaseValidity()
	nsp.np.Reset()
}

func (nsp *Nulls) Clear() {
	nsp.releaseValidity()
	nsp.np.Clear()
}

func (nsp *Nulls) GetBitmap() *bitmap.Bitmap {
	nsp.materializeValidity()
	return &nsp.np
}

// Or performs union operation on Nulls nsp,m and store the result in r
func Or(nsp, m, r *Nulls) {
	if nsp.EmptyByFlag() && m.EmptyByFlag() {
		if r.np.HasExternalStorage() {
			// External capacity belongs to the result owner and is not a row
			// count. Clear values while retaining the owner's current bound.
			r.Clear()
		} else {
			r.Reset()
		}
		return
	}

	r.materializeValidity()
	if nsp != nil {
		orBitmapInto(r, nsp.GetBitmap())
	}
	if m != nil {
		orBitmapInto(r, m.GetBitmap())
	}
}

func orBitmapInto(dst *Nulls, src *bitmap.Bitmap) {
	if src == nil || src.EmptyByFlag() || src == &dst.np {
		return
	}
	if dst.np.HasExternalStorage() {
		// External storage capacity can exceed the destination's current row
		// domain. The owner-established logical length is the only valid bound.
		dst.np.OrBounded(src, dst.np.Len())
		return
	}
	dst.np.Or(src)
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
	return nsp.Any()
}

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.GetBitmap().Ptr()
}

func (nsp *Nulls) RawPtrLen() (uintptr, uintptr) {
	if nsp == nil {
		return 0, 0
	}
	return nsp.GetBitmap().RawPtrLen()
}

// Size estimates the memory usage of the Nulls.
func Size(nsp *Nulls) int {
	if nsp == nil {
		return 0
	}
	if nsp.validityLease != nil {
		return len(nsp.validity)
	}
	return nsp.np.Size()
}

func String(nsp *Nulls) string {
	if nsp == nil || nsp.EmptyByFlag() {
		return "[]"
	}
	return fmt.Sprintf("%v", nsp.ToArray())
}

func TryExpand(nsp *Nulls, size int) {
	nsp.materializeValidity()
	nsp.np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
func (nsp *Nulls) Contains(row uint64) bool {
	if nsp == nil {
		return false
	}
	if nsp.validityLease != nil {
		return nsp.validityContainsNull(row)
	}
	return !nsp.np.EmptyByFlag() && nsp.np.Contains(row)
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
		TryExpand(nsp, int(end))
		nsp.np.AddRange(start, end)
	}
}

// AddRange add bits [start, end) to nsp
func AddRange(nsp *Nulls, start, end uint64) {
	nsp.AddRange(start, end)
}

func (nsp *Nulls) Del(sels ...uint64) {
	if nsp != nil {
		nsp.materializeValidity()
		for _, sel := range sels {
			nsp.np.Remove(sel)
		}
	}
}

func (nsp *Nulls) DelI64(rows ...int64) {
	if nsp != nil {
		nsp.materializeValidity()
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
	if other != nil {
		nsp.materializeValidity()
		orBitmapInto(nsp, other.GetBitmap())
	}
}

// FilterCount returns the number count that appears in both nsp and sel
func FilterCount(nsp *Nulls, sels []int64) int {
	var count int
	if nsp.EmptyByFlag() || len(sels) == 0 {
		return 0
	}

	// XXX WTF is this?  convert int64 to uint64?
	idxs := util.UnsafeSliceCast[uint64](sels)

	for _, idx := range idxs {
		if nsp.Contains(idx) {
			count++
		}
	}
	return count
}

func RemoveRange(nsp *Nulls, start, end uint64) {
	nsp.materializeValidity()
	if !nsp.np.EmptyByFlag() {
		nsp.np.RemoveRange(start, end)
	}
}

// Range adds the numbers in nsp starting at start and ending at end to m.
// `bias` represents the starting offset used for the Range Output
// Always update in place.
func Range(nsp *Nulls, start, end, bias uint64, b *Nulls) {
	if nsp.EmptyByFlag() {
		return
	}

	b.materializeValidity()
	b.np.InitWithSize(int64(end - bias))
	for ; start < end; start++ {
		if nsp.Contains(start) {
			b.np.Add(start - bias)
		}
	}
}

// XXX old API returns nsp, which is broken -- we update in place.
func Filter(nsp *Nulls, sels []int64, negate bool) {
	if nsp.EmptyByFlag() {
		return
	}
	nsp.materializeValidity()

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

// FilterInPlaceOrdered preserves Filter semantics for Vector.Shrink's ordered
// selection contract without allocating a second row-scaled bitmap.
func FilterInPlaceOrdered(nsp *Nulls, sels []int64, negate bool) {
	if nsp.EmptyByFlag() {
		return
	}
	nsp.materializeValidity()
	if !nsp.np.HasExternalStorage() {
		Filter(nsp, sels, negate)
		return
	}
	nsp.np.RemapOrdered(sels, negate)
}

func FilterByMask(nsp *Nulls, sels *bitmap.Bitmap, negate bool) {
	FilterByMaskWithOffset(nsp, sels, negate, 0)
}

// FilterByMaskWithOffset applies sels after translating every selected row by
// offset. The selection bitmap is relative to a window of the owning vector,
// while the null bitmap remains in the full vector's row domain.
func FilterByMaskWithOffset(nsp *Nulls, sels *bitmap.Bitmap, negate bool, offset uint64) {
	if nsp.EmptyByFlag() {
		return
	}
	nsp.materializeValidity()
	length := sels.Count()
	itr := sels.Iterator()
	if negate {
		oldLen := nsp.np.Len()
		var bm bitmap.Bitmap
		bm.InitWithSize(oldLen)
		var sel uint64
		hasSel := itr.HasNext()
		if hasSel {
			sel = itr.Next() + offset
		}
		for oldIdx, newIdx := int64(0), 0; oldIdx < oldLen; oldIdx++ {
			if !hasSel || uint64(oldIdx) != sel {
				if nsp.np.Contains(uint64(oldIdx)) {
					bm.Add(uint64(newIdx))
				}
				newIdx++
			} else {
				hasSel = itr.HasNext()
				if hasSel {
					sel = itr.Next() + offset
				}
			}
		}
		nsp.np.InitWith(&bm)
	} else {
		var bm bitmap.Bitmap
		bm.InitWithSize(int64(length))
		upperLimit := nsp.np.Len()
		idx := 0
		for itr.HasNext() {
			sel := itr.Next() + offset
			if sel >= uint64(upperLimit) {
				idx++
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

// FilterByMaskInPlace rewrites a null bitmap using the selection bitmap's
// naturally ordered iterator and therefore requires no row-scaled scratch.
func FilterByMaskInPlace(nsp *Nulls, sels *bitmap.Bitmap, negate bool) {
	FilterByMaskInPlaceWithOffset(nsp, sels, negate, 0)
}

// FilterByMaskInPlaceWithOffset is FilterByMaskInPlace for a selection whose
// row indexes are relative to a window beginning at offset.
func FilterByMaskInPlaceWithOffset(nsp *Nulls, sels *bitmap.Bitmap, negate bool, offset uint64) {
	if nsp.EmptyByFlag() {
		return
	}
	nsp.materializeValidity()
	if !nsp.np.HasExternalStorage() {
		FilterByMaskWithOffset(nsp, sels, negate, offset)
		return
	}
	nsp.np.RemapMaskOrderedWithOffset(sels, negate, offset)
}

// XXX This emptyFlag thing is broken -- it simply cannot be used concurrently.
// Make any an alias of EmptyByFlag, otherwise there will be hell lots of race conditions.
func (nsp *Nulls) Any() bool {
	return nsp != nil && (nsp.validityNulls > 0 || !nsp.np.EmptyByFlag())
}

func (nsp *Nulls) IsEmpty() bool {
	return nsp == nil || (nsp.validityNulls == 0 && nsp.np.IsEmpty())
}

func (nsp *Nulls) EmptyByFlag() bool {
	return nsp == nil || (nsp.validityNulls == 0 && nsp.np.EmptyByFlag())
}

func (nsp *Nulls) Set(row uint64) {
	TryExpand(nsp, int(row)+1)
	nsp.np.Add(row)
}

// Call it unset to match set.   Clear or reset are taken.
func (nsp *Nulls) Unset(row uint64) {
	if nsp != nil {
		nsp.materializeValidity()
		nsp.np.Remove(row)
	}
}

// pop count
func (nsp *Nulls) Count() int {
	if nsp == nil {
		return 0
	}
	if nsp.validityLease != nil {
		return nsp.validityNulls
	}
	return nsp.np.Count()
}

// CountRange returns the number of NULL rows in [start, end) without forcing
// a borrowed Arrow validity bitmap to materialize.
func (nsp *Nulls) CountRange(start, end uint64) int {
	if nsp == nil || start >= end {
		return 0
	}
	if nsp.validityLease != nil {
		if start >= uint64(nsp.validityLength) {
			return 0
		}
		if end > uint64(nsp.validityLength) {
			end = uint64(nsp.validityLength)
		}
		count := 0
		for row := start; row < end; row++ {
			if nsp.validityContainsNull(row) {
				count++
			}
		}
		return count
	}
	return nsp.np.CountRange(start, end)
}

// Len returns the logical bitmap domain without forcing a borrowed Arrow
// validity view to materialize.
func (nsp *Nulls) Len() int64 {
	if nsp == nil {
		return 0
	}
	if nsp.validityLease != nil {
		return int64(nsp.validityLength)
	}
	return nsp.np.Len()
}

func (nsp *Nulls) Show() ([]byte, error) {
	if nsp.EmptyByFlag() {
		return nil, nil
	}
	nsp.materializeValidity()
	return nsp.np.Marshal(), nil
}

func (nsp *Nulls) MarshalSize() int {
	if nsp == nil || nsp.EmptyByFlag() {
		return 0
	}
	if nsp.validityLease != nil {
		return bitmap.MarshalHeaderSize + (nsp.validityLength+63)/64*8
	}
	nsp.materializeValidity()
	return nsp.np.MarshalSize()
}

func (nsp *Nulls) MarshalTo(w io.Writer) error {
	if nsp == nil || nsp.EmptyByFlag() {
		return nil
	}
	if nsp.validityLease != nil {
		return nsp.marshalBorrowedValidityTo(w)
	}
	nsp.materializeValidity()
	return nsp.np.MarshalTo(w)
}

func (nsp *Nulls) marshalBorrowedValidityTo(w io.Writer) error {
	if w == nil {
		return io.ErrClosedPipe
	}
	words := (nsp.validityLength + 63) / 64
	var value [8]byte
	writeUint64 := func(v uint64) error {
		binary.LittleEndian.PutUint64(value[:], v)
		written, err := w.Write(value[:])
		if err != nil {
			return err
		}
		if written != len(value) {
			return io.ErrShortWrite
		}
		return nil
	}
	if err := writeUint64(uint64(nsp.validityNulls)); err != nil {
		return err
	}
	if err := writeUint64(uint64(nsp.validityLength)); err != nil {
		return err
	}
	if err := writeUint64(uint64(words * 8)); err != nil {
		return err
	}
	for wordIndex := 0; wordIndex < words; wordIndex++ {
		var word uint64
		start := wordIndex * 64
		end := min(start+64, nsp.validityLength)
		for row := start; row < end; row++ {
			if nsp.validityContainsNull(uint64(row)) {
				word |= uint64(1) << uint(row-start)
			}
		}
		if err := writeUint64(word); err != nil {
			return err
		}
	}
	return nil
}

// ShowV1 in version 1, bitmap is v1
func (nsp *Nulls) ShowV1() ([]byte, error) {
	if nsp.EmptyByFlag() {
		return nil, nil
	}
	nsp.materializeValidity()
	return nsp.np.MarshalV1(), nil
}

func (nsp *Nulls) Read(data []byte) error {
	nsp.releaseValidity()
	if len(data) == 0 {
		// don't we need to reset?   Or we always, Read into a blank Nulls?
		// nsp.np.Reset()
		return nil
	}
	nsp.np.Unmarshal(data)
	return nil
}

func (nsp *Nulls) ReadNoCopy(data []byte) error {
	nsp.releaseValidity()
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopy(data)
	return nil
}

func (nsp *Nulls) ReadNoCopyV1(data []byte) error {
	nsp.releaseValidity()
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopyV1(data)
	return nil
}

func (nsp *Nulls) OrBitmap(m *bitmap.Bitmap) {
	nsp.materializeValidity()
	orBitmapInto(nsp, m)
}

// Or the m Nulls into nsp.
func (nsp *Nulls) Or(m *Nulls) {
	if m != nil {
		nsp.materializeValidity()
		orBitmapInto(nsp, m.GetBitmap())
	}
}

func (nsp *Nulls) IsSame(m *Nulls) bool {
	if nsp == m {
		return true
	}
	if nsp == nil || m == nil {
		return false
	}

	return nsp.GetBitmap().IsSame(m.GetBitmap())
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp == nil || nsp.EmptyByFlag() {
		return []uint64{}
	}
	if nsp.validityLease != nil {
		rows := make([]uint64, 0, nsp.validityNulls)
		for row := 0; row < nsp.validityLength; row++ {
			if nsp.validityContainsNull(uint64(row)) {
				rows = append(rows, uint64(row))
			}
		}
		return rows
	}
	return nsp.np.ToArray()
}

func (nsp *Nulls) ToI64Array() []int64 {
	if nsp == nil || nsp.EmptyByFlag() {
		return []int64{}
	}
	if nsp.validityLease != nil {
		rows := make([]int64, 0, nsp.validityNulls)
		for row := 0; row < nsp.validityLength; row++ {
			if nsp.validityContainsNull(uint64(row)) {
				rows = append(rows, int64(row))
			}
		}
		return rows
	}
	return nsp.np.ToI64Array(nil)
}

func (nsp *Nulls) GetCardinality() int {
	return nsp.Count()
}

func (nsp *Nulls) Foreach(fn func(uint64) bool) {
	if nsp.IsEmpty() {
		return
	}
	if nsp.validityLease != nil {
		for row := 0; row < nsp.validityLength; row++ {
			if nsp.validityContainsNull(uint64(row)) && !fn(uint64(row)) {
				break
			}
		}
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
	if other != nil {
		nsp.materializeValidity()
		orBitmapInto(nsp, other.GetBitmap())
	}
}

func (nsp *Nulls) String() string {
	if nsp.IsEmpty() {
		return fmt.Sprintf("%v", []uint64{})
	}
	if nsp.validityLease != nil {
		return fmt.Sprintf("%v", nsp.ToArray())
	}
	return nsp.np.String()
}

func ToArray[T constraints.Integer](nsp *Nulls) []T {
	if nsp.IsEmpty() {
		return []T{}
	}
	ret := make([]T, 0, nsp.Count())
	nsp.Foreach(func(row uint64) bool {
		ret = append(ret, T(row))
		return true
	})
	return ret
}
