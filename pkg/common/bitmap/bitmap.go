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
	"io"
	"math"
	"math/bits"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

//
// In case len is not multiple of 64, many of these code following assumes the trailing
// bits of last uint64 are zero.   This may well be true in all our usage.  So let's
// leave as it is for now.
//

type bitmask = uint64

const MarshalHeaderSize = 24

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

func encodeTaggedLen(length int64, external bool) int64 {
	if length < 0 {
		panic("negative bitmap length")
	}
	if external {
		return ^length
	}
	return length
}

func (n *Bitmap) logicalLen() int64 {
	if n.taggedLen < 0 {
		return ^n.taggedLen
	}
	return n.taggedLen
}

func (n *Bitmap) setLogicalLen(length int64) {
	n.taggedLen = encodeTaggedLen(length, n.HasExternalStorage())
}

func (n *Bitmap) InitWith(m *Bitmap) {
	if n == m {
		return
	}
	n.setLogicalLen(m.logicalLen())
	n.count = m.count
	if n.HasExternalStorage() {
		if len(m.data) > cap(n.data) {
			panic("bitmap external storage capacity exceeded")
		}
		previousLength := len(n.data)
		storage := n.data[:cap(n.data)]
		clear(storage[len(m.data):max(previousLength, len(m.data))])
		n.data = storage[:len(m.data)]
		copy(n.data, m.data)
		return
	}
	n.data = append([]uint64(nil), m.data...)
}

func (n *Bitmap) InitWithSize(length int64) {
	n.setLogicalLen(length)
	n.count = 0
	words := int((length + 63) / 64)
	if n.HasExternalStorage() {
		if words > cap(n.data) {
			panic("bitmap external storage capacity exceeded")
		}
		previousLength := len(n.data)
		storage := n.data[:cap(n.data)]
		clear(storage[:max(previousLength, words)])
		n.data = storage[:words]
		return
	}
	n.data = make([]uint64, words)
}

func (n *Bitmap) Clone() *Bitmap {
	if n == nil {
		return nil
	}
	var res Bitmap
	res.InitWith(n)
	return &res
}

func (n *Bitmap) Iterator() Iterator {
	// When initialization, the itr.i is set to the first rightmost_one position.
	itr := BitmapIterator{i: 0, bm: n}
	if pos, has_next := itr.hasNext(0); has_next {
		itr.i = pos
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
	var res uint64
	for {
		if (word & 0xFF) == 0 {
			word >>= 8
			res += 8
		} else {
			break
		}
	}
	res += uint64(rightmost_one_pos_8[word&255])
	return res
}

func (itr *BitmapIterator) hasNext(i uint64) (uint64, bool) {
	// if the uint64 is 0, move forward to next word
	// if the uint64 is not 0, then calculate the rightest_one position in a word, add up prev result and return.
	// when there is 1 in Bitmap, return true, otherwise Bitmap is empty and return false.
	// either case loop over words not bits
	nwords := (itr.bm.logicalLen() + 63) / 64
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
	res := itr.i
	if next, has_next := itr.hasNext(itr.i + 1); has_next { // itr.i + 1 to ignore bits check before
		itr.i = next
		itr.has_next = true
		return res
	}
	itr.has_next = false
	return res
}

// Reset set n.data to nil
func (n *Bitmap) Reset() {
	n.setLogicalLen(0)
	n.count = 0
	if n.HasExternalStorage() {
		clear(n.data)
		storage := n.data[:cap(n.data)]
		n.data = storage[:0]
		return
	}
	n.data = nil
}

// InstallExternalStorage replaces the bitmap backing with caller-owned
// storage while preserving the logical bitmap. The caller remains responsible
// for releasing the returned previous external storage, if any.
func (n *Bitmap) InstallExternalStorage(storage []uint64) []uint64 {
	required := len(n.data)
	if required > cap(storage) {
		panic("bitmap external storage capacity exceeded")
	}
	var previous []uint64
	if n.HasExternalStorage() && cap(n.data) > 0 {
		previous = n.data[:cap(n.data)]
	}
	target := storage[:cap(storage)]
	if len(target) > required {
		clear(target[required:])
	}
	copy(target[:required], n.data)
	n.data = target[:required]
	n.taggedLen = encodeTaggedLen(n.logicalLen(), true)
	return previous
}

// ReleaseExternalStorage detaches caller-owned storage and clears the bitmap.
// It returns nil for a bitmap that owns its Go-allocated backing.
func (n *Bitmap) ReleaseExternalStorage() []uint64 {
	if !n.HasExternalStorage() {
		return nil
	}
	var storage []uint64
	if cap(n.data) > 0 {
		storage = n.data[:cap(n.data)]
	}
	n.count = 0
	n.taggedLen = 0
	n.data = nil
	return storage
}

func (n *Bitmap) ExternalStorageCapacity() int {
	if n == nil || !n.HasExternalStorage() {
		return 0
	}
	return cap(n.data)
}

func (n *Bitmap) HasExternalStorage() bool {
	return n != nil && n.taggedLen < 0
}

// Len returns the number of bits in the Bitmap.
func (n *Bitmap) Len() int64 {
	return n.logicalLen()
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

// EmptyByFlag is a quick and dirty way to check if the Bitmap is empty.
// If it retruns true, the Bitmap is empty.  Otherwise, it may or may not be empty.
func (n *Bitmap) EmptyByFlag() bool {
	return n == nil || n.count == 0 || len(n.data) == 0
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	return n.count == 0
}

// We always assume that Bitmap has been extended to at least row.
func (n *Bitmap) Add(row uint64) {
	if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
		n.count++
	}
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *Bitmap) AddMany(rows []uint64) {
	for _, row := range rows {
		if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
			n.count++
		}
		n.data[row>>6] |= 1 << (row & 0x3F)
	}
}

func (n *Bitmap) Remove(row uint64) {
	if row >= uint64(n.logicalLen()) {
		return
	}
	if n.data[row>>6]&(1<<(row&0x3F)) != 0 {
		n.count--
	}
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	if row >= uint64(n.logicalLen()) {
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

func (n *Bitmap) RemoveRange(start, end uint64) {
	if end > uint64(n.logicalLen()) {
		end = uint64(n.logicalLen())
	}
	if start >= end {
		return
	}
	count := 0
	i, j := start>>6, (end-1)>>6
	if i == j {
		mask := (^uint64(0) << uint(start&0x3F)) & (^uint64(0) >> (uint(-end) & 0x3F))
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

func (n *Bitmap) IsSame(b *Bitmap) bool {
	if len(b.data) != len(n.data) {
		return false
	}
	for i := 0; i < len(n.data); i++ {
		if n.data[i] != b.data[i] {
			return false
		}
	}
	return true
}

func (n *Bitmap) Or(b *Bitmap) {
	n.TryExpand(b)
	n.OrBounded(b, b.logicalLen())
}

// OrBounded merges source bits below limit without growing or changing the
// destination's logical length. It is intended for caller-owned storage whose
// physical capacity can be larger than its current logical row domain.
func (n *Bitmap) OrBounded(b *Bitmap, limit int64) {
	if n == nil || b == nil || limit <= 0 {
		return
	}
	limit = min(limit, n.logicalLen(), b.logicalLen())
	if limit <= 0 {
		return
	}

	words := int((limit + 63) / 64)
	for i := range words {
		source := b.data[i]
		if i == words-1 && limit&63 != 0 {
			source &= (uint64(1) << uint(limit&63)) - 1
		}
		added := source &^ n.data[i]
		n.data[i] |= source
		n.count += int64(bits.OnesCount64(added))
	}
}

func (n *Bitmap) And(b *Bitmap) {
	n.TryExpand(b)
	n.count = 0
	size := (int(b.logicalLen()) + 63) / 64
	for i := range size {
		n.data[i] &= b.data[i]
		n.count += int64(bits.OnesCount64(n.data[i]))
	}
	for i := size; i < len(n.data); i++ {
		n.data[i] = 0
	}
}

func (n *Bitmap) Negate() {
	nBlock, nTail := int(n.logicalLen())/64, int(n.logicalLen())%64
	n.count = 0
	for i := range nBlock {
		n.data[i] = ^n.data[i]
		n.count += int64(bits.OnesCount64(n.data[i]))
	}
	if nTail > 0 {
		mask := (uint64(1) << nTail) - 1
		n.data[nBlock] ^= mask
		n.count += int64(bits.OnesCount64(n.data[nBlock]))
	}
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	n.TryExpandWithSize(int(m.logicalLen()))
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if int(n.logicalLen()) >= size {
		return
	}
	requiredCap := (size + 63) / 64
	if requiredCap > cap(n.data) && n.HasExternalStorage() {
		// Keep the previous logical bitmap intact when caller-owned storage is
		// insufficient. A recovered panic must not expose a length that its
		// backing data cannot represent.
		panic("bitmap external storage capacity exceeded")
	}
	n.setLogicalLen(int64(size))
	if requiredCap > cap(n.data) {
		newCap := requiredCap
		currentCap := cap(n.data)
		if currentCap <= int(^uint(0)>>1)/2 {
			newCap = max(requiredCap, max(1, currentCap*2))
		}
		data := make([]uint64, newCap)
		copy(data, n.data)
		n.data = data[:requiredCap]
		return
	}
	if len(n.data) < requiredCap {
		n.data = n.data[:requiredCap]
	}
}

// RemapOrdered rewrites the bitmap in place for an ordered row selection.
// When negate is false, output row i comes from sels[i]. When negate is true,
// sels identifies rows to remove. The caller must provide strictly increasing,
// non-negative row indexes. Because every destination row is at or before its
// source row, one cached source word is sufficient to avoid allocating a
// second data-scaled bitmap. Selection rows beyond the bitmap's logical
// length are valid and read as clear: a null bitmap may be shorter than its
// owning vector when the vector's trailing rows are all non-null.
func (n *Bitmap) RemapOrdered(sels []int64, negate bool) {
	if n == nil {
		return
	}
	oldLength := n.logicalLen()
	previous := int64(-1)
	for _, sel := range sels {
		if sel <= previous || sel < 0 {
			panic("bitmap ordered remap requires strictly increasing non-negative rows")
		}
		previous = sel
	}
	logicalLength := int64(len(sels))
	if negate {
		logicalLength = oldLength
	}
	n.prepareOrderedRemap(logicalLength)

	sourceWordIndex := int64(-1)
	var sourceWord uint64
	readSource := func(row int64) bool {
		if row >= oldLength {
			return false
		}
		wordIndex := row >> 6
		if wordIndex != sourceWordIndex {
			sourceWordIndex = wordIndex
			sourceWord = n.data[wordIndex]
		}
		return sourceWord&(uint64(1)<<uint(row&63)) != 0
	}
	writeDestination := func(row int64, value bool) {
		wordIndex := row >> 6
		mask := uint64(1) << uint(row&63)
		if value {
			n.data[wordIndex] |= mask
		} else {
			n.data[wordIndex] &^= mask
		}
	}

	output := int64(0)
	if !negate {
		for _, sel := range sels {
			writeDestination(output, readSource(sel))
			output++
		}
	} else {
		selIndex := 0
		for source := int64(0); source < oldLength; source++ {
			if selIndex < len(sels) && source == sels[selIndex] {
				selIndex++
				continue
			}
			writeDestination(output, readSource(source))
			output++
		}
	}
	n.finishOrderedRemap(output, logicalLength)
}

// RemapMaskOrdered is RemapOrdered for an ordered bitmap selection. Selection
// bitmap iteration is monotonic, so the rewrite uses no row-scaled scratch.
func (n *Bitmap) RemapMaskOrdered(sels *Bitmap, negate bool) {
	n.RemapMaskOrderedWithOffset(sels, negate, 0)
}

// RemapMaskOrderedWithOffset applies an ordered bitmap selection after adding
// offset to every selected source row, without materializing an index slice.
func (n *Bitmap) RemapMaskOrderedWithOffset(sels *Bitmap, negate bool, offset uint64) {
	if n == nil || sels == nil {
		return
	}
	oldLength := n.logicalLen()
	logicalLength := int64(sels.Count())
	if negate {
		logicalLength = oldLength
	}
	n.prepareOrderedRemap(logicalLength)
	sourceWordIndex := int64(-1)
	var sourceWord uint64
	readSource := func(row int64) bool {
		if row >= oldLength {
			return false
		}
		wordIndex := row >> 6
		if wordIndex != sourceWordIndex {
			sourceWordIndex = wordIndex
			sourceWord = n.data[wordIndex]
		}
		return sourceWord&(uint64(1)<<uint(row&63)) != 0
	}
	writeDestination := func(row int64, value bool) {
		wordIndex := row >> 6
		mask := uint64(1) << uint(row&63)
		if value {
			n.data[wordIndex] |= mask
		} else {
			n.data[wordIndex] &^= mask
		}
	}

	output := int64(0)
	iterator := sels.Iterator()
	if !negate {
		for iterator.HasNext() {
			source := int64(iterator.Next() + offset)
			writeDestination(output, readSource(source))
			output++
		}
	} else {
		var selected int64 = -1
		if iterator.HasNext() {
			selected = int64(iterator.Next() + offset)
		}
		for source := int64(0); source < oldLength; source++ {
			if source == selected {
				if iterator.HasNext() {
					selected = int64(iterator.Next() + offset)
				} else {
					selected = -1
				}
				continue
			}
			writeDestination(output, readSource(source))
			output++
		}
	}
	n.finishOrderedRemap(output, logicalLength)
}

func (n *Bitmap) prepareOrderedRemap(logicalLength int64) {
	words := int((logicalLength + 63) / 64)
	if words > cap(n.data) {
		panic("bitmap external storage capacity exceeded")
	}
	if words > len(n.data) {
		storage := n.data[:cap(n.data)]
		clear(storage[len(n.data):words])
		n.data = storage[:words]
	}
}

func (n *Bitmap) finishOrderedRemap(written, logicalLength int64) {
	words := int((logicalLength + 63) / 64)
	if written < logicalLength {
		word := int(written >> 6)
		if tail := uint(written & 63); tail != 0 {
			n.data[word] &= (uint64(1) << tail) - 1
			word++
		}
		clear(n.data[word:words])
	}
	if words > 0 && logicalLength&63 != 0 {
		n.data[words-1] &= (uint64(1) << uint(logicalLength&63)) - 1
	}
	clear(n.data[words:])
	n.data = n.data[:words]
	n.setLogicalLen(logicalLength)
	n.count = 0
	for _, word := range n.data {
		n.count += int64(bits.OnesCount64(word))
	}
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	var b Bitmap
	b.InitWithSize(n.logicalLen())
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			b.Add(uint64(i))
		}
	}
	return &b
}

func (n *Bitmap) Count() int {
	return int(n.count)
}

// CountRange returns the number of set bits in [start, end). It never scans
// outside the bitmap's logical coverage and does not allocate.
func (n *Bitmap) CountRange(start, end uint64) int {
	if n == nil || start >= end || start >= uint64(n.logicalLen()) {
		return 0
	}
	if end > uint64(n.logicalLen()) {
		end = uint64(n.logicalLen())
	}
	first := start >> 6
	last := (end - 1) >> 6
	if first == last {
		mask := (^uint64(0) << (start & 63)) &
			(^uint64(0) >> ((-end) & 63))
		return bits.OnesCount64(n.data[first] & mask)
	}
	count := bits.OnesCount64(n.data[first] & (^uint64(0) << (start & 63)))
	for word := first + 1; word < last; word++ {
		count += bits.OnesCount64(n.data[word])
	}
	count += bits.OnesCount64(n.data[last] & (^uint64(0) >> ((-end) & 63)))
	return count
}

// AnySetNotIn reports whether [start, end) contains a bit set in n and not in
// other. It is used when one provenance bitmap (GROUPING) overrides another
// (SQL NULL) without expanding either bitmap row by row.
func (n *Bitmap) AnySetNotIn(other *Bitmap, start, end uint64) bool {
	if n == nil || start >= end || start >= uint64(n.logicalLen()) {
		return false
	}
	if end > uint64(n.logicalLen()) {
		end = uint64(n.logicalLen())
	}
	first := start >> 6
	last := (end - 1) >> 6
	for word := first; word <= last; word++ {
		mask := ^uint64(0)
		if word == first {
			mask &= ^uint64(0) << (start & 63)
		}
		if word == last {
			mask &= ^uint64(0) >> ((-end) & 63)
		}
		value := n.data[word] & mask
		if other != nil && word < uint64(len(other.data)) {
			value &^= other.data[word]
		}
		if value != 0 {
			return true
		}
	}
	return false
}

func (n *Bitmap) ToArray() []uint64 {
	rows := make([]uint64, 0, n.Count())
	ToArray(n, &rows)
	return rows
}

func (n *Bitmap) ToI64Array(out *[]int64) []int64 {
	var res []int64
	if out != nil {
		res = (*out)[:0]
	}

	if n.EmptyByFlag() {
		return res
	}

	itr := n.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		res = append(res, int64(r))
	}
	return res
}

func (n *Bitmap) Marshal() []byte {
	var buf bytes.Buffer
	_ = n.MarshalTo(&buf)
	return buf.Bytes()
}

func (n *Bitmap) MarshalSize() int {
	if n == nil {
		return 0
	}
	return MarshalHeaderSize + len(n.data)*8
}

// Validate checks the in-memory representation after streaming decode.
func (n *Bitmap) Validate() error {
	if n == nil || n.logicalLen() < 0 || n.count < 0 ||
		n.count > n.logicalLen() ||
		len(n.data) != int((n.logicalLen()+63)/64) {
		return moerr.NewInvalidInputNoCtx("invalid bitmap representation")
	}
	actual := int64(0)
	for i, word := range n.data {
		if i == len(n.data)-1 && n.logicalLen()%64 != 0 &&
			word>>uint(n.logicalLen()%64) != 0 {
			return moerr.NewInvalidInputNoCtx("invalid bitmap trailing bits")
		}
		actual += int64(bits.OnesCount64(word))
	}
	if actual != n.count {
		return moerr.NewInvalidInputNoCtx("invalid bitmap count")
	}
	return nil
}

// DecodeMarshalHeader validates the fixed bitmap wire header.
func DecodeMarshalHeader(data []byte) (
	count int64,
	bitLength int64,
	dataSize int,
	err error,
) {
	if len(data) < MarshalHeaderSize {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	count = types.DecodeInt64(data[:8])
	rawBitLength := types.DecodeUint64(data[8:16])
	rawDataSize := types.DecodeUint64(data[16:24])
	if count < 0 ||
		rawBitLength > math.MaxInt64 ||
		rawDataSize > math.MaxInt ||
		rawDataSize%8 != 0 {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid bitmap wire header")
	}
	bitLength = int64(rawBitLength)
	dataSize = int(rawDataSize)
	if count > bitLength ||
		uint64(dataSize/8) != (rawBitLength+63)/64 {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid bitmap wire header")
	}
	return count, bitLength, dataSize, nil
}

// PrepareExternalUnmarshal publishes a validated bitmap header into existing
// caller-owned storage and returns the payload bytes to fill.
func (n *Bitmap) PrepareExternalUnmarshal(
	header []byte,
	totalSize int,
) ([]byte, error) {
	if !n.HasExternalStorage() {
		return nil, moerr.NewInvalidInputNoCtx("bitmap does not use external storage")
	}
	count, bitLength, dataSize, err := DecodeMarshalHeader(header)
	if err != nil {
		return nil, err
	}
	if totalSize != MarshalHeaderSize+dataSize ||
		dataSize/8 > cap(n.data) {
		return nil, moerr.NewInvalidInputNoCtx("invalid bitmap external storage capacity")
	}
	storage := n.data[:cap(n.data)]
	clear(storage)
	n.data = storage[:dataSize/8]
	n.count = count
	n.setLogicalLen(bitLength)
	return types.EncodeSlice(n.data), nil
}

func (n *Bitmap) MarshalTo(w io.Writer) error {
	if n == nil {
		return nil
	}
	if w == nil {
		return io.ErrClosedPipe
	}
	bitLength := uint64(n.logicalLen())
	dataLength := uint64(len(n.data) * 8)
	if typed, ok := w.(interface {
		WriteInt64(int64) error
		WriteUint64(uint64) error
	}); ok {
		if err := typed.WriteInt64(n.count); err != nil {
			return err
		}
		if err := typed.WriteUint64(bitLength); err != nil {
			return err
		}
		if err := typed.WriteUint64(dataLength); err != nil {
			return err
		}
		return writeBitmapMarshalBytes(w, types.EncodeSlice(n.data))
	}
	if err := writeBitmapMarshalBytes(w, types.EncodeInt64(&n.count)); err != nil {
		return err
	}
	if err := writeBitmapMarshalBytes(w, types.EncodeUint64(&bitLength)); err != nil {
		return err
	}
	if err := writeBitmapMarshalBytes(w, types.EncodeUint64(&dataLength)); err != nil {
		return err
	}
	return writeBitmapMarshalBytes(w, types.EncodeSlice(n.data))
}

func writeBitmapMarshalBytes(w io.Writer, value []byte) error {
	written, err := w.Write(value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

// MarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.count replace it
func (n *Bitmap) MarshalV1() []byte {
	var buf bytes.Buffer
	empty := int32(0)
	u1 := uint64(n.logicalLen())
	u2 := uint64(len(n.data) * 8)
	buf.Write(types.EncodeInt32(&empty))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(n.data))
	return buf.Bytes()
}

func (n *Bitmap) Unmarshal(data []byte) {
	n.count = types.DecodeInt64(data[:8])
	data = data[8:]
	n.setLogicalLen(int64(types.DecodeUint64(data[:8])))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		if n.HasExternalStorage() {
			storage := n.data[:cap(n.data)]
			clear(storage)
			n.data = storage[:0]
		} else {
			n.data = nil
		}
	} else {
		if n.HasExternalStorage() {
			words := size / 8
			if size%8 != 0 || words > cap(n.data) {
				panic("bitmap external storage capacity exceeded")
			}
			storage := n.data[:cap(n.data)]
			clear(storage)
			n.data = storage[:words]
			copy(n.data, types.DecodeSlice[uint64](data[:size]))
			return
		}
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

func (n *Bitmap) UnmarshalNoCopy(data []byte) {
	if n.HasExternalStorage() {
		panic("cannot install alias into bitmap external storage")
	}
	n.count = types.DecodeInt64(data[:8])
	data = data[8:]
	n.setLogicalLen(int64(types.DecodeUint64(data[:8])))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), size/8)
	}
}

// UnmarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.count replace it
func (n *Bitmap) UnmarshalV1(data []byte) {
	data = data[4:]
	n.setLogicalLen(int64(types.DecodeUint64(data[:8])))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		if n.HasExternalStorage() {
			storage := n.data[:cap(n.data)]
			clear(storage)
			n.data = storage[:0]
		} else {
			n.data = nil
		}
	} else {
		if n.HasExternalStorage() {
			words := size / 8
			if size%8 != 0 || words > cap(n.data) {
				panic("bitmap external storage capacity exceeded")
			}
			storage := n.data[:cap(n.data)]
			clear(storage)
			n.data = storage[:words]
			copy(n.data, types.DecodeSlice[uint64](data[:size]))
		} else {
			n.data = types.DecodeSlice[uint64](data[:size])
		}
	}
	n.count = 0
	for i := 0; i < len(n.data); i++ {
		n.count += int64(bits.OnesCount64(n.data[i]))
	}
}

func (n *Bitmap) UnmarshalNoCopyV1(data []byte) {
	if n.HasExternalStorage() {
		panic("cannot install alias into bitmap external storage")
	}
	data = data[4:]
	n.setLogicalLen(int64(types.DecodeUint64(data[:8])))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), size/8)
	}
	n.count = 0
	for i := 0; i < len(n.data); i++ {
		n.count += int64(bits.OnesCount64(n.data[i]))
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

func (n *Bitmap) Clear() {
	n.count = 0
	for i := range n.data {
		n.data[i] = 0
	}
}

func ToArray[T int64 | uint64 | int | int32 | uint32](bm *Bitmap, rows *[]T) {
	if bm.IsEmpty() {
		return
	}
	it := bm.Iterator()
	for it.HasNext() {
		*rows = append(*rows, T(it.Next()))
	}
}
