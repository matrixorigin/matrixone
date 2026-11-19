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

func (n *Bitmap) InitWith(m *Bitmap) {
	if n == m {
		return
	}
	n.numBits = m.numBits
	n.popCount = m.popCount
	n.data = append([]uint64(nil), m.data...)
}

func (n *Bitmap) InitWithSize(length int64) {
	if length < 0 {
		panic("negative bitmap length")
	}
	n.numBits = length
	n.popCount = 0
	n.data = make([]uint64, (length+63)/64)
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
	itr := n.IteratorValue()
	return &itr
}

// IteratorValue returns a bitmap iterator by value for allocation-sensitive
// callers. Iterator keeps its interface return type for compatibility, while
// callers on hot paths can keep the iterator on their own stack.
func (n *Bitmap) IteratorValue() BitmapIterator {
	itr := BitmapIterator{bm: n}
	itr.i = itr.nextSet(0)
	return itr
}

// nextSet returns the position of the first set bit at or after i, or numBits
// if there is none. Bits at or beyond numBits are never read, and words missing
// from data read as zero.
func (itr *BitmapIterator) nextSet(i uint64) uint64 {
	end := uint64(itr.bm.numBits)
	if i >= end {
		return end
	}
	nwords := min((end+63)/64, uint64(len(itr.bm.data)))
	mask := ^bitmask(0) << (i & 63)
	for word := i >> 6; word < nwords; word++ {
		if v := itr.bm.data[word] & mask; v != 0 {
			return min(word*64+uint64(bits.TrailingZeros64(v)), end)
		}
		mask = ^bitmask(0)
	}
	return end
}

// HasNext reports whether another set bit remains. An exhausted iterator has
// i == numBits.
func (itr *BitmapIterator) HasNext() bool {
	return itr.i < uint64(itr.bm.numBits)
}

// PeekNext returns the next set bit without advancing, or 0 if there is none.
func (itr *BitmapIterator) PeekNext() uint64 {
	if itr.HasNext() {
		return itr.i
	}
	return 0
}

// Next returns the next set bit and advances. Call it only after HasNext
// returns true.
func (itr *BitmapIterator) Next() uint64 {
	res := itr.i
	itr.i = itr.nextSet(res + 1)
	return res
}

// Reset set n.data to nil
func (n *Bitmap) Reset() {
	n.numBits = 0
	n.popCount = 0
	n.data = nil
}

// Len returns the number of bits in the Bitmap.
func (n *Bitmap) Len() int64 {
	return n.numBits
}

// CapWords returns the backing capacity in 64-bit words.
func (n *Bitmap) CapWords() int {
	if n == nil {
		return 0
	}
	return cap(n.data)
}

// Size return number of bytes in n.data
// XXX WTF Note that this size is not the same as InitWithSize.
func (n *Bitmap) Size() int {
	return len(n.data) * 8
}

// RawPtrLen returns the address of the first data word and the logical length
// in bits, for passing the bitmap to C. It returns (0, 0) for an empty bitmap.
func (n *Bitmap) RawPtrLen() (uintptr, uintptr) {
	if n == nil || len(n.data) == 0 {
		return 0, 0
	}
	return uintptr(unsafe.Pointer(&n.data[0])), uintptr(n.numBits)
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
	return n == nil || n.popCount == 0 || len(n.data) == 0
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	return n.popCount == 0
}

// We always assume that Bitmap has been extended to at least row.
func (n *Bitmap) Add(row uint64) {
	if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
		n.popCount++
	}
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *Bitmap) AddMany(rows []uint64) {
	for _, row := range rows {
		if n.data[row>>6]&(1<<(row&0x3F)) == 0 {
			n.popCount++
		}
		n.data[row>>6] |= 1 << (row & 0x3F)
	}
}

func (n *Bitmap) Remove(row uint64) {
	if row >= uint64(n.numBits) {
		return
	}
	if n.data[row>>6]&(1<<(row&0x3F)) != 0 {
		n.popCount--
	}
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	if row >= uint64(n.numBits) {
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
		n.popCount += int64(count)
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
	n.popCount += int64(count)
}

func (n *Bitmap) RemoveRange(start, end uint64) {
	if end > uint64(n.numBits) {
		end = uint64(n.numBits)
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
		n.popCount -= int64(count)
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
	n.popCount -= int64(count)
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
	n.OrBounded(b, b.numBits)
}

// OrBounded merges source bits below limit without growing or changing the
// destination's logical length. It is intended for caller-owned storage whose
// physical capacity can be larger than its current logical row domain.
func (n *Bitmap) OrBounded(b *Bitmap, limit int64) {
	if n == nil || b == nil || limit <= 0 {
		return
	}
	limit = min(limit, n.numBits, b.numBits)
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
		n.popCount += int64(bits.OnesCount64(added))
	}
}

func (n *Bitmap) And(b *Bitmap) {
	n.TryExpand(b)
	n.popCount = 0
	size := (int(b.numBits) + 63) / 64
	for i := range size {
		n.data[i] &= b.data[i]
		n.popCount += int64(bits.OnesCount64(n.data[i]))
	}
	for i := size; i < len(n.data); i++ {
		n.data[i] = 0
	}
}

func (n *Bitmap) Negate() {
	nBlock, nTail := int(n.numBits)/64, int(n.numBits)%64
	n.popCount = 0
	for i := range nBlock {
		n.data[i] = ^n.data[i]
		n.popCount += int64(bits.OnesCount64(n.data[i]))
	}
	if nTail > 0 {
		mask := (uint64(1) << nTail) - 1
		n.data[nBlock] ^= mask
		n.popCount += int64(bits.OnesCount64(n.data[nBlock]))
	}
}

// Reserve grows the backing capacity so the bitmap can hold nbits bits
// without reallocating. The logical length and the bits are unchanged.
func (n *Bitmap) Reserve(nbits int) {
	words := (nbits + 63) / 64
	if words <= cap(n.data) {
		return
	}
	data := make([]uint64, len(n.data), words)
	copy(data, n.data)
	n.data = data
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	n.TryExpandWithSize(int(m.numBits))
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if int(n.numBits) >= size {
		return
	}
	requiredCap := (size + 63) / 64
	n.numBits = int64(size)
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
// owning vector when the vector's trailing rows are all non-null. The result
// keeps that implicit clear tail rather than growing to the selection length:
// ordered compaction cannot move a set bit beyond the original bitmap extent.
func (n *Bitmap) RemapOrdered(sels []int64, negate bool) {
	if n == nil {
		return
	}
	oldLength := n.numBits
	previous := int64(-1)
	for _, sel := range sels {
		if sel <= previous || sel < 0 {
			panic("bitmap ordered remap requires strictly increasing non-negative rows")
		}
		previous = sel
	}
	logicalLength := min(oldLength, int64(len(sels)))
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
		for _, sel := range sels[:logicalLength] {
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
	oldLength := n.numBits
	logicalLength := min(oldLength, int64(sels.Count()))
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
		for output < logicalLength && iterator.HasNext() {
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
		data := make([]uint64, words)
		copy(data, n.data)
		n.data = data
		return
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
	n.numBits = logicalLength
	n.popCount = 0
	for _, word := range n.data {
		n.popCount += int64(bits.OnesCount64(word))
	}
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	var b Bitmap
	b.InitWithSize(n.numBits)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			b.Add(uint64(i))
		}
	}
	return &b
}

func (n *Bitmap) Count() int {
	return int(n.popCount)
}

// CountRange returns the number of set bits in [start, end). It never scans
// outside the bitmap's logical coverage and does not allocate.
func (n *Bitmap) CountRange(start, end uint64) int {
	if n == nil || start >= end || start >= uint64(n.numBits) {
		return 0
	}
	if end > uint64(n.numBits) {
		end = uint64(n.numBits)
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
	if n == nil || start >= end || start >= uint64(n.numBits) {
		return false
	}
	if end > uint64(n.numBits) {
		end = uint64(n.numBits)
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
	if n == nil || n.numBits < 0 || n.popCount < 0 ||
		n.popCount > n.numBits ||
		len(n.data) != int((n.numBits+63)/64) {
		return moerr.NewInvalidInputNoCtx("invalid bitmap representation")
	}
	actual := int64(0)
	for i, word := range n.data {
		if i == len(n.data)-1 && n.numBits%64 != 0 &&
			word>>uint(n.numBits%64) != 0 {
			return moerr.NewInvalidInputNoCtx("invalid bitmap trailing bits")
		}
		actual += int64(bits.OnesCount64(word))
	}
	if actual != n.popCount {
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

// PrepareUnmarshal publishes a validated bitmap header and returns the
// freshly allocated payload bytes for the caller to fill, so a stream decoder
// can read the words in place.
func (n *Bitmap) PrepareUnmarshal(header []byte, totalSize int) ([]byte, error) {
	popCount, numBits, dataSize, err := DecodeMarshalHeader(header)
	if err != nil {
		return nil, err
	}
	if totalSize != MarshalHeaderSize+dataSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid bitmap payload size")
	}
	n.data = make([]uint64, dataSize/8)
	n.popCount = popCount
	n.numBits = numBits
	return types.EncodeSlice(n.data), nil
}

func (n *Bitmap) MarshalTo(w io.Writer) error {
	if n == nil {
		return nil
	}
	if w == nil {
		return io.ErrClosedPipe
	}
	bitLength := uint64(n.numBits)
	dataLength := uint64(len(n.data) * 8)
	if typed, ok := w.(interface {
		WriteInt64(int64) error
		WriteUint64(uint64) error
	}); ok {
		if err := typed.WriteInt64(n.popCount); err != nil {
			return err
		}
		if err := typed.WriteUint64(bitLength); err != nil {
			return err
		}
		if err := typed.WriteUint64(dataLength); err != nil {
			return err
		}
		return types.WriteFull(w, types.EncodeSlice(n.data))
	}
	if err := types.WriteFull(w, types.EncodeInt64(&n.popCount)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&bitLength)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&dataLength)); err != nil {
		return err
	}
	return types.WriteFull(w, types.EncodeSlice(n.data))
}

// MarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.popCount replace it
func (n *Bitmap) MarshalV1() []byte {
	var buf bytes.Buffer
	empty := int32(0)
	u1 := uint64(n.numBits)
	u2 := uint64(len(n.data) * 8)
	buf.Write(types.EncodeInt32(&empty))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(n.data))
	return buf.Bytes()
}

func (n *Bitmap) Unmarshal(data []byte) {
	n.popCount = types.DecodeInt64(data[:8])
	data = data[8:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

func (n *Bitmap) UnmarshalNoCopy(data []byte) {
	n.popCount = types.DecodeInt64(data[:8])
	data = data[8:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), size/8)
	}
}

// UnmarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.popCount replace it
func (n *Bitmap) UnmarshalV1(data []byte) {
	data = data[4:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
	n.popCount = 0
	for i := 0; i < len(n.data); i++ {
		n.popCount += int64(bits.OnesCount64(n.data[i]))
	}
}

func (n *Bitmap) UnmarshalNoCopyV1(data []byte) {
	data = data[4:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), size/8)
	}
	n.popCount = 0
	for i := 0; i < len(n.data); i++ {
		n.popCount += int64(bits.OnesCount64(n.data[i]))
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
	n.popCount = 0
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
