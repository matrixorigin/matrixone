// Copyright 2026 Matrix Origin
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
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Read-only bitmaps and views.
//
// A read-only bitmap (no pool) references words it does not own. Its words
// may hold set bits at or beyond Len: a view shares the last word with the
// rows after it, and a decoded bitmap's words are only as clean as the
// encoding. A view (InitView) also starts at bit shift of data[0]. The
// verified core in bitmap.go assumes clean, unshifted words; it sends
// shifted reads here, and every reader that consumes whole words of a
// read-only bitmap goes through lword, which applies shift and masks at Len.

// InitView makes n a read-only view of rows [start, end) of src, sharing
// src's words: row r of n is row start+r of src. Nothing is allocated or
// copied. n's previous words are released first. The view is valid only
// while src's words are unchanged and alive; it must never outlive src.
func (n *Bitmap) InitView(src *Bitmap, start, end int64) {
	if start < 0 || end < start {
		panic(moerr.NewInternalErrorNoCtxf("invalid bitmap view [%d, %d)", start, end))
	}
	n.Free()
	n.mp = nil
	if src == nil || end <= start || start >= src.numBits || src.popCount == 0 {
		n.numBits = end - start
		return
	}
	bit := uint64(start) + src.shift
	first := int(bit / 64)
	if first < len(src.data) {
		last := int((uint64(end) + src.shift + 63) / 64)
		if last > len(src.data) {
			last = len(src.data)
		}
		n.data = src.data[first:last:last]
	}
	n.shift = bit % 64
	n.numBits = end - start
	n.popCount = int64(src.CountRange(uint64(start), uint64(end)))
}

// UnmarshalNoCopy makes n a read-only bitmap over the words encoded in
// data, which must outlive n. n's previous words are released first.
func (n *Bitmap) UnmarshalNoCopy(data []byte) {
	n.Free()
	n.mp = nil
	n.popCount = types.DecodeInt64(data[:8])
	data = data[8:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size != 0 {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

// lword returns logical word i of n, rows 64i..64i+63, with rows at or
// beyond Len clear. Words missing from data read as zero.
func (n *Bitmap) lword(i int) uint64 {
	if i < 0 || int64(i)*64 >= n.numBits {
		return 0
	}
	bit := uint64(i)*64 + n.shift
	j := int(bit / 64)
	var w uint64
	if j < len(n.data) {
		w = n.data[j]
	}
	if off := bit % 64; off != 0 {
		w >>= off
		if j+1 < len(n.data) {
			w |= n.data[j+1] << (64 - off)
		}
	}
	if rem := n.numBits - int64(i)*64; rem < 64 {
		w &= uint64(1)<<uint(rem) - 1
	}
	return w
}

// nwords is the number of logical words, ceil(Len/64).
func (n *Bitmap) nwords() int {
	return int((n.numBits + 63) / 64)
}

// lbitsFrom is bitsFrom for a read-only bitmap: the 64 rows starting at
// pos, rows outside [0, Len) clear.
func (n *Bitmap) lbitsFrom(pos int64) uint64 {
	if pos <= -64 || pos >= n.numBits {
		return 0
	}
	if pos < 0 {
		return n.lword(0) << uint64(-pos)
	}
	i, off := int(pos/64), uint64(pos%64)
	w := n.lword(i) >> off
	if off != 0 {
		w |= n.lword(i+1) << (64 - off)
	}
	return w
}

// shiftedContains and shiftedCountRange serve every read-only bitmap: its
// words may be shifted, and may cover fewer or more rows than Len.
func (n *Bitmap) shiftedContains(row uint64) bool {
	if row >= uint64(n.numBits) {
		return false
	}
	bit := row + n.shift
	j := int(bit / 64)
	return j < len(n.data) && n.data[j]&(uint64(1)<<(bit%64)) != 0
}

func (n *Bitmap) shiftedNextSet(i uint64) uint64 {
	end := uint64(n.numBits)
	if i >= end {
		return end
	}
	word := int(i / 64)
	w := n.lword(word) & (^uint64(0) << (i % 64))
	for {
		if w != 0 {
			return uint64(word)*64 + uint64(bits.TrailingZeros64(w))
		}
		word++
		if word >= n.nwords() {
			return end
		}
		w = n.lword(word)
	}
}

func (n *Bitmap) shiftedCountRange(start, end uint64) int {
	if start >= end || start >= uint64(n.numBits) {
		return 0
	}
	if end > uint64(n.numBits) {
		end = uint64(n.numBits)
	}
	first, last := int(start/64), int((end-1)/64)
	count := 0
	for i := first; i <= last; i++ {
		w := n.lword(i)
		if i == first {
			w &= ^uint64(0) << (start % 64)
		}
		if i == last && end%64 != 0 {
			w &= ^uint64(0) >> (64 - end%64)
		}
		count += bits.OnesCount64(w)
	}
	return count
}

// anySetNotInWords is AnySetNotIn through lword, for read-only operands.
func anySetNotInWords(n, other *Bitmap, start, end uint64) bool {
	if start >= end || start >= uint64(n.numBits) {
		return false
	}
	if end > uint64(n.numBits) {
		end = uint64(n.numBits)
	}
	first, last := int(start/64), int((end-1)/64)
	for i := first; i <= last; i++ {
		w := n.lword(i)
		if i == first {
			w &= ^uint64(0) << (start % 64)
		}
		if i == last && end%64 != 0 {
			w &= ^uint64(0) >> (64 - end%64)
		}
		if other != nil {
			w &^= other.lword(i)
		}
		if w != 0 {
			return true
		}
	}
	return false
}

// isSameWords is IsSame through lword: the same number of logical words,
// each equal.
func isSameWords(n, b *Bitmap) bool {
	if n.nwords() != b.nwords() {
		return false
	}
	for i := range n.nwords() {
		if n.lword(i) != b.lword(i) {
			return false
		}
	}
	return true
}

// orBoundedWords is OrBounded for a read-only b; lim is already clamped to
// both lengths and positive.
func (n *Bitmap) orBoundedWords(b *Bitmap, lim int64) {
	words := int((lim + 63) / 64)
	for i := range words {
		w := b.lword(i)
		if i == words-1 && lim%64 != 0 {
			w &= ^uint64(0) >> uint64(64-lim%64)
		}
		n.orWord(uint64(i), w)
	}
}

// andWords is And for a read-only b; n has already been grown to b's
// length.
func (n *Bitmap) andWords(b *Bitmap) {
	size := b.nwords()
	n.popCount = 0
	for i := range n.data {
		if i < size {
			n.data[i] &= b.lword(i)
		} else {
			n.data[i] = 0
		}
		n.popCount += int64(bits.OnesCount64(n.data[i]))
	}
}

// initWithReadOnly is InitWith for a read-only m: n becomes an owned copy
// of m's rows, with clean, unshifted words.
func (n *Bitmap) initWithReadOnly(m *Bitmap, mp *mpool.MPool) error {
	if err := n.prepareInit(mp); err != nil {
		return err
	}
	words := m.nwords()
	if err := n.resetWords(words); err != nil {
		return err
	}
	for i := range words {
		n.data[i] = m.lword(i)
	}
	n.numBits = m.numBits
	n.popCount = m.popCount
	return nil
}
