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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// Row-selection primitives (issue #33181). A selection is a bitmap over a
// batch's physical rows; these build and narrow one a word at a time,
// reusing n's words. They read other bitmaps only through the verified
// bitsFrom. They are not yet part of the Gobra proof: moving them into
// bitmap.go with contracts is a follow-up.

// InitWithBools makes n a bitmap of len(values) rows in which row r is set
// when values[r] is true and r is not in nulls. nulls may be nil, and may
// be read-only. n's words are reused when they are large enough.
func (n *Bitmap) InitWithBools(values []bool, nulls *Bitmap, mp *mpool.MPool) (err error) {
	if err = n.InitWithSize(int64(len(values)), mp); err != nil {
		return err
	}
	hasNulls := nulls != nil && !nulls.IsEmpty()
	count := 0
	for i := 0; i < len(n.data); i++ {
		base := 64 * i
		end := base + 64
		if end > len(values) {
			end = len(values)
		}
		var word uint64
		for r := base; r < end; r++ {
			var b uint64
			if values[r] {
				b = 1
			}
			word |= b << uint(r-base)
		}
		if hasNulls {
			word &^= nulls.bitsFrom(int64(base))
		}
		n.data[i] = word
		count += bits.OnesCount64(word)
	}
	n.popCount = int64(count)
	return nil
}

// InitDeposit makes n a bitmap of base.Len() rows in which row r is set when
// r is in base and sel holds rank(r), the number of base rows below r. It
// maps a selection over base's rows (for example the live rows of a block)
// back to base's positions. Rows of sel at or beyond base.Count() are
// ignored. base and sel may be read-only; neither may be n.
func (n *Bitmap) InitDeposit(base, sel *Bitmap, mp *mpool.MPool) (err error) {
	if err = n.InitWithSize(base.Len(), mp); err != nil {
		return err
	}
	var rank int64
	count := 0
	for i := 0; i < len(n.data); i++ {
		w := base.bitsFrom(64 * int64(i))
		k := bits.OnesCount64(w)
		if k == 0 {
			continue
		}
		s := sel.bitsFrom(rank)
		if k < 64 {
			s &= ^uint64(0) >> uint(64-k)
		}
		word := depositBits(w, s)
		n.data[i] = word
		count += bits.OnesCount64(word)
		rank += int64(k)
	}
	n.popCount = int64(count)
	return nil
}

// Word returns rows 64i..64i+63 of n as one word: bit b is row 64i+b. Rows
// at or beyond Len read as clear. n may be read-only. Callers that visit
// the set rows of a selection loop over words with bits.TrailingZeros64
// rather than calling Contains or an iterator per row.
func (n *Bitmap) Word(i int) uint64 {
	return n.bitsFrom(64 * int64(i))
}

// NumWords returns the number of words that cover n's rows, ceil(Len/64).
func (n *Bitmap) NumWords() int {
	return int((n.Len() + 63) / 64)
}

// depositBits places the low bits of s, in order, at the set bits of w:
// the j-th set bit of w (from the bottom) is kept when bit j of s is set.
func depositBits(w, s uint64) uint64 {
	if s == ^uint64(0)>>uint(64-bits.OnesCount64(w)) || s == ^uint64(0) {
		return w
	}
	var res uint64
	for w != 0 {
		low := w & -w
		if s&1 != 0 {
			res |= low
		}
		s >>= 1
		w &^= low
	}
	return res
}

// KeepFirstN clears every set row of n after the first k, so that n keeps
// its k lowest set rows. It does nothing when n has at most k set rows.
func (n *Bitmap) KeepFirstN(k int) {
	if k < 0 {
		panic("negative row count")
	}
	if int64(k) >= int64(n.Count()) {
		return
	}
	n.mustWrite()
	remain := k
	i := 0
	for ; i < len(n.data); i++ {
		c := bits.OnesCount64(n.data[i])
		if remain < c {
			break
		}
		remain -= c
	}
	w := n.data[i]
	var kept uint64
	for ; remain > 0; remain-- {
		low := w & -w
		kept |= low
		w &^= low
	}
	n.data[i] = kept
	for j := i + 1; j < len(n.data); j++ {
		n.data[j] = 0
	}
	n.popCount = int64(k)
}
