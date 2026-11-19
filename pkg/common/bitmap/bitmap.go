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
	"math/bits"
)

// This file is the Gobra-verified core of Bitmap; run gobra/verify.sh. The
// `// @` and `/*@ @*/` comments are Gobra specifications and ghost code; Go
// ignores them. Lemmas live in bitmap_lemmas.gobra and the bit-operation
// model in gobra/stubs/math/bits. Word and bit indexes use /64 and %64 rather
// than >>6 and &63: the compiler emits the same instructions for unsigned
// operands, and Gobra can reason about division but not about shifts.
//
// Invariant (Inv):
//   - len(data) == ceil(numBits/64), and the bitmap owns every word up to
//     cap(data);
//   - no bit at or beyond numBits is set;
//   - popCount is the number of set bits.

type bitmask = uint64

// Mem is the layout part of the invariant; loops keep it while popCount is
// being recomputed.
// @ pred (n *Bitmap) Mem() {
// @ 	acc(n) && 0 <= n.numBits &&
// @ 	len(n.data) == int((n.numBits+63)/64) &&
// @ 	(forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])) &&
// @ 	(forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])) &&
// @ 	TailClear(seq(n.data), n.numBits)
// @ }

// @ pred (n *Bitmap) Inv() {
// @ 	n.Mem() && unfolding n.Mem() in int(n.popCount) == SumPc(seq(n.data), len(n.data))
// @ }

// @ ghost
// @ requires acc(n.Mem(), _)
// @ ensures 0 <= res
// @ decreases
// @ pure func (n *Bitmap) NB() (res int64) {
// @ 	return unfolding acc(n.Mem(), _) in n.numBits
// @ }

// @ ghost
// @ requires acc(n.Mem(), _)
// @ decreases
// @ pure func (n *Bitmap) PC() int {
// @ 	return unfolding acc(n.Mem(), _) in int(n.popCount)
// @ }

// @ ghost
// @ requires acc(n.Mem(), _)
// @ ensures len(res) == int((n.NB()+63)/64)
// @ decreases
// @ pure func (n *Bitmap) Seq() (res seq[uint64]) {
// @ 	return unfolding acc(n.Mem(), _) in seq(n.data)
// @ }

// Has reports whether row r is set: r is below Len and its bit is set.
// @ ghost
// @ requires acc(n.Inv(), _)
// @ decreases
// @ pure func (n *Bitmap) Has(r int) bool {
// @ 	return unfolding acc(n.Inv(), _) in HasS(n.Seq(), n.NB(), r)
// @ }

// HasM is Has for code that holds only Mem.
// @ ghost
// @ requires acc(n.Mem(), _)
// @ decreases
// @ pure func (n *Bitmap) HasM(r int) bool {
// @ 	return HasS(n.Seq(), n.NB(), r)
// @ }

// ISeq is the words of a bitmap held with Inv.
// @ ghost
// @ requires acc(n.Inv(), _)
// @ ensures len(res) == int((n.Len()+63)/64)
// @ decreases
// @ pure func (n *Bitmap) ISeq() (res seq[uint64]) {
// @ 	return unfolding acc(n.Inv(), _) in n.Seq()
// @ }

// tailFact: a set bit lies below Len, so Has is just the bit.
// @ ghost
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4)
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r/64 < len(n.ISeq()) && bits.Bit(n.ISeq()[r/64], r%64))
// @ decreases
// @ func (n *Bitmap) tailFact() {
// @ 	unfold acc(n.Inv(), 1/4)
// @ 	unfold acc(n.Mem(), 1/4)
// @ 	assert forall r int :: {bits.Bit(n.data[r/64], r%64)} 0 <= r && r/64 < len(n.data) && bits.Bit(n.data[r/64], r%64) ==> r < int(n.numBits)
// @ 	fold acc(n.Mem(), 1/4)
// @ 	fold acc(n.Inv(), 1/4)
// @ }

// NumRows is the number of set rows.
// @ ghost
// @ requires acc(n.Inv(), _)
// @ decreases
// @ pure func (n *Bitmap) NumRows() int {
// @ 	return CntS(n.ISeq(), n.Len(), int(n.Len()))
// @ }

// CountIsRows: Count is the number of set rows.
// @ ghost
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && n.Count() == n.NumRows()
// @ decreases
// @ func (n *Bitmap) CountIsRows() {
// @ 	unfold acc(n.Inv(), 1/4)
// @ 	unfold acc(n.Mem(), 1/4)
// @ 	cntWords(seq(n.data), n.numBits, len(n.data))
// @ 	cntBeyond(seq(n.data), n.numBits, 64*len(n.data))
// @ 	fold acc(n.Mem(), 1/4)
// @ 	fold acc(n.Inv(), 1/4)
// @ }

// EmptyIsNone: IsEmpty holds exactly when no row is set.
// @ ghost
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && n.IsEmpty() == (forall r int :: {n.Has(r)} 0 <= r && r < int(n.Len()) ==> !n.Has(r))
// @ decreases
// @ func (n *Bitmap) EmptyIsNone() {
// @ 	n.CountIsRows()
// @ 	cntZero(n.ISeq(), n.Len(), int(n.Len()))
// @ 	assert forall r int :: {n.Has(r)} {HasS(n.ISeq(), n.Len(), r)} n.Has(r) == HasS(n.ISeq(), n.Len(), r)
// @ }

// Words is the number of data words.
// @ ghost
// @ requires acc(n.Inv(), _)
// @ ensures res == int((n.Len()+63)/64)
// @ decreases
// @ pure func (n *Bitmap) Words() (res int) {
// @ 	return unfolding acc(n.Inv(), _) in len(n.Seq())
// @ }

// @ requires acc(n)
// @ requires 0 <= length
// @ ensures n.Inv() && n.Len() == length
// @ ensures forall r int :: {n.Has(r)} !n.Has(r)
// @ decreases
func (n *Bitmap) InitWithSize(length int64) {
	if length < 0 {
		panic("negative bitmap length")
	}
	n.numBits = length
	n.popCount = 0
	n.data = make([]uint64, int((length+63)/64))
	// @ sumZero(seq(n.data), len(n.data))
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// IteratorValue returns a bitmap iterator by value for allocation-sensitive
// callers. Iterator keeps its interface return type for compatibility, while
// callers on hot paths can keep the iterator on their own stack.
// @ requires acc(n.Inv(), 1/2)
// @ ensures acc(n.Inv(), 1/2)
// @ ensures itr.bm == n && 0 <= itr.i && itr.i <= uint64(n.Len())
// @ ensures itr.i < uint64(n.Len()) ==> n.Has(int(itr.i))
// @ ensures forall r int :: {n.Has(r)} 0 <= r && r < int(itr.i) ==> !n.Has(r)
// @ decreases
func (n *Bitmap) IteratorValue() (itr BitmapIterator) {
	itr = BitmapIterator{bm: n}
	itr.i = n.nextSet(0)
	return itr
}

// nextSet returns the position of the first set bit at or after i, or numBits
// if there is none. Bits at or beyond numBits are never read, and words missing
// from data read as zero.
// @ requires 0 <= i
// @ requires acc(n.Inv(), 1/2)
// @ ensures acc(n.Inv(), 1/2)
// @ ensures 0 <= res && res <= uint64(n.Len())
// @ ensures res < uint64(n.Len()) ==> res >= i && n.Has(int(res))
// @ ensures forall r int :: {n.Has(r)} int(i) <= r && r < int(res) ==> !n.Has(r)
// @ ensures n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) nextSet(i uint64) (res uint64) {
	end := uint64(n.Len())
	if i >= end {
		return end
	}
	// @ n.tailFact()
	nwords := (end + 63) / 64
	if words := uint64(n.Size() / 8); words < nwords {
		nwords = words
	}
	mask := ^bitmask(0) << (i % 64)
	// @ invariant acc(n.Inv(), 1/2) && end == uint64(n.Len()) && nwords == uint64(n.Words())
	// @ invariant n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
	// @ invariant 0 <= i/64 && i/64 <= word && word <= nwords && i < end
	// @ invariant mask == (word == i/64 ? ^bitmask(0) << (i % 64) : ^bitmask(0))
	// @ invariant forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r/64 < len(n.ISeq()) && bits.Bit(n.ISeq()[r/64], r%64))
	// @ invariant forall r int :: {n.Has(r)} int(i) <= r && r < 64*int(word) ==> !n.Has(r)
	// @ decreases nwords - word
	for word := i / 64; word < nwords; word++ {
		// @ ghost w := n.ISeq()[word]
		if v := /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[word] & mask; v != 0 {
			// @ assert v == w & mask
			res = word*64 + uint64(bits.TrailingZeros64(v))
			// @ assert word == i/64 ==> int(i%64) == int(i)-64*int(word)
			// @ assert forall k int :: {bits.Bit(v, k)} 0 <= k && 64*int(word)+k < int(res) ==> !bits.Bit(v, k)
			// @ assert forall k int :: {bits.Bit(mask, k)} 0 <= k && k < 64 && 64*int(word)+k >= int(i) ==> bits.Bit(mask, k)
			// @ assert forall k int :: {bits.Bit(v, k)} {bits.Bit(w, k)} 0 <= k && 64*int(word)+k < int(res) && 64*int(word)+k >= int(i) ==> (!bits.Bit(v, k) && bits.Bit(mask, k) && !bits.Bit(w, k))
			// @ assert forall r int :: {n.Has(r)} int(i) <= r && r < int(res) && r >= 64*int(word) ==> !bits.Bit(w, r%64)
			// @ assert forall r int :: {n.Has(r)} int(i) <= r && r < int(res) ==> !n.Has(r)
			if res > end {
				res = end
			}
			return res
		}
		// @ assert forall k int :: {bits.Bit(w, k)} 0 <= k && k < 64 && bits.Bit(mask, k) ==> !bits.Bit(w, k)
		mask = ^bitmask(0)
	}
	return end
}

// HasNext reports whether another set bit remains. An exhausted iterator has
// i == numBits.
// @ requires acc(itr, _) && acc(itr.bm.Inv(), _)
// @ decreases
// @ pure
func (itr *BitmapIterator) HasNext() bool {
	return itr.i < uint64(itr.bm.Len())
}

// PeekNext returns the next set bit without advancing, or 0 if there is none.
// @ requires acc(itr, 1/2) && acc(itr.bm.Inv(), 1/2)
// @ ensures acc(itr, 1/2) && acc(itr.bm.Inv(), 1/2)
// @ decreases
func (itr *BitmapIterator) PeekNext() uint64 {
	if itr.HasNext() {
		return itr.i
	}
	return 0
}

// Next returns the next set bit and advances. Call it only after HasNext
// returns true.
// @ requires acc(itr) && acc(itr.bm.Inv(), 1/2) && 0 <= itr.i && itr.i < uint64(itr.bm.Len())
// @ ensures acc(itr) && acc(itr.bm.Inv(), 1/2) && itr.bm == old(itr.bm)
// @ ensures 0 <= itr.i && itr.i <= uint64(itr.bm.Len())
// @ ensures res == old(itr.i) && itr.i > res
// @ ensures itr.i < uint64(itr.bm.Len()) ==> itr.bm.Has(int(itr.i))
// @ ensures forall r int :: {itr.bm.Has(r)} int(res) < r && r < int(itr.i) ==> !itr.bm.Has(r)
// @ decreases
func (itr *BitmapIterator) Next() (res uint64) {
	res = itr.i
	itr.i = itr.bm.nextSet(res + 1)
	return res
}

// Reset set n.data to nil
// @ requires acc(n)
// @ ensures n.Inv() && n.Len() == 0
// @ ensures forall r int :: {n.Has(r)} !n.Has(r)
// @ decreases
func (n *Bitmap) Reset() {
	n.numBits = 0
	n.popCount = 0
	n.data = nil
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// Len returns the number of bits in the Bitmap.
// @ requires acc(n.Inv(), _)
// @ ensures 0 <= res
// @ decreases
// @ pure
func (n *Bitmap) Len() (res int64) {
	return /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.numBits
}

// CapWords returns the backing capacity in 64-bit words.
// @ requires n != nil ==> acc(n.Inv(), 1/2)
// @ ensures n != nil ==> acc(n.Inv(), 1/2) && res >= n.Words()
// @ decreases
func (n *Bitmap) CapWords() (res int) {
	if n == nil {
		return 0
	}
	return /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ cap(n.data)
}

// Size return number of bytes in n.data
// XXX WTF Note that this size is not the same as InitWithSize.
// @ requires acc(n.Inv(), _)
// @ ensures res == n.Words()*8
// @ decreases
// @ pure
func (n *Bitmap) Size() (res int) {
	return /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ len(n.data) * 8
}

// EmptyByFlag is a quick and dirty way to check if the Bitmap is empty.
// If it retruns true, the Bitmap is empty.  Otherwise, it may or may not be empty.
// @ requires n != nil ==> acc(n.Inv(), _)
// @ decreases
// @ pure
func (n *Bitmap) EmptyByFlag() bool {
	return n == nil || /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ (n.popCount == 0 || len(n.data) == 0)
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
// @ requires acc(n.Inv(), _)
// @ decreases
// @ pure
func (n *Bitmap) IsEmpty() bool {
	return /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.popCount == 0
}

// Add sets row, which must be below Len.
// @ requires n.Inv() && 0 <= row && row < uint64(n.Len())
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || r == int(row))
// @ decreases
func (n *Bitmap) Add(row uint64) {
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ ghost w := int(row / 64)
	mask := uint64(1) << (row % 64)
	// @ testBit(s0[w], row%64)
	// @ pcOr(s0[w], mask, 64)
	// @ pcBit(s0[w], row%64, 64)
	if n.data[row/64]&mask == 0 {
		n.popCount++
	}
	n.data[row/64] |= mask
	// @ assert seq(n.data) == s0[w = s0[w]|mask]
	// @ sumUpdate(s0, w, s0[w]|mask, len(s0))
	// @ assert w == len(n.data)-1 && n.numBits%64 != 0 ==> int(row%64) < int(n.numBits%64)
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// AddMany sets every row in rows; each must be below Len.
// @ requires n.Inv()
// @ requires forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2)
// @ requires forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> 0 <= rows[k] && rows[k] < uint64(n.Len())
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2)
// @ decreases
func (n *Bitmap) AddMany(rows []uint64) {
	// @ invariant n.Inv() && n.Len() == old(n.Len())
	// @ invariant 0 <= k && k <= len(rows)
	// @ invariant forall j int :: {&rows[j]} 0 <= j && j < len(rows) ==> acc(&rows[j], 1/2)
	// @ invariant forall j int :: {&rows[j]} 0 <= j && j < len(rows) ==> 0 <= rows[j] && rows[j] < uint64(n.Len())
	// @ decreases len(rows) - k
	for k := 0; k < len(rows); k++ {
		n.Add(rows[k])
	}
}

// Remove clears row; rows at or beyond Len are ignored.
// @ requires n.Inv() && 0 <= row
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) && r != int(row))
// @ decreases
func (n *Bitmap) Remove(row uint64) {
	if row >= uint64(n.Len()) {
		return
	}
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ ghost w := int(row / 64)
	mask := uint64(1) << (row % 64)
	// @ testBit(s0[w], row%64)
	// @ pcSplit(s0[w], mask, 64)
	// @ pcBit(s0[w], row%64, 64)
	if n.data[row/64]&mask != 0 {
		n.popCount--
	}
	n.data[row/64] &^= mask
	// @ assert seq(n.data) == s0[w = s0[w]&^mask]
	// @ sumUpdate(s0, w, s0[w]&^mask, len(s0))
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// Contains returns true if the row is contained in the Bitmap
// @ requires acc(n.Inv(), _) && 0 <= row
// @ ensures res == n.Has(int(row))
// @ decreases
// @ pure
func (n *Bitmap) Contains(row uint64) (res bool) {
	return row < uint64(n.Len()) &&
		/*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.data[row/64]&(uint64(1)<<(row%64)) != 0
}

// AddRange sets rows [start, end); end must not exceed Len.
// @ requires n.Inv() && 0 <= start && start <= end && end <= uint64(n.Len())
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || (int(start) <= r && r < int(end)))
// @ decreases
func (n *Bitmap) AddRange(start, end uint64) {
	if start >= end {
		return
	}
	i, j := start/64, (end-1)/64
	tail := (64 - end%64) % 64
	if i == j {
		mask := (^uint64(0) << (start % 64)) & (^uint64(0) >> tail)
		n.orWord(i, mask)
		return
	}
	n.orWord(i, ^uint64(0)<<(start%64))
	// Middle words become all ones; count their new bits locally and
	// publish once.
	// @ ghost s1 := unfolding n.Inv() in n.Seq()
	// @ ghost nb1 := n.Len()
	// @ assert forall r int :: {HasS(s1, nb1, r)} HasS(s1, nb1, r) == n.Has(r)
	// @ unfold n.Inv()
	// @ assert n.Seq() == s1 && n.NB() == nb1
	count := 0
	// @ invariant n.Mem() && n.NB() == old(n.Len()) && i < k && k <= j && j < uint64(len(n.Seq())) && j == (end-1)/64
	// @ invariant n.PC()+count == SumPc(n.Seq(), len(n.Seq()))
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (old(n.Has(r)) || (int(start) <= r && r < 64*int(k)))
	// @ decreases j - k
	for k := i + 1; k < j; k++ {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ ghost w0 := s0[k]
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (old(n.Has(r)) || (int(start) <= r && r < 64*int(k)))
		// @ pcSplit(^uint64(0), w0, 64)
		// @ pcNot(w0, 64)
		// @ bits.BitsEq(^uint64(0)&w0, w0)
		count += bits.OnesCount64(^n.data[k])
		n.data[k] = ^uint64(0)
		// @ assert seq(n.data) == s0[int(k) = ^uint64(0)]
		// @ sumUpdate(s0, int(k), ^uint64(0), len(s0))
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != int(k) ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == int(k) ==> HasS(seq(n.data), n.numBits, r) == (0 <= r && r < int(n.numBits))
		// @ fold n.Mem()
	}
	// @ unfold n.Mem()
	n.popCount += int64(count)
	// @ fold n.Mem()
	// @ fold n.Inv()
	n.orWord(j, ^uint64(0)>>tail)
}

// orWord sets the bits of mask in word w. Bits at or beyond Len must not be
// in mask.
// @ requires n.Inv() && 0 <= w && w < uint64(n.Words())
// @ requires w == uint64(n.Words()-1) && n.Len()%64 != 0 ==>
// @ 	forall k int :: {bits.Bit(mask, k)} int(n.Len()%64) <= k && k < 64 ==> !bits.Bit(mask, k)
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) ||
// @ 	(0 <= r && r < int(n.Len()) && r/64 == int(w) && bits.Bit(mask, r%64)))
// @ decreases
func (n *Bitmap) orWord(w uint64, mask uint64) {
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ pcOr(s0[w], mask, 64)
	n.popCount += int64(bits.OnesCount64(mask &^ n.data[w]))
	n.data[w] |= mask
	// @ assert seq(n.data) == s0[int(w) = s0[w]|mask]
	// @ sumUpdate(s0, int(w), s0[w]|mask, len(s0))
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// clearWord clears the bits of mask in word w.
// @ requires n.Inv() && 0 <= w && w < uint64(n.Words())
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) && !(r/64 == int(w) && bits.Bit(mask, r%64)))
// @ decreases
func (n *Bitmap) clearWord(w uint64, mask uint64) {
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ pcSplit(s0[w], mask, 64)
	// @ pcAndNot(s0[w], mask, 64)
	n.popCount -= int64(bits.OnesCount64(n.data[w] & mask))
	n.data[w] &= ^mask
	// @ assert seq(n.data) == s0[int(w) = s0[w] & ^mask]
	// @ sumUpdate(s0, int(w), s0[w] & ^mask, len(s0))
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// RemoveRange clears rows [start, end); rows at or beyond Len are ignored.
// @ requires n.Inv() && 0 <= start && 0 <= end
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) && !(int(start) <= r && r < int(end)))
// @ decreases
func (n *Bitmap) RemoveRange(start, end uint64) {
	if end > uint64(n.Len()) {
		end = uint64(n.Len())
	}
	if start >= end {
		return
	}
	i, j := start/64, (end-1)/64
	tail := (64 - end%64) % 64
	if i == j {
		mask := (^uint64(0) << (start % 64)) & (^uint64(0) >> tail)
		n.clearWord(i, mask)
		return
	}
	n.clearWord(i, ^uint64(0)<<(start%64))
	// Middle words become zero; count their old bits locally and publish
	// once.
	// @ ghost s1 := unfolding n.Inv() in n.Seq()
	// @ ghost nb1 := n.Len()
	// @ assert forall r int :: {HasS(s1, nb1, r)} HasS(s1, nb1, r) == n.Has(r)
	// @ unfold n.Inv()
	// @ assert n.Seq() == s1 && n.NB() == nb1
	count := 0
	// @ invariant n.Mem() && n.NB() == old(n.Len()) && i < k && k <= j && j < uint64(len(n.Seq())) && j == (end-1)/64 && end <= uint64(n.NB())
	// @ invariant n.PC()-count == SumPc(n.Seq(), len(n.Seq()))
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (old(n.Has(r)) && !(int(start) <= r && r < 64*int(k)))
	// @ decreases j - k
	for k := i + 1; k < j; k++ {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (old(n.Has(r)) && !(int(start) <= r && r < 64*int(k)))
		// @ pcZero(64)
		count += bits.OnesCount64(n.data[k])
		n.data[k] = 0
		// @ assert seq(n.data) == s0[int(k) = 0]
		// @ sumUpdate(s0, int(k), 0, len(s0))
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != int(k) ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == int(k) ==> !HasS(seq(n.data), n.numBits, r)
		// @ fold n.Mem()
	}
	// @ unfold n.Mem()
	n.popCount -= int64(count)
	// @ fold n.Mem()
	// @ fold n.Inv()
	n.clearWord(j, ^uint64(0)>>tail)
}

// Or merges b into n, growing n to b's length if needed. b must not be n.
// @ requires n.Inv() && acc(b.Inv(), 1/2)
// @ ensures n.Inv() && acc(b.Inv(), 1/2)
// @ ensures n.Len() >= old(n.Len()) && n.Len() >= b.Len()
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || b.Has(r))
// @ ensures forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
// @ decreases
func (n *Bitmap) Or(b *Bitmap) {
	// @ assert unfolding n.Inv() in unfolding n.Mem() in n != nil
	// @ assert unfolding acc(b.Inv(), 1/2) in unfolding acc(b.Mem(), 1/2) in b != nil
	n.TryExpand(b)
	n.OrBounded(b, b.Len())
}

// OrBounded merges source bits below limit without growing or changing the
// destination's logical length.
// @ requires n != nil ==> n.Inv()
// @ requires b != nil ==> acc(b.Inv(), 1/2)
// @ ensures n != nil ==> n.Inv() && n.Len() == old(n.Len())
// @ ensures b != nil ==> acc(b.Inv(), 1/2) && b.Len() == old(b.Len())
// @ ensures b != nil ==> forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
// @ ensures n != nil && b != nil ==> forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || (r < int(limit) && r < int(n.Len()) && b.Has(r)))
// @ decreases
func (n *Bitmap) OrBounded(b *Bitmap, limit int64) {
	if n == nil || b == nil || limit <= 0 {
		return
	}
	lim := limit
	if lim > n.Len() {
		lim = n.Len()
	}
	if lim > b.Len() {
		lim = b.Len()
	}
	if lim <= 0 {
		return
	}

	words := int((lim + 63) / 64)
	// @ invariant n.Inv() && n.Len() == old(n.Len())
	// @ invariant acc(b.Inv(), 1/2) && b.Len() == old(b.Len())
	// @ invariant 0 < lim && lim <= n.Len() && lim <= b.Len() && lim <= limit && words == int((lim+63)/64)
	// @ invariant (lim == limit || lim == n.Len() || lim == b.Len())
	// @ invariant 0 <= i && i <= words
	// @ invariant forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
	// @ invariant forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || (r < 64*i && r < int(lim) && b.Has(r)))
	// @ decreases words - i
	for i := 0; i < words; i++ {
		source := /*@ unfolding acc(b.Inv(), 1/2) in unfolding acc(b.Mem(), 1/2) in @*/ b.data[i]
		if i == words-1 && lim%64 != 0 {
			// Keep the low lim%64 bits.
			source &= ^uint64(0) >> uint64(64-lim%64)
		}
		// @ ghost if i == n.Words()-1 && n.Len()%64 != 0 {
		// @ 	assert i == words-1 && lim%64 != 0 && lim%64 <= n.Len()%64
		// @ }
		n.orWord(uint64(i), source)
	}
}

// And intersects n with b, growing n to b's length if needed. b must not be n.
// @ requires n.Inv() && acc(b.Inv(), 1/2)
// @ ensures n.Inv() && acc(b.Inv(), 1/2)
// @ ensures n.Len() >= old(n.Len()) && n.Len() >= b.Len()
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) && b.Has(r))
// @ ensures forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
// @ decreases
func (n *Bitmap) And(b *Bitmap) {
	n.TryExpand(b)
	size := (int(b.Len()) + 63) / 64
	words := n.Size() / 8
	// @ ghost s1 := unfolding n.Inv() in n.Seq()
	// @ ghost nb1 := n.Len()
	// @ assert forall r int :: {HasS(s1, nb1, r)} HasS(s1, nb1, r) == n.Has(r)
	// @ assert forall r int :: {HasS(s1, nb1, r)} HasS(s1, nb1, r) == old(n.Has(r))
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == s1 && n.numBits == nb1
	n.popCount = 0
	// @ fold n.Mem()
	// @ assert n.Seq() == s1 && n.NB() == nb1
	// @ invariant n.Mem() && acc(b.Inv(), 1/2)
	// @ invariant n.NB() >= b.Len() && n.NB() >= old(n.Len()) && size == b.Words() && words == len(n.Seq())
	// @ invariant 0 <= i && i <= size && n.PC() == SumPc(n.Seq(), i)
	// @ invariant forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (r/64 < i ? (old(n.Has(r)) && b.Has(r)) : old(n.Has(r)))
	// @ decreases size - i
	for i := 0; i < size; i++ {
		// @ b.tailFact()
		// @ ghost bw := b.ISeq()[i]
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (r/64 < i ? (old(n.Has(r)) && b.Has(r)) : old(n.Has(r)))
		n.data[i] &= /*@ unfolding acc(b.Inv(), 1/2) in unfolding acc(b.Mem(), 1/2) in @*/ b.data[i]
		n.popCount += int64(bits.OnesCount64(n.data[i]))
		// @ assert seq(n.data) == s0[i = n.data[i]] && n.data[i] == s0[i] & bw
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != i ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == i ==> HasS(seq(n.data), n.numBits, r) == (HasS(s0, n.numBits, r) && b.Has(r))
		// @ sumUpdate(s0, i, n.data[i], i)
		// @ fold n.Mem()
	}
	// @ invariant n.Mem() && acc(b.Inv(), 1/2)
	// @ invariant n.NB() >= b.Len() && n.NB() >= old(n.Len()) && words == len(n.Seq())
	// @ invariant size <= i && i <= words && n.PC() == SumPc(n.Seq(), size)
	// @ invariant forall x int :: {n.Seq()[x]} size <= x && x < i ==> n.Seq()[x] == 0
	// @ invariant forall r int :: {b.Has(r)} b.Has(r) == old(b.Has(r))
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (r/64 < size ? (old(n.Has(r)) && b.Has(r)) : (r/64 >= i && old(n.Has(r))))
	// @ decreases words - i
	for i := size; i < words; i++ {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (r/64 < size ? (old(n.Has(r)) && b.Has(r)) : (r/64 >= i && old(n.Has(r))))
		n.data[i] = 0
		// @ assert seq(n.data) == s0[i = 0]
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != i ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == i ==> !HasS(seq(n.data), n.numBits, r)
		// @ sumUpdate(s0, i, 0, size)
		// @ fold n.Mem()
	}
	// @ sumZeroTail(n.Seq(), size, words)
	// @ fold n.Inv()
}

// Negate flips every row below Len.
// @ requires n.Inv()
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r < int(n.Len()) && !old(n.Has(r)))
// @ decreases
func (n *Bitmap) Negate() {
	nBlock, nTail := int(n.Len())/64, int(n.Len())%64
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	n.popCount = 0
	// @ fold n.Mem()
	// @ invariant n.Mem() && n.NB() == old(n.Len())
	// @ invariant nBlock == int(n.NB())/64 && nTail == int(n.NB())%64
	// @ invariant 0 <= i && i <= nBlock && n.PC() == SumPc(n.Seq(), i)
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (0 <= r && r < int(n.NB()) && (r/64 < i ? !old(n.Has(r)) : old(n.Has(r))))
	// @ decreases nBlock - i
	for i := 0; i < nBlock; i++ {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (0 <= r && r < int(n.numBits) && (r/64 < i ? !old(n.Has(r)) : old(n.Has(r))))
		n.data[i] = ^n.data[i]
		n.popCount += int64(bits.OnesCount64(n.data[i]))
		// @ assert seq(n.data) == s0[i = n.data[i]]
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != i ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == i ==> HasS(seq(n.data), n.numBits, r) == (0 <= r && r < int(n.numBits) && !HasS(s0, n.numBits, r))
		// @ sumUpdate(s0, i, n.data[i], i)
		// @ fold n.Mem()
	}
	if nTail > 0 {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// The low nTail bits, 0 < nTail < 64.
		mask := ^uint64(0) >> uint64(64-nTail)
		// @ assert nBlock == len(s0)-1 && int(n.numBits%64) == nTail
		// @ assert forall k int :: {bits.Bit(s0[nBlock], k)} nTail <= k && k < 64 ==> !bits.Bit(s0[nBlock], k)
		// @ assert forall k int :: {bits.Bit(mask, k)} nTail <= k && k < 64 ==> !bits.Bit(mask, k)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} HasS(s0, n.numBits, r) == (0 <= r && r < int(n.numBits) && (r/64 < nBlock ? !old(n.Has(r)) : old(n.Has(r))))
		n.data[nBlock] ^= mask
		n.popCount += int64(bits.OnesCount64(n.data[nBlock]))
		// @ assert seq(n.data) == s0[nBlock = n.data[nBlock]]
		// @ assert forall k int :: {bits.Bit(n.data[nBlock], k)} nTail <= k && k < 64 ==> !bits.Bit(n.data[nBlock], k)
		// @ assert TailClear(seq(n.data), n.numBits)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 != nBlock ==> HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} r/64 == nBlock ==> HasS(seq(n.data), n.numBits, r) == (0 <= r && r < int(n.numBits) && !HasS(s0, n.numBits, r))
		// @ sumUpdate(s0, nBlock, n.data[nBlock], nBlock)
		// @ fold n.Mem()
	}
	// @ assert forall r int :: {HasS(n.Seq(), n.NB(), r)} HasS(n.Seq(), n.NB(), r) == (0 <= r && r < int(n.NB()) && !old(n.Has(r)))
	// @ fold n.Inv()
}

// Reserve grows the backing capacity so the bitmap can hold nbits bits
// without reallocating. The logical length and the bits are unchanged.
// @ requires n.Inv() && 0 <= nbits
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) Reserve(nbits int) {
	words := (nbits + 63) / 64
	if words <= n.CapWords() {
		return
	}
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	data := make([]uint64, len(n.data), words)
	copy(data, n.data /*@, perm(1/2) @*/)
	n.data = data
	// @ assert seq(n.data) == s0
	// @ fold n.Mem()
	// @ fold n.Inv()
}

// @ requires n.Inv() && acc(m.Inv(), 1/2)
// @ ensures n.Inv() && acc(m.Inv(), 1/2) && m.Len() == old(m.Len())
// @ ensures n.Len() >= old(n.Len()) && n.Len() >= m.Len()
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ ensures forall r int :: {m.Has(r)} m.Has(r) == old(m.Has(r))
// @ decreases
func (n *Bitmap) TryExpand(m *Bitmap) {
	n.TryExpandWithSize(int(m.Len()))
}

// TryExpandWithSize grows the logical length to size bits; new bits are
// clear. It never shrinks the bitmap.
// @ requires n.Inv()
// @ ensures n.Inv() && n.Len() >= old(n.Len()) && n.Len() >= int64(size)
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) TryExpandWithSize(size int) {
	if int(n.Len()) >= size {
		return
	}
	requiredCap := (size + 63) / 64
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ ghost nb0 := n.numBits
	n.numBits = int64(size)
	if requiredCap > cap(n.data) {
		newCap := requiredCap
		currentCap := cap(n.data)
		if currentCap <= maxInt/2 {
			grown := currentCap * 2
			if grown < 1 {
				grown = 1
			}
			if grown > newCap {
				newCap = grown
			}
		}
		data := make([]uint64, newCap)
		copy(data, n.data /*@, perm(1/2) @*/)
		n.data = data[:requiredCap]
		// @ assert forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> &n.data[x] == &data[x]
		// @ assert forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> &n.data[:cap(n.data)][x] == &data[x]
		// @ sumPrefix(seq(n.data), s0, len(s0))
		// @ sumZeroTail(seq(n.data), len(s0), len(n.data))
		// @ fold n.Mem()
		// @ fold n.Inv()
		return
	}
	if prevLen := len(n.data); prevLen < requiredCap {
		// @ ghost prev := n.data
		n.data = n.data[:requiredCap]
		// @ assert forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> &n.data[x] == &prev[:cap(prev)][x]
		// @ assert forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> &n.data[:cap(n.data)][x] == &prev[:cap(prev)][x]
		// Spare capacity may hold stale words; new rows must read as clear.
		// @ invariant acc(n) && len(n.data) == requiredCap && n.numBits == int64(size)
		// @ invariant int(n.popCount) == SumPc(s0, len(s0)) && prevLen == len(s0)
		// @ invariant forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> acc(&n.data[x])
		// @ invariant forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> acc(&n.data[:cap(n.data)][x])
		// @ invariant prevLen <= k && k <= requiredCap
		// @ invariant forall x int :: {n.data[x]} 0 <= x && x < prevLen ==> n.data[x] == s0[x]
		// @ invariant forall x int :: {n.data[x]} prevLen <= x && x < k ==> n.data[x] == 0
		// @ decreases requiredCap - k
		for k := prevLen; k < requiredCap; k++ {
			n.data[k] = 0
		}
		// @ sumPrefix(seq(n.data), s0, len(s0))
		// @ sumZeroTail(seq(n.data), len(s0), len(n.data))
		// @ fold n.Mem()
		// @ fold n.Inv()
		return
	}
	// @ fold n.Mem()
	// @ fold n.Inv()
}

const maxInt = int(^uint(0) >> 1)

// Count returns the number of set bits.
// @ requires acc(n.Inv(), _)
// @ decreases
// @ pure
func (n *Bitmap) Count() int {
	return int( /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.popCount)
}

// Clear clears every bit, keeping the length.
// @ requires n.Inv()
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} !n.Has(r)
// @ decreases
func (n *Bitmap) Clear() {
	words := n.Size() / 8
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	n.popCount = 0
	// @ fold n.Mem()
	// @ invariant n.Mem() && n.NB() == old(n.Len()) && words == len(n.Seq()) && n.PC() == 0
	// @ invariant 0 <= i && i <= words
	// @ invariant forall x int :: {n.Seq()[x]} 0 <= x && x < i ==> n.Seq()[x] == 0
	// @ decreases words - i
	for i := 0; i < words; i++ {
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		n.data[i] = 0
		// @ assert seq(n.data) == s0[i = 0]
		// @ fold n.Mem()
	}
	// @ sumZero(n.Seq(), words)
	// @ fold n.Inv()
}
