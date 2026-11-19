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
//
// With negate false the length becomes min(Len, len(sels)); with negate true
// it stays Len and the kept rows are packed at the front, the rest clear.
// @ requires n.Inv() && acc(sels, 1/2) && SortedRows(seq(sels))
// @ ensures n.Inv() && acc(sels, 1/2)
// @ ensures !negate ==> n.Len() == (old(n.Len()) < int64(len(sels)) ? old(n.Len()) : int64(len(sels)))
// @ ensures !negate ==> forall i int :: {n.Has(i)} 0 <= i && i < len(sels) ==> n.Has(i) == old(n.Has(int(sels[i])))
// @ ensures !negate ==> forall i int :: {n.Has(i)} len(sels) <= i ==> !n.Has(i)
// @ ensures negate ==> n.Len() == old(n.Len())
// @ ensures negate ==> forall s int :: {old(n.Has(s))} 0 <= s && s < int(old(n.Len())) &&
// @ 	Rank(seq(sels), int64(s+1)) == Rank(seq(sels), int64(s)) ==>
// @ 	n.Has(s-Rank(seq(sels), int64(s))) == old(n.Has(s))
// @ ensures negate ==> forall i int :: {n.Has(i)} int(old(n.Len()))-Rank(seq(sels), old(n.Len())) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) RemapOrdered(sels []int64, negate bool) {
	// @ assert unfolding n.Inv() in unfolding n.Mem() in n != nil
	if n == nil {
		return
	}
	oldLength := n.Len()
	previous := int64(-1)
	// @ ghost ss := seq(sels)
	// @ invariant acc(sels, 1/2) && seq(sels) == ss && SortedRows(seq(sels)) && 0 <= k && k <= len(sels)
	// @ invariant previous == (k == 0 ? int64(-1) : sels[k-1])
	// @ decreases len(sels) - k
	for k := 0; k < len(sels); k++ {
		if sels[k] <= previous || sels[k] < 0 {
			panic("bitmap ordered remap requires strictly increasing non-negative rows")
		}
		previous = sels[k]
	}
	logicalLength := oldLength
	if !negate && int64(len(sels)) < logicalLength {
		logicalLength = int64(len(sels))
	}
	// @ ghost orig := n.ISeq()
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == n.Has(r)
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == old(n.Has(r))
	// @ selsAtLeastIndex(ss, len(ss))
	// @ unfold n.Inv()
	// @ assert n.Seq() == orig && n.NB() == oldLength
	n.prepareOrderedRemap(logicalLength)

	sourceWordIndex := int64(-1)
	var sourceWord uint64
	output := int64(0)
	if !negate {
		// @ invariant acc(sels, 1/2) && seq(sels) == ss && n.Mem() && n.NB() == oldLength && n.Seq() == n.Seq()
		// @ invariant len(n.Seq()) == len(orig) && 0 <= k && k <= logicalLength && output == k
		// @ invariant logicalLength <= oldLength && logicalLength <= int64(len(sels))
		// @ invariant forall w int :: {n.Seq()[w]} 0 <= w && w < len(orig) && 64*int64(w) >= k ==> n.Seq()[w] == orig[w]
		// @ invariant sourceWordIndex == -1 || (0 <= sourceWordIndex && sourceWordIndex < int64(len(orig)) && sourceWord == orig[sourceWordIndex])
		// @ invariant k > 0 && sels[k-1] < oldLength ==> sourceWordIndex == sels[k-1]/64
		// @ invariant forall r int :: {HasS(n.Seq(), oldLength, r)} 0 <= r && int64(r) < k ==> HasS(n.Seq(), oldLength, r) == HasS(orig, oldLength, int(sels[r]))
		// @ decreases logicalLength - k
		for k := int64(0); k < logicalLength; k++ {
			sel := sels[k]
			value := false
			if sel < oldLength {
				if w := int64(uint64(sel) / 64); w != sourceWordIndex {
					// @ assert 64*w >= k
					sourceWordIndex = w
					sourceWord = /*@ unfolding n.Mem() in @*/ n.data[w]
				}
				value = sourceWord&(uint64(1)<<(uint64(sel)%64)) != 0
			}
			// @ assert value == HasS(orig, oldLength, int(sel))
			// @ unfold n.Mem()
			// @ ghost s0 := seq(n.data)
			mask := uint64(1) << (uint64(output) % 64)
			if value {
				n.data[uint64(output)/64] |= mask
			} else {
				n.data[uint64(output)/64] &^= mask
			}
			// @ assert seq(n.data) == s0[int(output/64) = n.data[output/64]]
			// @ assert forall r int :: {HasS(seq(n.data), oldLength, r)} r/64 != int(output/64) ==> HasS(seq(n.data), oldLength, r) == HasS(s0, oldLength, r)
			// @ assert forall r int :: {HasS(seq(n.data), oldLength, r)} r/64 == int(output/64) && r != int(output) ==> HasS(seq(n.data), oldLength, r) == HasS(s0, oldLength, r)
			// @ assert HasS(seq(n.data), oldLength, int(output)) == value
			// @ fold n.Mem()
			output++
		}
	} else {
		selIndex := 0
		// @ ghost lastSrc := int64(-1)
		// @ ghost kept := seq[int]{}
		// @ invariant acc(sels, 1/2) && seq(sels) == ss && n.Mem() && n.NB() == oldLength && logicalLength == oldLength
		// @ invariant len(n.Seq()) == len(orig) && 0 <= source && source <= oldLength
		// @ invariant 0 <= selIndex && selIndex <= len(sels) && output == source-int64(selIndex) && int64(selIndex) <= source
		// @ invariant forall j int :: {sels[j]} 0 <= j && j < selIndex ==> sels[j] < source
		// @ invariant forall j int :: {sels[j]} selIndex <= j && j < len(sels) ==> sels[j] >= source
		// @ invariant forall w int :: {n.Seq()[w]} 0 <= w && w < len(orig) && 64*int64(w) >= output ==> n.Seq()[w] == orig[w]
		// @ invariant sourceWordIndex == -1 || (0 <= sourceWordIndex && sourceWordIndex < int64(len(orig)) && sourceWord == orig[sourceWordIndex])
		// @ invariant output > 0 ==> output-1 <= lastSrc && lastSrc < source && sourceWordIndex == lastSrc/64
		// @ invariant len(kept) == int(output)
		// @ invariant forall p int :: {HasS(n.Seq(), oldLength, p)} 0 <= p && p < len(kept) ==> HasS(n.Seq(), oldLength, p) == HasS(orig, oldLength, kept[p])
		// @ invariant forall s int :: {Rank(ss, int64(s))} 0 <= s && int64(s) < source && Rank(ss, int64(s+1)) == Rank(ss, int64(s)) ==>
		// @ 	0 <= s-Rank(ss, int64(s)) && s-Rank(ss, int64(s)) < len(kept) && kept[s-Rank(ss, int64(s))] == s
		// @ decreases oldLength - source
		for source := int64(0); source < oldLength; source++ {
			// @ rankAt(ss, len(ss), source, selIndex)
			if selIndex < len(sels) && source == sels[selIndex] {
				// @ rankAt(ss, len(ss), source+1, selIndex+1)
				selIndex++
				continue
			}
			// @ rankAt(ss, len(ss), source+1, selIndex)
			if w := int64(uint64(source) / 64); w != sourceWordIndex {
				// @ assert 64*w >= output
				sourceWordIndex = w
				sourceWord = /*@ unfolding n.Mem() in @*/ n.data[w]
			}
			value := sourceWord&(uint64(1)<<(uint64(source)%64)) != 0
			// @ assert value == HasS(orig, oldLength, int(source))
			// @ ghost sq := n.Seq()
			// @ unfold n.Mem()
			// @ ghost s0 := seq(n.data)
			// @ assert s0 == sq
			mask := uint64(1) << (uint64(output) % 64)
			if value {
				n.data[uint64(output)/64] |= mask
			} else {
				n.data[uint64(output)/64] &^= mask
			}
			// @ ghost s3 := seq(n.data)
			// @ assert s3 == s0[int(output/64) = n.data[output/64]]
			// @ assert forall r int :: {HasS(s3, oldLength, r)} r/64 != int(output/64) ==> HasS(s3, oldLength, r) == HasS(s0, oldLength, r)
			// @ assert forall r int :: {HasS(s3, oldLength, r)} r/64 == int(output/64) && r != int(output) ==> HasS(s3, oldLength, r) == HasS(s0, oldLength, r)
			// @ assert HasS(s3, oldLength, int(output)) == value
			// @ fold n.Mem()
			// @ assert n.Seq() == s3
			// @ lastSrc = source
			// @ kept = kept ++ seq[int]{int(source)}
			output++
		}
		// @ rankAt(ss, len(ss), oldLength, selIndex)
	}
	n.finishOrderedRemap(output, logicalLength)
	// @ assert !negate ==> forall i int :: {n.Has(i)} 0 <= i && int64(i) < logicalLength ==> n.Has(i) == HasS(orig, oldLength, int(sels[i]))
	// @ assert !negate ==> forall i int :: {n.Has(i)} logicalLength <= int64(i) && i < len(sels) ==> !n.Has(i) && !HasS(orig, oldLength, int(sels[i]))
}

// prepareOrderedRemap makes data cover logicalLength bits. Under Inv,
// logicalLength never exceeds Len, so it changes nothing.
// @ requires n.Mem() && 0 <= logicalLength && logicalLength <= n.NB()
// @ ensures n.Mem() && n.NB() == old(n.NB()) && n.Seq() == old(n.Seq())
// @ decreases
func (n *Bitmap) prepareOrderedRemap(logicalLength int64) {
	words := int((logicalLength + 63) / 64)
	// @ unfold n.Mem()
	if words > cap(n.data) {
		data := make([]uint64, words)
		copy(data, n.data /*@, perm(1/2) @*/)
		n.data = data
		// @ fold n.Mem()
		return
	}
	if words > len(n.data) {
		prevLen := len(n.data)
		n.data = n.data[:words]
		// @ invariant acc(n) && len(n.data) == words && prevLen <= x && x <= words
		// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
		// @ decreases words - x
		for x := prevLen; x < words; x++ {
			n.data[x] = 0
		}
	}
	// @ fold n.Mem()
}

// finishOrderedRemap keeps the rows below written, clears the rest, sets the
// length to logicalLength and recounts popCount.
// @ requires n.Mem() && 0 <= written && written <= logicalLength && logicalLength <= n.NB()
// @ ensures n.Inv() && n.Len() == logicalLength
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (r < int(written) && HasS(old(n.Seq()), old(n.NB()), r))
// @ decreases
func (n *Bitmap) finishOrderedRemap(written, logicalLength int64) {
	words := int((logicalLength + 63) / 64)
	// @ ghost s1 := n.Seq()
	// @ ghost nb1 := n.NB()
	if written < logicalLength {
		word := int(written / 64)
		if tail := written % 64; tail != 0 {
			// @ unfold n.Mem()
			// @ ghost s0 := seq(n.data)
			// @ assert s0 == s1 && n.numBits == nb1
			n.data[word] &= ^uint64(0) >> uint64(64-tail)
			// @ assert seq(n.data) == s0[word = n.data[word]]
			// @ assert forall r int :: {HasS(seq(n.data), nb1, r)} r/64 != word ==> HasS(seq(n.data), nb1, r) == HasS(s0, nb1, r)
			// @ assert forall r int :: {HasS(seq(n.data), nb1, r)} r/64 == word ==> HasS(seq(n.data), nb1, r) == (r < int(written) && HasS(s0, nb1, r))
			// @ ghost s3 := seq(n.data)
			// @ assert forall r int :: {HasS(s3, nb1, r)} 0 <= r && r/64 < word ==> r < int(written)
			// @ assert forall r int :: {HasS(s3, nb1, r)} HasS(s3, nb1, r) == ((r < int(written) || r/64 >= word+1) && HasS(s1, nb1, r))
			// @ fold n.Mem()
			// @ assert n.Seq() == s3
			word++
		}
		// @ invariant n.Mem() && n.NB() == nb1 && len(n.Seq()) == len(s1) && words <= len(s1)
		// @ invariant int(written/64) <= word && word <= words && 64*word >= int(written)
		// @ invariant forall r int :: {HasS(n.Seq(), nb1, r)} HasS(n.Seq(), nb1, r) == ((r < int(written) || r/64 >= word) && HasS(s1, nb1, r))
		// @ decreases words - word
		for ; word < words; word++ {
			// @ ghost sq := n.Seq()
			// @ unfold n.Mem()
			// @ ghost s0 := seq(n.data)
			// @ assert s0 == sq
			n.data[word] = 0
			// @ ghost s3 := seq(n.data)
			// @ assert s3 == s0[word = 0]
			// @ assert forall r int :: {HasS(s3, nb1, r)} r/64 != word ==> HasS(s3, nb1, r) == HasS(s0, nb1, r)
			// @ assert forall r int :: {HasS(s3, nb1, r)} r/64 == word ==> !HasS(s3, nb1, r)
			// @ assert forall r int :: {HasS(s3, nb1, r)} 0 <= r && r/64 >= word ==> r >= int(written)
			// @ assert forall r int :: {HasS(s3, nb1, r)} HasS(s3, nb1, r) == ((r < int(written) || r/64 >= word+1) && HasS(s1, nb1, r))
			// @ fold n.Mem()
			// @ assert n.Seq() == s3
		}
	}
	// @ assert forall r int :: {HasS(n.Seq(), nb1, r)} 0 <= r && r < int(logicalLength) ==> r/64 < words
	// @ assert forall r int :: {HasS(n.Seq(), nb1, r)} r < int(logicalLength) ==> HasS(n.Seq(), nb1, r) == (r < int(written) && HasS(s1, nb1, r))
	// @ ghost s4 := n.Seq()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == s4
	if words > 0 && logicalLength%64 != 0 {
		// @ ghost s0 := seq(n.data)
		n.data[words-1] &= ^uint64(0) >> uint64(64-logicalLength%64)
		// @ assert seq(n.data) == s0[words-1 = n.data[words-1]]
		// @ assert forall r int :: {HasS(seq(n.data), logicalLength, r)} r/64 != words-1 ==> HasS(seq(n.data), logicalLength, r) == (r < int(logicalLength) && HasS(s4, nb1, r))
		// @ assert forall r int :: {HasS(seq(n.data), logicalLength, r)} r/64 == words-1 ==> HasS(seq(n.data), logicalLength, r) == (r < int(logicalLength) && HasS(s4, nb1, r))
	}
	// @ ghost s2 := seq(n.data)
	// @ assert forall r int :: {HasS(s2, logicalLength, r)} HasS(s2, logicalLength, r) == (r < int(logicalLength) && HasS(s4, nb1, r))
	// @ assert forall r int :: {HasS(s2, logicalLength, r)} HasS(s2, logicalLength, r) == (r < int(written) && HasS(s1, nb1, r))
	// @ invariant acc(n) && len(n.data) == len(s1) && words <= x && x <= len(n.data)
	// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
	// @ invariant forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
	// @ invariant forall i int :: {n.data[i]} 0 <= i && i < words ==> n.data[i] == s2[i]
	// @ decreases len(n.data) - x
	for x := words; x < len(n.data); x++ {
		n.data[x] = 0
	}
	// @ ghost prev := n.data
	n.data = n.data[:words]
	// @ assert forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> &n.data[i] == &prev[i]
	// @ assert forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < len(prev) ==> &n.data[:cap(n.data)][i] == &prev[i]
	// @ assert forall i int :: {&n.data[:cap(n.data)][i]} len(prev) <= i && i < cap(n.data) ==> &n.data[:cap(n.data)][i] == &prev[:cap(prev)][i]
	// @ assert forall i int :: {seq(n.data)[i]} 0 <= i && i < words ==> seq(n.data)[i] == s2[i]
	// @ assert forall r int :: {HasS(seq(n.data), logicalLength, r)} HasS(seq(n.data), logicalLength, r) == HasS(s2, logicalLength, r)
	// @ ghost s5 := seq(n.data)
	n.numBits = logicalLength
	n.popCount = 0
	// @ fold n.Mem()
	// @ assert n.Seq() == s5
	// @ invariant n.Mem() && n.NB() == logicalLength && len(n.Seq()) == words
	// @ invariant 0 <= x && x <= words && n.PC() == SumPc(n.Seq(), x)
	// @ invariant forall r int :: {HasS(n.Seq(), logicalLength, r)} HasS(n.Seq(), logicalLength, r) == (r < int(written) && HasS(s1, nb1, r))
	// @ decreases words - x
	for x := 0; x < words; x++ {
		// @ unfold n.Mem()
		n.popCount += int64(bits.OnesCount64(n.data[x]))
		// @ fold n.Mem()
	}
	// @ fold n.Inv()
}
