// +gobra

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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// This file is the Gobra-verified core of Bitmap; run gobra/verify.sh. The
// `// @` and `/*@ @*/` comments are Gobra specifications and ghost code; Go
// ignores them. Lemmas live in bitmap_lemmas.gobra and the bit-operation
// model in gobra/stubs/math/bits. Word and bit indexes use /64 and %64 rather
// than >>6 and &63: the compiler emits the same instructions for unsigned
// operands, and Gobra can reason about division but not about shifts.
//
// Invariant (Inv):
//   - the bitmap is owned: it has a pool (mp) and no view shift. Read-only
//     bitmaps and views (bitmap_view.go) are outside the proof;
//   - len(data) == ceil(numBits/64), and the bitmap owns every word up to
//     cap(data);
//   - no bit at or beyond numBits is set;
//   - popCount is the number of set bits.

type bitmask = uint64

// Mem is the layout part of the invariant; loops keep it while popCount is
// being recomputed.
// @ pred (n *Bitmap) Mem() {
// @ 	acc(n) && n.mp != nil && n.shift == 0 && 0 <= n.numBits &&
// @ 	len(n.data) == int((n.numBits+63)/64) &&
// @ 	(forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])) &&
// @ 	(forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])) &&
// @ 	TailClear(seq(n.data), n.numBits)
// @ }

// Raw is a bitmap that may not keep the invariant, together with the words
// it owns: all of them up to cap(data) when it has a pool, none otherwise.
// It is what Release leaves and what the Init* constructors take, so that a
// bitmap reinitialized from its own pool may reuse its words.
// @ pred (n *Bitmap) Raw() {
// @ 	acc(n) && (n.mp != nil ==> n.shift == 0 &&
// @ 		(forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])) &&
// @ 		(forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])))
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
// @ requires acc(n.Inv(), 1/8)
// @ ensures acc(n.Inv(), 1/8) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r/64 < len(n.ISeq()) && bits.Bit(n.ISeq()[r/64], r%64))
// @ decreases
// @ func (n *Bitmap) tailFact() {
// @ 	unfold acc(n.Inv(), 1/8)
// @ 	unfold acc(n.Mem(), 1/8)
// @ 	assert forall r int :: {bits.Bit(n.data[r/64], r%64)} 0 <= r && r/64 < len(n.data) && bits.Bit(n.data[r/64], r%64) ==> r < int(n.numBits)
// @ 	fold acc(n.Mem(), 1/8)
// @ 	fold acc(n.Inv(), 1/8)
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

// rowFacts exposes the row-count structure of a bitmap held with Inv.
// @ ghost
// @ requires acc(n.Inv(), 1/8)
// @ ensures acc(n.Inv(), 1/8) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall j int :: {CntS(n.ISeq(), n.Len(), 64*j)} 0 <= j && j <= len(n.ISeq()) ==> CntS(n.ISeq(), n.Len(), 64*j) == SumPc(n.ISeq(), j)
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == HasS(n.ISeq(), n.Len(), r)
// @ ensures TailClear(n.ISeq(), n.Len())
// @ decreases
// @ func (n *Bitmap) rowFacts() {
// @ 	unfold acc(n.Inv(), 1/8)
// @ 	unfold acc(n.Mem(), 1/8)
// @ 	cntAllWords(seq(n.data), n.numBits, len(n.data))
// @ 	fold acc(n.Mem(), 1/8)
// @ 	fold acc(n.Inv(), 1/8)
// @ }

// rowCnt is the number of set rows below m, with m clamped to Len.
// @ ghost
// @ requires acc(n.Inv(), _) && 0 <= m
// @ decreases
// @ pure func (n *Bitmap) rowCnt(m int) int {
// @ 	return CntS(n.ISeq(), n.Len(), m < int(n.Len()) ? m : int(n.Len()))
// @ }

// CntBelow is the number of set rows below m.
// @ ghost
// @ requires acc(n.Inv(), _) && 0 <= m
// @ decreases
// @ pure func (n *Bitmap) CntBelow(m int) int {
// @ 	return CntS(n.ISeq(), n.Len(), m)
// @ }

// Row-count lemmas in terms of Has and CntBelow.
// @ ghost
// @ requires acc(n.Inv(), 1/4) && 0 <= m
// @ requires forall r int :: {n.Has(r)} 0 <= r && r < m ==> !n.Has(r)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures n.CntBelow(m) == 0
// @ decreases
// @ func (n *Bitmap) cntZeroBelow(m int) {
// @ 	n.rowFacts()
// @ 	assert forall r int :: {HasS(n.ISeq(), n.Len(), r)} 0 <= r && r < m ==> !n.Has(r) && !HasS(n.ISeq(), n.Len(), r)
// @ 	cntZero(n.ISeq(), n.Len(), m)
// @ }

// @ ghost
// @ requires acc(n.Inv(), 1/4) && 0 <= a && a < b && n.Has(a)
// @ requires forall r int :: {n.Has(r)} a < r && r < b ==> !n.Has(r)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures n.CntBelow(b) == n.CntBelow(a)+1
// @ decreases
// @ func (n *Bitmap) cntNext(a, b int) {
// @ 	n.rowFacts()
// @ 	assert HasS(n.ISeq(), n.Len(), a)
// @ 	assert forall r int :: {HasS(n.ISeq(), n.Len(), r)} a < r && r < b ==> !n.Has(r) && !HasS(n.ISeq(), n.Len(), r)
// @ 	cntStep(n.ISeq(), n.Len(), a, b)
// @ }

// @ ghost
// @ requires acc(n.Inv(), 1/4) && 0 <= a && n.Has(a)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) && n.CntBelow(r) == n.CntBelow(a) ==> r == a
// @ decreases
// @ func (n *Bitmap) cntInj(a int) {
// @ 	n.rowFacts()
// @ 	cntInjS(n.ISeq(), n.Len(), a)
// @ }

// @ ghost
// @ requires acc(n.Inv(), 1/4) && 0 <= m
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures n.CntBelow(m+1) == n.CntBelow(m)+(n.Has(m) ? 1 : 0)
// @ decreases
// @ func (n *Bitmap) cntSucc(m int) {
// @ 	n.rowFacts()
// @ }

// @ ghost
// @ requires acc(n.Inv(), 1/4) && 0 <= a && a <= b && n.CntBelow(a) == n.CntBelow(b)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} a <= r && r < b ==> !n.Has(r)
// @ decreases
// @ func (n *Bitmap) cntGap(a, b int) {
// @ 	n.rowFacts()
// @ 	cntFlat(n.ISeq(), n.Len(), a, b)
// @ }

// Words is the number of data words.
// @ ghost
// @ requires acc(n.Inv(), _)
// @ ensures res == int((n.Len()+63)/64)
// @ decreases
// @ pure func (n *Bitmap) Words() (res int) {
// @ 	return unfolding acc(n.Inv(), _) in len(n.Seq())
// @ }

// @ requires n.Raw()
// @ requires 0 <= length
// @ ensures err == nil ==> n.Inv() && n.Len() == length
// @ ensures err == nil ==> forall r int :: {n.Has(r)} !n.Has(r)
// @ ensures err != nil ==> n.Raw()
// @ decreases
func (n *Bitmap) InitWithSize(length int64, mp *mpool.MPool) (err error) {
	if length < 0 {
		panic("negative bitmap length")
	}
	if err = n.prepareInit(mp); err != nil {
		return err
	}
	// @ unfold n.Raw()
	if err = n.resetWords(int((length + 63) / 64)); err != nil {
		// @ fold n.Raw()
		return err
	}
	n.numBits = length
	n.popCount = 0
	// @ sumZero(seq(n.data), len(n.data))
	// @ fold n.Mem()
	// @ fold n.Inv()
	return nil
}

// IteratorValue returns a bitmap iterator by value for allocation-sensitive
// callers. Iterator keeps its interface return type for compatibility, while
// callers on hot paths can keep the iterator on their own stack.
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4)
// @ ensures itr.bm == n && 0 <= itr.i && itr.i <= uint64(n.Len())
// @ ensures itr.i < uint64(n.Len()) ==> n.Has(int(itr.i))
// @ ensures forall r int :: {n.Has(r)} 0 <= r && r < int(itr.i) ==> !n.Has(r)
// @ ensures n.Len() == old(n.Len()) && n.ISeq() == old(n.ISeq()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
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
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4)
// @ ensures 0 <= res && res <= uint64(n.Len())
// @ ensures res < uint64(n.Len()) ==> res >= i && n.Has(int(res))
// @ ensures forall r int :: {n.Has(r)} int(i) <= r && r < int(res) ==> !n.Has(r)
// @ ensures n.Len() == old(n.Len()) && n.ISeq() == old(n.ISeq()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) nextSet(i uint64) (res uint64) {
	if /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.shift != 0 {
		return n.shiftedNextSet(i)
	}
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
	// @ invariant acc(n.Inv(), 1/4) && end == uint64(n.Len()) && nwords == uint64(n.Words())
	// @ invariant n.Len() == old(n.Len()) && n.ISeq() == old(n.ISeq()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
	// @ invariant 0 <= i/64 && i/64 <= word && word <= nwords && i < end
	// @ invariant mask == (word == i/64 ? ^bitmask(0) << (i % 64) : ^bitmask(0))
	// @ invariant forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r/64 < len(n.ISeq()) && bits.Bit(n.ISeq()[r/64], r%64))
	// @ invariant forall r int :: {n.Has(r)} int(i) <= r && r < 64*int(word) ==> !n.Has(r)
	// @ decreases nwords - word
	for word := i / 64; word < nwords; word++ {
		// @ ghost w := n.ISeq()[word]
		if v := /*@ unfolding acc(n.Inv(), 1/4) in unfolding acc(n.Mem(), 1/4) in @*/ n.data[word] & mask; v != 0 {
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
// @ requires acc(itr, 1/2) && acc(itr.bm.Inv(), 1/4)
// @ ensures acc(itr, 1/2) && acc(itr.bm.Inv(), 1/4)
// @ decreases
func (itr *BitmapIterator) PeekNext() uint64 {
	if itr.HasNext() {
		return itr.i
	}
	return 0
}

// Next returns the next set bit and advances. Call it only after HasNext
// returns true.
// @ requires acc(itr) && acc(itr.bm.Inv(), 1/4) && 0 <= itr.i && itr.i < uint64(itr.bm.Len())
// @ ensures acc(itr) && acc(itr.bm.Inv(), 1/4) && itr.bm == old(itr.bm)
// @ ensures 0 <= itr.i && itr.i <= uint64(itr.bm.Len())
// @ ensures res == old(itr.i) && itr.i > res
// @ ensures itr.i < uint64(itr.bm.Len()) ==> itr.bm.Has(int(itr.i))
// @ ensures forall r int :: {itr.bm.Has(r)} int(res) < r && r < int(itr.i) ==> !itr.bm.Has(r)
// @ ensures itr.bm.Len() == old(itr.bm.Len()) && itr.bm.ISeq() == old(itr.bm.ISeq()) && forall r int :: {itr.bm.Has(r)} itr.bm.Has(r) == old(itr.bm.Has(r))
// @ decreases
func (itr *BitmapIterator) Next() (res uint64) {
	res = itr.i
	itr.i = itr.bm.nextSet(res + 1)
	return res
}

// InitWith makes n a copy of m. The make+copy pair compiles to a single
// non-zeroing allocation followed by a memmove.
// @ requires n.Raw() && acc(m.Inv(), 1/2)
// @ ensures acc(m.Inv(), 1/2)
// @ ensures m.Len() == old(m.Len()) && m.ISeq() == old(m.ISeq())
// @ ensures forall r int :: {m.Has(r)} m.Has(r) == old(m.Has(r))
// @ ensures err == nil ==> n.Inv() && n.Len() == m.Len() && n.ISeq() == m.ISeq()
// @ ensures err == nil ==> forall r int :: {n.Has(r)} n.Has(r) == old(m.Has(r))
// @ ensures err != nil ==> n.Raw()
// @ decreases
func (n *Bitmap) InitWith(m *Bitmap, mp *mpool.MPool) (err error) {
	// @ assert unfolding n.Raw() in unfolding acc(m.Inv(), 1/2) in unfolding acc(m.Mem(), 1/2) in n != m
	if n == m {
		return nil
	}
	if /*@ unfolding acc(m.Inv(), _) in unfolding acc(m.Mem(), _) in @*/ m.mp == nil {
		return n.initWithReadOnly(m, mp)
	}
	if err = n.prepareInit(mp); err != nil {
		return err
	}
	// @ ghost s1 := m.ISeq()
	// @ assert forall r int :: {m.Has(r)} m.Has(r) == HasS(s1, m.Len(), r)
	// @ unfold n.Raw()
	// @ unfold acc(m.Inv(), 1/2)
	// @ unfold acc(m.Mem(), 1/2)
	if err = n.resetWords(len(m.data)); err != nil {
		// @ fold n.Raw()
		// @ fold acc(m.Mem(), 1/2)
		// @ fold acc(m.Inv(), 1/2)
		return err
	}
	copy(n.data, m.data /*@, perm(1/4) @*/)
	n.numBits = m.numBits
	n.popCount = m.popCount
	// @ assert seq(n.data) == s1
	// @ fold n.Mem()
	// @ fold n.Inv()
	// @ fold acc(m.Mem(), 1/2)
	// @ fold acc(m.Inv(), 1/2)
	return nil
}

// Swap exchanges the contents of n and m without copying any words.
// @ requires n.Inv() && m.Inv()
// @ ensures n.Inv() && m.Inv()
// @ ensures n.Len() == old(m.Len()) && m.Len() == old(n.Len())
// @ ensures n.ISeq() == old(m.ISeq()) && m.ISeq() == old(n.ISeq())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(m.Has(r))
// @ ensures forall r int :: {m.Has(r)} m.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) Swap(m *Bitmap) {
	// @ ghost sn := n.ISeq()
	// @ ghost sm := m.ISeq()
	// @ assert forall r int :: {n.Has(r)} n.Has(r) == HasS(sn, n.Len(), r)
	// @ assert forall r int :: {m.Has(r)} m.Has(r) == HasS(sm, m.Len(), r)
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ unfold m.Inv()
	// @ unfold m.Mem()
	n.numBits, m.numBits = m.numBits, n.numBits
	n.popCount, m.popCount = m.popCount, n.popCount
	n.data, m.data = m.data, n.data
	n.mp, m.mp = m.mp, n.mp
	n.shift, m.shift = m.shift, n.shift
	// @ fold n.Mem()
	// @ fold n.Inv()
	// @ fold m.Mem()
	// @ fold m.Inv()
}

// Release gives up the invariant of n and keeps its words (Raw), for
// callers that are about to reinitialize n or free it.
// @ ghost
// @ requires n.Inv()
// @ ensures n.Raw() && n.HasPool()
// @ decreases
// @ func (n *Bitmap) Release() {
// @ 	unfold n.Inv()
// @ 	unfold n.Mem()
// @ 	fold n.Raw()
// @ }

// HasPool holds when a Raw bitmap has a pool, and so owns its words.
// @ ghost
// @ requires acc(n.Raw(), _)
// @ decreases
// @ pure func (n *Bitmap) HasPool() bool {
// @ 	return unfolding acc(n.Raw(), _) in n.mp != nil
// @ }

// NoPool holds when n has no pool. The zero Bitmap has none.
// @ ghost
// @ requires acc(n, _)
// @ decreases
// @ pure func (n *Bitmap) NoPool() bool {
// @ 	return n.mp == nil
// @ }

// Unowned makes a bitmap without a pool Raw. It owns no words, and the
// Init* constructors drop whatever words it references.
// @ ghost
// @ requires acc(n) && n.NoPool()
// @ ensures n.Raw() && !n.HasPool()
// @ decreases
// @ func (n *Bitmap) Unowned() {
// @ 	fold n.Raw()
// @ }

// Reset returns n's words to its pool and leaves it empty, keeping the pool.
// @ requires n.Raw() && n.HasPool()
// @ ensures n.Inv() && n.Len() == 0
// @ ensures forall r int :: {n.Has(r)} !n.Has(r)
// @ decreases
func (n *Bitmap) Reset() {
	n.Free()
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
	n.mustWrite()
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
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || InRowsU(seq(rows), len(rows), r))
// @ decreases
func (n *Bitmap) AddMany(rows []uint64) {
	// @ invariant n.Inv() && n.Len() == old(n.Len())
	// @ invariant 0 <= k && k <= len(rows)
	// @ invariant forall j int :: {&rows[j]} 0 <= j && j < len(rows) ==> acc(&rows[j], 1/2)
	// @ invariant forall j int :: {&rows[j]} 0 <= j && j < len(rows) ==> 0 <= rows[j] && rows[j] < uint64(n.Len())
	// @ invariant forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || InRowsU(seq(rows), k, r))
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
	n.mustWrite()
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
	// A pure function is a single expression, so the read-only dispatch is
	// the second disjunct; it compiles to the same branch.
	return /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp != nil &&
		row < uint64(n.numBits) && n.data[row/64]&(uint64(1)<<(row%64)) != 0 ||
		n.mp == nil && n.shiftedContains(row)
}

// AddRange sets rows [start, end); end must not exceed Len. An empty range
// (start >= end) is a no-op.
// @ requires n.Inv() && 0 <= start && 0 <= end && (start < end ==> end <= uint64(n.Len()))
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) || (int(start) <= r && r < int(end)))
// @ decreases
func (n *Bitmap) AddRange(start, end uint64) {
	if start >= end {
		return
	}
	n.mustWrite()
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
	if start >= end || /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.popCount == 0 {
		// @ n.EmptyIsNone()
		return
	}
	n.mustWrite()
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
	mustOK(n.TryExpand(b))
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
	n.mustWrite()
	if /*@ unfolding acc(b.Inv(), _) in unfolding acc(b.Mem(), _) in @*/ b.mp == nil {
		n.orBoundedWords(b, lim)
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
	mustOK(n.TryExpand(b))
	n.mustWrite()
	if /*@ unfolding acc(b.Inv(), _) in unfolding acc(b.Mem(), _) in @*/ b.mp == nil {
		n.andWords(b)
		return
	}
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
	n.mustWrite()
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
func (n *Bitmap) Reserve(nbits int) error {
	return n.growWords((nbits + 63) / 64)
}

// @ requires n.Inv() && acc(m.Inv(), 1/2)
// @ ensures n.Inv() && acc(m.Inv(), 1/2) && m.Len() == old(m.Len())
// @ ensures n.Len() >= old(n.Len()) && (err == nil ==> n.Len() >= m.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ ensures forall r int :: {m.Has(r)} m.Has(r) == old(m.Has(r))
// @ decreases
func (n *Bitmap) TryExpand(m *Bitmap) (err error) {
	return n.TryExpandWithSize(int(m.Len()))
}

// TryExpandWithSize grows the logical length to size bits; new bits are
// clear. It never shrinks the bitmap. If it cannot allocate, it returns the
// error and leaves n unchanged.
// @ requires n.Inv()
// @ ensures n.Inv() && n.Len() == (err != nil || old(n.Len()) >= int64(size) ? old(n.Len()) : int64(size))
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) TryExpandWithSize(size int) (err error) {
	if int(n.Len()) >= size {
		return nil
	}
	if /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp == nil {
		return errNoPool()
	}
	requiredCap := (size + 63) / 64
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	// @ ghost nb0 := n.numBits
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
		data, err := n.allocWords(requiredCap, newCap)
		if err != nil {
			// @ fold n.Mem()
			// @ fold n.Inv()
			return err
		}
		copy(data, n.data /*@, perm(1/2) @*/)
		n.replaceWords(data)
		n.numBits = int64(size)
		// @ assert forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> &n.data[x] == &data[x]
		// @ assert forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> &n.data[:cap(n.data)][x] == &data[x]
		// @ sumPrefix(seq(n.data), s0, len(s0))
		// @ sumZeroTail(seq(n.data), len(s0), len(n.data))
		// @ fold n.Mem()
		// @ fold n.Inv()
		return nil
	}
	n.numBits = int64(size)
	if prevLen := len(n.data); prevLen < requiredCap {
		// @ ghost prev := n.data
		n.data = n.data[:requiredCap]
		// @ assert forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> &n.data[x] == &prev[:cap(prev)][x]
		// @ assert forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> &n.data[:cap(n.data)][x] == &prev[:cap(prev)][x]
		// Spare capacity may hold stale words; new rows must read as clear.
		// @ invariant acc(n) && n.mp != nil && n.shift == 0 && len(n.data) == requiredCap && n.numBits == int64(size)
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
		return nil
	}
	// @ fold n.Mem()
	// @ fold n.Inv()
	return nil
}

// resetWords sets n.data to words zero words, reusing the current block
// when it is large enough. On an error n is unchanged.
// @ requires acc(n) && n.mp != nil && 0 <= words
// @ requires forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
// @ requires forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
// @ ensures acc(n) && n.mp == old(n.mp) && n.shift == old(n.shift)
// @ ensures n.numBits == old(n.numBits) && n.popCount == old(n.popCount)
// @ ensures forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
// @ ensures forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
// @ ensures err == nil ==> len(n.data) == words && forall i int :: {n.data[i]} 0 <= i && i < words ==> n.data[i] == 0
// @ decreases
func (n *Bitmap) resetWords(words int) (err error) {
	if cap(n.data) >= words {
		// @ ghost prev := n.data
		d := n.data[:words]
		// @ assert forall x int :: {&d[x]} 0 <= x && x < len(d) ==> &d[x] == &prev[:cap(prev)][x]
		// @ assert forall x int :: {&d[:cap(d)][x]} len(d) <= x && x < cap(d) ==> &d[:cap(d)][x] == &prev[:cap(prev)][x]
		// @ invariant acc(n, 1/2) && n.data === prev && len(d) == words && cap(d) == cap(prev)
		// @ invariant forall x int :: {&d[x]} 0 <= x && x < len(d) ==> acc(&d[x])
		// @ invariant forall x int :: {&d[:cap(d)][x]} len(d) <= x && x < cap(d) ==> acc(&d[:cap(d)][x])
		// Gobra cannot establish bounds on the range index of an empty
		// slice, so they are guarded by len(d) > 0.
		// @ invariant len(d) > 0 ==> 0 <= i && i <= len(d) && 0 <= i0 && i0 <= len(d)
		// @ invariant forall x int :: {d[x]} 0 <= x && x < i0 && x < len(d) ==> d[x] == 0
		// @ decreases len(d) - i0
		for i := range d /*@ with i0 @*/ {
			d[i] = 0
		}
		n.data = d
		return nil
	}
	data, err := n.allocWords(words, words)
	if err != nil {
		return err
	}
	n.replaceWords(data)
	return nil
}

// growWords makes room for capWords words without changing len(n.data) or
// any word. The new words are zero. On an error n is unchanged.
// @ requires n.Inv() && 0 <= capWords
// @ ensures n.Inv() && n.Len() == old(n.Len()) && n.ISeq() == old(n.ISeq())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ decreases
func (n *Bitmap) growWords(capWords int) (err error) {
	// @ ghost s1 := n.ISeq()
	// @ assert forall r int :: {n.Has(r)} n.Has(r) == HasS(s1, n.Len(), r)
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ ghost s0 := seq(n.data)
	if capWords <= cap(n.data) {
		// @ fold n.Mem()
		// @ fold n.Inv()
		return nil
	}
	if n.mp == nil {
		// @ fold n.Mem()
		// @ fold n.Inv()
		return errNoPool()
	}
	if c := cap(n.data); c <= maxInt/2 && capWords < 2*c {
		capWords = 2 * c
	}
	words, err := n.allocWords(len(n.data), capWords)
	if err != nil {
		// @ fold n.Mem()
		// @ fold n.Inv()
		return err
	}
	copy(words, n.data /*@, perm(1/2) @*/)
	n.replaceWords(words)
	// @ assert forall x int :: {&n.data[x]} 0 <= x && x < len(n.data) ==> &n.data[x] == &words[x]
	// @ assert forall x int :: {&n.data[:cap(n.data)][x]} len(n.data) <= x && x < cap(n.data) ==> &n.data[:cap(n.data)][x] == &words[:cap(words)][x]
	// @ assert seq(n.data) == s0
	// @ fold n.Mem()
	// @ fold n.Inv()
	return nil
}

// mustOK panics with err when it is not nil. A call that returns had a nil
// err, which is all the trusted contract says.
// @ trusted
// @ ensures err == nil
// @ decreases
func mustOK(err error) {
	if err != nil {
		panic(err)
	}
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
	if /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.popCount == 0 {
		// @ n.EmptyIsNone()
		return
	}
	n.mustWrite()
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

// loadBits returns the 64 bits of data starting at bit pos. Bits past the
// end of data read as zero.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
// @ requires 0 <= pos
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
// @ ensures forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==>
// @ 	bits.Bit(res, k) == (int(pos)+k < 64*len(data) && bits.Bit(data[(int(pos)+k)/64], (int(pos)+k)%64))
// @ decreases
func loadBits(data []uint64, pos uint64) (res uint64) {
	w, b := pos/64, pos%64
	// @ assert int(pos) == 64*int(w)+int(b)
	var lo, hi uint64
	if w < uint64(len(data)) {
		lo = data[w] >> b
	}
	if b != 0 && w+1 < uint64(len(data)) {
		hi = data[w+1] << (64 - b)
	}
	// @ assert forall k int :: {bits.Bit(lo|hi, k)} 0 <= k && k < 64 && k+int(b) < 64 ==> (int(pos)+k)/64 == int(w) && (int(pos)+k)%64 == k+int(b)
	// @ assert forall k int :: {bits.Bit(lo|hi, k)} 0 <= k && k < 64 && k+int(b) >= 64 ==> (int(pos)+k)/64 == int(w)+1 && (int(pos)+k)%64 == k+int(b)-64
	return lo | hi
}

// compactBits gathers the bits of word at the set positions of keep, in
// order, into the done bits of c; cnt is the number of set bits in keep. It
// visits only the positions set in both word and keep.
// Bits never move up, so bits of word clear from top up stay clear in c.
// @ requires 0 <= top && top <= 64 && forall k int :: {bits.Bit(word, k)} top <= k && k < 64 ==> !bits.Bit(word, k)
// @ ensures 0 <= cnt && cnt <= 64 && int(cnt) == bits.PcUpTo(keep, 64)
// @ ensures forall k int :: {bits.Bit(c, k)} int(cnt) <= k && k < 64 ==> !bits.Bit(c, k)
// @ ensures forall k int :: {bits.Bit(c, k)} top <= k && k < 64 ==> !bits.Bit(c, k)
// @ ensures forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 && bits.Bit(keep, b) ==> bits.Bit(c, bits.PcUpTo(keep, b)) == bits.Bit(word, b)
// @ decreases
func compactBits(word, keep uint64 /*@, ghost top int @*/) (c, cnt uint64) {
	// @ pcMonoAll(keep, 64)
	// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 ==> bits.PcUpTo(keep, b+1) == bits.PcUpTo(keep, b)+(bits.Bit(keep, b) ? 1 : 0)
	if keep == ^uint64(0) {
		// @ pcOnes(64)
		return word, 64
	}
	m := word & keep
	// @ ghost done := 0
	// @ invariant 0 <= done && done <= 64
	// @ invariant forall b int :: {bits.Bit(m, b)} 0 <= b && b < 64 ==> bits.Bit(m, b) == (bits.Bit(word, b) && bits.Bit(keep, b) && b >= done)
	// @ invariant forall b int :: {bits.Bit(keep, b)} 0 <= b && b < done && bits.Bit(keep, b) ==> bits.Bit(c, bits.PcUpTo(keep, b)) == bits.Bit(word, b)
	// @ invariant forall k int :: {bits.Bit(c, k)} bits.PcUpTo(keep, done) <= k && k < 64 ==> !bits.Bit(c, k)
	// @ invariant forall k int :: {bits.Bit(c, k)} top <= k && k < 64 ==> !bits.Bit(c, k)
	// @ invariant forall a, b int :: {bits.PcUpTo(keep, a), bits.PcUpTo(keep, b)} 0 <= a && a <= b && b <= 64 ==> bits.PcUpTo(keep, a) <= bits.PcUpTo(keep, b)
	// @ invariant forall a int :: {bits.PcUpTo(keep, a)} 0 <= a && a <= 64 ==> 0 <= bits.PcUpTo(keep, a) && bits.PcUpTo(keep, a) <= a
	// @ invariant forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 ==> bits.PcUpTo(keep, b+1) == bits.PcUpTo(keep, b)+(bits.Bit(keep, b) ? 1 : 0)
	// @ decreases 64 - done
	for m != 0 {
		t := uint64(bits.TrailingZeros64(m))
		// @ assert int(t) >= done
		// @ pcLow(keep, int(t))
		q := uint64(bits.OnesCount64(keep & (^uint64(0) >> (64 - t))))
		// @ assert int(q) == bits.PcUpTo(keep, int(t)) && bits.PcUpTo(keep, int(t)+1) == int(q)+1
		// @ assert int(t) < top && int(q) <= int(t)
		// @ assert forall b int :: {bits.Bit(keep, b)} done <= b && b < int(t) ==> !bits.Bit(m, b)
		// @ assert forall b int :: {bits.Bit(keep, b)} done <= b && b < int(t) && bits.Bit(keep, b) ==> !bits.Bit(word, b) && bits.PcUpTo(keep, done) <= bits.PcUpTo(keep, b) && bits.PcUpTo(keep, b) < int(q)
		// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < done && bits.Bit(keep, b) ==> bits.PcUpTo(keep, b) < int(q)
		// @ ghost c0 := c
		c |= uint64(1) << q
		// @ assert forall k int :: {bits.Bit(c, k)} 0 <= k && k < 64 ==> bits.Bit(c, k) == (bits.Bit(c0, k) || k == int(q))
		m &^= uint64(1) << t
		// @ done = int(t) + 1
	}
	// @ assert forall b int :: {bits.Bit(keep, b)} done <= b && b < 64 ==> !bits.Bit(m, b)
	// @ assert forall b int :: {bits.Bit(keep, b)} done <= b && b < 64 && bits.Bit(keep, b) ==> !bits.Bit(word, b) && bits.PcUpTo(keep, b) >= bits.PcUpTo(keep, done)
	return c, uint64(bits.OnesCount64(keep))
}

// remapPut appends the done cnt bits of c to an output stream written into
// data in place: whole words go to data[word], data[word+1], ... and the
// first nbits bits of the next word wait in pend. It returns the new state.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ requires 0 <= nbits && nbits < 64 && 0 <= cnt && cnt <= 64 && 0 <= word && word <= len(data)
// @ requires int(nbits)+int(cnt) >= 64 ==> word < len(data)
// @ requires forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
// @ requires forall k int :: {bits.Bit(c, k)} int(cnt) <= k && k < 64 ==> !bits.Bit(c, k)
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ ensures 0 <= nbits1 && nbits1 < 64 && (word1 == word || word1 == word+1) && word1 <= len(data)
// @ ensures 64*word1+int(nbits1) == 64*word+int(nbits)+int(cnt)
// @ ensures forall k int :: {bits.Bit(pend1, k)} int(nbits1) <= k && k < 64 ==> !bits.Bit(pend1, k)
// @ ensures forall i int :: {data[i]} 0 <= i && i < len(data) && i != word ==> data[i] == old(data[i])
// @ ensures word1 == word && word < len(data) ==> data[word] == old(data[word])
// @ ensures forall p int :: {OutB(seq(data), pend1, word1, p)} 0 <= p && p < 64*word+int(nbits) ==>
// @ 	OutB(seq(data), pend1, word1, p) == OutB(old(seq(data)), pend, word, p)
// @ ensures forall j int :: {bits.Bit(c, j)} 0 <= j && j < int(cnt) ==> OutB(seq(data), pend1, word1, 64*word+int(nbits)+j) == bits.Bit(c, j)
// @ decreases
func remapPut(data []uint64, pend, nbits uint64, word int, c, cnt uint64) (pend1, nbits1 uint64, word1 int) {
	// @ ghost s0 := seq(data)
	pend |= c << nbits
	// @ assert forall k int :: {bits.Bit(pend, k)} 0 <= k && k < 64 ==> bits.Bit(pend, k) == (k < int(nbits) ? bits.Bit(old(pend), k) : bits.Bit(c, k-int(nbits)))
	if nbits+cnt >= 64 {
		data[word] = pend
		// @ assert seq(data) == s0[word = pend]
		return c >> (64 - nbits), nbits + cnt - 64, word + 1
	}
	return pend, nbits + cnt, word
}

// appendKept compacts the bits of src at the set positions of keep and
// appends them to the in-place output stream (see remapPut). Kept bit b of
// src lands at stream position pos+PcUpTo(keep, b), where pos is the stream
// length before the call.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ requires 0 <= nbits && nbits < 64 && 0 <= out && out <= len(data) && pos == 64*out+int(nbits)
// @ requires 64*out+int(nbits)+bits.PcUpTo(keep, 64) < 64*len(data)+64
// @ requires forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
// @ requires 0 <= top && top <= 64 && forall k int :: {bits.Bit(src, k)} top <= k && k < 64 ==> !bits.Bit(src, k)
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ ensures 0 <= nbits1 && nbits1 < 64 && (out1 == out || out1 == out+1) && out1 <= len(data)
// @ ensures 64*out1+int(nbits1) == pos+bits.PcUpTo(keep, 64)
// @ ensures forall k int :: {bits.Bit(pend1, k)} int(nbits1) <= k && k < 64 ==> !bits.Bit(pend1, k)
// @ ensures forall i int :: {data[i]} 0 <= i && i < len(data) && i != out ==> data[i] == old(data[i])
// @ ensures out1 == out && out < len(data) ==> data[out] == old(data[out])
// @ ensures forall p int :: {OutB(seq(data), pend1, out1, p)} 0 <= p && p < pos ==>
// @ 	OutB(seq(data), pend1, out1, p) == OutB(old(seq(data)), pend, out, p)
// @ ensures forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 && bits.Bit(keep, b) ==>
// @ 	0 <= bits.PcUpTo(keep, b) && bits.PcUpTo(keep, b) < bits.PcUpTo(keep, 64) &&
// @ 	OutB(seq(data), pend1, out1, pos+bits.PcUpTo(keep, b)) == bits.Bit(src, b)
// @ ensures forall j int :: {OutB(seq(data), pend1, out1, j)} pos+top <= j && j < 64*out1+int(nbits1) ==>
// @ 	!OutB(seq(data), pend1, out1, j)
// @ decreases
func appendKept(data []uint64, src, keep uint64, pend, nbits uint64, out int /*@, ghost top int, ghost pos int @*/) (pend1, nbits1 uint64, out1 int) {
	c, cnt := compactBits(src, keep /*@, top @*/)
	// @ pcMonoAll(keep, 64)
	// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 && bits.Bit(keep, b) ==>
	// @ 	bits.PcUpTo(keep, b+1) == bits.PcUpTo(keep, b)+1 && bits.PcUpTo(keep, b+1) <= bits.PcUpTo(keep, 64)
	pend1, nbits1, out1 = remapPut(data, pend, nbits, out, c, cnt)
	// @ assert forall j int :: {OutB(seq(data), pend1, out1, j)} pos <= j && j < 64*out1+int(nbits1) ==>
	// @ 	OutB(seq(data), pend1, out1, j) == bits.Bit(c, j-pos)
	return pend1, nbits1, out1
}

// gatherSpan returns, at bit shift+i, the bit of data at row blk[i], and
// zero for rows at or beyond oldLength. blk must be strictly increasing. It
// skips the per-row work when every data word the rows span is zero, which
// is common when few rows are set.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
// @ requires forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> acc(&blk[i], 1/8)
// @ requires SortedRows(seq(blk)) && 0 <= oldLength && int(oldLength) <= 64*len(data)
// @ requires 0 <= shift && int(shift)+len(blk) <= 64
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
// @ ensures forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> acc(&blk[i], 1/8)
// @ ensures forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) ==
// @ 	(int(shift) <= k && k < int(shift)+len(blk) && 0 <= blk[k-int(shift)] && blk[k-int(shift)] < oldLength &&
// @ 	bits.Bit(data[blk[k-int(shift)]/64], int(blk[k-int(shift)]%64)))
// @ decreases
func gatherSpan(data []uint64, blk []int64, oldLength int64, shift uint64) (res uint64) {
	// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) ==> seq(blk)[i] == blk[i] && blk[i] >= 0
	if len(blk) == 0 {
		return 0
	}
	first, last := blk[0], blk[len(blk)-1]
	// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) ==> first <= blk[i] && blk[i] <= last
	if first >= oldLength {
		return 0
	}
	lw := uint64(last) / 64
	if l := uint64(oldLength-1) / 64; l < lw {
		lw = l
	}
	var span uint64
	// @ invariant forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
	// @ invariant uint64(first)/64 <= w && w <= lw+1 && int(lw) < len(data)
	// @ invariant forall x, k int :: {bits.Bit(data[x], k)} int(uint64(first)/64) <= x && x < int(w) && 0 <= k && k < 64 && bits.Bit(data[x], k) ==> bits.Bit(span, k)
	// @ decreases lw + 1 - w
	for w := uint64(first) / 64; w <= lw; w++ {
		// @ ghost s0 := span
		span |= data[w]
		// @ assert forall k int :: {bits.Bit(span, k)} 0 <= k && k < 64 ==> bits.Bit(span, k) == (bits.Bit(s0, k) || bits.Bit(data[w], k))
	}
	if span == 0 {
		// @ assert forall k int :: {bits.Bit(span, k)} 0 <= k && k < 64 ==> !bits.Bit(span, k)
		// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) && blk[i] < oldLength ==> int(uint64(first)/64) <= int(blk[i]/64) && int(blk[i]/64) <= int(lw) && !bits.Bit(span, int(blk[i]%64))
		return 0
	}
	// @ invariant forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/8)
	// @ invariant forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> acc(&blk[i], 1/8)
	// @ invariant 0 <= i && i <= len(blk)
	// @ invariant forall j int :: {blk[j]} 0 <= j && j < len(blk) ==> blk[j] >= 0
	// @ invariant forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) ==
	// @ 	(int(shift) <= k && k < int(shift)+i && 0 <= blk[k-int(shift)] && blk[k-int(shift)] < oldLength &&
	// @ 	bits.Bit(data[blk[k-int(shift)]/64], int(blk[k-int(shift)]%64)))
	// @ decreases len(blk) - i
	for i := 0; i < len(blk); i++ {
		if sel := blk[i]; sel < oldLength {
			// @ ghost r0 := res
			res |= (data[uint64(sel)/64] >> (uint64(sel) % 64) & 1) << (shift + uint64(i))
			// @ assert forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) == (bits.Bit(r0, k) || (k == int(shift)+i && bits.Bit(data[sel/64], int(sel%64))))
		}
	}
	return res
}

// selectWord returns the output word for the strictly increasing rows blk,
// at most 64: bit k is the data bit of row blk[k], zero at or beyond
// oldLength. A run of 64 consecutive rows is one unaligned load; otherwise
// each half of blk is gathered separately, so that a half whose rows span
// only zero words costs a few loads.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/4)
// @ requires forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> acc(&blk[i], 1/4)
// @ requires SortedRows(seq(blk)) && 0 < len(blk) && len(blk) <= 64
// @ requires 0 <= oldLength && int(oldLength) <= 64*len(data)
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i], 1/4)
// @ ensures forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> acc(&blk[i], 1/4)
// @ ensures forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) ==
// @ 	(k < len(blk) && 0 <= blk[k] && blk[k] < oldLength && bits.Bit(data[blk[k]/64], int(blk[k]%64)))
// @ decreases
func selectWord(data []uint64, blk []int64, oldLength int64) (res uint64) {
	// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) ==> seq(blk)[i] == blk[i] && blk[i] >= 0
	// @ selsGap(seq(blk), len(blk))
	first, last := blk[0], blk[len(blk)-1]
	if len(blk) == 64 && last-first == 63 && last < oldLength {
		// @ assert forall i int :: {blk[i]} 0 <= i && i < 64 ==> seq(blk)[i]-seq(blk)[0] >= int64(i) && seq(blk)[63]-seq(blk)[i] >= int64(63-i)
		// @ assert forall i int :: {blk[i]} 0 <= i && i < 64 ==> blk[i] == first+int64(i)
		res = loadBits(data, uint64(first))
		// @ assert forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> blk[k] == first+int64(k) && int(first)+k < int(oldLength)
		// @ assert forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) == (k < len(blk) && 0 <= blk[k] && blk[k] < oldLength && bits.Bit(data[blk[k]/64], int(blk[k]%64)))
		return res
	}
	if len(blk) > 32 {
		lo, hi := blk[:32], blk[32:]
		// @ assert forall i int :: {&lo[i]} 0 <= i && i < 32 ==> &lo[i] == &blk[i]
		// @ assert forall i int :: {&hi[i]} 0 <= i && i < len(hi) ==> &hi[i] == &blk[32+i]
		// @ assert forall i int :: {lo[i]} 0 <= i && i < 32 ==> lo[i] == blk[i] && seq(lo)[i] == lo[i]
		// @ assert forall i int :: {hi[i]} 0 <= i && i < len(hi) ==> hi[i] == blk[32+i] && seq(hi)[i] == hi[i]
		// @ assert SortedRows(seq(lo)) && SortedRows(seq(hi))
		a := gatherSpan(data, lo, oldLength, 0)
		b := gatherSpan(data, hi, oldLength, 32)
		res = a | b
		// @ assert forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) == (bits.Bit(a, k) || bits.Bit(b, k))
		// @ assert forall k int :: {bits.Bit(res, k)} 0 <= k && k < 64 ==> bits.Bit(res, k) == (k < len(blk) && 0 <= blk[k] && blk[k] < oldLength && bits.Bit(data[blk[k]/64], int(blk[k]%64)))
		return res
	}
	return gatherSpan(data, blk, oldLength, 0)
}

// selectOrdered is RemapOrdered with negate false. Each output word takes
// 64 selected rows: a run of 64 consecutive rows is one unaligned load;
// otherwise each half gathers its rows unless the data it spans is zero.
// @ requires n.Inv() && acc(sels, 1/2) && SortedRows(seq(sels))
// @ ensures n.Inv() && acc(sels, 1/2)
// @ ensures n.Len() == (old(n.Len()) < int64(len(sels)) ? old(n.Len()) : int64(len(sels)))
// @ ensures forall i int :: {n.Has(i)} 0 <= i && i < len(sels) ==> n.Has(i) == old(n.Has(int(sels[i])))
// @ ensures forall i int :: {n.Has(i)} len(sels) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) selectOrdered(sels []int64) {
	oldLength := n.Len()
	L := oldLength
	if int64(len(sels)) < L {
		L = int64(len(sels))
	}
	// @ ghost ss := seq(sels)
	// @ ghost orig := n.ISeq()
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == old(n.Has(r))
	// @ selsAtLeastIndex(ss, len(ss))
	// @ selsGap(ss, len(ss))
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == orig && TailClear(orig, oldLength)
	base := int64(0)
	// @ invariant acc(n) && n.mp != nil && n.shift == 0 && n.numBits == oldLength && len(n.data) == len(orig) && len(orig) == int((oldLength+63)/64)
	// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
	// @ invariant forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
	// @ invariant acc(sels, 1/2) && seq(sels) == ss && SortedRows(ss)
	// @ invariant 0 <= L && L <= oldLength && L <= int64(len(sels))
	// @ invariant 0 <= base && base%64 == 0 && base < L+64
	// @ invariant forall w int :: {n.data[w]} int(base/64) <= w && w < len(orig) ==> n.data[w] == orig[w]
	// @ invariant forall p int :: {bits.Bit(n.data[p/64], p%64)} 0 <= p && int64(p) < base && int64(p) < L ==>
	// @ 	bits.Bit(n.data[p/64], p%64) == HasS(orig, oldLength, int(ss[p]))
	// @ invariant forall p int :: {bits.Bit(n.data[p/64], p%64)} L <= int64(p) && int64(p) < base ==> !bits.Bit(n.data[p/64], p%64)
	// @ decreases L - base
	for ; base < L; base += 64 {
		end := base + 64
		if end > L {
			end = L
		}
		blk := sels[base:end]
		// @ assert forall i int :: {&blk[i]} 0 <= i && i < len(blk) ==> &blk[i] == &sels[int(base)+i]
		// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) ==> blk[i] == ss[int(base)+i] && seq(blk)[i] == blk[i]
		// @ assert SortedRows(seq(blk))
		// @ assert forall i int :: {blk[i]} 0 <= i && i < len(blk) ==> blk[i] >= int64(int(base)+i)
		w := selectWord(n.data, blk, oldLength)
		// @ assert forall k int :: {blk[k]} 0 <= k && k < len(blk) ==> int(base) <= int(blk[k]) && int(base/64) <= int(blk[k]/64)
		// @ assert forall k int :: {bits.Bit(w, k)} 0 <= k && k < len(blk) && blk[k] < oldLength ==> n.data[blk[k]/64] == orig[blk[k]/64]
		// @ assert forall k int :: {bits.Bit(w, k)} 0 <= k && k < 64 ==> bits.Bit(w, k) == (k < len(blk) && HasS(orig, oldLength, int(ss[int(base)+k])))
		// @ ghost s0 := seq(n.data)
		n.data[uint64(base)/64] = w
		// @ assert seq(n.data) == s0[int(base/64) = w]
	}
	// @ ghost lw := len(n.data) - 1
	// @ ghost if lw >= 0 && oldLength%64 != 0 {
	// @ 	if lw >= int(base/64) {
	// @ 		assert n.data[lw] == orig[lw]
	// @ 		assert forall k int :: {bits.Bit(n.data[lw], k)} int(oldLength%64) <= k && k < 64 ==> !bits.Bit(orig[lw], k)
	// @ 	} else {
	// @ 		assert 64*(lw+1) <= int(base) && 64*lw+int(oldLength%64) == int(oldLength)
	// @ 		assert forall k int :: {bits.Bit(n.data[lw], k)} int(oldLength%64) <= k && k < 64 ==>
	// @ 			(64*lw+k)/64 == lw && (64*lw+k)%64 == k && !bits.Bit(n.data[(64*lw+k)/64], (64*lw+k)%64)
	// @ 	}
	// @ }
	// @ fold n.Mem()
	n.finishOrderedRemap(L, L)
	// @ assert forall i int :: {n.Has(i)} 0 <= i && int64(i) < L ==> n.Has(i) == HasS(orig, oldLength, int(ss[i]))
	// @ assert forall i int :: {n.Has(i)} L <= int64(i) && i < len(sels) ==> !n.Has(i) && !HasS(orig, oldLength, int(ss[i]))
}

// removedMask consumes the rows of sels in word w, starting at selIdx, and
// returns them as a bit mask with the index of the first row past the word.
// A run of 64 removed rows takes one step.
// @ requires acc(sels, 1/4) && SortedRows(seq(sels)) && 0 <= w && 0 <= selIdx && selIdx <= len(sels)
// @ requires forall j int :: {sels[j]} 0 <= j && j < selIdx ==> sels[j] < 64*w
// @ requires forall j int :: {sels[j]} selIdx <= j && j < len(sels) ==> sels[j] >= 64*w
// @ ensures acc(sels, 1/4) && selIdx <= next && next <= len(sels)
// @ ensures forall j int :: {sels[j]} 0 <= j && j < next ==> sels[j] < 64*(w+1)
// @ ensures forall j int :: {sels[j]} next <= j && j < len(sels) ==> sels[j] >= 64*(w+1)
// @ ensures forall b int :: {bits.PcUpTo(rm, b)} 0 <= b && b <= 64 ==> Rank(seq(sels), 64*w+int64(b)) == selIdx+bits.PcUpTo(rm, b)
// @ decreases
func removedMask(sels []int64, selIdx int, w int64) (rm uint64, next int) {
	// @ ghost ss := seq(sels)
	// @ assert forall j int :: {sels[j]} 0 <= j && j < len(sels) ==> ss[j] == sels[j]
	// @ selsGap(ss, len(ss))
	limit := 64 * (w + 1)
	// @ ghost i0 := selIdx
	// @ rankPrefix(ss, i0, 64*w)
	// @ pcZeroAll(64)
	if selIdx+63 < len(sels) && sels[selIdx] == 64*w && sels[selIdx+63] == limit-1 {
		// @ assert forall j int :: {ss[j]} i0 <= j && j < i0+64 ==> ss[j]-ss[i0] >= int64(j-i0) && ss[i0+63]-ss[j] >= int64(i0+63-j)
		// @ assert forall j int :: {ss[j]} i0 <= j && j < i0+64 ==> ss[j] == 64*w+int64(j-i0)
		// @ rankRun(ss, i0, 64, 64*w)
		// @ pcOnesAll(64)
		rm = ^uint64(0)
		selIdx += 64
		// @ assert forall j int :: {ss[j]} selIdx <= j && j < len(ss) ==> ss[j]-ss[selIdx-1] >= int64(j-selIdx+1)
		// @ assert forall b int :: {bits.PcUpTo(rm, b)} 0 <= b && b <= 64 ==> bits.PcUpTo(rm, b) == RankUpTo(ss, selIdx, 64*w+int64(b))-i0
	} else {
		// @ invariant acc(sels, 1/4) && seq(sels) == ss && SortedRows(ss)
		// @ invariant i0 <= selIdx && selIdx <= len(ss)
		// @ invariant forall j int :: {ss[j]} i0 <= j && j < selIdx ==> ss[j] < limit
		// @ invariant forall j int :: {ss[j]} selIdx <= j && j < len(ss) ==> ss[j] >= 64*w
		// @ invariant forall k int :: {bits.Bit(rm, k)} 0 <= k && k < 64 && bits.Bit(rm, k) ==> selIdx > i0 && 64*w+int64(k) <= ss[selIdx-1]
		// @ invariant forall b int :: {bits.PcUpTo(rm, b)} 0 <= b && b <= 64 ==> bits.PcUpTo(rm, b) == RankUpTo(ss, selIdx, 64*w+int64(b))-i0
		// @ decreases len(ss) - selIdx
		for selIdx < len(sels) && sels[selIdx] < limit {
			sel := sels[selIdx]
			// @ ghost v := int(sel - 64*w)
			// @ assert !bits.Bit(rm, v)
			// @ pcAddBitAll(rm, v, 64)
			// @ ghost r0 := rm
			rm |= uint64(1) << (uint64(sel) % 64)
			// @ assert uint64(sel)%64 == uint64(v)
			// @ assert forall k int :: {bits.Bit(rm, k)} 0 <= k && k < 64 ==> bits.Bit(rm, k) == (bits.Bit(r0, k) || k == v)
			selIdx++
			// @ assert forall b int :: {bits.PcUpTo(rm, b)} 0 <= b && b <= 64 ==> RankUpTo(ss, selIdx, 64*w+int64(b)) == RankUpTo(ss, selIdx-1, 64*w+int64(b))+(v < b ? 1 : 0)
		}
		// @ assert forall j int :: {ss[j]} selIdx <= j && j < len(ss) ==> ss[j] >= limit
	}
	// @ rankTail(ss, selIdx, len(ss), limit)
	return rm, selIdx
}

// removeOrderedWord is one step of removeOrdered: it removes the rows of sels
// in word w of data and appends the kept rows. The contract is
// removeOrdered's loop invariant at w and w+1, where e0 and e1 are
// min(64*w, Len) and min(64*(w+1), Len).
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ requires acc(sels, 1/2) && seq(sels) == ss && SortedRows(ss)
// @ requires len(data) == len(orig) && len(orig) == int((oldLength+63)/64) && 0 <= w && 64*w < oldLength
// @ requires 0 <= selIdx && selIdx <= len(ss)
// @ requires forall j int :: {ss[j]} 0 <= j && j < selIdx ==> ss[j] < 64*w
// @ requires forall j int :: {ss[j]} selIdx <= j && j < len(ss) ==> ss[j] >= 64*w
// @ requires 0 <= nbits && nbits < 64 && 0 <= out && out <= int(w)
// @ requires forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
// @ requires e0 == int(64*w < oldLength ? 64*w : oldLength) && e1 == int(64*(w+1) < oldLength ? 64*(w+1) : oldLength)
// @ requires 64*out+int(nbits) == e0-Rank(ss, int64(e0))
// @ requires forall x int :: {data[x]} out <= x && x < len(data) ==> data[x] == orig[x]
// @ requires seq(data) == sd0
// @ requires 0 <= out && out <= len(sd0) && RemoveRel(ss, orig, oldLength, e0, sd0, pend, out, 64*out+int(nbits))
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ ensures acc(sels, 1/2) && seq(sels) == ss && seq(data) == sd1
// @ ensures selIdx <= selIdx1 && selIdx1 <= len(ss)
// @ ensures forall j int :: {ss[j]} 0 <= j && j < selIdx1 ==> ss[j] < 64*(w+1)
// @ ensures forall j int :: {ss[j]} selIdx1 <= j && j < len(ss) ==> ss[j] >= 64*(w+1)
// @ ensures 0 <= nbits1 && nbits1 < 64 && out <= out1 && out1 <= int(w)+1
// @ ensures forall k int :: {bits.Bit(pend1, k)} int(nbits1) <= k && k < 64 ==> !bits.Bit(pend1, k)
// @ ensures 64*out1+int(nbits1) == e1-Rank(ss, int64(e1))
// @ ensures forall x int :: {data[x]} out1 <= x && x < len(data) ==> data[x] == orig[x]
// @ ensures 0 <= out1 && out1 <= len(sd1) && RemoveRel(ss, orig, oldLength, e1, sd1, pend1, out1, 64*out1+int(nbits1))
// @ decreases
func removeOrderedWord(data []uint64, sels []int64, selIdx int, w int64, oldLength int64, pend, nbits uint64, out int /*@, ghost ss seq[int64], ghost orig seq[uint64], ghost e0 int, ghost e1 int, ghost sd0 seq[uint64] @*/) (pend1, nbits1 uint64, out1 int, selIdx1 int /*@, ghost sd1 seq[uint64] @*/) {
	// @ ghost pos := 64*out + int(nbits)
	var rm uint64
	rm, selIdx1 = removedMask(sels, selIdx, w)
	keep := ^rm
	// @ ghost V := 64
	if 64*(w+1) > oldLength {
		keep &= ^uint64(0) >> (64 - uint64(oldLength)%64)
		// @ V = int(oldLength % 64)
	}
	// @ assert 64*int(w)+V == e1 && 64*int(w) == e0 && 0 < V && V <= 64
	// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 ==> bits.Bit(keep, b) == (b < V && !bits.Bit(rm, b))
	// @ pcKeep(rm, keep, V, 64)
	// @ assert bits.PcUpTo(rm, 0) == 0 && Rank(ss, 64*w) == selIdx && pos == 64*int(w)-selIdx
	// @ assert bits.PcUpTo(keep, 64) == V-bits.PcUpTo(rm, V) && Rank(ss, 64*w+int64(V)) == selIdx+bits.PcUpTo(rm, V)
	// @ rankBounds(ss, len(ss), 64*w+int64(V))
	// @ assert pos+bits.PcUpTo(keep, 64) == 64*int(w)+V-Rank(ss, 64*w+int64(V)) && 64*int(w)+V <= int(oldLength) && int(oldLength) <= 64*len(data)
	src := data[w]
	// @ assert src == orig[w]
	// @ assert sd0 == seq(data)
	pend1, nbits1, out1 = appendKept(data, src, keep, pend, nbits, out /*@, 64, pos @*/)
	// @ sd1 = seq(data)
	// @ removeStepRank(ss, orig, oldLength, int(w), V, selIdx, rm, keep, src, e0, e1, sd0, pend, out, 64*out+int(nbits), sd1, pend1, out1, 64*out1+int(nbits1))
	return pend1, nbits1, out1, selIdx1 /*@, sd1 @*/
}

// removeOrdered is RemapOrdered with negate true. It works a word at a
// time: the removed rows of word w form the mask rm, and the bits of the
// kept rows are compacted and appended to the output stream, which never
// overtakes the word being read.
// @ requires n.Inv() && acc(sels, 1/2) && SortedRows(seq(sels))
// @ ensures n.Inv() && acc(sels, 1/2) && n.Len() == old(n.Len())
// @ ensures forall s int :: {old(n.Has(s))} 0 <= s && s < int(old(n.Len())) &&
// @ 	Rank(seq(sels), int64(s+1)) == Rank(seq(sels), int64(s)) ==>
// @ 	n.Has(s-Rank(seq(sels), int64(s))) == old(n.Has(s))
// @ ensures forall i int :: {n.Has(i)} int(old(n.Len()))-Rank(seq(sels), old(n.Len())) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) removeOrdered(sels []int64) {
	oldLength := n.Len()
	words := (oldLength + 63) / 64
	// @ ghost ss := seq(sels)
	// @ ghost orig := n.ISeq()
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == old(n.Has(r))
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == orig && TailClear(orig, oldLength)
	var pend, nbits uint64
	out := 0
	selIdx := 0
	w := int64(0)
	// @ ghost E := 0
	// @ ghost SD := seq(n.data)
	// @ rankZero(ss)
	// @ invariant acc(n) && n.mp != nil && n.shift == 0 && n.numBits == oldLength && len(n.data) == len(orig) && len(orig) == int(words)
	// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
	// @ invariant forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
	// @ invariant acc(sels, 1/2) && seq(sels) == ss && SortedRows(ss)
	// @ invariant seq(n.data) == SD
	// @ invariant 0 <= w && w <= words && 0 <= selIdx && selIdx <= len(ss)
	// @ invariant forall j int :: {ss[j]} 0 <= j && j < selIdx ==> ss[j] < 64*w
	// @ invariant forall j int :: {ss[j]} selIdx <= j && j < len(ss) ==> ss[j] >= 64*w
	// @ invariant 0 <= nbits && nbits < 64 && 0 <= out && out <= int(w)
	// @ invariant forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
	// @ invariant E == int(64*w < oldLength ? 64*w : oldLength)
	// @ invariant 64*out+int(nbits) == E-Rank(ss, int64(E))
	// @ invariant forall x int :: {n.data[x]} out <= x && x < len(orig) ==> n.data[x] == orig[x]
	// @ invariant 0 <= out && out <= len(SD) && RemoveRel(ss, orig, oldLength, E, SD, pend, out, 64*out+int(nbits))
	// @ decreases words - w
	for w < words {
		// @ ghost e1 := int(64*(w+1) < oldLength ? 64*(w+1) : oldLength)
		pend, nbits, out, selIdx /*@, SD @*/ = removeOrderedWord(n.data, sels, selIdx, w, oldLength, pend, nbits, out /*@, ss, orig, E, e1, SD @*/)
		w++
		// @ E = e1
		// @ carryRemove(ss, orig, oldLength, E, SD, pend, out, 64*out+int(nbits))
	}

	// @ rankBounds(ss, len(ss), oldLength)
	// @ assert 64*out+int(nbits) == int(oldLength)-Rank(ss, oldLength) && int(oldLength) <= 64*len(n.data)
	written := 64*int64(out) + int64(nbits)
	// @ ghost s2 := seq(n.data)
	// @ assert s2 == SD
	if nbits != 0 {
		n.data[out] = pend
		// @ assert seq(n.data) == s2[out = pend]
	}
	// @ assert forall p int :: {bits.Bit(n.data[p/64], p%64)} 0 <= p && int64(p) < written ==> bits.Bit(n.data[p/64], p%64) == OutB(s2, pend, out, p)
	// @ ghost lw := len(n.data) - 1
	// @ ghost if lw >= 0 && oldLength%64 != 0 {
	// @ 	if lw >= out {
	// @ 		assert lw == out ==> nbits == 0 || int(oldLength%64) >= int(nbits)
	// @ 		assert forall k int :: {bits.Bit(n.data[lw], k)} int(oldLength%64) <= k && k < 64 ==> !bits.Bit(n.data[lw], k)
	// @ 	}
	// @ }
	// @ fold n.Mem()
	n.finishOrderedRemap(written, oldLength)
	// @ assert forall s int :: {old(n.Has(s))} 0 <= s && s < int(oldLength) && Rank(ss, int64(s+1)) == Rank(ss, int64(s)) ==>
	// @ 	HasS(orig, oldLength, s) == old(n.Has(s)) && s-Rank(ss, int64(s)) < int(written)
}

// RemapOrdered rewrites the bitmap in place for an ordered row selection.
// When negate is false, output row i comes from sels[i]. When negate is true,
// sels identifies rows to remove. The caller must provide strictly increasing,
// non-negative row indexes; RemapOrdered checks this first and panics,
// leaving the bitmap unchanged, if they are not. Selection rows beyond the
// bitmap's logical length are valid and read as clear: a null bitmap may be
// shorter than its owning vector when the vector's trailing rows are all
// non-null.
//
// With negate false the length becomes min(Len, len(sels)); with negate true
// it stays Len and the kept rows are packed at the front, the rest clear.
// The rewrite works a word at a time and never allocates; see selectOrdered
// and removeOrdered.
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
	n.mustWrite()
	// Reject an invalid selection before changing anything.
	previous := int64(-1)
	// @ ghost ss := seq(sels)
	// @ invariant acc(sels, 1/2) && seq(sels) == ss && SortedRows(seq(sels)) && 0 <= k && k <= len(sels)
	// @ invariant previous == (k == 0 ? int64(-1) : sels[k-1])
	// @ decreases len(sels) - k
	for k := 0; k < len(sels); k++ {
		if sels[k] <= previous {
			panic("bitmap ordered remap requires strictly increasing non-negative rows")
		}
		previous = sels[k]
	}
	n.RemapOrderedUnchecked(sels, negate)
}

// RemapOrderedUnchecked is RemapOrdered without the check of sels, for
// callers that already guarantee strictly increasing, non-negative rows.
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
func (n *Bitmap) RemapOrderedUnchecked(sels []int64, negate bool) {
	if n != nil {
		n.mustWrite()
	}
	// @ assert unfolding n.Inv() in unfolding n.Mem() in n != nil
	if n == nil {
		return
	}
	if negate {
		n.removeOrdered(sels)
		return
	}
	n.selectOrdered(sels)
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
	// Words past the new length need not be cleared: TryExpandWithSize
	// clears spare capacity when it grows into it.
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

// IsSame reports whether n and b have the same data words. It compares words,
// not lengths: bitmaps with the same rows but a different number of words
// compare unequal, and equal words imply the same rows.
// @ requires acc(n.Inv(), 1/4) && acc(b.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && acc(b.Inv(), 1/4)
// @ ensures n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len()) && b.ISeq() == old(b.ISeq()) && b.Len() == old(b.Len())
// @ ensures res == (n.ISeq() == b.ISeq())
// @ ensures res ==> forall r int :: {n.Has(r)} {b.Has(r)} n.Has(r) == b.Has(r)
// @ decreases
func (n *Bitmap) IsSame(b *Bitmap) (res bool) {
	if /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp == nil || /*@ unfolding acc(b.Inv(), _) in unfolding acc(b.Mem(), _) in @*/ b.mp == nil {
		return isSameWords(n, b)
	}
	if b.Size() != n.Size() {
		return false
	}
	words := n.Size() / 8
	// @ invariant acc(n.Inv(), 1/4) && acc(b.Inv(), 1/4)
	// @ invariant n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len()) && b.ISeq() == old(b.ISeq()) && b.Len() == old(b.Len())
	// @ invariant 0 <= i && i <= words && words == len(n.ISeq()) && words == len(b.ISeq())
	// @ invariant forall j int :: {n.ISeq()[j]} 0 <= j && j < i ==> n.ISeq()[j] == b.ISeq()[j]
	// @ decreases words - i
	for i := 0; i < words; i++ {
		if /*@ unfolding acc(n.Inv(), 1/4) in unfolding acc(n.Mem(), 1/4) in @*/ n.data[i] != /*@ unfolding acc(b.Inv(), 1/4) in unfolding acc(b.Mem(), 1/4) in @*/ b.data[i] {
			return false
		}
	}
	// @ assert n.ISeq() == b.ISeq()
	// @ n.rowFacts()
	// @ b.rowFacts()
	// @ sameWordsSameRows(n.ISeq(), n.Len(), b.Len())
	return true
}

// Filter returns a bitmap whose row i is row sels[i] of n. Selected rows that
// are negative or at or beyond Len read as clear.
// @ requires acc(n.Inv(), 1/2) && acc(sels, 1/2)
// @ ensures acc(n.Inv(), 1/2) && acc(sels, 1/2)
// @ ensures n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ ensures err != nil ==> res == nil
// @ ensures err == nil ==> res.Inv() && res.Len() == int64(len(sels))
// @ ensures err == nil ==> forall i int :: {res.Has(i)} 0 <= i && i < len(sels) ==> res.Has(i) == (sels[i] >= 0 && n.Has(int(sels[i])))
// @ ensures err == nil ==> forall i int :: {res.Has(i)} len(sels) <= i ==> !res.Has(i)
// @ decreases
func (n *Bitmap) Filter(sels []int64, mp *mpool.MPool) (res *Bitmap, err error) {
	res = &Bitmap{}
	// @ fold res.Raw()
	if err = res.InitWithSize(int64(len(sels)), mp); err != nil {
		return nil, err
	}
	// @ ghost ss := seq(sels)
	// @ invariant acc(n.Inv(), 1/2) && acc(sels, 1/2) && seq(sels) == ss && res.Inv()
	// @ invariant n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
	// @ invariant 0 <= i && i <= len(sels) && res.Len() == int64(len(sels))
	// @ invariant forall j int :: {res.Has(j)} 0 <= j && j < i ==> res.Has(j) == (sels[j] >= 0 && n.Has(int(sels[j])))
	// @ invariant forall j int :: {res.Has(j)} i <= j ==> !res.Has(j)
	// @ decreases len(sels) - i
	for i := 0; i < len(sels); i++ {
		if sels[i] >= 0 && n.Contains(uint64(sels[i])) {
			res.Add(uint64(i))
		}
	}
	return res, nil
}

// CountRange returns the number of set bits in [start, end). It never scans
// outside the bitmap's logical coverage and does not allocate.
// @ requires acc(n.Inv(), 1/2) && 0 <= start && 0 <= end
// @ ensures acc(n.Inv(), 1/2) && n.Len() == old(n.Len()) && n.ISeq() == old(n.ISeq())
// @ ensures res == (start < end ? n.rowCnt(int(end))-n.rowCnt(int(start)) : 0)
// @ decreases
func (n *Bitmap) CountRange(start, end uint64) (res int) {
	if n != nil && /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp == nil {
		return n.shiftedCountRange(start, end)
	}
	// @ assert unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in n != nil
	if n == nil || start >= end || start >= uint64(n.Len()) {
		return 0
	}
	if end > uint64(n.Len()) {
		end = uint64(n.Len())
	}
	// @ n.rowFacts()
	// @ ghost s := n.ISeq()
	// @ ghost nb := n.Len()
	first := start / 64
	last := (end - 1) / 64
	tail := (64 - end%64) % 64
	// @ assert end == 64*last+(64-tail) && start == 64*first+start%64
	if first == last {
		mask := (^uint64(0) << (start % 64)) & (^uint64(0) >> tail)
		w := /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[first]
		// @ pcMask(w, mask, int(start%64), 64-int(tail), 64)
		// @ cntWord(s, nb, int(first), int(start%64))
		// @ cntWord(s, nb, int(first), 64-int(tail))
		return bits.OnesCount64(w & mask)
	}
	w := /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[first]
	// @ assert w == s[first] && CntS(s, nb, 64*int(first)) == SumPc(s, int(first))
	// @ pcMask(w, ^uint64(0)<<(start%64), int(start%64), 64, 64)
	// @ cntWord(s, nb, int(first), int(start%64))
	// @ cntWord(s, nb, int(first), 64)
	count := bits.OnesCount64(w & (^uint64(0) << (start % 64)))
	// @ assert count == CntS(s, nb, 64*int(first)+64)-CntS(s, nb, int(start))
	// @ invariant acc(n.Inv(), 1/2) && n.Len() == old(n.Len()) && n.ISeq() == s && nb == n.Len()
	// @ invariant first < word && word <= last && last < uint64(len(s))
	// @ invariant count == CntS(s, nb, 64*int(word))-CntS(s, nb, int(start))
	// @ decreases last - word
	for word := first + 1; word < last; word++ {
		// @ assert CntS(s, nb, 64*int(word)) == SumPc(s, int(word))
		// @ cntWord(s, nb, int(word), 64)
		// @ assert CntS(s, nb, 64*int(word)+64) == CntS(s, nb, 64*int(word))+bits.Pc(s[word])
		// @ ghost w := s[word]
		count += bits.OnesCount64( /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[word])
		// @ assert count == CntS(s, nb, 64*int(word+1))-CntS(s, nb, int(start))
	}
	w = /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[last]
	// @ assert w == s[last] && CntS(s, nb, 64*int(last)) == SumPc(s, int(last))
	// @ pcMask(w, ^uint64(0)>>tail, 0, 64-int(tail), 64)
	// @ cntWord(s, nb, int(last), 64-int(tail))
	count += bits.OnesCount64(w & (^uint64(0) >> tail))
	return count
}

// AnySetNotIn reports whether [start, end) contains a bit set in n and not in
// other. It is used when one provenance bitmap (GROUPING) overrides another
// (SQL NULL) without expanding either bitmap row by row.
// @ requires acc(n.Inv(), 1/4) && (other != nil ==> acc(other.Inv(), 1/4)) && 0 <= start && 0 <= end
// @ ensures acc(n.Inv(), 1/4) && (other != nil ==> acc(other.Inv(), 1/4))
// @ ensures n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ ensures other != nil ==> other.Len() == old(other.Len()) && forall r int :: {other.Has(r)} other.Has(r) == old(other.Has(r))
// @ ensures res == (exists r int :: {n.Has(r)} int(start) <= r && r < int(end) && n.Has(r) && !(other != nil && other.Has(r)))
// @ decreases
func (n *Bitmap) AnySetNotIn(other *Bitmap, start, end uint64) (res bool) {
	if n != nil && ( /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp == nil || other != nil && /*@ unfolding acc(other.Inv(), _) in unfolding acc(other.Mem(), _) in @*/ other.mp == nil) {
		return anySetNotInWords(n, other, start, end)
	}
	// @ assert unfolding acc(n.Inv(), 1/4) in unfolding acc(n.Mem(), 1/4) in n != nil
	if n == nil || start >= end || start >= uint64(n.Len()) {
		return false
	}
	if end > uint64(n.Len()) {
		end = uint64(n.Len())
	}
	// @ n.rowFacts()
	// @ ghost if other != nil { other.rowFacts() }
	first := start / 64
	last := (end - 1) / 64
	tail := (64 - end%64) % 64
	otherWords := uint64(0)
	if other != nil {
		otherWords = uint64(other.Size() / 8)
	}
	// @ invariant acc(n.Inv(), 1/4) && (other != nil ==> acc(other.Inv(), 1/4))
	// @ invariant n.Len() == old(n.Len()) && forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
	// @ invariant other != nil ==> other.Len() == old(other.Len()) && forall r int :: {other.Has(r)} other.Has(r) == old(other.Has(r))
	// @ invariant forall r int :: {n.Has(r)} n.Has(r) == HasS(n.ISeq(), n.Len(), r)
	// @ invariant other != nil ==> forall r int :: {other.Has(r)} other.Has(r) == HasS(other.ISeq(), other.Len(), r)
	// @ invariant other != nil ==> otherWords == uint64(len(other.ISeq())) && TailClear(other.ISeq(), other.Len()) && len(other.ISeq()) == int((other.Len()+63)/64)
	// @ invariant TailClear(n.ISeq(), n.Len()) && first == start/64 && last == (end-1)/64 && tail == (64-end%64)%64 && start < end
	// @ invariant first <= word && word <= last+1 && last < uint64(len(n.ISeq())) && end <= uint64(n.Len())
	// @ invariant forall r int :: {n.Has(r)} int(start) <= r && r < 64*int(word) && r < int(end) ==> !(n.Has(r) && !(other != nil && other.Has(r)))
	// @ decreases last + 1 - word
	for word := first; word <= last; word++ {
		mask := ^uint64(0)
		if word == first {
			mask &= ^uint64(0) << (start % 64)
		}
		if word == last {
			mask &= ^uint64(0) >> tail
		}
		// @ assert forall k int :: {bits.Bit(mask, k)} 0 <= k && k < 64 ==> bits.Bit(mask, k) == ((word != first || k >= int(start%64)) && (word != last || k < 64-int(tail)))
		// @ ghost nw := n.ISeq()[word]
		value := /*@ unfolding acc(n.Inv(), 1/4) in unfolding acc(n.Mem(), 1/4) in @*/ n.data[word] & mask
		// @ assert value == nw & mask
		// @ ghost hasOther := other != nil && word < otherWords
		// @ ghost ow := uint64(0)
		if other != nil && word < otherWords {
			// @ ow = other.ISeq()[word]
			value &^= /*@ unfolding acc(other.Inv(), 1/4) in unfolding acc(other.Mem(), 1/4) in @*/ other.data[word]
		}
		// @ assert forall k int :: {bits.Bit(value, k)} 0 <= k && k < 64 ==> bits.Bit(value, k) == (bits.Bit(nw, k) && bits.Bit(mask, k) && !(hasOther && bits.Bit(ow, k)))
		if value != 0 {
			// @ ghost k := bits.SomeBit(value)
			// @ ghost r := 64*int(word) + k
			// @ assert r/64 == int(word) && r%64 == k
			// @ assert n.Has(r) && !(other != nil && other.Has(r))
			return true
		}
		// @ assert forall k int :: {bits.Bit(value, k)} 0 <= k && k < 64 ==> !bits.Bit(value, k)
		// @ assert forall r int :: {n.Has(r)} r/64 == int(word) && int(start) <= r && r < int(end) ==> bits.Bit(mask, r%64) && !bits.Bit(value, r%64)
		// @ assert forall r int :: {n.Has(r)} r/64 == int(word) && int(start) <= r && r < int(end) && n.Has(r) ==> bits.Bit(nw, r%64)
		// @ assert forall r int :: {n.Has(r)} r/64 == int(word) && int(start) <= r && r < int(end) && n.Has(r) ==> hasOther && bits.Bit(ow, r%64)
	}
	return false
}

// RemapMaskOrdered is RemapOrdered for an ordered bitmap selection. Selection
// bitmap iteration is monotonic, so the rewrite uses no row-scaled scratch.
// @ requires n.Inv() && (sels != nil ==> acc(sels.Inv(), 1/2))
// @ ensures n.Inv() && (sels != nil ==> acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq()))
// @ ensures sels != nil && !negate ==> n.Len() == (old(n.Len()) < int64(old(sels.Count())) ? old(n.Len()) : int64(old(sels.Count())))
// @ ensures sels != nil && !negate ==> forall r int :: {sels.Has(r)} sels.Has(r) && sels.CntBelow(r) < int(n.Len()) ==> n.Has(sels.CntBelow(r)) == old(n.Has(r))
// @ ensures sels != nil && negate ==> n.Len() == old(n.Len())
// @ ensures sels != nil && negate ==> forall s int :: {old(n.Has(s))} 0 <= s && s < int(old(n.Len())) && !sels.Has(s) ==>
// @ 	n.Has(s-RankOff(sels.ISeq(), sels.Len(), 0, s)) == old(n.Has(s))
// @ ensures sels != nil && negate ==> forall i int :: {n.Has(i)} int(old(n.Len()))-RankOff(sels.ISeq(), sels.Len(), 0, int(old(n.Len()))) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) RemapMaskOrdered(sels *Bitmap, negate bool) {
	n.RemapMaskOrderedWithOffset(sels, negate, 0)
}

// RemapMaskOrderedWithOffset applies an ordered bitmap selection after adding
// offset to every selected source row, without materializing an index slice.
// The selected source rows are r + offset for the set rows r of sels, in
// increasing order. When negate is false, output row i comes from the i-th
// selected row and the length becomes min(Len, sels.Count()). When negate is
// true, the selected rows are removed: the kept rows are packed at the front,
// the length stays Len and the freed tail is clear. sels must not be n.
// @ requires n.Inv() && (sels != nil ==> acc(sels.Inv(), 1/2)) && 0 <= offset
// @ ensures n.Inv() && (sels != nil ==> acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq()))
// @ ensures sels != nil && !negate ==> n.Len() == (old(n.Len()) < int64(old(sels.Count())) ? old(n.Len()) : int64(old(sels.Count())))
// @ ensures sels != nil && !negate ==> forall r int :: {sels.Has(r)} sels.Has(r) && sels.CntBelow(r) < int(n.Len()) ==> n.Has(sels.CntBelow(r)) == old(n.Has(r+int(offset)))
// @ ensures sels != nil && negate ==> n.Len() == old(n.Len())
// @ ensures sels != nil && negate ==> forall s int :: {old(n.Has(s))} 0 <= s && s < int(old(n.Len())) && !(s >= int(offset) && sels.Has(s-int(offset))) ==>
// @ 	n.Has(s-RankOff(sels.ISeq(), sels.Len(), int(offset), s)) == old(n.Has(s))
// @ ensures sels != nil && negate ==> forall i int :: {n.Has(i)} int(old(n.Len()))-RankOff(sels.ISeq(), sels.Len(), int(offset), int(old(n.Len()))) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) RemapMaskOrderedWithOffset(sels *Bitmap, negate bool, offset uint64) {
	if n != nil {
		n.mustWrite()
	}
	// @ assert unfolding n.Inv() in unfolding n.Mem() in n != nil
	if n == nil || sels == nil {
		return
	}
	if negate {
		n.removeMask(sels, offset)
		return
	}
	n.selectMask(sels, offset)
}

// bitsFrom returns the 64 rows of n starting at row pos: bit b is row
// pos+b. Rows below zero or past the data read as clear.
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == HasS(n.ISeq(), n.Len(), int(pos)+b)
// @ decreases
func (n *Bitmap) bitsFrom(pos int64) (res uint64) {
	if /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.mp == nil {
		return n.lbitsFrom(pos)
	}
	// @ ghost sq := n.ISeq()
	// @ ghost nb := n.Len()
	// @ n.tailFact()
	// @ assert forall r int :: {HasS(sq, nb, r)} HasS(sq, nb, r) == n.Has(r)
	// @ unfold acc(n.Inv(), 1/4)
	// @ unfold acc(n.Mem(), 1/4)
	// @ assert seq(n.data) == sq
	if pos >= 0 {
		res = loadBits(n.data, uint64(pos))
	} else if pos > -64 && len(n.data) > 0 {
		res = n.data[0] << uint64(-pos)
		// @ assert forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 && int(pos)+b >= 0 ==> (int(pos)+b)/64 == 0 && (int(pos)+b)%64 == int(pos)+b
	}
	// @ fold acc(n.Mem(), 1/4)
	// @ fold acc(n.Inv(), 1/4)
	return res
}

// word returns data word i of an owned n. A read-only n reads through lword
// instead; see the caller.
// @ requires acc(n.Inv(), _) && 0 <= i && i < n.Words()
// @ ensures res == n.ISeq()[i]
// @ decreases
// @ pure
func (n *Bitmap) word(i int) (res uint64) {
	return /*@ unfolding acc(n.Inv(), _) in unfolding acc(n.Mem(), _) in @*/ n.data[i]
}

// selectMaskWord is one step of selectMask: it appends the rows that word ws
// of the selection, keep, selects. at is the first source row of the word,
// 64*ws+offset. The contract is selectMask's loop invariant at ws and ws+1.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ requires len(data) == len(orig) && 0 <= oldLength && int(oldLength) <= 64*len(orig)
// @ requires forall r int :: {bits.Bit(orig[r/64], r%64)} 0 <= r && r/64 < len(orig) && int(oldLength) <= r ==> !bits.Bit(orig[r/64], r%64)
// @ requires 0 <= snb && len(sq) == int((snb+63)/64) && TailClear(sq, snb)
// @ requires 0 <= ws && ws < len(sq) && keep == sq[ws] && 0 <= off && int(at) == 64*ws+off
// @ requires 0 <= nbits && nbits < 64 && 0 <= out && out < len(data)
// @ requires forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
// @ requires 64*out+int(nbits) == SumPc(sq, ws) && SumPc(sq, ws) <= 64*ws && CntS(sq, snb, 64*ws) == SumPc(sq, ws)
// @ requires forall x int :: {data[x]} out <= x && x < len(data) ==> data[x] == orig[x]
// @ requires forall r int :: {HasS(sq, snb, r)} 0 <= r && r < 64*ws && HasS(sq, snb, r) ==>
// @ 	0 <= CntS(sq, snb, r) && CntS(sq, snb, r) < 64*out+int(nbits) &&
// @ 	OutB(seq(data), pend, out, CntS(sq, snb, r)) == HasS(orig, oldLength, r+off)
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ ensures 0 <= nbits1 && nbits1 < 64 && out <= out1 && out1 <= out+1 && out1 <= len(data)
// @ ensures forall k int :: {bits.Bit(pend1, k)} int(nbits1) <= k && k < 64 ==> !bits.Bit(pend1, k)
// @ ensures 64*out1+int(nbits1) == SumPc(sq, ws+1) && SumPc(sq, ws+1) <= 64*(ws+1) && CntS(sq, snb, 64*(ws+1)) == SumPc(sq, ws+1)
// @ ensures forall x int :: {data[x]} out1 <= x && x < len(data) ==> data[x] == orig[x]
// @ ensures forall r int :: {HasS(sq, snb, r)} 0 <= r && r < 64*(ws+1) && HasS(sq, snb, r) ==>
// @ 	0 <= CntS(sq, snb, r) && CntS(sq, snb, r) < 64*out1+int(nbits1) &&
// @ 	OutB(seq(data), pend1, out1, CntS(sq, snb, r)) == HasS(orig, oldLength, r+off)
// @ decreases
func selectMaskWord(data []uint64, keep, at uint64, pend, nbits uint64, out int /*@, ghost sq seq[uint64], ghost snb int64, ghost orig seq[uint64], ghost oldLength int64, ghost off int, ghost ws int @*/) (pend1, nbits1 uint64, out1 int) {
	// @ ghost pos := 64*out + int(nbits)
	// @ tailBits(sq, snb)
	// @ sumPcBound(sq, ws+1)
	// @ cntWordAll(sq, snb, ws, 64)
	// @ assert pos == SumPc(sq, ws) && keep == sq[ws]
	// @ cntWords(sq, snb, ws+1)
	// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 ==> (64*ws+b)/64 == ws && (64*ws+b)%64 == b && HasS(sq, snb, 64*ws+b) == bits.Bit(keep, b)
	if keep == 0 {
		// @ pcZeroAll(64)
		// @ assert SumPc(sq, ws+1) == SumPc(sq, ws) && bits.Pc(keep) == 0
		// @ assert forall r int :: {HasS(sq, snb, r)} 64*ws <= r && r < 64*ws+64 ==> !bits.Bit(keep, r-64*ws) && !HasS(sq, snb, r)
		return pend, nbits, out
	}
	src := loadBits(data, at)
	// @ assert forall b int :: {bits.Bit(src, b)} 0 <= b && b < 64 ==> bits.Bit(src, b) == HasS(orig, oldLength, 64*ws+off+b)
	// @ pcMonoAll(keep, 64)
	// @ assert old(seq(data)) == seq(data)
	pend1, nbits1, out1 = appendKept(data, src, keep, pend, nbits, out /*@, 64, pos @*/)
	// @ selectMaskStep(sq, snb, orig, oldLength, off, ws, keep, src, old(seq(data)), pend, out, pos, seq(data), pend1, out1, 64*out1+int(nbits1))
	// @ assert SumPc(sq, ws+1) == SumPc(sq, ws)+bits.Pc(keep)
	return pend1, nbits1, out1
}

// removeMaskWord is one step of removeMask: it appends the rows of word w of
// data that the selection sels, shifted by offset, does not remove. The
// contract is removeMask's loop invariant at w and w+1.
// @ requires forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ requires acc(sels.Inv(), 1/2) && sels.ISeq() == sq && sels.Len() == snb
// @ requires len(data) == len(orig) && len(orig) == int((oldLength+63)/64) && 0 <= w && 64*w < oldLength
// @ requires 0 <= offset && off == int(offset)
// @ requires 0 <= nbits && nbits < 64 && 0 <= out && out <= int(w)
// @ requires forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
// @ requires e0 == int(64*w < oldLength ? 64*w : oldLength) && e1 == int(64*(w+1) < oldLength ? 64*(w+1) : oldLength)
// @ requires 64*out+int(nbits) == e0-RankOff(sq, snb, off, e0)
// @ requires forall x int :: {data[x]} out <= x && x < len(data) ==> data[x] == orig[x]
// @ requires 0 <= out && out <= len(data) && RemoveMaskRel(sq, snb, off, orig, oldLength, e0, seq(data), pend, out, 64*out+int(nbits))
// @ ensures forall i int :: {&data[i]} 0 <= i && i < len(data) ==> acc(&data[i])
// @ ensures acc(sels.Inv(), 1/2) && sels.ISeq() == sq && sels.Len() == snb
// @ ensures 0 <= nbits1 && nbits1 < 64 && out <= out1 && out1 <= int(w)+1
// @ ensures forall k int :: {bits.Bit(pend1, k)} int(nbits1) <= k && k < 64 ==> !bits.Bit(pend1, k)
// @ ensures 64*out1+int(nbits1) == e1-RankOff(sq, snb, off, e1)
// @ ensures forall x int :: {data[x]} out1 <= x && x < len(data) ==> data[x] == orig[x]
// @ ensures 0 <= out1 && out1 <= len(data) && RemoveMaskRel(sq, snb, off, orig, oldLength, e1, seq(data), pend1, out1, 64*out1+int(nbits1))
// @ decreases
func removeMaskWord(data []uint64, sels *Bitmap, w int64, offset uint64, oldLength int64, pend, nbits uint64, out int /*@, ghost sq seq[uint64], ghost snb int64, ghost orig seq[uint64], ghost off int, ghost e0 int, ghost e1 int @*/) (pend1, nbits1 uint64, out1 int) {
	// @ ghost pos := 64*out + int(nbits)
	rm := sels.bitsFrom(64*w - int64(offset))
	// @ assert forall b int :: {bits.Bit(rm, b)} 0 <= b && b < 64 ==> bits.Bit(rm, b) == (64*int(w)+b >= off && HasS(sq, snb, 64*int(w)+b-off))
	// @ ghost i0 := RankOff(sq, snb, off, 64*int(w))
	// @ rankOffWord(sq, snb, off, 64*int(w), rm, 64)
	keep := ^rm
	// @ ghost V := 64
	if 64*(w+1) > oldLength {
		keep &= ^uint64(0) >> (64 - uint64(oldLength)%64)
		// @ V = int(oldLength % 64)
	}
	// @ assert 64*int(w)+V == int(64*(w+1) < oldLength ? 64*(w+1) : oldLength) && 0 < V && V <= 64
	// @ assert forall b int :: {bits.Bit(keep, b)} 0 <= b && b < 64 ==> bits.Bit(keep, b) == (b < V && !bits.Bit(rm, b))
	// @ pcKeep(rm, keep, V, 64)
	// @ assert bits.PcUpTo(rm, 0) == 0 && pos == 64*int(w)-i0
	// @ assert bits.PcUpTo(keep, 64) == V-bits.PcUpTo(rm, V)
	src := data[w]
	// @ assert src == orig[w]
	// @ assert old(seq(data)) == seq(data)
	pend1, nbits1, out1 = appendKept(data, src, keep, pend, nbits, out /*@, 64, pos @*/)
	// @ removeStepOff(sq, snb, off, orig, oldLength, e0, e1, int(w), V, i0, rm, keep, src, old(seq(data)), pend, out, pos, seq(data), pend1, out1, 64*out1+int(nbits1))
	return pend1, nbits1, out1
}

// selectMask is RemapMaskOrderedWithOffset with negate false. Word ws of sels
// selects the rows 64*ws+offset+b at its set bits b: one unaligned load reads
// them and appendKept packs them onto the output. Zero words are skipped.
// @ requires n.Inv() && acc(sels.Inv(), 1/2) && 0 <= offset
// @ ensures n.Inv() && acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ ensures n.Len() == (old(n.Len()) < int64(old(sels.Count())) ? old(n.Len()) : int64(old(sels.Count())))
// @ ensures forall r int :: {sels.Has(r)} sels.Has(r) && sels.CntBelow(r) < int(n.Len()) ==> n.Has(sels.CntBelow(r)) == old(n.Has(r+int(offset)))
// @ decreases
func (n *Bitmap) selectMask(sels *Bitmap, offset uint64) {
	oldLength := n.Len()
	L := oldLength
	// @ sels.CountIsRows()
	if c := int64(sels.Count()); c < L {
		L = c
	}
	wordsL := int((L + 63) / 64)
	// @ ghost off := int(offset)
	// @ ghost orig := n.ISeq()
	// @ n.tailFact()
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == old(n.Has(r))
	// @ ghost sq := sels.ISeq()
	// @ ghost snb := sels.Len()
	// @ sels.rowFacts()
	// @ sels.tailFact()
	// @ cntBeyond(sq, snb, 64*len(sq))
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == orig && TailClear(orig, oldLength)
	// @ tailBits(orig, oldLength)
	var pend, nbits uint64
	out := 0
	ws := 0
	nw := int((sels.Len() + 63) / 64)
	// @ invariant acc(n) && n.mp != nil && n.shift == 0 && n.numBits == oldLength && len(n.data) == len(orig) && len(orig) == int((oldLength+63)/64)
	// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
	// @ invariant forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
	// @ invariant acc(sels.Inv(), 1/2) && sels.ISeq() == sq && sels.Len() == snb && nw == len(sq)
	// @ invariant 0 <= oldLength && int(oldLength) <= 64*len(orig)
	// @ invariant forall r int :: {bits.Bit(orig[r/64], r%64)} 0 <= r && r/64 < len(orig) && int(oldLength) <= r ==> !bits.Bit(orig[r/64], r%64)
	// @ invariant 0 <= snb && len(sq) == int((snb+63)/64) && TailClear(sq, snb)
	// @ invariant 0 <= ws && ws <= len(sq) && 0 <= out && out <= wordsL && wordsL <= len(orig) && int(L) <= 64*wordsL
	// @ invariant 0 <= nbits && nbits < 64 && forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
	// @ invariant 64*out+int(nbits) == SumPc(sq, ws) && SumPc(sq, ws) <= 64*ws && CntS(sq, snb, 64*ws) == SumPc(sq, ws)
	// @ invariant forall x int :: {n.data[x]} out <= x && x < len(orig) ==> n.data[x] == orig[x]
	// @ invariant forall r int :: {HasS(sq, snb, r)} 0 <= r && r < 64*ws && HasS(sq, snb, r) ==>
	// @ 	0 <= CntS(sq, snb, r) && CntS(sq, snb, r) < 64*out+int(nbits) &&
	// @ 	OutB(seq(n.data), pend, out, CntS(sq, snb, r)) == HasS(orig, oldLength, r+off)
	// @ decreases len(sq) - ws
	for ; ws < nw && out < wordsL; ws++ {
		var keep uint64
		if /*@ unfolding acc(sels.Inv(), _) in unfolding acc(sels.Mem(), _) in @*/ sels.mp == nil {
			keep = sels.lword(ws)
		} else {
			keep = sels.word(ws)
		}
		// @ assert keep == sq[ws]
		if keep == 0 {
			// @ selectMaskSkip(sq, snb, orig, oldLength, off, ws, seq(n.data), pend, out, 64*out+int(nbits))
			continue
		}
		pend, nbits, out = selectMaskWord(n.data, keep, 64*uint64(ws)+offset, pend, nbits, out /*@, sq, snb, orig, oldLength, off, ws @*/)
		// @ assert forall r int :: {HasS(sq, snb, r)} 0 <= r && r < 64*(ws+1) && HasS(sq, snb, r) ==>
		// @ 	0 <= CntS(sq, snb, r) && CntS(sq, snb, r) < 64*out+int(nbits) &&
		// @ 	OutB(seq(n.data), pend, out, CntS(sq, snb, r)) == HasS(orig, oldLength, r+off)
		// @ carrySelectMask(sq, snb, orig, oldLength, off, ws, seq(n.data), pend, out, 64*out+int(nbits))
	}

	// @ assert 64*out+int(nbits) >= int(L)
	// @ ghost s2 := seq(n.data)
	// @ assert s2 == seq(n.data)
	if nbits != 0 && out < wordsL {
		n.data[out] = pend
		// @ assert seq(n.data) == s2[out = pend]
	}
	// @ assert !(nbits != 0 && out < wordsL) ==> seq(n.data) == s2
	// @ ghost s3 := seq(n.data)
	// @ assert s3 == seq(n.data)
	// @ flushPending(s2, pend, out, int(nbits), wordsL, s3, int(L))
	// Rows at or beyond oldLength are already clear; clearing them again
	// makes that evident without tracking it through the loop.
	if r := uint64(oldLength) % 64; r != 0 {
		n.data[len(n.data)-1] &= ^uint64(0) >> (64 - r)
		// @ assert seq(n.data) == s3[len(n.data)-1 = s3[len(n.data)-1]&(^uint64(0)>>(64-uint64(oldLength%64)))]
	}
	// @ assert oldLength%64 == 0 ==> seq(n.data) == s3
	// @ ghost s4 := seq(n.data)
	// @ maskTail(s3, s4, oldLength, int(L))
	// @ chainOut(s2, pend, out, s3, s4, int(L))
	// @ selectMaskPost(sq, snb, orig, oldLength, off, ws, s2, pend, out, 64*out+int(nbits), s4, int(L))
	// @ fold n.Mem()
	n.finishOrderedRemap(L, L)
	// @ sels.rowFacts()

}

// removeMask is RemapMaskOrderedWithOffset with negate true: removeOrdered
// with the removed rows of word w read from sels at bit 64*w-offset.
// @ requires n.Inv() && acc(sels.Inv(), 1/2) && 0 <= offset
// @ ensures n.Inv() && acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ ensures n.Len() == old(n.Len())
// @ ensures forall s int :: {old(n.Has(s))} 0 <= s && s < int(old(n.Len())) && !(s >= int(offset) && sels.Has(s-int(offset))) ==>
// @ 	n.Has(s-RankOff(sels.ISeq(), sels.Len(), int(offset), s)) == old(n.Has(s))
// @ ensures forall i int :: {n.Has(i)} int(old(n.Len()))-RankOff(sels.ISeq(), sels.Len(), int(offset), int(old(n.Len()))) <= i ==> !n.Has(i)
// @ decreases
func (n *Bitmap) removeMask(sels *Bitmap, offset uint64) {
	oldLength := n.Len()
	words := (oldLength + 63) / 64
	// @ ghost off := int(offset)
	// @ ghost orig := n.ISeq()
	// @ assert forall r int :: {HasS(orig, oldLength, r)} HasS(orig, oldLength, r) == old(n.Has(r))
	// @ ghost sq := sels.ISeq()
	// @ ghost snb := sels.Len()
	// @ sels.rowFacts()
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == orig && TailClear(orig, oldLength)
	var pend, nbits uint64
	out := 0
	w := int64(0)
	// @ ghost E := 0
	// @ invariant acc(n) && n.mp != nil && n.shift == 0 && n.numBits == oldLength && len(n.data) == len(orig) && len(orig) == int(words)
	// @ invariant forall i int :: {&n.data[i]} 0 <= i && i < len(n.data) ==> acc(&n.data[i])
	// @ invariant forall i int :: {&n.data[:cap(n.data)][i]} len(n.data) <= i && i < cap(n.data) ==> acc(&n.data[:cap(n.data)][i])
	// @ invariant acc(sels.Inv(), 1/2) && sels.ISeq() == sq && sels.Len() == snb
	// @ invariant 0 <= w && w <= words && 0 <= snb && len(sq) == int((snb+63)/64)
	// @ invariant 0 <= nbits && nbits < 64 && 0 <= out && out <= int(w)
	// @ invariant forall k int :: {bits.Bit(pend, k)} int(nbits) <= k && k < 64 ==> !bits.Bit(pend, k)
	// @ invariant E == int(64*w < oldLength ? 64*w : oldLength)
	// @ invariant 64*out+int(nbits) == E-RankOff(sq, snb, off, E)
	// @ invariant forall x int :: {n.data[x]} out <= x && x < len(orig) ==> n.data[x] == orig[x]
	// @ invariant 0 <= out && out <= len(n.data) && RemoveMaskRel(sq, snb, off, orig, oldLength, E, seq(n.data), pend, out, 64*out+int(nbits))
	// @ decreases words - w
	for w < words {
		// @ assert 64*w < oldLength && out <= len(n.data)
		// @ ghost e1 := int(64*(w+1) < oldLength ? 64*(w+1) : oldLength)
		pend, nbits, out = removeMaskWord(n.data, sels, w, offset, oldLength, pend, nbits, out /*@, sq, snb, orig, off, E, e1 @*/)
		w++
		// @ E = e1
	}

	// @ assert 64*out+int(nbits) == int(oldLength)-RankOff(sq, snb, off, int(oldLength)) && int(oldLength) <= 64*len(n.data)
	// @ ghost if int(oldLength) >= off {
	// @ 	cntMono(sq, snb, 0, int(oldLength)-off)
	// @ }
	written := 64*int64(out) + int64(nbits)
	// @ ghost s2 := seq(n.data)
	if nbits != 0 {
		n.data[out] = pend
		// @ assert seq(n.data) == s2[out = pend]
	}
	// @ assert forall p int :: {bits.Bit(n.data[p/64], p%64)} 0 <= p && int64(p) < written ==> bits.Bit(n.data[p/64], p%64) == OutB(s2, pend, out, p)
	// @ ghost lw := len(n.data) - 1
	// @ ghost if lw >= 0 && oldLength%64 != 0 {
	// @ 	if lw >= out {
	// @ 		assert lw == out ==> nbits == 0 || int(oldLength%64) >= int(nbits)
	// @ 		assert forall k int :: {bits.Bit(n.data[lw], k)} int(oldLength%64) <= k && k < 64 ==> !bits.Bit(n.data[lw], k)
	// @ 	}
	// @ }
	// @ fold n.Mem()
	n.finishOrderedRemap(written, oldLength)
	// @ sels.rowFacts()
	// @ assert forall s int :: {old(n.Has(s))} 0 <= s && s < int(oldLength) && !(s >= off && sels.Has(s-off)) ==>
	// @ 	HasS(orig, oldLength, s) == old(n.Has(s)) && s-RankOff(sq, snb, off, s) < int(written)
}
