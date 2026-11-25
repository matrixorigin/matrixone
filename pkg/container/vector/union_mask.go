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

package vector

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// UnionByMask appends to v, in order, every row r of w that is set in mask.
// Rows of mask at or beyond w.Length() are ignored. It is the row-selection
// form of Union: a selection is a bitmap, and no row list is built. w may be
// v itself or a window of it; mask is only read.
func (v *Vector) UnionByMask(w *Vector, mask *bitmap.Bitmap, mp *mpool.MPool) error {
	rows := w.Length()
	cnt := mask.CountRange(0, uint64(rows))
	if cnt == 0 {
		return nil
	}
	if cnt == rows || w.IsConst() {
		// Every row, or copies of one constant: the contiguous path.
		return v.UnionBatch(w, 0, cnt, nil, mp)
	}
	// As in UnionBatch: growing v may release storage an aliased w reads.
	if v.dataAliases(w) || v.typ.IsVarlen() && byteSlicesOverlap(v.area, w.area) {
		return v.withClonedSource(w, 0, rows, mp, func(source *Vector) error {
			return v.UnionByMask(source, mask, mp)
		})
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	hasNull := !w.nsp.IsEmpty()
	if err := extendWithBitmaps(v, cnt, mp, hasNull); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		return v.unionVarlenByMask(w, mask, rows, cnt, hasNull, mp)
	}
	oldLen := v.length
	switch v.typ.TypeSize() {
	case 1:
		gatherFixedByMask[uint8](v, w, mask, rows, oldLen, cnt)
	case 2:
		gatherFixedByMask[uint16](v, w, mask, rows, oldLen, cnt)
	case 4:
		gatherFixedByMask[uint32](v, w, mask, rows, oldLen, cnt)
	case 8:
		gatherFixedByMask[uint64](v, w, mask, rows, oldLen, cnt)
	case 16:
		gatherFixedByMask[types.Decimal128](v, w, mask, rows, oldLen, cnt)
	default:
		tlen := v.typ.TypeSize()
		out := oldLen
		forSelectedRows(mask, rows, func(r int) {
			copy(v.data[out*tlen:(out+1)*tlen], w.data[r*tlen:(r+1)*tlen])
			out++
		})
	}
	if hasNull {
		unionNullsByMask(v, w, mask, rows, oldLen)
	}
	v.setLengthAfterExtend(oldLen + cnt)
	return nil
}

// gatherFixedByMask copies the selected rows of w into v starting at row
// out. NULL slots are copied too; their bitmap, not their bytes, is visible.
func gatherFixedByMask[T any](v, w *Vector, mask *bitmap.Bitmap, rows, out, cnt int) {
	dst := toSliceOfLengthNoTypeCheck[T](v, out+cnt)
	src := toSliceOfLengthNoTypeCheck[T](w, rows)
	words := maskWords(mask, rows)
	mr := newMaskReader(mask)
	for i := 0; i < words; i++ {
		word := mr.word(i)
		if i == words-1 && rows%64 != 0 {
			word &= ^uint64(0) >> uint(64-rows%64)
		}
		base := 64 * i
		if word == ^uint64(0) {
			out += copy(dst[out:out+64], src[base:base+64])
			continue
		}
		for word != 0 {
			dst[out] = src[base+bits.TrailingZeros64(word)]
			out++
			word &= word - 1
		}
	}
}

// maskReader reads a selection a word at a time: straight from the words of
// an owned bitmap, through Word for a read-only view.
type maskReader struct {
	mask  *bitmap.Bitmap
	words []uint64
}

func newMaskReader(mask *bitmap.Bitmap) maskReader {
	words := mask.OwnedWords()
	if len(words) < mask.NumWords() {
		// Not the shape the invariant promises; read through Word.
		words = nil
	}
	return maskReader{mask: mask, words: words}
}

// word returns rows 64i..64i+63 of the mask, as mask.Word(i) does, for
// i < mask.NumWords().
func (r *maskReader) word(i int) uint64 {
	if r.words != nil {
		return r.words[i]
	}
	return r.mask.Word(i)
}

// maskWords is the number of words of mask that cover rows [0, rows).
func maskWords(mask *bitmap.Bitmap, rows int) int {
	words := (rows + 63) / 64
	if w := mask.NumWords(); w < words {
		words = w
	}
	return words
}

// forSelectedRows calls visit, in order, for every row below rows set in
// mask, a word at a time.
func forSelectedRows(mask *bitmap.Bitmap, rows int, visit func(r int)) {
	words := maskWords(mask, rows)
	mr := newMaskReader(mask)
	for i := 0; i < words; i++ {
		word := mr.word(i)
		if i == words-1 && rows%64 != 0 {
			word &= ^uint64(0) >> uint(64-rows%64)
		}
		base := 64 * i
		for word != 0 {
			visit(base + bits.TrailingZeros64(word))
			word &= word - 1
		}
	}
}

// nullWords reads the words of a vector's NULL bitmap, which may cover
// fewer rows than the vector: words past it read as zero.
type nullWords struct {
	r maskReader
	n int
}

func newNullWords(w *Vector) nullWords {
	if w.nsp.IsEmpty() {
		return nullWords{}
	}
	nsp := w.nsp.GetBitmap()
	return nullWords{r: newMaskReader(nsp), n: nsp.NumWords()}
}

func (z *nullWords) word(i int) uint64 {
	if i < z.n {
		return z.r.word(i)
	}
	return 0
}

// selectedWord is word i of mask limited to rows [0, rows).
func selectedWord(mr *maskReader, i, words, rows int) uint64 {
	word := mr.word(i)
	if i == words-1 && rows%64 != 0 {
		word &= ^uint64(0) >> uint(64-rows%64)
	}
	return word
}

// unionNullsByMask sets the NULL rows of v for the selected rows of w, which
// were appended starting at row out. Only the selected NULL rows are
// visited: such a row's destination is out plus the number of selected rows
// below it.
func unionNullsByMask(v, w *Vector, mask *bitmap.Bitmap, rows, out int) {
	mr := newMaskReader(mask)
	nw := newNullWords(w)
	words := maskWords(mask, rows)
	for i := 0; i < words; i++ {
		m := selectedWord(&mr, i, words, rows)
		for x := m & nw.word(i); x != 0; x &= x - 1 {
			t := uint(bits.TrailingZeros64(x))
			v.nsp.Add(uint64(out + bits.OnesCount64(m&(uint64(1)<<t-1))))
		}
		out += bits.OnesCount64(m)
	}
}

func (v *Vector) unionVarlenByMask(w *Vector, mask *bitmap.Bitmap, rows, cnt int, hasNull bool, mp *mpool.MPool) error {
	vCol := toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+cnt)
	var wCol []types.Varlena
	ToSliceNoTypeCheck(w, &wCol)
	mr := newMaskReader(mask)
	var nw nullWords
	if hasNull {
		nw = newNullWords(w)
	}
	words := maskWords(mask, rows)
	// Grow the area once for the selected non-NULL, non-inline values.
	total := 0
	for i := 0; i < words; i++ {
		base := 64 * i
		for m := selectedWord(&mr, i, words, rows) &^ nw.word(i); m != 0; m &= m - 1 {
			if s := &wCol[base+bits.TrailingZeros64(m)]; !s.IsSmall() {
				_, l := s.OffsetLen()
				total += int(l)
			}
		}
	}
	if err := pregrowVarlenaArea(v, total, mp); err != nil {
		return err
	}
	out := v.length
	for i := 0; i < words; i++ {
		base := 64 * i
		m := selectedWord(&mr, i, words, rows)
		nulls := m & nw.word(i)
		for ; m != 0; m &= m - 1 {
			t := uint(bits.TrailingZeros64(m))
			if nulls&(uint64(1)<<t) != 0 {
				// A NULL slot holds no payload; clear any stale descriptor.
				vCol[out] = types.Varlena{}
				v.nsp.Add(uint64(out))
			} else if src := &wCol[base+int(t)]; src.IsSmall() {
				BuildVarlenaInline(&vCol[out], src)
			} else {
				off, l := src.OffsetLen()
				payload := w.area[off : off+l]
				if err := buildVarlenaNoInline(v, &vCol[out], &payload, mp); err != nil {
					v.setLengthAfterExtend(out)
					return err
				}
			}
			out++
		}
	}
	v.setLengthAfterExtend(out)
	return nil
}
