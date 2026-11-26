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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// gatherRows is the one row-list gather of the vector package, behind Union;
// ShrinkByRows uses its value loop in place.
func gatherRows(v, w *Vector, sels []int64, mp *mpool.MPool) error {
	if len(sels) == 0 {
		return nil
	}
	if v.dataAliases(w) {
		// Keep source row indices unchanged, including reordered selections.
		return v.withClonedSource(w, 0, w.length, mp, func(source *Vector) error {
			return gatherRows(v, source, sels, mp)
		})
	}
	// Classify the source cheaply before considering a reused destination.
	// Metadata-bearing, nullable and constant sources keep the ordinary path.
	if cap(v.data) > 0 && w.class == FLAT && w.nsp.IsEmpty() {
		width := v.typ.TypeSize()
		if (width == 4 || width == 8) && v.class == FLAT && v.typ == w.typ &&
			len(sels) <= cap(v.data)/width-v.length && v.nsp.IsEmpty() {
			oldLen := v.length
			v.data = v.data[:cap(v.data)]
			if width == 8 {
				unionFixedSelected[uint64](v.data[oldLen*8:(oldLen+len(sels))*8], w.data, sels)
			} else {
				unionFixedSelected[uint32](v.data[oldLen*4:(oldLen+len(sels))*4], w.data, sels)
			}
			v.length = oldLen + len(sels)
			return nil
		}
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}

	srcNulls := w.IsConstNull() || !w.nsp.IsEmpty()
	if err := extendWithBitmaps(v, len(sels), mp, srcNulls); err != nil {
		return err
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + len(sels))
	if w.IsConst() {
		if w.IsConstNull() {
			v.nsp.AddRange(uint64(oldLen), uint64(oldLen+len(sels)))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			var ws []types.Varlena
			ToSliceNoTypeCheck(w, &ws)
			err = BuildVarlenaFromVarlena(v, &va, &ws[0], &w.area, mp)
			if err != nil {
				return err
			}
			var col []types.Varlena
			ToSliceNoTypeCheck(v, &col)
			fillSlice(col, oldLen, v.length, va)
		} else {
			tlen := v.GetType().TypeSize()
			dst := v.data[oldLen*tlen : v.length*tlen]
			copy(dst[:tlen], w.data[:tlen])
			broadcastFixed(dst, tlen)
		}

		return nil
	}

	// NULLs first, a word of output rows at a time. The value loops below
	// then read the destination bitmap, which is in cache, to skip NULL rows
	// where a value must not be read.
	// nullWords are the destination's NULL words, written just above; a set
	// bit for row oldLen+i means row i is skipped.
	var nullWords []uint64
	if srcNulls {
		if err := gatherNulls(&v.nsp, oldLen, &w.nsp, sels); err != nil {
			return err
		}
		nullWords = v.nsp.GetBitmap().OwnedWords()
	}

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena
		ToSliceNoTypeCheck(v, &vCol)
		ToSliceNoTypeCheck(w, &wCol)
		// pre-grow the area once for the selected non-inline, non-null rows so the
		// per-row BuildVarlenaNoInline appends below never realloc. Null rows are NOT
		// copied (the loops below skip them), and a reused vector can retain
		// a stale non-inline header in a null slot — counting those would reserve area
		// for dead payload (large needless mp.Grow / alloc failure), so exclude them.
		total := 0
		// With both areas empty, valid non-NULL descriptors are either inline
		// or empty non-inline values at offset zero. Neither needs area growth
		// or rebasing, so copy them directly without a separate sizing pass.
		emptyAreas := !srcNulls && len(w.area) == 0 && len(v.area) == 0
		if !emptyAreas {
			for i, sel := range sels {
				if p := oldLen + i; srcNulls && nullWords[p>>6]>>(p&63)&1 != 0 {
					continue
				}
				if !wCol[sel].IsSmall() {
					_, l := wCol[sel].OffsetLen()
					total += int(l)
				}
			}
			if err = pregrowVarlenaArea(v, total, mp); err != nil {
				return err
			}
		}
		// Reuse the area sizing pass to select an inline-copy batch. The small
		// check still handles non-inline zero-length descriptors correctly.
		// Mixed/area payloads keep the original loops.
		if emptyAreas {
			target := vCol[oldLen : oldLen+len(sels)]
			for i, sel := range sels {
				target[i] = wCol[sel]
			}
		} else if total == 0 {
			for i, sel := range sels {
				if p := oldLen + i; srcNulls && nullWords[p>>6]>>(p&63)&1 != 0 {
					continue
				}
				if wCol[sel].IsSmall() {
					vCol[oldLen+i] = wCol[sel]
				} else if err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp); err != nil {
					return err
				}
			}
		} else {
			for i, sel := range sels {
				if p := oldLen + i; srcNulls && nullWords[p>>6]>>(p&63)&1 != 0 {
					continue
				}
				err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp)
				if err != nil {
					return err
				}
			}
		}
		// Selection materialization copies every non-inline value into a fresh
		// destination range. It cannot introduce aliases for a non-const source,
		// even when source rows repeat or arrive out of order.
		return nil
	}

	// Fixed width: copy every value, NULL rows included. A NULL row's value
	// is never read, so copying it is harmless and keeps the loop branch-free.
	// Cases are payload widths in bytes; len(sels) is the output row count.
	gatherFixedValues(v, w, oldLen, sels)
	return nil
}

// gatherFixedValues copies the fixed-width values of w's rows sels to v's
// rows oldLen, oldLen+1, ... One typed loop per width serves every type.
// v and w may be the same vector when sels is ascending and oldLen is 0:
// each row is then read before it is overwritten.
func gatherFixedValues(v, w *Vector, oldLen int, sels []int64) {
	switch tlen := v.GetType().TypeSize(); tlen {
	case 8:
		unionFixedSelected[uint64](v.data[oldLen*8:(oldLen+len(sels))*8], w.data, sels)
	case 4:
		unionFixedSelected[uint32](v.data[oldLen*4:(oldLen+len(sels))*4], w.data, sels)
	case 2:
		unionFixedSelected[uint16](v.data[oldLen*2:(oldLen+len(sels))*2], w.data, sels)
	case 1:
		unionFixedSelected[uint8](v.data[oldLen:oldLen+len(sels)], w.data, sels)
	default:
		unionWideFixedSelected(v, w, oldLen, sels, tlen)
	}
}

// gatherNulls sets dst row base+i when src row rows[i] is NULL. It builds
// each output word in a register, 64 rows at a time, and publishes it with
// one OrWord, instead of a Contains and an Add per row.
func gatherNulls(dst *nulls.Nulls, base int, src *nulls.Nulls, rows []int64) error {
	out := dst.GetBitmap()
	if err := out.TryExpandWithSize(base + len(rows)); err != nil {
		return err
	}
	in := newNullRowReader(src)
	words := in.words
	if words == nil {
		// A read-only source bitmap: read through Word.
		for i, r := range rows {
			if in.bit(int(r)) != 0 {
				out.Add(uint64(base + i))
			}
		}
		return nil
	}
	nw := uint64(len(words))
	for i := 0; i < len(rows); {
		p := base + i
		lo := p & 63
		chunk := rows[i:min(len(rows), i+64-lo)]
		var acc uint64
		for k, r := range chunk {
			if w := uint64(r) >> 6; w < nw {
				acc |= (words[w] >> (uint64(r) & 63) & 1) << (uint(lo+k) & 63)
			}
		}
		if acc != 0 {
			out.OrWord(p>>6, acc)
		}
		i += len(chunk)
	}
	return nil
}

// nullRowReader reads single NULL bits straight from a bitmap's words. A
// read-only bitmap (a view) is read through Word.
type nullRowReader struct {
	np    *bitmap.Bitmap
	words []uint64
	n     int
}

func newNullRowReader(nsp *nulls.Nulls) nullRowReader {
	np := nsp.GetBitmap()
	n := np.NumWords()
	words := np.OwnedWords()
	if len(words) < n {
		words = nil
	} else {
		words = words[:n]
	}
	return nullRowReader{np: np, words: words, n: n}
}

// bit returns 1 when row is NULL and 0 otherwise. Rows past the bitmap read
// as not NULL.
func (r *nullRowReader) bit(row int) uint64 {
	w := row >> 6
	if w >= r.n {
		return 0
	}
	if r.words != nil {
		return r.words[w] >> (uint(row) & 63) & 1
	}
	return r.np.Word(w) >> (uint(row) & 63) & 1
}
