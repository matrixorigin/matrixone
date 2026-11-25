// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBatchSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := int64(0); i < 6; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], i*10, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte('a' + i)}, i == 4, mp))
	}
	bat.SetRowCount(6)
	require.False(t, bat.HasSelection())
	require.Nil(t, bat.SelectionMask(nil))

	var selection bitmap.Bitmap

	selection.Init(bitmapTestMP)
	mustBitmapOK(selection.InitWithSize(6, bitmapTestMP))
	selection.AddMany([]uint64{1, 4, 5})
	bat.SetSelection(&selection)
	require.Equal(t, []int64{1, 4, 5}, bat.SelectionRows(nil))
	require.True(t, bat.HasSelection())
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, 6, bat.PhysicalRowCount())
	require.Equal(t, []bool{false, true, false, false, true, true}, bat.SelectionMask(nil))

	var view Batch
	bat.PhysicalView(&view)
	require.False(t, view.HasSelection())
	require.Equal(t, 6, view.RowCount())

	// A selected batch never reaches a codec.
	var w bytes.Buffer
	require.Error(t, bat.MarshalBinaryTo(&w))
	_, err := bat.MarshalBinarySize()
	require.Error(t, err)

	compact := NewWithSize(2)
	compact.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	compact.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	// Compacting without and with a row scratch gives the same rows; the
	// scratch holds the selected rows and is reused by the next call.
	var rows []int64
	for _, scratch := range []*[]int64{nil, &rows, &rows} {
		require.NoError(t, bat.CompactInto(compact, mp, scratch))
		require.False(t, compact.HasSelection())
		require.Equal(t, 3, compact.RowCount())
		require.Equal(t, []int64{10, 40, 50}, vector.MustFixedColNoTypeCheck[int64](compact.Vecs[0]))
		require.Equal(t, "b", compact.Vecs[1].GetStringAt(0))
		require.True(t, compact.Vecs[1].IsNull(1))
		require.Equal(t, "f", compact.Vecs[1].GetStringAt(2))
	}
	require.Equal(t, bat.SelectionRows(nil), rows)
	reused := &rows[0]
	require.NoError(t, bat.CompactInto(compact, mp, &rows))
	require.Same(t, reused, &rows[0], "the row scratch is reused")

	// Any explicit row count replaces the selection.
	bat.ClearSelection()
	require.Equal(t, 6, bat.RowCount())
	bat.SetSelection(&selection)
	bat.SetRowCount(6)
	require.False(t, bat.HasSelection())

	compact.Clean(mp)
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// A batch's selection is owned or borrowed, like its vectors: OwnSelection
// fills the batch's own storage, a forwarded or viewed selection borrows the
// input's bitmap, and Clean releases only the batch's own storage.
func TestBatchSelectionOwnedOrBorrowed(t *testing.T) {
	mp := mpool.MustNewZero()
	producer := NewWithSize(0)
	own, err := producer.OwnSelection(mp, 6)
	require.NoError(t, err)
	require.Same(t, &producer.ownSel, own)
	require.Equal(t, int64(6), own.Len())
	require.Zero(t, own.Count())
	own.AddMany([]uint64{0, 3})
	producer.SetSelection(own)
	require.Same(t, &producer.ownSel, producer.Selection())
	require.Equal(t, 2, producer.RowCount())
	require.Equal(t, 6, producer.PhysicalRowCount())

	// A forwarder borrows: no copy, the same bitmap.
	forwarder := NewWithSize(0)
	forwarder.SetSelection(producer.Selection())
	require.Same(t, producer.Selection(), forwarder.Selection())
	require.NotSame(t, &forwarder.ownSel, forwarder.Selection())

	// A view borrows the parent's selection; a physical view has none.
	var view Batch
	producer.ViewInto(&view)
	require.Same(t, producer.Selection(), view.Selection())
	var physical Batch
	producer.PhysicalView(&physical)
	require.False(t, physical.HasSelection())

	// A narrower copies into its own storage and never touches the input's.
	narrower := NewWithSize(0)
	narrowed, err := narrower.OwnSelection(mp, 0)
	require.NoError(t, err)
	require.NoError(t, narrowed.InitWith(forwarder.Selection(), mp))
	narrowed.Remove(3)
	narrower.SetSelection(narrowed)
	require.Equal(t, []int64{0}, narrower.SelectionRows(nil))
	require.Equal(t, []int64{0, 3}, producer.SelectionRows(nil))

	// Re-initializing the own storage for the next batch drops the published
	// selection first.
	again, err := producer.OwnSelection(mp, 4)
	require.NoError(t, err)
	require.False(t, producer.HasSelection())
	require.Equal(t, 6, producer.RowCount(), "the physical row count comes back")
	require.Equal(t, int64(4), again.Len())

	// Cleaning a borrower leaves the lender's selection alone; cleaning the
	// owner releases its storage to the pool it came from.
	forwarder.Clean(mp)
	require.Equal(t, int64(4), producer.ownSel.Len())
	producer.Clean(mp)
	require.False(t, producer.HasSelection())
	require.Zero(t, producer.ownSel.Len())
	narrower.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// OwnSelection reuses its storage across calls with the same pool, moves to a
// new pool by releasing the old words, and FreeSelection returns them for a
// batch that is never Cleaned.
func TestBatchOwnSelectionPool(t *testing.T) {
	mp1 := mpool.MustNewZero()
	mp2 := mpool.MustNewZero()
	var view Batch
	sel, err := view.OwnSelection(mp1, 100)
	require.NoError(t, err)
	sel.Add(7)
	view.SetSelection(sel)
	require.Positive(t, mp1.CurrNB())

	sel, err = view.OwnSelection(mp2, 100)
	require.NoError(t, err)
	require.False(t, view.HasSelection())
	require.Zero(t, sel.Count())
	require.Zero(t, mp1.CurrNB(), "a pool change frees the old words")
	require.Same(t, mp2, sel.MPool())
	view.SetSelection(sel)

	view.FreeSelection()
	require.False(t, view.HasSelection())
	require.Zero(t, mp2.CurrNB())
	view.FreeSelection() // idempotent
}

// Every release of a batch that may own selection storage returns its words:
// FreeWindow and CleanOwned as well as Clean. A spill view never carries the
// storage, so it cannot free it.
func TestBatchReleasesFreeOwnSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	for name, release := range map[string]func(*Batch){
		"Clean":      func(b *Batch) { b.Clean(mp) },
		"FreeWindow": func(b *Batch) { b.FreeWindow() },
		"CleanOwned": func(b *Batch) { b.CleanOwned() },
	} {
		t.Run(name, func(t *testing.T) {
			bat := NewWithSize(0)
			sel, err := bat.OwnSelection(mp, 128)
			require.NoError(t, err)
			sel.Add(3)
			bat.SetSelection(sel)
			require.True(t, bat.HoldsSelectionMemory())

			view := bat.spillView()
			require.False(t, view.HoldsSelectionMemory())
			require.Same(t, bat.Selection(), view.Selection())

			release(bat)
			require.False(t, bat.HasSelection())
			require.False(t, bat.HoldsSelectionMemory())
			require.Zero(t, mp.CurrNB())
		})
	}
}

// Operations that read or rewrite physical rows as the logical rows reject a
// selected batch on either side and leave it unchanged: the selection would
// go stale or the row count would include rows it excludes. One-batch
// operations still work on an unselected batch.
func TestBatchRowOpsRejectSelectedBatch(t *testing.T) {
	ctx := context.Background()
	ops := map[string]func(dst, src *Batch, mask *bitmap.Bitmap, mp *mpool.MPool) error{
		"Shrink": func(dst, _ *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return dst.Shrink([]int64{0}, false, mp) },
		"ShrinkByMask": func(dst, _ *Batch, mask *bitmap.Bitmap, mp *mpool.MPool) error {
			return dst.ShrinkByMask(mask, false, 0, mp)
		},
		"Shuffle":     func(dst, _ *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return dst.Shuffle([]int64{1, 0}, mp) },
		"Union":       func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return dst.Union(src, []int64{0}, mp) },
		"UnionWindow": func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return dst.UnionWindow(src, 0, 2, mp) },
		"UnionOne":    func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return dst.UnionOne(src, 0, mp) },
		"CloneTo":     func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error { return src.CloneTo(dst, mp) },
		"CloneSelectedColumnsTo": func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error {
			return src.CloneSelectedColumnsTo([]int{0}, dst, mp)
		},
		"Append": func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error {
			_, err := dst.Append(ctx, mp, src)
			return err
		},
		"AppendWithCopy": func(dst, src *Batch, _ *bitmap.Bitmap, mp *mpool.MPool) error {
			_, err := dst.AppendWithCopy(ctx, mp, src)
			return err
		},
	}
	oneBatch := map[string]bool{"Shrink": true, "ShrinkByMask": true, "Shuffle": true}
	for name, op := range ops {
		for _, side := range []string{"dst", "src"} {
			t.Run(name+"/"+side, func(t *testing.T) {
				// Each case has its own pool, so a failure cannot leak into
				// the next case's accounting.
				mp := mpool.MustNewZero()
				newBatch := func(selected bool) *Batch {
					bat := NewWithSize(1)
					bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
					require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{10, 20, 30, 40}, nil, mp))
					bat.SetRowCount(4)
					if selected {
						sel, err := bat.OwnSelection(mp, 4)
						require.NoError(t, err)
						sel.AddMany([]uint64{1, 3})
						bat.SetSelection(sel)
					}
					return bat
				}
				var mask bitmap.Bitmap
				mask.Init(mp)
				require.NoError(t, mask.InitWithSize(4, mp))
				mask.Add(1)
				dst, src := newBatch(side == "dst"), newBatch(side == "src")
				defer func() {
					dst.Clean(mp)
					src.Clean(mp)
					mask.Free()
					require.Zero(t, mp.CurrNB())
				}()

				err := op(dst, src, &mask, mp)
				if oneBatch[name] && side == "src" {
					// A one-batch operation only reads its unselected receiver.
					require.NoError(t, err)
					return
				}
				selected := dst
				if side == "src" {
					selected = src
				}
				require.Error(t, err)
				require.Contains(t, err.Error(), "selected batch requires compacted rows")
				require.True(t, selected.HasSelection())
				require.Equal(t, 2, selected.RowCount())
				require.Equal(t, 4, selected.Vecs[0].Length())
				require.Equal(t, []int64{1, 3}, selected.SelectionRows(nil))
			})
		}
	}
}
