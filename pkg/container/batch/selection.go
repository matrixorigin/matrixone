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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// A selected batch carries a row selection shared by all of its columns: a
// bitmap over the vectors' physical rows. Its vectors keep their physical
// rows; the logical rows are the set bits in ascending order, and RowCount()
// is the number of set bits. A bitmap cannot express a reordering or a
// repeated row, so a selection always preserves row order (and any sorted
// property of the vectors).
//
// Only an operator that declares it accepts a selection on that input edge
// receives one (see vm.SelectionConsumer); every other consumer, codec and
// remote boundary gets compacted data. A selected batch borrows its vectors
// and its bitmap from the producer, so it is valid until the producer's next
// Call, the same as any other operator output.

// SetSelection publishes the set bits of sel as this batch's logical rows over
// its physical vectors. sel must have length physicalRows and must not be
// modified while the batch is published; the producer keeps ownership.
func (bat *Batch) SetSelection(physicalRows int, sel *bitmap.Bitmap) {
	bat.selection = sel
	bat.physicalRows = physicalRows
	bat.rowCount = sel.Count()
}

// ClearSelection drops the selection and restores the physical row count.
func (bat *Batch) ClearSelection() {
	if bat.selection == nil {
		return
	}
	bat.rowCount = bat.physicalRows
	bat.selection = nil
	bat.physicalRows = 0
}

// HasSelection reports whether the batch's logical rows are a selection of
// its physical rows.
func (bat *Batch) HasSelection() bool {
	return bat != nil && bat.selection != nil
}

// Selection returns the selection bitmap, or nil when the batch is not
// selected. The caller must not modify it.
func (bat *Batch) Selection() *bitmap.Bitmap {
	if bat == nil {
		return nil
	}
	return bat.selection
}

// SelectionRows appends the selected physical rows, ascending, to dst[:0].
// It returns nil when the batch is not selected.
func (bat *Batch) SelectionRows(dst []int64) []int64 {
	if bat.selection == nil {
		return nil
	}
	return bitmap.AppendRows(bat.selection, dst[:0])
}

// PhysicalRowCount returns the number of rows in the batch's vectors.
func (bat *Batch) PhysicalRowCount() int {
	if bat.selection != nil {
		return bat.physicalRows
	}
	return bat.rowCount
}

// PhysicalView sets dst to an unselected header over bat's vectors with the
// physical row count, for evaluating expressions on the physical rows. dst
// borrows bat's vectors and must not be cleaned.
func (bat *Batch) PhysicalView(dst *Batch) {
	dst.Recursive = bat.Recursive
	dst.ShuffleIDX = bat.ShuffleIDX
	dst.Attrs = bat.Attrs
	dst.Vecs = bat.Vecs
	dst.ExtraBuf = bat.ExtraBuf
	dst.selection = nil
	dst.physicalRows = 0
	dst.rowCount = bat.PhysicalRowCount()
}

// CompactInto copies the selected rows of bat into dst, which must be an
// owned batch with the same column layout (its vectors are reset and
// refilled). An unselected bat is copied whole.
func (bat *Batch) CompactInto(dst *Batch, mp *mpool.MPool) error {
	if len(dst.Vecs) != len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("compact batch column count mismatch")
	}
	dst.CleanOnlyData()
	if bat.selection == nil {
		for i, vec := range bat.Vecs {
			if vec == nil {
				continue
			}
			if err := dst.Vecs[i].UnionBatch(vec, 0, vec.Length(), nil, mp); err != nil {
				return err
			}
		}
	} else {
		sels := bat.SelectionRows(nil)
		for i, vec := range bat.Vecs {
			if vec == nil {
				continue
			}
			if err := dst.Vecs[i].Union(vec, sels, mp); err != nil {
				return err
			}
		}
	}
	dst.Attrs = append(dst.Attrs[:0], bat.Attrs...)
	dst.Recursive, dst.ShuffleIDX = bat.Recursive, bat.ShuffleIDX
	dst.SetRowCount(bat.rowCount)
	return nil
}

// SelectionMask sets mask[physical] to true for every selected row and
// returns it sized to the physical row count; nil means every row.
func (bat *Batch) SelectionMask(mask []bool) []bool {
	if bat.selection == nil {
		return nil
	}
	return SelectionMaskOf(bat.selection, bat.physicalRows, mask)
}

// SelectionMaskOf expands sel into a per-row mask of length physicalRows.
func SelectionMaskOf(sel *bitmap.Bitmap, physicalRows int, mask []bool) []bool {
	return sel.ExpandBools(mask, physicalRows)
}

func errSelectedBatch() error {
	return moerr.NewInternalErrorNoCtx("selected batch reached a boundary that requires compacted rows")
}
