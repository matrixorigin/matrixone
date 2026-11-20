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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// A selected batch carries a row selection shared by all of its columns.
// Its vectors keep their physical rows; logical row i is physical row
// Selection()[i] of every vector, and RowCount() is len(Selection()).
//
// Only an operator that declares it accepts a selection on that input edge
// receives one (see vm.SelectionConsumer); every other consumer, codec and
// remote boundary gets compacted data. A selected batch borrows its vectors
// and its selection from the producer, so it is valid until the producer's
// next Call, the same as any other operator output.

// SetSelection publishes sels as this batch's logical rows over its physical
// vectors. sels must be strictly increasing physical row numbers. The batch
// borrows sels; the producer keeps ownership.
func (bat *Batch) SetSelection(physicalRows int, sels []int32) {
	bat.selection = sels
	bat.physicalRows = physicalRows
	bat.rowCount = len(sels)
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

// Selection returns the physical row of every logical row, or nil when the
// batch is not selected.
func (bat *Batch) Selection() []int32 {
	if bat == nil {
		return nil
	}
	return bat.selection
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
		sels := make([]int64, len(bat.selection))
		for i, row := range bat.selection {
			sels[i] = int64(row)
		}
		for i, vec := range bat.Vecs {
			if vec == nil {
				continue
			}
			if err := dst.Vecs[i].Union(vec, sels, mp); err != nil {
				return err
			}
		}
	}
	for i, vec := range bat.Vecs {
		if vec != nil {
			vec.CopyExpressionMetadataTo(dst.Vecs[i])
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
	n := bat.physicalRows
	if cap(mask) < n {
		mask = make([]bool, n)
	} else {
		mask = mask[:n]
		clear(mask)
	}
	for _, row := range bat.selection {
		mask[row] = true
	}
	return mask
}

func errSelectedBatch() error {
	return moerr.NewInternalErrorNoCtx("selected batch reached a boundary that requires compacted rows")
}
