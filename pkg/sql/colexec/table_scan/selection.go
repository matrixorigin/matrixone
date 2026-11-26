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

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectionState lets the scan publish filtered blocks without copying them.
// The reader keeps every column a read-only reference to the cache (pinned by
// the scan's output batch until its next CleanOnlyData) and records the rows
// the filter kept in rows. The scan's projection then runs on the physical
// rows, masked to the selection, and the result either keeps the selection,
// when the parent reads selected batches, or is compacted into compact, a
// batch owned by the scan's pool. Either way it is valid until the next Call.
type selectionState struct {
	// output is set by the parent's preparation (vm.SelectionProducer).
	output bool
	rows   bitmap.Bitmap
	// keep, pred and dep evaluate the reader filter's predicate chain over a
	// block: keep is the result over the block's rows, pred one predicate's
	// TRUE rows over the rows still kept, and dep the composition of the two.
	keep, pred, dep bitmap.Bitmap
	mask            []bool
	physical        batch.Batch
	compact         *batch.Batch
	// uses is the reader column policy for selected reads: the columns the
	// scan outputs are borrowed (ReaderColumnRetain) instead of copied.
	uses []engine.ReaderColumnUse
	// gather holds a selection decoded into row positions, for gathers of
	// several columns: one decode serves every column, where UnionByMask
	// scans the bitmap once per column. It is reused across blocks.
	gather []int64
}

// minRowListColumns is the number of columns from which a gather decodes
// the selection into a row list once instead of reading the mask per column.
const minRowListColumns = 2

// gatherRows decodes the rows set in sel into the scan's row list. It returns
// nil, and the caller gathers by mask, when sel has a row at or past rows.
// The list is valid until the next call.
func (s *selectionState) gatherRows(sel *bitmap.Bitmap, rows int) []int64 {
	if n := sel.Len(); n > int64(rows) && sel.CountRange(uint64(rows), uint64(n)) != 0 {
		return nil
	}
	s.gather = bitmap.AppendRows(sel, s.gather[:0])
	return s.gather
}

// unionSelected appends the rows of src selected by sel to dst: through
// rows when they are decoded, else by mask.
func unionSelected(dst, src *vector.Vector, sel *bitmap.Bitmap, rows []int64, mp *mpool.MPool) error {
	if rows != nil {
		return dst.Union(src, rows, mp)
	}
	return dst.UnionByMask(src, sel, mp)
}

// countGathered is the number of vecs a selection gather copies: the
// non-nil ones that are not left empty by the reader.
func countGathered(vecs []*vector.Vector, physical int) int {
	n := 0
	for _, vec := range vecs {
		if vec != nil && (vec.Length() != 0 || physical == 0) {
			n++
		}
	}
	return n
}

// borrowedUses returns columnUses with every owned column borrowed instead.
func (s *selectionState) borrowedUses(columnUses []engine.ReaderColumnUse) []engine.ReaderColumnUse {
	if len(s.uses) == len(columnUses) && s.uses != nil {
		return s.uses
	}
	s.uses = make([]engine.ReaderColumnUse, len(columnUses))
	for i, use := range columnUses {
		if use == engine.ReaderColumnOwned {
			use = engine.ReaderColumnRetain
		}
		s.uses[i] = use
	}
	return s.uses
}

// SetSelectionOutput implements vm.SelectionProducer.
func (tableScan *TableScan) SetSelectionOutput(allowed bool) {
	tableScan.ctr.selection.output = allowed
}

// publishesSelection reports whether the reader may publish selected blocks:
// the parent must read selected batches (otherwise the reader's own
// compaction, done while the cache entries are pinned, is the cheapest copy;
// compacting a published selection later costs a bitmap round trip per
// batch), the scan must evaluate its filter through the reader's Select path,
// and no later step may need flat rows (CHAR padding rewrites values in
// place; local domains read and shrink the batch).
func (tableScan *TableScan) publishesSelection() bool {
	request := tableScan.ctr.readerFilter
	return tableScan.ctr.selection.output && request != nil && request.Select != nil && request.Membership == nil &&
		!tableScan.ctr.padCharToFullLength &&
		len(tableScan.ctr.domainSources) == 0 && len(tableScan.ctr.domainProbes) == 0
}

// execSelectedProjection projects a selected block and publishes it with its
// selection, or compacts it when the parent needs flat rows.
func (tableScan *TableScan) execSelectedProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	s := &tableScan.ctr.selection
	out := input
	if tableScan.ProjectList != nil {
		input.PhysicalView(&s.physical)
		s.mask = input.SelectionMask(s.mask)
		var err error
		if out, err = tableScan.EvalProjectionSelected(input, &s.physical, s.mask, proc); err != nil {
			return nil, err
		}
	}
	if s.output {
		return out, nil
	}
	return s.compactInto(out, tableScan.mp(proc))
}

// compactInto copies out's selected rows into the scan-owned compact batch.
// Columns the reader left empty (unused after the filter) stay empty.
func (s *selectionState) compactInto(out *batch.Batch, mp *mpool.MPool) (*batch.Batch, error) {
	if s.compact == nil || len(s.compact.Vecs) != len(out.Vecs) {
		// out may still read s.rows: free only the compact batch.
		s.freeCompact(mp)
		s.compact = batch.NewWithSize(len(out.Vecs))
	}
	compact := s.compact
	compact.CleanOnlyData()
	physical := out.PhysicalRowCount()
	sel := out.Selection()
	var rows []int64
	if countGathered(out.Vecs, physical) >= minRowListColumns {
		rows = s.gatherRows(sel, physical)
	}
	for i, vec := range out.Vecs {
		if vec == nil {
			compact.Vecs[i] = nil
			continue
		}
		dst := compact.Vecs[i]
		if dst == nil || *dst.GetType() != *vec.GetType() {
			if dst != nil {
				dst.Free(mp)
			}
			dst = vector.NewVec(*vec.GetType(), mp)
			compact.Vecs[i] = dst
		}
		if vec.Length() == 0 && physical != 0 {
			continue
		}
		if err := unionSelected(dst, vec, sel, rows, mp); err != nil {
			return nil, err
		}
		dst.SetSorted(vec.GetSorted())
	}
	compact.Attrs = append(compact.Attrs[:0], out.Attrs...)
	compact.Recursive, compact.ShuffleIDX = out.Recursive, out.ShuffleIDX
	compact.SetRowCount(out.RowCount())
	return compact, nil
}

func (s *selectionState) reset() {
	if s.compact != nil {
		s.compact.CleanOnlyData()
	}
}

func (s *selectionState) free(mp *mpool.MPool) {
	s.uses = nil
	s.rows.Free()
	s.keep.Free()
	s.pred.Free()
	s.dep.Free()
	s.gather = nil
	s.freeCompact(mp)
}

func (s *selectionState) freeCompact(mp *mpool.MPool) {
	if s.compact != nil {
		s.compact.Clean(mp)
		s.compact = nil
	}
}
