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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectionState lets the scan publish filtered blocks without copying them.
// The reader keeps every column a read-only reference to the cache and records
// the rows the filter kept as the output batch's own selection. The cache pins
// belong to the scan's ReaderFilterRequest, the lender: the reader releases
// them before it reuses the output batch for the next block, and the scan
// releases them in Reset and Free. The scan's projection then runs on the
// physical rows, masked to the selection, and the result either keeps the
// selection, when the parent reads selected batches, or is compacted into
// compact, a batch owned by the scan's pool. Either way it is valid until the
// next Call.
type selectionState struct {
	// output is set by the parent's preparation (vm.SelectionProducer).
	output   bool
	mask     []bool
	physical batch.Batch
	compact  *batch.Batch
	sels     []int64
	// uses is the reader column policy for selected reads: the columns the
	// scan outputs are borrowed (ReaderColumnRetain) instead of copied.
	uses []engine.ReaderColumnUse
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
		// out may still read its selection: free only the compact batch.
		s.freeCompact(mp)
		s.compact = batch.NewWithSize(len(out.Vecs))
	}
	compact := s.compact
	compact.CleanOnlyData()
	physical := out.PhysicalRowCount()
	s.sels = out.SelectionRows(s.sels)
	sels := s.sels
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
		if err := dst.Union(vec, sels, mp); err != nil {
			return nil, err
		}
		dst.SetSorted(vec.GetSorted())
	}
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
	s.freeCompact(mp)
}

func (s *selectionState) freeCompact(mp *mpool.MPool) {
	if s.compact != nil {
		s.compact.Clean(mp)
		s.compact = nil
	}
}
