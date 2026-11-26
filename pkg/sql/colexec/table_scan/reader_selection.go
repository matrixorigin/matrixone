// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_scan

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectReaderFilter evaluates the ordered predicate chain without mutating
// bat. The kept rows are returned as a mask over bat's rows, owned by the
// scan and borrowed until the next callback.
func (tableScan *TableScan) selectReaderFilter(proc *process.Process, bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
	start := time.Now()
	defer func() { tableScan.ctr.filterActiveDuration += time.Since(start) }()
	tableScan.recordFilterInput(bat, loaded)
	tableScan.ctr.filterLateMaterialized = loaded != nil && len(loaded) < len(tableScan.Types)
	return tableScan.selectPredicateMask(proc, bat, loaded)
}

// selectPredicateMask preserves evaluation order: each predicate after the
// first partial selection is evaluated only over the surviving rows, gathered
// into a private flat batch, and its result is composed back onto the
// block's rows with InitDeposit. No row list is built.
func (tableScan *TableScan) selectPredicateMask(proc *process.Process, input *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
	executors := tableScan.ctr.allFilterExecutors
	first := !tableScan.ctr.subjectReuseAdmissionDone
	tableScan.ctr.subjectReuseAdmissionDone = true
	analyzer := tableScan.OpAnalyzer
	s := &tableScan.ctr.selection
	mp := tableScan.mp(proc)
	for _, bm := range []*bitmap.Bitmap{&s.keep, &s.pred, &s.dep} {
		if bm.MPool() != mp {
			bm.Free()
			bm.Init(mp)
		}
	}
	keep := &s.keep

	var gathered *batch.Batch
	defer func() {
		if gathered != nil {
			gathered.Clean(tableScan.gatherMP(proc))
		}
	}()
	none := func() (engine.ReaderFilterResult, error) {
		if err := keep.InitWithSize(int64(input.RowCount()), mp); err != nil {
			return engine.ReaderFilterResult{}, err
		}
		return engine.ReaderFilterResult{Mask: keep}, nil
	}

	current := input
	selected := false
	for i := range executors {
		if first && i >= len(tableScan.ctr.runtimeFilterExecutors) {
			index := i - len(tableScan.ctr.runtimeFilterExecutors)
			executor := colexec.ReuseFilterExpressionExecutor(tableScan.FilterExprs[index], tableScan.ctr.filterExecutors[index], current.RowCount())
			tableScan.ctr.filterExecutors[index] = executor
			tableScan.ctr.allFilterExecutors[i] = executor
		}
		value, err := executors[i].Eval(proc, []*batch.Batch{current}, nil)
		if err != nil {
			return engine.ReaderFilterResult{}, err
		}
		if !value.GetType().IsBoolean() {
			return engine.ReaderFilterResult{}, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}
		if proc.OperatorOutofMemory(int64(value.Size())) {
			return engine.ReaderFilterResult{}, moerr.NewOOM(proc.Ctx)
		}
		if analyzer != nil {
			analyzer.Alloc(int64(value.Size()))
		}
		if value.IsConst() {
			if value.IsConstNull() || !vector.GetFixedAtNoTypeCheck[bool](value, 0) {
				return none()
			}
			continue
		}
		count := current.RowCount()
		if value.Length() < count {
			return engine.ReaderFilterResult{}, moerr.NewInternalErrorf(proc.Ctx,
				"filter condition has %d rows for %d input rows", value.Length(), count)
		}
		values := vector.MustFixedColNoTypeCheck[bool](value)[:count]
		var nulls *bitmap.Bitmap
		if nsp := value.GetNulls(); !nsp.IsEmpty() {
			nulls = nsp.GetBitmap()
		}
		if !selected {
			if err = keep.InitWithBools(values, nulls, mp); err != nil {
				return engine.ReaderFilterResult{}, err
			}
			if keep.Count() == count {
				continue
			}
		} else {
			// values covers the kept rows only: keep the kept row of rank
			// k when the predicate holds for row k.
			if err = s.pred.InitWithBools(values, nulls, mp); err != nil {
				return engine.ReaderFilterResult{}, err
			}
			if s.pred.Count() == count {
				continue
			}
			if err = s.dep.InitDeposit(keep, &s.pred, mp); err != nil {
				return engine.ReaderFilterResult{}, err
			}
			keep.Swap(&s.dep)
		}
		selected = true
		if keep.IsEmpty() {
			return engine.ReaderFilterResult{Mask: keep}, nil
		}
		if i+1 < len(executors) {
			next, err := gatherSelectedColumns(proc, input, loaded, keep, s, tableScan.OutputAllocation)
			if err != nil {
				return engine.ReaderFilterResult{}, err
			}
			if gathered != nil {
				gathered.Clean(tableScan.gatherMP(proc))
			}
			gathered, current = next, next
		}
	}
	if !selected {
		return engine.ReaderFilterResult{All: true}, nil
	}
	return engine.ReaderFilterResult{Mask: keep}, nil
}

// gatherSelectedColumns copies the selected rows of the loaded (or, for nil,
// every populated) column into a private flat batch with input's schema.
//
// With several columns to copy, the selection is decoded once into the
// scan's row list (s.gather) and every column is gathered through it.
func gatherSelectedColumns(proc *process.Process, input *batch.Batch, loaded []int, rows *bitmap.Bitmap, s *selectionState, allocation *mpool.MPool) (_ *batch.Batch, retErr error) {
	mp := proc.Mp()
	if allocation != nil {
		mp = allocation
	}
	output := batch.NewWithSize(len(input.Vecs))
	defer func() {
		if retErr != nil {
			output.Clean(mp)
		}
	}()
	columns := len(loaded)
	if loaded == nil {
		columns = countGathered(input.Vecs, input.RowCount())
	}
	var list []int64
	if columns >= minRowListColumns {
		list = s.gatherRows(rows, input.RowCount())
	}
	gather := func(pos int) error {
		source := input.Vecs[pos]
		if source == nil || (source.Length() == 0 && input.RowCount() != 0) {
			return nil
		}
		dst := vector.NewVec(*source.GetType(), mp)
		output.Vecs[pos] = dst
		return unionSelected(dst, source, rows, list, mp)
	}
	if loaded == nil {
		for pos := range input.Vecs {
			if err := gather(pos); err != nil {
				return nil, err
			}
		}
	} else {
		for _, pos := range loaded {
			if pos < 0 || pos >= len(input.Vecs) || input.Vecs[pos] == nil {
				return nil, moerr.NewInvalidInputNoCtx("invalid selected reader column")
			}
			if err := gather(pos); err != nil {
				return nil, err
			}
		}
	}
	output.Attrs = append(output.Attrs, input.Attrs...)
	output.Recursive, output.ShuffleIDX = input.Recursive, input.ShuffleIDX
	output.SetRowCount(rows.Count())
	return output, nil
}

// prepareColumnUses tells the reader which columns leave this scan. Columns
// read only by the filter chain may be dropped after selection; every other
// read column must be owned, aligned output.
func (tableScan *TableScan) prepareColumnUses() {
	if tableScan.ctr.columnUsesReady {
		return
	}
	tableScan.ctr.columnUsesReady = true
	tableScan.ctr.columnUses = nil
	uses := make([]engine.ReaderColumnUse, len(tableScan.Types))
	if len(tableScan.ProjectList) == 0 || len(tableScan.ProjectExecutors) != len(tableScan.ProjectList) {
		for i := range uses {
			uses[i] = engine.ReaderColumnOwned
		}
		tableScan.ctr.columnUses = uses
		return
	}
	for i := range uses {
		uses[i] = engine.ReaderColumnUnused
	}
	used := make([]bool, len(uses))
	for _, expr := range tableScan.ProjectList {
		if !CollectFilterColumnPositions(expr, used) {
			for column := range uses {
				uses[column] = engine.ReaderColumnOwned
			}
			tableScan.ctr.columnUses = uses
			return
		}
	}
	for column, use := range used {
		if use {
			uses[column] = engine.ReaderColumnOwned
		}
	}
	// A source builder still needs its key after projection. Handled membership
	// reads may drop other private inputs; unsupported readers need aligned data.
	for _, source := range tableScan.ctr.domainSources {
		if uses[source.column] == engine.ReaderColumnUnused {
			uses[source.column] = engine.ReaderColumnOwned
		}
	}
	if request := tableScan.ctr.readerFilter; request != nil && request.Membership != nil {
		request.Membership.ColumnUses = append(request.Membership.ColumnUses[:0], uses...)
	}
	active := false
	for _, probe := range tableScan.ctr.domainProbes {
		active = active || !probe.pass
	}
	if active {
		for i, use := range uses {
			if use == engine.ReaderColumnUnused {
				uses[i] = engine.ReaderColumnOwned
			}
		}
	}
	tableScan.ctr.columnUses = uses
}

// gatherMP is the pool gatherSelectedColumns creates the gathered batch with.
func (tableScan *TableScan) gatherMP(proc *process.Process) *mpool.MPool {
	if tableScan.OutputAllocation != nil {
		return tableScan.OutputAllocation
	}
	return proc.Mp()
}
