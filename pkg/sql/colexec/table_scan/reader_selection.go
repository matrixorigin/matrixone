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
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectReaderFilter evaluates the ordered predicate chain without mutating
// bat. The returned Sels are positions in bat's original row domain and are
// borrowed until the next callback.
func (tableScan *TableScan) selectReaderFilter(proc *process.Process, bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
	start := time.Now()
	defer func() { tableScan.ctr.filterActiveDuration += time.Since(start) }()
	tableScan.recordFilterInput(bat, loaded)
	tableScan.ctr.filterLateMaterialized = loaded != nil && len(loaded) < len(tableScan.Types)
	rows, all, err := tableScan.selectPredicateRows(proc, bat, loaded)
	return engine.ReaderFilterResult{Sels: rows, All: all}, err
}

// selectPredicateRows preserves evaluation order: each predicate after the
// first partial selection is evaluated only over the surviving rows, gathered
// into a private flat batch.
func (tableScan *TableScan) selectPredicateRows(proc *process.Process, input *batch.Batch, loaded []int) ([]int64, bool, error) {
	executors := tableScan.ctr.allFilterExecutors
	rows := tableScan.ctr.filterRows[:0]
	first := !tableScan.ctr.subjectReuseAdmissionDone
	tableScan.ctr.subjectReuseAdmissionDone = true
	analyzer := tableScan.OpAnalyzer

	var gathered *batch.Batch
	defer func() {
		if gathered != nil {
			gathered.Clean(tableScan.gatherMP(proc))
		}
		tableScan.ctr.filterRows = rows
	}()

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
			return nil, false, err
		}
		if !value.GetType().IsBoolean() {
			return nil, false, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}
		if proc.OperatorOutofMemory(int64(value.Size())) {
			return nil, false, moerr.NewOOM(proc.Ctx)
		}
		if analyzer != nil {
			analyzer.Alloc(int64(value.Size()))
		}
		if value.IsConst() {
			if tableScan.ctr.filterBs == nil || !vector.ReuseFunctionFixedTypeParameter(value, tableScan.ctr.filterBs) {
				tableScan.ctr.filterBs = vector.GenerateFunctionFixedTypeParameter[bool](value)
			}
			keep, isNull := tableScan.ctr.filterBs.GetValue(0)
			if isNull || !keep {
				rows = rows[:0]
				return rows, false, nil
			}
			continue
		}
		count := current.RowCount()
		rows = compactPredicateRows(value, count, rows, selected)
		if len(rows) == count {
			if !selected {
				rows = rows[:0]
			}
			continue
		}
		selected = true
		if len(rows) == 0 {
			return rows, false, nil
		}
		if i+1 < len(executors) {
			next, err := gatherSelectedColumns(proc, input, loaded, rows, tableScan.OutputAllocation)
			if err != nil {
				return nil, false, err
			}
			if gathered != nil {
				gathered.Clean(tableScan.gatherMP(proc))
			}
			gathered, current = next, next
		}
	}
	return rows, !selected, nil
}

// compactPredicateRows keeps the original positions of the TRUE rows. When
// selected, rows already holds the original positions of value's rows and is
// compacted in place.
func compactPredicateRows(value *vector.Vector, count int, rows []int64, selected bool) []int64 {
	if !selected {
		rows = slices.Grow(rows[:0], count)[:count]
		for i := range rows {
			rows[i] = int64(i)
		}
	}
	values := vector.MustFixedColNoTypeCheck[bool](value)
	nulls := value.GetNulls()
	hasNull := !nulls.IsEmpty()
	kept := 0
	for row := 0; row < count; row++ {
		if values[row] && (!hasNull || !nulls.Contains(uint64(row))) {
			rows[kept] = rows[row]
			kept++
		}
	}
	return rows[:kept]
}

// gatherSelectedColumns copies the selected rows of the loaded (or, for nil,
// every populated) column into a private flat batch with input's schema.
func gatherSelectedColumns(proc *process.Process, input *batch.Batch, loaded []int, rows []int64, allocation *mpool.MPool) (_ *batch.Batch, retErr error) {
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
	gather := func(pos int) error {
		source := input.Vecs[pos]
		if source == nil || (source.Length() == 0 && input.RowCount() != 0) {
			return nil
		}
		dst := vector.NewVec(*source.GetType(), mp)
		output.Vecs[pos] = dst
		if err := dst.Union(source, rows, mp); err != nil {
			return err
		}
		return nil
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
	output.Recursive, output.ShuffleIDX = input.Recursive, input.ShuffleIDX
	output.SetRowCount(len(rows))
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
