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

package filter

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectionState is the filter's selected-batch machinery. A selected output,
// view, borrows the input's vectors and owns its selection; both stay valid
// until the next Call.
type selectionState struct {
	// output is set by the parent's preparation (vm.SelectionProducer).
	output   bool
	mask     []bool
	physical batch.Batch
	view     batch.Batch
	sels     []int64
}

// AcceptsSelection: the filter evaluates its predicates on a selected input's
// physical rows, masked to the selection.
func (filter *Filter) AcceptsSelection(child int) bool { return child == 0 }

// SetSelectionOutput implements vm.SelectionProducer.
func (filter *Filter) SetSelectionOutput(allowed bool) { filter.ctr.selection.output = allowed }

// selectionCall evaluates the predicates without copying the input. Each
// predicate after a partial match sees only the surviving rows (the others
// are masked and not evaluated), as with the compacting path, and its result
// is ANDed into the selection bitmap. The result is the input itself, an
// empty batch, a selected view (when the parent accepts it) or a compacted
// copy.
func (filter *Filter) selectionCall(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	analyzer := filter.OpAnalyzer
	s := &filter.ctr.selection
	input.PhysicalView(&s.physical)
	physical := &s.physical
	physicalRows := physical.RowCount()

	// selected == false means every physical row is selected.
	// rows is view's own selection: a copy of the input's selection, narrowed
	// below, or built from the first predicate that drops a row.
	selected := input.HasSelection()
	var rows *bitmap.Bitmap
	if selected {
		rows = s.view.OwnSelection(0)
		rows.InitWith(input.Selection())
	}

	firstInvocation := !filter.ctr.subjectReuseAdmissionDone
	filter.ctr.subjectReuseAdmissionDone = true
	for i := range filter.ctr.allExecutors {
		if firstInvocation && !filter.OwnsConstantCastWarnings && i >= len(filter.ctr.runtimeExecutors) {
			staticIndex := i - len(filter.ctr.runtimeExecutors)
			executor := colexec.ReuseFilterExpressionExecutor(filter.FilterExprs[staticIndex], filter.ctr.executors[staticIndex], physicalRows)
			filter.ctr.executors[staticIndex] = executor
			filter.ctr.allExecutors[i] = executor
		}
		var mask []bool
		if selected {
			s.mask = batch.SelectionMaskOf(rows, physicalRows, s.mask)
			mask = s.mask
		}
		vec, err := filter.ctr.allExecutors[i].Eval(proc, []*batch.Batch{physical}, mask)
		if err != nil {
			return nil, err
		}
		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return nil, moerr.NewOOM(proc.Ctx)
		}
		analyzer.Alloc(int64(vec.Size()))
		if !vec.GetType().IsBoolean() {
			return nil, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}
		if filter.IsAssert && i >= len(filter.ctr.runtimeExecutors) {
			continue
		}
		if filter.ctr.bs == nil || !vector.ReuseFunctionFixedTypeParameter[bool](vec, filter.ctr.bs) {
			filter.ctr.bs = vector.GenerateFunctionFixedTypeParameter[bool](vec)
		}
		bs := filter.ctr.bs
		keep := func(row uint64) bool {
			if vec.IsConst() {
				row = 0
			}
			v, null := bs.GetValue(row)
			return !null && v
		}
		if !selected {
			rows = s.view.OwnSelection(physicalRows)
			for row := 0; row < physicalRows; row++ {
				if keep(uint64(row)) {
					rows.Add(uint64(row))
				}
			}
			if rows.Count() == physicalRows {
				continue // every row survives; stay unselected
			}
			selected = true
		} else {
			itr := rows.IteratorValue()
			for itr.HasNext() {
				row := itr.Next()
				if !keep(row) {
					rows.Remove(row)
				}
			}
		}
		if rows.IsEmpty() {
			return batch.EmptyBatch, nil
		}
	}

	if !selected {
		return input, nil
	}
	// PhysicalView resets every field a view uses and keeps view's own
	// selection storage, which rows points into.
	input.PhysicalView(&s.view)
	s.view.SetSelection(rows)
	if s.output {
		return &s.view, nil
	}
	s.sels = s.view.SelectionRows(s.sels)
	return filter.ctr.shrinkWithSels(proc, physical, s.sels)
}
