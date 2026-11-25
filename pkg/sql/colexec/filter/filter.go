// Copyright 2021 Matrix Origin
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

package filter

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "filter"

func (filter *Filter) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(fmt.Sprintf("filter(%s)", filter.FilterExprs))
}

func (filter *Filter) OpType() vm.OpType {
	return vm.Filter
}

func (filter *Filter) Prepare(proc *process.Process) (err error) {
	if filter.OpAnalyzer == nil {
		filter.OpAnalyzer = process.NewAnalyzer(filter.GetIdx(), filter.IsFirst, filter.IsLast, "filter")
	} else {
		filter.OpAnalyzer.Reset()
	}

	if err = filter.ctr.bindOutputAllocation(proc); err != nil {
		return
	}

	if len(filter.ctr.executors) == 0 && filter.FilterExprs != nil {
		if filter.OwnsConstantCastWarnings {
			filter.ctr.executors, err = colexec.NewOwnedConstantFilterExecutors(proc, filter.FilterExprs)
		} else {
			filter.ctr.executors, err = colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, filter.FilterExprs, filter.ctr.outputAllocation)
		}
		if err != nil {
			return
		}
		filter.ctr.subjectReuseAdmissionDone = false
	}

	if len(filter.RuntimeFilterExprs) > 0 {
		filter.ctr.runtimeExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, filter.RuntimeFilterExprs, filter.ctr.outputAllocation)
		if err != nil {
			return
		}
	}

	if filter.ctr.allExecutors == nil {
		filter.ctr.allExecutors = make([]colexec.ExpressionExecutor, 0, len(filter.ctr.runtimeExecutors)+len(filter.ctr.executors))
	} else {
		filter.ctr.allExecutors = filter.ctr.allExecutors[:0]
	}

	filter.ctr.allExecutors = append(filter.ctr.allExecutors, filter.ctr.runtimeExecutors...)
	filter.ctr.allExecutors = append(filter.ctr.allExecutors, filter.ctr.executors...)

	return
}

func (filter *Filter) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := filter.OpAnalyzer

	inputResult, err := vm.ChildrenCall(filter.GetChildren(0), proc, analyzer)
	if err != nil {
		return inputResult, err
	}

	if inputResult.Batch == nil || inputResult.Batch.IsEmpty() || inputResult.Batch.Last() || len(filter.ctr.allExecutors) == 0 {
		return inputResult, nil
	}

	if filter.ctr.selection.output || inputResult.Batch.HasSelection() {
		output, err := filter.selectionCall(proc, inputResult.Batch)
		if err != nil {
			return vm.CancelResult, err
		}
		result := vm.NewCallResult()
		if !filter.IsEnd {
			result.Batch = output
		}
		return result, nil
	}

	filterBat := inputResult.Batch
	rows := filter.ctr.selectionBitmaps(proc)
	firstInvocation := !filter.ctr.subjectReuseAdmissionDone
	filter.ctr.subjectReuseAdmissionDone = true
	for i := range filter.ctr.allExecutors {
		if filterBat.IsEmpty() {
			break
		}
		if firstInvocation && !filter.OwnsConstantCastWarnings && i >= len(filter.ctr.runtimeExecutors) {
			staticIndex := i - len(filter.ctr.runtimeExecutors)
			executor := colexec.ReuseFilterExpressionExecutor(filter.FilterExprs[staticIndex], filter.ctr.executors[staticIndex], filterBat.RowCount())
			filter.ctr.executors[staticIndex] = executor
			filter.ctr.allExecutors[i] = executor
		}

		var vec *vector.Vector
		vec, err = filter.ctr.allExecutors[i].Eval(proc, []*batch.Batch{filterBat}, nil)
		if err != nil {
			return vm.CancelResult, err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return vm.CancelResult, moerr.NewOOM(proc.Ctx)
		}
		analyzer.Alloc(int64(vec.Size()))

		if !vec.GetType().IsBoolean() {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}
		if filter.IsAssert && i >= len(filter.ctr.runtimeExecutors) {
			// ASSERT expressions report violations as executor errors. Successful
			// evaluation is row preserving, so scanning a TRUE vector and building
			// a selection vector would only add per-row work. Runtime filters, if
			// attached, remain cardinality-changing and still use the normal path.
			continue
		}

		if vec.IsConst() {
			if vec.IsConstNull() || !vector.GetFixedAtNoTypeCheck[bool](vec, 0) {
				filterBat = batch.EmptyBatch
			}
			continue
		}
		// The TRUE, non-NULL rows, built a word at a time, then gathered by
		// the bitmap.
		var nulls *bitmap.Bitmap
		if nsp := vec.GetNulls(); !nsp.IsEmpty() {
			nulls = nsp.GetBitmap()
		}
		if err = rows.InitWithBools(vector.MustFixedColNoTypeCheck[bool](vec), nulls, filter.ctr.mp(proc)); err != nil {
			return vm.CancelResult, err
		}
		if rows.Count() != filterBat.RowCount() {
			filterBat, err = filter.ctr.compactByMask(proc, filterBat, rows)
			if err != nil {
				return vm.CancelResult, err
			}
		}
	}

	// bad design here. should compile a pipeline like `-> restrict -> output (just do clean work or memory reuse) -> `
	// but not use the IsEnd flag to do the clean work.
	result := vm.NewCallResult()
	if filter.IsEnd {
		result.Batch = nil
	} else {
		result.Batch = filterBat
	}
	return result, nil
}

// compactByMask keeps the rows of bat selected by sel, gathering by the
// bitmap itself: into the filter's own output batch, or in place when bat is
// that batch already.
// selectionBitmaps returns the filter's selection bitmap, owned by the
// filter's pool; a pool change (a new execution) releases the old words.
func (ctr *container) selectionBitmaps(proc *process.Process) *bitmap.Bitmap {
	s := &ctr.selection
	if mp := ctr.mp(proc); s.rows.MPool() != mp {
		s.rows.Free()
		s.rows.Init(mp)
		s.pred.Free()
		s.pred.Init(mp)
	}
	return &s.rows
}

func (ctr *container) compactByMask(proc *process.Process, bat *batch.Batch, sel *bitmap.Bitmap) (*batch.Batch, error) {
	if sel.IsEmpty() {
		return batch.EmptyBatch, nil
	}
	mp := ctr.mp(proc)
	if bat == ctr.buf {
		// A later predicate over the filter's own output: shrink in place.
		if err := ctr.buf.ShrinkByMask(sel, false, 0, mp); err != nil {
			return nil, err
		}
		return ctr.buf, nil
	}
	if ctr.buf == nil {
		ctr.buf = batch.NewWithSize(len(bat.Vecs))
		ctr.buf.SetAttributes(bat.Attrs)
		ctr.buf.Recursive = bat.Recursive
		for j, vec := range bat.Vecs {
			ctr.buf.Vecs[j] = vector.NewVec(*vec.GetType(), mp)
			ctr.buf.Vecs[j].SetSorted(vec.GetSorted())
		}
		ctr.buf.ShuffleIDX = bat.ShuffleIDX
		if err := ctr.buf.PreExtend(mp, sel.Count()); err != nil {
			return nil, err
		}
	} else {
		ctr.buf.CleanOnlyData()
	}
	if err := ctr.buf.UnionByMask(bat, sel, mp); err != nil {
		return nil, err
	}
	return ctr.buf, nil
}
