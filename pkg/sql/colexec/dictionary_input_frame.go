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

package colexec

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"slices"
)

// DictionaryInputFrame owns one lazy flat copy per input column and evaluation
// domain. Begin must run even when the caller reuses the same batch/header.
// End clears borrowed headers; only Free releases the reusable owned storage.
// Masked evaluation compacts operands first, so those vectors cannot match these
// full-domain input slots. The frame is execution-lane local, never concurrent.
type DictionaryInputFrame struct {
	predicateInput   *batch.Batch
	predicateVectors []*vector.Vector
	predicateSeen    []bool
	predicateColumns []int
	// Gathers counts committed materializations in the current owner generation.
	Gathers                                      uint64
	gatherRows, gatherBytes                      uint64
	reportedGathers, reportedRows, reportedBytes uint64
	inputs                                       []*vector.Vector
	flat                                         []*vector.Vector
	ready                                        []bool
	allocation                                   *vector.AllocationAccountSelection
}

func (f *DictionaryInputFrame) Begin(bat *batch.Batch, allocation *vector.AllocationAccountSelection, mp *mpool.MPool) {
	// Begin can bind the predicate bank itself; retiring that bank belongs to
	// End after its Boolean result has been consumed, not this rebind.
	clear(f.inputs)
	clear(f.ready)
	if !vector.AllocationAccountSelectionsEqual(f.allocation, allocation) || len(f.flat) != len(bat.Vecs) {
		f.Free(mp)
		f.inputs = make([]*vector.Vector, len(bat.Vecs))
		f.flat = make([]*vector.Vector, len(bat.Vecs))
		f.ready = make([]bool, len(bat.Vecs))
	}
	f.allocation = allocation
	copy(f.inputs, bat.Vecs)
}

func (f *DictionaryInputFrame) End() {
	clear(f.inputs)
	clear(f.ready)
	if f.predicateInput != nil {
		clear(f.predicateInput.Vecs)
	}
}
func (f *DictionaryInputFrame) Free(mp *mpool.MPool) {
	for _, v := range f.flat {
		if v != nil {
			v.Free(mp)
		}
	}
	f.releasePredicateBuffers(mp)
	if f.predicateInput != nil {
		clear(f.predicateInput.Vecs)
		f.predicateInput.Clean(mp)
	}
	*f = DictionaryInputFrame{}
}

func (f *DictionaryInputFrame) materialize(source *vector.Vector, allocation *vector.AllocationAccountSelection, mp *mpool.MPool) (*vector.Vector, bool, error) {
	if f == nil || !vector.AllocationAccountSelectionsEqual(f.allocation, allocation) {
		return nil, false, nil
	}
	for i, input := range f.inputs {
		if input != source {
			continue
		}
		if !f.ready[i] {
			if f.flat[i] == nil {
				v, err := vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), allocation)
				if err != nil {
					return nil, true, err
				}
				f.flat[i] = v
			} else {
				f.flat[i].Reset(*source.GetType())
			}
			if err := f.flat[i].UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
				return nil, true, err
			}
			source.CopyExpressionMetadataTo(f.flat[i])
			f.ready[i] = true
			f.Gathers++
			f.gatherRows += uint64(source.Length())
			f.gatherBytes += uint64(source.Size())
		}
		return f.flat[i], true, nil
	}
	return nil, false, nil
}

// Bind attaches borrowed frame pointers, including fused/memo executor roots.
// Executors never free the frame; its containing operator is the sole owner.
func (f *DictionaryInputFrame) Bind(executors []ExpressionExecutor) {
	for _, executor := range executors {
		switch e := executor.(type) {
		case *FunctionExpressionExecutor:
			e.dictionaryFrame = f
			f.Bind(e.parameterExecutor)
		case *decimalTypedExecutor:
			f.Bind([]ExpressionExecutor{e.root})
		case *decimalMulSubExecutor:
			f.Bind([]ExpressionExecutor{e.root})
		case *decimalChainExecutor:
			f.Bind([]ExpressionExecutor{e.root})
		case *memoExpressionExecutor:
			f.Bind([]ExpressionExecutor{e.state.executor})
		case *memoRootExpressionExecutor:
			f.Bind([]ExpressionExecutor{e.executor})
		}
	}
}

// Observe reports committed work, never an estimate of copies avoided.
func (f *DictionaryInputFrame) Observe(analyzer process.Analyzer) {
	if analyzer == nil || f.Gathers == f.reportedGathers {
		return
	}
	stats := analyzer.GetOpStats()
	stats.AddExtraStat("DictionaryExpressionGathers", int64(f.Gathers-f.reportedGathers))
	stats.AddExtraStat("DictionaryExpressionGatherRows", int64(f.gatherRows-f.reportedRows))
	stats.AddExtraStat("DictionaryExpressionGatherLogicalBytes", int64(f.gatherBytes-f.reportedBytes))
	var allocated int64
	for _, v := range f.flat {
		if v != nil {
			allocated += int64(v.Allocated())
		}
	}
	stats.SetMaxExtraStat("DictionaryExpressionScratchBytes", allocated)
	f.reportedGathers, f.reportedRows, f.reportedBytes = f.Gathers, f.gatherRows, f.gatherBytes
}

// releasePredicateBuffers also makes optional scratch retention relinquishable
// under account pressure. A retry then has the same owned-gather footprint as
// a fresh evaluation, rather than failing because unused banks occupy capacity.
func (f *DictionaryInputFrame) releasePredicateBuffers(mp *mpool.MPool) {
	if f.predicateInput != nil {
		clear(f.predicateInput.Vecs)
	}
	for i, v := range f.predicateVectors {
		if v != nil {
			v.Free(mp)
			f.predicateVectors[i] = nil
		}
	}
}

// predicateBatch returns frame-owned scratch for an audited expression tree.
// The result is borrowed only through this Eval and its Boolean result scan.
// Unknown executors receive independent owned input via the existing fallback.
func (f *DictionaryInputFrame) predicateBatch(proc *process.Process, input *batch.Batch, loaded []int, executor ExpressionExecutor, rows []int64, account *vector.AllocationAccountSelection) (result *batch.Batch, borrowed bool, retErr error) {
	if len(f.predicateSeen) != len(input.Vecs) {
		f.predicateSeen = make([]bool, len(input.Vecs))
	} else {
		clear(f.predicateSeen)
	}
	f.predicateColumns = f.predicateColumns[:0]
	if !predicateColumns(executor, f.predicateSeen, &f.predicateColumns) {
		output, _, err := SelectBatchColumns(input, loaded, rows, proc.Mp(), account, false)
		return output, false, err
	}
	if f.predicateInput == nil {
		f.predicateInput = batch.NewOffHeapWithSize(len(input.Vecs))
		if err := f.predicateInput.SetAllocationAccount(account); err != nil {
			return nil, false, err
		}
		f.predicateVectors = make([]*vector.Vector, len(input.Vecs))
	}
	output := f.predicateInput
	clear(output.Vecs)
	output.Attrs = append(output.Attrs[:0], input.Attrs...)
	output.Recursive, output.ShuffleIDX = input.Recursive, input.ShuffleIDX
	output.SetRowCount(len(rows))
	defer func() {
		if retErr != nil && errors.Is(retErr, mpool.ErrAllocationAccountCapacity) {
			f.releasePredicateBuffers(proc.Mp())
			result, _, retErr = SelectBatchColumns(input, f.predicateColumns, rows, proc.Mp(), account, false)
			borrowed = false
		}
	}()
	for _, col := range f.predicateColumns {
		source := input.Vecs[col]
		if source == nil {
			return nil, false, moerr.NewInvalidInput(proc.Ctx, "predicate references unloaded column")
		}
		destination := f.predicateVectors[col]
		if destination == nil {
			var err error
			destination, err = vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), account)
			if err != nil {
				return nil, false, err
			}
			f.predicateVectors[col] = destination
		} else {
			destination.Reset(*source.GetType())
		}
		if err := destination.Union(source, rows, proc.Mp()); err != nil {
			return nil, false, err
		}
		source.CopyExpressionMetadataTo(destination)
		destination.SetSorted(source.GetSorted() && slices.IsSorted(rows))
		f.Gathers++
		f.gatherRows += uint64(len(rows))
		f.gatherBytes += uint64(destination.Size())
		output.Vecs[col] = destination
	}
	return output, true, nil
}
