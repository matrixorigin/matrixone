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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DictionaryInputFrame owns one lazy flat copy per input column and evaluation
// domain. Begin must run even when the caller reuses the same batch/header.
// End clears borrowed headers; only Free releases the reusable owned storage.
// Masked evaluation compacts operands first, so those vectors cannot match these
// full-domain input slots. The frame is execution-lane local, never concurrent.
type DictionaryInputFrame struct {
	predicateHeaders []*vector.Vector
	snapshots        []*vector.DictionaryVectorSlot
	selectionSlot    *vector.DictionaryVectorSlot
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
	f.End()
	if !vector.AllocationAccountSelectionsEqual(f.allocation, allocation) || len(f.flat) != len(bat.Vecs) {
		f.Free(mp)
		f.inputs = make([]*vector.Vector, len(bat.Vecs))
		f.flat = make([]*vector.Vector, len(bat.Vecs))
		f.ready = make([]bool, len(bat.Vecs))
	}
	if len(f.snapshots) != len(bat.Vecs) {
		f.snapshots = make([]*vector.DictionaryVectorSlot, len(bat.Vecs))
		for i := range f.snapshots {
			f.snapshots[i] = &vector.DictionaryVectorSlot{}
		}
		f.selectionSlot = &vector.DictionaryVectorSlot{}
	}
	f.allocation = allocation
	copy(f.inputs, bat.Vecs)
}

func (f *DictionaryInputFrame) End() { clear(f.inputs); clear(f.ready) }
func (f *DictionaryInputFrame) Free(mp *mpool.MPool) {
	for _, v := range f.flat {
		if v != nil {
			v.Free(mp)
		}
	}
	for _, v := range f.predicateHeaders {
		if v != nil {
			v.Free(mp)
		}
	}
	for _, slot := range f.snapshots {
		slot.Free(mp)
	}
	f.selectionSlot.Free(mp)
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

func (f *DictionaryInputFrame) recyclePredicate(input *batch.Batch, mp *mpool.MPool) {
	if len(f.predicateHeaders) != len(input.Vecs) {
		input.Clean(mp)
		return
	}
	for col, v := range input.Vecs {
		if v == nil {
			continue
		}
		if !v.IsDictionary() {
			v.Free(mp)
			input.Vecs[col] = nil
			continue
		}
		v.CleanOnlyData()
		if old := f.predicateHeaders[col]; old != nil {
			old.Free(mp)
		}
		f.predicateHeaders[col] = v
		input.Vecs[col] = nil
	}
	input.Clean(mp)
}
