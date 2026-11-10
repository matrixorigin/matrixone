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

package filter

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (filter *Filter) ActivatesAllocationAccountLifecycle() bool {
	return !filter.IsAssert && !filter.OwnsConstantCastWarnings && len(filter.FilterExprs)+len(filter.RuntimeFilterExprs) > 0
}
func (filter *Filter) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if filter.ctr.outputAllocation != nil {
		if filter.ctr.outputAllocation.UsesAccount(account) {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if !filter.ActivatesAllocationAccountLifecycle() {
		return nil
	}
	if filter.ctr.buf != nil || len(filter.ctr.executors)+len(filter.ctr.runtimeExecutors) > 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 5, 6, 7, 8)
	if err != nil {
		return err
	}
	filter.ctr.outputAllocation = selection
	return nil
}
func (filter *Filter) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if filter.ctr.outputAllocation == nil {
		return nil
	}
	if !filter.ctr.outputAllocation.UsesAccount(account) {
		return mpool.ErrAllocationAccountMismatch
	}
	if filter.ctr.buf != nil || filter.ctr.viewOutput != nil || len(filter.ctr.executors)+len(filter.ctr.runtimeExecutors) > 0 {
		return mpool.ErrAllocationAccountLive
	}
	filter.ctr.outputAllocation = nil
	return nil
}
func (filter *Filter) AcceptsDictionary(child int) bool {
	return child == 0 && filter.ctr.outputAllocation != nil && !filter.IsAssert && !filter.OwnsConstantCastWarnings &&
		colexec.ExpressionsAcceptDictionary(filter.ctr.allExecutors) && (len(filter.ctr.allExecutors) > 0 || filter.ctr.dictionaryBoundary.AllowsDictionaryOutput())
}
func (filter *Filter) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return !filter.IsEnd && filter.ctr.outputAllocation != nil && !filter.IsAssert && !filter.OwnsConstantCastWarnings &&
		colexec.ExpressionsAcceptDictionary(filter.ctr.allExecutors), filter.ctr.outputAllocation
}
func (filter *Filter) DictionaryOutputState() *vm.DictionaryOutputState {
	return &filter.ctr.dictionaryBoundary
}
func (filter *Filter) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return filter.ctr.dictionaryBoundary.Normalize(input, proc.Mp())
}

func (filter *Filter) selectedCall(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	first := !filter.ctr.subjectReuseAdmissionDone
	filter.ctr.subjectReuseAdmissionDone = true
	var before func(int, int)
	if first {
		before = func(i, rows int) {
			if i >= len(filter.ctr.runtimeExecutors) {
				index := i - len(filter.ctr.runtimeExecutors)
				executor := colexec.ReuseFilterExpressionExecutor(filter.FilterExprs[index], filter.ctr.executors[index], rows)
				filter.ctr.executors[index] = executor
				filter.ctr.allExecutors[i] = executor
			}
		}
	}
	rows, all, err := colexec.SelectPredicateRows(proc, input, nil, filter.ctr.allExecutors, filter.ctr.outputAllocation,
		&filter.ctr.selectedRows, &filter.ctr.bs, filter.OpAnalyzer, before)
	if err != nil {
		return nil, err
	}
	if all {
		return input, nil
	}
	if len(rows) == 0 {
		return batch.EmptyBatch, nil
	}
	output, retained, err := colexec.SelectBatchColumns(input, nil, rows, proc.Mp(), filter.ctr.outputAllocation, true)
	if err != nil {
		return nil, err
	}
	filter.ctr.viewOutput = output
	stats := filter.OpAnalyzer.GetOpStats()
	if retained {
		stats.AddExtraStat("DictionaryFilterSelectedRows", int64(output.RowCount()))
	} else {
		stats.AddExtraStat("DictionaryFilterFlatFallbackRows", int64(output.RowCount()))
	}
	return output, nil
}
