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

package group

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type localReductionState struct {
	limit      int64
	inputRows  uint64
	bypassRows uint64
	// Only the header is owned here. Vectors belong to the child or prepared
	// expression executors, as in Projection; never call Clean on this batch.
	output batch.Batch
}

func (group *Group) prepareLocalReduction() error {
	if group.LocalReductionMem < 0 || group.LocalReductionMem > 0 &&
		(group.NeedEval || len(group.Aggs) != 0 || len(group.GroupBy) == 0 ||
			len(group.GroupingFlag) != 0 || group.DynamicGrouping || group.SortRollup ||
			len(group.GroupByHashKey) != 0) {
		return moerr.NewInternalErrorNoCtx("adaptive local reduction requires a plain partial key Group")
	}
	group.ctr.localReduction = localReductionState{limit: group.LocalReductionMem}
	return nil
}

// observeLocalReduction runs only after a whole input batch has committed. A
// non-reducing local hash pass adds work to the completing Group, so temporarily
// use projection when this epoch failed to remove at least half the rows.
func (group *Group) observeLocalReduction(rows int) bool {
	local := &group.ctr.localReduction
	if local.limit == 0 {
		return false
	}
	local.inputRows += min(uint64(rows), math.MaxUint64-local.inputRows)
	// Payload width does not determine the probe working set. A few wide
	// keys should keep reducing across batches; resource admission still
	// governs their retained payload and expression memory.
	if group.ctr.hr.Hash.Size() < local.limit {
		return false
	}
	if group.ctr.hr.Hash.GroupCount() > local.inputRows/2 {
		// Periodically sample again: later input may have a different reduction
		// ratio. Saturation also keeps a large batch from wrapping the interval.
		local.bypassRows = min(local.inputRows, math.MaxUint64/8) * 8
	}
	group.OpAnalyzer.GetOpStats().AddExtraStat("GroupLocalReductionFlushes", 1)
	return true
}

func (group *Group) projectLocalKeys(proc *process.Process) (vm.CallResult, error) {
	local := &group.ctr.localReduction
	local.output.Vecs = nil
	result, err := vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
	if err != nil {
		return result, err
	}
	if err, cancelled := vm.CancelCheck(proc); cancelled {
		return vm.CancelResult, err
	}
	if result.Status == vm.ExecYield {
		return result, nil
	}
	if result.Batch == nil {
		group.ctr.inputDone = true
		group.ctr.state = vm.End
		return vm.CancelResult, nil
	}
	// Group consumes input until nil Batch, even when a child marks a
	// non-empty result ExecStop. Preserve that contract while bypassing.
	result.Status = vm.ExecNext
	if result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	if err = group.evaluateGroupByAndAggArgs(proc, result.Batch); err != nil {
		return vm.CancelResult, err
	}
	if err, cancelled := vm.CancelCheck(proc); cancelled {
		return vm.CancelResult, err
	}
	rows := result.Batch.RowCount()
	local.bypassRows -= min(local.bypassRows, uint64(rows))
	group.ctr.inputRowCount += uint64(rows)
	group.OpAnalyzer.SetMemUsed(group.ctr.memUsed())
	group.OpAnalyzer.GetOpStats().AddExtraStat("GroupLocalBypassRows", int64(rows))
	local.output.Vecs = group.ctr.groupByEvaluate.Vec
	local.output.SetRowCount(rows)
	result.Batch = &local.output
	return result, nil
}
