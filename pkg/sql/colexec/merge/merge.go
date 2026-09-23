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

package merge

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge"

func (merge *Merge) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": union all ")
}

func (merge *Merge) OpType() vm.OpType {
	return vm.Merge
}

func (merge *Merge) Prepare(proc *process.Process) error {
	if merge.OpAnalyzer == nil {
		merge.OpAnalyzer = process.NewAnalyzer(merge.GetIdx(), merge.IsFirst, merge.IsLast, "merge")
	} else {
		merge.OpAnalyzer.Reset()
	}
	if merge.MaterializedSource != nil {
		merge.ctr.receiver = nil
		merge.ctr.materializedPosition = 0
		merge.ctr.materializedReleased = false
		merge.cleanMaterializedBatch(proc)
		return nil
	}

	if merge.Partial {
		merge.ctr.receiver = process.InitPipelineSignalReceiverFromProcess(proc, proc.Reg.MergeReceivers[merge.StartIDX:merge.EndIDX])
	} else {
		merge.ctr.receiver = process.InitPipelineSignalReceiverFromProcess(proc, proc.Reg.MergeReceivers)
	}
	return nil
}

func (merge *Merge) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := merge.OpAnalyzer
	if merge.MaterializedSource != nil {
		merge.cleanMaterializedBatch(proc)
	}

	var info error
	result := vm.NewCallResult()
	for {
		if merge.MaterializedSource != nil {
			bat, end, err, ready := merge.MaterializedSource.TryNext(
				merge.MaterializedReaderID, merge.ctr.materializedPosition)
			if !ready {
				result.Status = vm.ExecWaiting
				result.OnReady = func(callback func()) error {
					return merge.MaterializedSource.RegisterReady(
						merge.MaterializedReaderID,
						merge.ctr.materializedPosition,
						callback,
					)
				}
				return result, nil
			}
			if err != nil {
				return vm.CancelResult, err
			}
			if end {
				result.Status = vm.ExecStop
				return result, nil
			}
			merge.ctr.materializedPosition++
			merge.ctr.materializedBatch = bat
			if merge.SinkScan && (bat.Last() || bat.End()) {
				// Materialized recursive sinks retain generation markers so
				// MergeRecursive can delimit its input.  A normal outer SINK_SCAN
				// must consume, but never expose, those control batches.
				merge.cleanMaterializedBatch(proc)
				continue
			}
			result.Batch = bat
			return result, nil
		}

		var progressed bool
		result.Batch, info, progressed = merge.ctr.receiver.TryGetNextBatch(analyzer)
		if info != nil {
			return vm.CancelResult, info
		}
		if !progressed {
			result.Status = vm.ExecWaiting
			result.OnReady = merge.ctr.receiver.RegisterReady
			return result, nil
		}

		if result.Batch == nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if merge.SinkScan && (result.Batch.Last() || result.Batch.End()) {
			continue
		}
		break
	}

	return result, nil
}
