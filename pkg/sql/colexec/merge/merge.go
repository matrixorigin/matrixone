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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		return nil
	}

	if merge.Partial {
		merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, proc.Reg.MergeReceivers[merge.StartIDX:merge.EndIDX])
	} else {
		merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, proc.Reg.MergeReceivers)
	}
	return nil
}

// ActivateReceiverRange 只切换已完全消费的局部输入；调用者随后才启动对应
// producer。未选分支没有 receiver，也不会被 Reset 当作活动输入等待。
func (merge *Merge) ActivateReceiverRange(proc *process.Process, start, end int32) error {
	if !merge.Partial || merge.MaterializedSource != nil || start < 0 || end <= start || int(end) > len(proc.Reg.MergeReceivers) {
		return moerr.NewInternalErrorNoCtx("invalid scalar vector receiver range")
	}
	if merge.ctr.receiver == nil || merge.ctr.receiver.State().Alive != 0 {
		return moerr.NewInternalErrorNoCtx("scalar vector receiver is still active")
	}
	merge.WithPartial(start, end)
	merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, proc.Reg.MergeReceivers[start:end])
	return nil
}

func (merge *Merge) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := merge.OpAnalyzer
	if merge.MaterializedSource != nil {
		bat, end, err := merge.MaterializedSource.Next(proc.Ctx, merge.MaterializedReaderID, merge.ctr.materializedPosition)
		if err != nil {
			return vm.CancelResult, err
		}
		result := vm.NewCallResult()
		if end {
			result.Status = vm.ExecStop
			return result, nil
		}
		if merge.MaterializedSingleBatch {
			if merge.ctr.materializedBatch != nil {
				bat.Clean(proc.Mp())
				return vm.CancelResult, moerr.NewInternalErrorNoCtx("scalar vector source exceeded one batch")
			}
			merge.ctr.materializedBatch = bat
		}
		merge.ctr.materializedPosition++
		result.Batch = bat
		return result, nil
	}

	var info error
	result := vm.NewCallResult()
	for {
		result.Batch, info = merge.ctr.receiver.GetNextBatch(analyzer)
		if info != nil {
			return vm.CancelResult, info
		}

		if result.Batch == nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if merge.SinkScan && result.Batch.Last() {
			continue
		}
		break
	}

	return result, nil
}
