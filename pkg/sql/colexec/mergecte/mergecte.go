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

package mergecte

import (
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_cte"

func (mergeCTE *MergeCTE) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge cte ")
}

func (mergeCTE *MergeCTE) OpType() vm.OpType {
	return vm.MergeCTE
}

func (mergeCTE *MergeCTE) Prepare(proc *process.Process) error {
	mergeCTE.ctr = new(container)

	mergeCTE.ctr.nodeCnt = int32(len(proc.Reg.MergeReceivers)) - 1
	mergeCTE.ctr.curNodeCnt = mergeCTE.ctr.nodeCnt
	mergeCTE.ctr.status = sendInitial
	return nil
}

func (mergeCTE *MergeCTE) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(mergeCTE.GetIdx(), mergeCTE.GetParallelIdx(), mergeCTE.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var err error
	if mergeCTE.ctr.buf != nil {
		proc.PutBatch(mergeCTE.ctr.buf)
		mergeCTE.ctr.buf = nil
	}
	switch mergeCTE.ctr.status {
	case sendInitial:
		result, err = mergeCTE.GetChildren(0).Call(proc)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}

		if result.Batch == nil {
			mergeCTE.ctr.status = sendLastTag
		}
		if result.Batch != nil {
			atomic.AddInt64(&result.Batch.Cnt, 1)
		}
		mergeCTE.ctr.bats = append(mergeCTE.ctr.bats, result.Batch)
		fallthrough
	case sendLastTag:
		if mergeCTE.ctr.status == sendLastTag {
			mergeCTE.ctr.status = sendRecursive
			mergeCTE.ctr.bats[0] = makeRecursiveBatch(proc)
		}
	case sendRecursive:
		for !mergeCTE.ctr.last {
			result, err = mergeCTE.GetChildren(1).Call(proc)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if result.Batch == nil {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
			atomic.AddInt64(&result.Batch.Cnt, 1)
			if result.Batch.Last() {
				mergeCTE.ctr.curNodeCnt--
				if mergeCTE.ctr.curNodeCnt == 0 {
					mergeCTE.ctr.last = true
					mergeCTE.ctr.curNodeCnt = mergeCTE.ctr.nodeCnt
					mergeCTE.ctr.bats = append(mergeCTE.ctr.bats, result.Batch)
					break
				}
			} else {
				mergeCTE.ctr.bats = append(mergeCTE.ctr.bats, result.Batch)
			}

		}
	}

	mergeCTE.ctr.buf = mergeCTE.ctr.bats[0]
	mergeCTE.ctr.bats = mergeCTE.ctr.bats[1:]
	if mergeCTE.ctr.buf.Last() {
		mergeCTE.ctr.last = false
	}

	anal.Input(mergeCTE.ctr.buf, mergeCTE.GetIsFirst())
	anal.Output(mergeCTE.ctr.buf, mergeCTE.GetIsLast())
	result.Batch = mergeCTE.ctr.buf
	result.Status = vm.ExecHasMore
	return result, nil
}

func makeRecursiveBatch(proc *process.Process) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, proc.GetVector(types.T_varchar.ToType()))
	vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	batch.SetLength(b, 1)
	b.SetLast()
	return b
}
