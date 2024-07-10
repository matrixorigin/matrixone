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

package mergeoffset

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_offset"

func (mergeOffset *MergeOffset) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(fmt.Sprintf("mergeOffset(%v)", mergeOffset.Offset))
}

func (mergeOffset *MergeOffset) Prepare(proc *process.Process) error {
	var err error
	mergeOffset.ctr = new(container)
	mergeOffset.ctr.InitReceiver(proc, true)
	if mergeOffset.ctr.offsetExecutor == nil {
		mergeOffset.ctr.offsetExecutor, err = colexec.NewExpressionExecutor(proc, mergeOffset.Offset)
		if err != nil {
			return err
		}
	}
	vec, err := mergeOffset.ctr.offsetExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	mergeOffset.ctr.offset = uint64(vector.MustFixedCol[uint64](vec)[0])

	mergeOffset.ctr.seen = 0
	return nil
}

func (mergeOffset *MergeOffset) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(mergeOffset.GetIdx(), mergeOffset.GetParallelIdx(), mergeOffset.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var msg *process.RegisterMessage
	if mergeOffset.ctr.buf != nil {
		proc.PutBatch(mergeOffset.ctr.buf)
		mergeOffset.ctr.buf = nil
	}

	for {
		msg = mergeOffset.ctr.ReceiveFromAllRegs(anal)
		if msg.Err != nil {
			// WTF, nil?
			result.Status = vm.ExecStop
			return result, msg.Err
		}

		if msg.Batch == nil {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}

		mergeOffset.ctr.buf = msg.Batch
		anal.Input(mergeOffset.ctr.buf, mergeOffset.GetIsFirst())
		if mergeOffset.ctr.seen > mergeOffset.ctr.offset {
			anal.Output(mergeOffset.ctr.buf, mergeOffset.GetIsLast())
			result.Batch = mergeOffset.ctr.buf
			return result, nil
		}
		length := mergeOffset.ctr.buf.RowCount()
		// bat = PartOne + PartTwo, and PartTwo is required.
		if mergeOffset.ctr.seen+uint64(length) > mergeOffset.ctr.offset {
			sels := newSels(int64(mergeOffset.ctr.offset-mergeOffset.ctr.seen), int64(length)-int64(mergeOffset.ctr.offset-mergeOffset.ctr.seen), proc)
			mergeOffset.ctr.seen += uint64(length)
			mergeOffset.ctr.buf.Shrink(sels, false)
			proc.Mp().PutSels(sels)
			anal.Output(mergeOffset.ctr.buf, mergeOffset.GetIsLast())
			result.Batch = mergeOffset.ctr.buf
			return result, nil
		}
		mergeOffset.ctr.seen += uint64(length)
		proc.PutBatch(mergeOffset.ctr.buf)
	}
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
