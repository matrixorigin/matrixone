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

const argName = "merge_offset"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(fmt.Sprintf("mergeOffset(%v)", arg.Offset))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, true)
	if arg.ctr.offsetExecutor == nil {
		arg.ctr.offsetExecutor, err = colexec.NewExpressionExecutor(proc, arg.Offset)
		if err != nil {
			return err
		}
	}
	vec, err := arg.ctr.offsetExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return err
	}
	arg.ctr.offset = uint64(vector.MustFixedCol[uint64](vec)[0])

	arg.ctr.seen = 0
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	var msg *process.RegisterMessage
	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	for {
		msg = arg.ctr.ReceiveFromAllRegs(anal)
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

		arg.buf = msg.Batch
		anal.Input(arg.buf, arg.GetIsFirst())
		if arg.ctr.seen > arg.ctr.offset {
			anal.Output(arg.buf, arg.GetIsLast())
			result.Batch = arg.buf
			return result, nil
		}
		length := arg.buf.RowCount()
		// bat = PartOne + PartTwo, and PartTwo is required.
		if arg.ctr.seen+uint64(length) > arg.ctr.offset {
			sels := newSels(int64(arg.ctr.offset-arg.ctr.seen), int64(length)-int64(arg.ctr.offset-arg.ctr.seen), proc)
			arg.ctr.seen += uint64(length)
			arg.buf.Shrink(sels, false)
			proc.Mp().PutSels(sels)
			anal.Output(arg.buf, arg.GetIsLast())
			result.Batch = arg.buf
			return result, nil
		}
		arg.ctr.seen += uint64(length)
		proc.PutBatch(arg.buf)
	}
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
