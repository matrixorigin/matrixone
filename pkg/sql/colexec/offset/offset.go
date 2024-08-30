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

package offset

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "offset"

func (offset *Offset) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(fmt.Sprintf("offset(%v)", offset.OffsetExpr))
}

func (offset *Offset) OpType() vm.OpType {
	return vm.Offset
}

func (offset *Offset) Prepare(proc *process.Process) error {
	var err error
	if offset.ctr.offsetExecutor == nil {
		offset.ctr.offsetExecutor, err = colexec.NewExpressionExecutor(proc, offset.OffsetExpr)
		if err != nil {
			return err
		}
	}
	vec, err := offset.ctr.offsetExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	offset.ctr.offset = uint64(vector.MustFixedCol[uint64](vec)[0])

	return nil
}

func (offset *Offset) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(offset.GetIdx(), offset.GetParallelIdx(), offset.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if offset.ctr.seen > offset.ctr.offset {
		return vm.CancelResult, nil
	}

	for {
		input, err := vm.ChildrenCall(offset.GetChildren(0), proc, anal)
		if err != nil {
			return vm.CancelResult, err
		}
		if input.Batch == nil || input.Batch.Last() {
			return input, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		if offset.ctr.seen > offset.ctr.offset {
			return input, nil
		}
		anal.Input(input.Batch, offset.GetIsFirst())
		length := input.Batch.RowCount()
		if offset.ctr.buf != nil {
			offset.ctr.buf.CleanOnlyData()
		}
		if offset.ctr.seen+uint64(length) > offset.ctr.offset {
			sels := newSels(int64(offset.ctr.offset-offset.ctr.seen), int64(length)-int64(offset.ctr.offset-offset.ctr.seen), proc)
			offset.ctr.buf, err = offset.ctr.buf.AppendWithCopy(proc.Ctx, proc.GetMPool(), input.Batch)
			if err != nil {
				return vm.CancelResult, err
			}
			offset.ctr.buf.Shrink(sels, false)
			proc.Mp().PutSels(sels)
			offset.ctr.seen += uint64(length)
			return vm.CallResult{Batch: offset.ctr.buf, Status: vm.ExecNext}, nil
		}
		offset.ctr.seen += uint64(length)
	}
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
