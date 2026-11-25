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

	if offset.OpAnalyzer == nil {
		offset.OpAnalyzer = process.NewAnalyzer(offset.GetIdx(), offset.IsFirst, offset.IsLast, "offset")
	} else {
		offset.OpAnalyzer.Reset()
	}

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
	offset.ctr.offset = uint64(vector.MustFixedColWithTypeCheck[uint64](vec)[0])

	return nil
}

func (offset *Offset) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := offset.OpAnalyzer

	for {
		input, err := vm.ChildrenCall(offset.GetChildren(0), proc, analyzer)
		if err != nil {
			return vm.CancelResult, err
		}
		if input.Batch == nil {
			if offset.calcFoundRows && proc.IsSqlCalcFoundRows() && !proc.FoundRowsRecorded() {
				proc.SetFoundRows(offset.ctr.inputRows)
			}
			return input, nil
		}
		if input.Batch.Last() {
			if offset.calcFoundRows && proc.IsSqlCalcFoundRows() && !proc.FoundRowsRecorded() {
				proc.SetFoundRows(offset.ctr.inputRows)
			}
			return input, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		length := input.Batch.RowCount()
		offset.ctr.inputRows += uint64(length)
		if offset.ctr.seen > offset.ctr.offset {
			return input, nil
		}

		if offset.ctr.buf != nil {
			offset.ctr.buf.CleanOnlyData()
		}
		if offset.ctr.seen+uint64(length) > offset.ctr.offset {
			// The rows past the offset are a contiguous tail: copy that window.
			if offset.ctr.buf == nil {
				offset.ctr.buf = batch.NewWithSize(len(input.Batch.Vecs))
				for i, vec := range input.Batch.Vecs {
					offset.ctr.buf.Vecs[i] = vector.NewVec(*vec.GetType(), proc.Mp())
				}
			}
			offset.ctr.buf.Attrs = append(offset.ctr.buf.Attrs[:0], input.Batch.Attrs...)
			start := int(offset.ctr.offset - offset.ctr.seen)
			if err = offset.ctr.buf.UnionWindow(input.Batch, start, length-start, proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
			offset.ctr.seen += uint64(length)

			return vm.CallResult{Batch: offset.ctr.buf, Status: vm.ExecNext}, nil
		}
		offset.ctr.seen += uint64(length)
	}
}
