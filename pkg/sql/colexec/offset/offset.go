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

const argName = "offset"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(fmt.Sprintf("offset(%v)", arg.OffsetExpr))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error
	if arg.offsetExecutor == nil {
		arg.offsetExecutor, err = colexec.NewExpressionExecutor(proc, arg.OffsetExpr)
		if err != nil {
			return err
		}
	}
	vec, err := arg.offsetExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return err
	}
	arg.offset = uint64(vector.MustFixedCol[uint64](vec)[0])

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, arg.GetIsFirst())

	if arg.Seen > arg.offset {
		return result, nil
	}
	length := bat.RowCount()
	if arg.Seen+uint64(length) > arg.offset {
		sels := newSels(int64(arg.offset-arg.Seen), int64(length)-int64(arg.offset-arg.Seen), proc)
		arg.Seen += uint64(length)
		bat.Shrink(sels, false)
		proc.Mp().PutSels(sels)
		result.Batch = bat
		return result, nil
	}
	arg.Seen += uint64(length)
	result.Batch = batch.EmptyBatch
	return result, nil
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
