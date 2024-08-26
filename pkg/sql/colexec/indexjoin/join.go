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

package indexjoin

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "index"

func (indexJoin *IndexJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": index join ")
}

func (indexJoin *IndexJoin) OpType() vm.OpType {
	return vm.IndexJoin
}

func (indexJoin *IndexJoin) Prepare(proc *process.Process) (err error) {
	indexJoin.OpAnalyzer = process.NewAnalyzer(indexJoin.GetIdx(), indexJoin.IsFirst, indexJoin.IsLast, "index join")
	if indexJoin.ProjectList != nil {
		err = indexJoin.PrepareProjection(proc)
	}
	if indexJoin.ctr.buf == nil {
		indexJoin.ctr.buf = batch.NewWithSize(len(indexJoin.Result))
	}
	return err
}

func (indexJoin *IndexJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(indexJoin.GetIdx(), indexJoin.GetParallelIdx(), indexJoin.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := indexJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ap := indexJoin
	ctr := &ap.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {

		case Probe:
			// TODO: index join 算子原本没有input统计，该处后期需要验证
			result, err = vm.ChildrenCallV1(indexJoin.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			bat := result.Batch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			indexJoin.ctr.buf.CleanOnlyData()
			for i, pos := range ap.Result {
				srcVec := bat.Vecs[pos]
				if ctr.buf.Vecs[i] == nil {
					ctr.buf.Vecs[i] = vector.NewVec(*srcVec.GetType())
				}
				if err = vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(ctr.buf.Vecs[i], srcVec); err != nil {
					return result, err
				}
			}
			indexJoin.ctr.buf.AddRowCount(bat.RowCount())
			result.Batch = indexJoin.ctr.buf
			if indexJoin.ProjectList != nil {
				var err error
				result.Batch, err = indexJoin.EvalProjection(result.Batch, proc)
				if err != nil {
					return result, err
				}
			}
			//anal.Output(result.Batch, indexJoin.GetIsLast())
			analyzer.Output(result.Batch)
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			analyzer.Output(result.Batch)
			return result, nil
		}
	}
}
