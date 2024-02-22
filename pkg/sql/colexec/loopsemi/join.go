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

package loopsemi

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "loop_semi"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": ⨝ ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error

	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)

	if arg.Cond != nil {
		arg.ctr.expr, err = colexec.NewExpressionExecutor(proc, arg.Cond)
	}
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := arg.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(arg, proc, anal); err != nil {
				return result, err
			}
			if ctr.bat == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if arg.bat == nil {
				bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
				if err != nil {
					return result, err
				}

				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.IsEmpty() {
					proc.PutBatch(bat)
					continue
				}
				if ctr.bat == nil || ctr.bat.RowCount() == 0 {
					proc.PutBatch(bat)
					continue
				}
				arg.bat = bat
				arg.lastrow = 0
			}

			err := ctr.probe(arg, proc, anal, arg.GetIsFirst(), arg.GetIsLast(), &result)
			if arg.lastrow == 0 {
				proc.PutBatch(arg.bat)
				arg.bat = nil
			}
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
		if err != nil {
			return err
		}
		if bat == nil {
			break
		}
		ctr.bat, _, err = proc.AppendBatchFromOffset(ctr.bat, bat, 0)
		if err != nil {
			return err
		}
		proc.PutBatch(bat)
	}
	return nil
}

func (ctr *container) probe(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(ap.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		ctr.rbat.Vecs[i] = proc.GetVector(*ap.bat.Vecs[pos].GetType())
	}
	count := ap.bat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ap.bat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ap.lastrow; i < count; i++ {
		if rowCountIncrease >= colexec.DefaultBatchSize {
			ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ap.lastrow = i
			return nil
		}
		if err := colexec.SetJoinBatchValues(ctr.joinBat, ap.bat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat})
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		for k := uint64(0); k < uint64(vec.Length()); k++ {
			b, null := rs.GetValue(k)
			if !null && b {
				for k, pos := range ap.Result {
					if err = ctr.rbat.Vecs[k].UnionOne(ap.bat.Vecs[pos], int64(i), proc.Mp()); err != nil {
						vec.Free(proc.Mp())
						return err
					}
				}
				rowCountIncrease++
				break
			}
		}
	}
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.lastrow = 0
	return nil
}
