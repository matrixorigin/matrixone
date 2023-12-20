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

package loopjoin

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" loop join ")
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

	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
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
			err = ctr.probe(bat, arg, proc, anal, arg.info.IsFirst, arg.info.IsLast, &result)
			proc.PutBatch(bat)
			return result, err
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
	if err != nil {
		return err
	}

	if bat != nil {
		if ctr.bat != nil {
			proc.PutBatch(ctr.bat)
			ctr.bat = nil
		}
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*bat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}
	count := bat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(bat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := 0; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, bat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat})
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			b, null := rs.GetValue(0)
			if !null && b {
				for j := 0; j < ctr.bat.RowCount(); j++ {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		} else {
			l := uint64(ctr.bat.RowCount())
			for j := uint64(0); j < l; j++ {
				b, null := rs.GetValue(j)
				if !null && b {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		}
	}

	ctr.rbat.SetRowCount(rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	return nil
}
