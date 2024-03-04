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

package loopleft

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "loop_left"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": loop left join ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error

	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)
	arg.ctr.bat = batch.NewWithSize(len(arg.Typs))
	for i, typ := range arg.Typs {
		arg.ctr.bat.Vecs[i] = proc.GetVector(typ)
	}

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
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = ctr.build(arg, proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat != nil {
				err = ctr.probe(arg, proc, anal, arg.GetIsLast(), &result)
				return result, err
			}
			ctr.inBat, _, err = ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if ctr.inBat == nil {
				ctr.state = End
				continue
			}
			if ctr.inBat.IsEmpty() {
				proc.PutBatch(ctr.inBat)
				ctr.inBat = nil
				continue
			}
			anal.Input(ctr.inBat, arg.GetIsFirst())
			if ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(arg, proc, anal, arg.GetIsLast(), &result)
			} else {
				err = ctr.probe(arg, proc, anal, arg.GetIsLast(), &result)
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

func (ctr *container) emptyProbe(ap *Argument, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			// rbat.Vecs[i] = bat.Vecs[rp.Pos]
			// bat.Vecs[rp.Pos] = nil
			typ := *ctr.inBat.Vecs[rp.Pos].GetType()
			ctr.rbat.Vecs[i] = proc.GetVector(typ)
			if err := vector.GetUnionAllFunction(typ, proc.Mp())(ctr.rbat.Vecs[i], ctr.inBat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			ctr.rbat.Vecs[i] = vector.NewConstNull(ap.Typs[rp.Pos], ctr.inBat.RowCount(), proc.Mp())
		}
	}
	ctr.probeIdx = 0
	ctr.rbat.AddRowCount(ctr.inBat.RowCount())
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}

func (ctr *container) probe(ap *Argument, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.inBat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.Typs[rp.Pos])
		}
	}

	count := ctr.inBat.RowCount()
	rowCountIncrease := 0
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ctr.inBat, proc.Mp())
	}
	for i := ctr.probeIdx; i < count; i++ {
		if ctr.expr == nil {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionMulti(ctr.inBat.Vecs[rp.Pos], int64(i), ctr.bat.RowCount(), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.rbat.Vecs[k].UnionBatch(ctr.bat.Vecs[rp.Pos], 0, ctr.bat.RowCount(), nil, proc.Mp()); err != nil {
						return err
					}
				}
			}
			rowCountIncrease += ctr.bat.RowCount()
		} else {
			matched := false
			if err := colexec.SetJoinBatchValues(ctr.joinBat, ctr.inBat, int64(i),
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
					matched = true
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[k].UnionMulti(ctr.inBat.Vecs[rp.Pos], int64(i), ctr.bat.RowCount(), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[k].UnionBatch(ctr.bat.Vecs[rp.Pos], 0, ctr.bat.RowCount(), nil, proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease += ctr.bat.RowCount()
				}
			} else {
				l := vec.Length()
				for j := uint64(0); j < uint64(l); j++ {
					b, null := rs.GetValue(j)
					if !null && b {
						matched = true
						for k, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
									return err
								}
							}
						}
						rowCountIncrease++
					}
				}
			}
			if !matched {
				for k, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[k].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
				rowCountIncrease++
			}
		}
		if rowCountIncrease >= colexec.DefaultBatchSize {
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(rowCountIncrease)
			ctr.probeIdx = i + 1
			return nil
		}
	}
	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}
